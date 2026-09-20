//! The network-transport seam.
//!
//! The core never touches sockets, tonic, or any concrete networking stack.
//! It sends and receives opaque, already-framed payloads through this trait.
//! *How* a byte string reaches a peer (gRPC, TCP, in-memory) is entirely the
//! transport's concern.
//!
//! # Design notes
//!
//! * The traits return `impl Future` in return position (RPITIT, stable). The
//!   node runtime is *generic* over these traits — it is **not** required to be
//!   object-safe / `dyn`-compatible.
//! * A node is built from a single [`TransportFactory`] instance, which mints
//!   the sending half ([`Transport`]) and the receiving half ([`TransportRx`])
//!   for that node. This keeps the node decoupled from any concrete transport
//!   type.
//! * [`TransportMessage`] is deliberately minimal. More variants (e.g.
//!   membership/cluster control traffic) land in later phases; the enum is the
//!   stable extension point.

use core::future::Future;

use crate::types::NodeId;

/// A message in transit between two nodes.
///
/// # Extensibility
///
/// Only the `Raft` variant exists today. This enum is the designated extension
/// point: as the protocol grows (membership changes, snapshot transfers,
/// cluster control), new variants are added here. Because the payload is an
/// opaque, already-framed byte string, *encoding and versioning are the
/// transport's concern*, not the core's. `#[non_exhaustive]` guarantees
/// downstream crates can never pattern-match this enum exhaustively, so new
/// variants are a non-breaking change.
///
/// **NOTE:** a core-owned transport error taxonomy is needed before M1
/// handshake work; until then `Transport::Error` is transport-specific.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportMessage {
    /// An opaque, already-framed raft wire payload.
    Raft(Vec<u8>),
}

/// The outbound half of a node's transport.
///
/// The core calls [`send`](Transport::send) to deliver a message to a peer.
/// The returned future resolves to `Ok(())` once the transport has accepted
/// the message for delivery, or to a transport-specific error.
///
/// This trait is `Send + Sync` so a single sender can be shared across tasks.
pub trait Transport: Send + Sync + 'static {
    /// The error type reported by this transport.
    type Error: core::error::Error + Send + Sync + 'static;

    /// Deliver `msg` to the node `to`.
    ///
    /// The returned future is `Send` so it can be moved across tasks. Whether
    /// delivery is synchronous, buffered, or performed over the network is up
    /// to the implementation.
    fn send(
        &self,
        to: NodeId,
        msg: TransportMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// The inbound half of a node's transport.
///
/// A core task repeatedly calls [`recv`](TransportRx::recv) to pull delivered
/// messages off the wire. Each future resolves to the next `(sender, message)`
/// pair, or `None` once the transport is permanently closed and no further
/// messages are possible.
///
/// This trait is `Send` (it is polled from within a task). It is deliberately
/// *not* `Sync` — there is a single consumer per receiving half.
pub trait TransportRx: Send + 'static {
    /// Receive the next inbound message, if any.
    ///
    /// Resolves to `Some((sender, message))` for each delivered message, or
    /// `None` when the transport is closed and drained.
    fn recv(&mut self) -> impl Future<Output = Option<(NodeId, TransportMessage)>> + Send;

    /// Receive the next **already-queued** message without suspending.
    ///
    /// `None` means "nothing is queued right now" (or the transport is closed
    /// and drained); it never blocks. The runtime uses this to fold everything
    /// that has already arrived into one durability cycle, because each cycle
    /// costs a real `fsync` and a burst of writes is therefore bounded by the
    /// *number of cycles* rather than by the number of messages (propsol
    /// v0.2.12 O).
    ///
    /// The default reports "nothing queued", which is always safe: a message it
    /// declines to hand over is still delivered by [`recv`](TransportRx::recv).
    fn try_recv(&mut self) -> Option<(NodeId, TransportMessage)> {
        None
    }
}

/// Mints the transport halves for a single node.
///
/// A node is constructed by calling [`create`](TransportFactory::create) once
/// with its own `NodeId`, obtaining the `(Tx, Rx)` pair it will use for its
/// lifetime. This factory is the single injection point that lets the node be
/// built against *any* concrete transport implementation.
pub trait TransportFactory: Send + Sync + 'static {
    /// The sending half this factory produces.
    type Tx: Transport;
    /// The receiving half this factory produces.
    type Rx: TransportRx;

    /// Create the transport halves for the node `me`.
    fn create(&self, me: NodeId) -> (Self::Tx, Self::Rx);
}
