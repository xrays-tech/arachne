//! Network forwarding: let a non-leader node pass client commands on to the
//! leader over the wire.
//!
//! In a multi-process deployment a `Handle` attached to a follower has no
//! in-process peer for the leader, so the in-process client-side redirect
//! ([`Transport`]-agnostic) cannot reach it. The leader of such a node is a
//! *separate process*. The only way the follower's client commands can reach it
//! is over the transport.
//!
//! These traits split that capability into two halves:
//!
//! * **[`CommandSink`]** — what the *leader* offers: a sink it hands the
//!   transport so that a forwarded command is run against its own runtime
//!   (`Command`) and the outcome (`value` / stable error code) is sent back.
//!   `ArachneError` is mapped to a stable code string *here*, at the core, so
//!   the transport (which only knows the seam vocabulary) can carry the result
//!   on the wire.
//! * **[`RemoteForwarder`]** — what the *follower* (client side) uses: a handle
//!   to send a [`ForwardCommand`] to a specific peer node, identified by
//!   `(NodeId, SocketAddr)`, and await the leader's outcome.
//!
//! The follower's transport exposes a forwarder via
//! [`ForwardTransport::forwarder`]; the default `None` means the transport does
//! not support remote forwarding (in-process / peerless), and the client falls
//! back to the in-process redirect / quorum-unavailable path.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use crate::seam::transport::Transport;
use crate::seam::ApplyOutcome;
use crate::types::NodeId;

/// A client command that a non-leader node forwards to the leader.
///
/// Mirrors the subset of [`crate::types`]-level client commands that a
/// non-leader cannot serve itself: proposals (writes) and reads. `GetStale`
/// is a local read the follower *can* serve on its own, but is included for
/// uniformity so a single redirect path covers all client operations.
#[derive(Clone, Debug)]
pub enum ForwardCommand {
    /// Propose a command (write). The encoded state-machine command plus the
    /// session envelope (`client_id` / `seq_no`).
    Propose {
        /// The encoded state-machine command.
        cmd: Vec<u8>,
        /// The session client id (propsol §2.3).
        client_id: u64,
        /// The session sequence number.
        seq_no: u64,
    },
    /// Linearizable read (ReadIndex on the leader).
    Read {
        /// The key to read.
        key: Vec<u8>,
    },
    /// Stale read (direct local state machine read).
    GetStale {
        /// The key to read.
        key: Vec<u8>,
    },
}

/// The outcome of a forwarded command.
///
/// `value` is the read's answer (`None` for a successful write or a missing
/// key). When the *forwarded target* is itself not the leader (stale hint, or
/// the client was sent to the wrong node), `leader_hint` is set to that node's
/// current hint and `value` is `None`; the caller re-feeds the hint into its
/// redirect loop. Errors that carry no hint (quorum lost, timeout, busy, …)
/// surface as the `Err(String)` half of the sink's result rather than as a hint
/// here, so "failure without a new leader" and "failure that yields a new
/// leader" are distinguishable.
///
/// `result` is the per-command apply outcome of a forwarded `Propose` (write)
/// that committed and applied on the target: `Value(v)` when the write produced
/// a value, `None` for a `Delete`/no-op. It is present **only** on a successful
/// `Propose`; on a read/stale-read the answer rides in `value`, and on any
/// failure `result` is absent. This is what lets a future compare-and-swap
/// surface its outcome to the client over the same path (M3). It is *separate*
/// from `value` (the read's answer) so the two cannot be confused.
///
/// # Rolling compatibility
///
/// A node predating the outcome field sends a `ForwardReply` whose
/// `result` is absent (the proto default). The decode side therefore maps it
/// to `result: None`, indistinguishable from a write whose outcome was
/// genuinely `None`. This is a deliberate, documented collapse: M1 writes
/// (`Put`/`Delete`) have outcomes the public API discards anyway, so nothing
/// observable changes; the field only starts to *matter* when M3 adds an
/// outcome a caller must distinguish from `None` (and only a new-node pair can
/// exchange it, since an old node can neither encode nor decode it).
#[derive(Clone, Debug)]
pub struct ForwardOutcome {
    /// The read's value, or `None` for a successful write / missing key.
    pub value: Option<Vec<u8>>,
    /// Present only on a `not_leader` outcome: the leader hint to re-redirect
    /// to.
    pub leader_hint: Option<(NodeId, SocketAddr)>,
    /// Present only on a successful `Propose`: the apply outcome the target
    /// computed. See the doc on the struct for the rolling-compatibility note.
    pub result: Option<ApplyOutcome>,
}

/// Sink the *leader* offers to the transport: it accepts forwarded commands
/// and runs them against its own runtime, returning an outcome or a stable
/// error code string.
///
/// The core (`Runtime`) implements this around its command channel; the
/// transport stores it (set per node) and calls it on every inbound `Forward`
/// RPC.
pub trait CommandSink: Send + Sync + 'static {
    /// Accept one forwarded command and run it, returning the outcome or a
    /// stable error code string.
    fn sink(
        &self,
        c: ForwardCommand,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardOutcome, String>> + Send + '_>>;
}

/// Forwarder the *follower* uses to reach a specific remote node (the leader).
///
/// `to` names the target node (used for the handshake / sender tag), `addr`
/// its socket address, and `c` the command. Returns the leader's outcome, or
/// an error mapped to a stable code string.
pub trait RemoteForwarder: Send + Sync + 'static {
    /// Forward `c` to the node at `addr` and await the leader's outcome.
    fn forward(
        &self,
        to: NodeId,
        addr: SocketAddr,
        c: ForwardCommand,
    ) -> Pin<Box<dyn Future<Output = Result<ForwardOutcome, String>> + Send + '_>>;
}

/// A transport that can additionally forward commands to a remote node.
///
/// Supertrait of [`Transport`]. The default `forwarder` returns `None`, so
/// transports that do not support remote forwarding (in-memory, peerless,
/// non-leader-capable test doubles) implement nothing and leave the client's
/// in-process redirect / quorum-unavailable path unchanged.
pub trait ForwardTransport: Transport {
    /// The forwarder this node's transports use for remote forwarding, if any.
    fn forwarder(&self) -> Option<Arc<dyn RemoteForwarder>> {
        None
    }
}
