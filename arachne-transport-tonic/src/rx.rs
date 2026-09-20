//! The inbound half of the tonic transport: [`TonicRx`].
//!
//! The gRPC server pushes each accepted payload into a **bounded** channel;
//! [`TonicRx`] is the reading end. `recv` yields the next `(sender, message)`
//! pair, tagged with the *sender's* `NodeId` (the authoritative identity from
//! its handshake), or `None` once the channel is closed and drained (i.e. after
//! `shutdown`, which awaits the server teardown so every sender is dropped).
//!
//! The channel is bounded (see [`crate::factory::INBOUND_QUEUE_CAPACITY`]) so a
//! fast peer cannot push unbounded memory into a slow node: when the queue is
//! full the server answers `resource_exhausted` and raft retransmits.

use arachne_seam::seam::{TransportMessage, TransportRx};
use arachne_seam::types::NodeId;
use tokio::sync::mpsc::Receiver;

/// The inbound half for one node.
pub struct TonicRx {
    receiver: Receiver<(NodeId, TransportMessage)>,
}

impl TonicRx {
    /// Wrap a channel's receiving end.
    pub(crate) fn new(receiver: Receiver<(NodeId, TransportMessage)>) -> Self {
        Self { receiver }
    }

    /// Non-blocking drain of the next queued message, if one is available.
    ///
    /// Returns `None` when the queue is empty *right now* (poll again later) or
    /// when it has been closed and drained. This is what a deterministic pump
    /// loop uses to pull out every message that has landed since the last round.
    pub fn try_recv(&mut self) -> Option<(NodeId, TransportMessage)> {
        self.receiver.try_recv().ok()
    }
}

impl TransportRx for TonicRx {
    async fn recv(&mut self) -> Option<(NodeId, TransportMessage)> {
        self.receiver.recv().await
    }

    fn try_recv(&mut self) -> Option<(NodeId, TransportMessage)> {
        self.receiver.try_recv().ok()
    }
}
