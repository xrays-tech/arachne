//! A no-peer placeholder transport for M0.
//!
//! The real transport (tonic + mTLS) lands at M1. M0 runs a single node with
//! no peers, so every outbound raft message is undeliverable by construction;
//! the node's `step()` treats send failures as non-fatal and raft retransmits.
//! The inbound half never yields.
//!
//! This intentionally lives in `arachne-node` (NOT in `arachne-testsupport`)
//! so that production code never depends on the test-only support crate.

use std::future::Future;

use arachne::{NodeId, Transport, TransportMessage, TransportRx};

/// Undeliverable message: an M0 single-node cluster has no peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoPeerError {
    /// The intended recipient.
    pub to: NodeId,
}

impl std::fmt::Display for NoPeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no peers configured (M0 single-node); cannot send to {}",
            self.to
        )
    }
}

impl std::error::Error for NoPeerError {}

/// Outbound half: every send fails (there are no peers).
#[derive(Debug, Default)]
pub struct PlaceholderTx;

impl Transport for PlaceholderTx {
    type Error = NoPeerError;

    fn send(
        &self,
        to: NodeId,
        _msg: TransportMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Err(NoPeerError { to }))
    }
}

/// Inbound half: never yields a message (there are no peers).
#[derive(Debug, Default)]
pub struct PlaceholderRx;

impl TransportRx for PlaceholderRx {
    fn recv(&mut self) -> impl Future<Output = Option<(NodeId, TransportMessage)>> + Send {
        std::future::pending()
    }
}
