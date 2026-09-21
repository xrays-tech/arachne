//! The client API layer (propsol §3): the error model and the [`Handle`].
//!
//! * [`ArachneError`] — the full propsol §3.2 error enumeration. Every client
//!   operation (`put`/`delete`/`get`/`get_stale`) resolves to a value or one of
//!   these. The variants are present in full for completeness; the subset that
//!   M1 can actually produce is documented per-variant.
//! * [`Handle`] — the cheap-`Clone` client handle. It validates arguments
//!   (fail-stop discipline, propsol §3.2), proposes through the node runtime
//!   actor, and performs the client-side redirect/retry policy (propsol §3.3).
//!
//! # Consistency semantics (propsol §2)
//!
//! | Operation | Semantics | Implementation |
//! |---|---|---|
//! | `put` / `delete` | linearizable write | Raft log; committed after quorum persistence, replied after apply |
//! | `get` | linearizable read (default) | **ReadIndex** (propsol §5.4): a quorum-confirmed read on the leader, served after `applied ≥ read_index`; a non-leader returns `NotLeader` and is redirected |
//! | `get_stale` | arbitrary stale read allowed | direct local state-machine read; **not monotone** (propsol N1) |
//! | loss of quorum | writes/linearizable reads → `QuorumUnavailable`; `get_stale` still works | CheckQuorum + leader step-down |
//!
//! # Redirect policy (propsol §3.3)
//!
//! When an operation returns [`ArachneError::NotLeader`] with a hint, the
//! [`Handle`] retries against the hinted peer (if it is a known in-process peer)
//! or, failing that, the next known peer in a deterministic order. Up to
//! [`Handle::MAX_REDIRECTS`] (3) redirects are followed; a total deadline of one
//! election timeout bounds the whole attempt, after which the call returns
//! [`ArachneError::Timeout`]. No leader known / quorum lost →
//! [`ArachneError::QuorumUnavailable`].
//!
//! The embedded/L1/L2 model assumes client and node share a process, so
//! redirects are client-side (one hop, node stays stateless) rather than a
//! server-side proxy.
//!
//! # Follower-served reads (M1 boundary)
//!
//! propsol §5.4 says a follower must "forward" a linearizable read. The raft
//! layer supports exactly that round-trip — an L2 scenario
//! (`follower_read_index_round_trip_completes`) drives a follower's ReadIndex
//! request through forward → quorum → response → local serve — but the M1
//! **runtime** deliberately does not expose it to clients: a read on a follower
//! is rejected with [`ArachneError::NotLeader`] and the **client** redirects to
//! the leader (a client-side redirect, propsol §3.3 — the node stays
//! stateless). So the client-visible contract is leader-served reads, and a
//! server-side read proxy is **not** implemented at M1. Enabling follower-served
//! reads later is a runtime-policy change, not a protocol change.

mod handle;

pub use handle::Handle;

use std::net::SocketAddr;

use crate::NodeId;

/// The full error model for Arachne client operations (propsol §3.2).
///
/// `Send + Sync + 'static` so it can cross a `oneshot` boundary back to the
/// client. `Clone` + `PartialEq` for test assertions.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArachneError {
    /// This node is not the leader. `leader_hint` (if known) names the current
    /// leader and its address; the client should redirect (propsol §3.3). The
    /// hint may be stale.
    #[error("not leader (hint: {leader_hint:?})")]
    NotLeader { leader_hint: Option<(NodeId, SocketAddr)> },
    /// The quorum is unavailable: the leader has stepped down, no leader is
    /// known, or a ReadIndex quorum round failed.
    #[error("quorum unavailable")]
    QuorumUnavailable,
    /// A bounded wait timed out. The result is **unknown** (propsol §2.4 N3):
    /// the write may or may not have landed; retry with the same
    /// `(client_id, seq_no)`.
    #[error("operation timed out (result unknown)")]
    Timeout,
    /// Back-pressure: a queue is full — the proposal queue, or the ReadIndex
    /// read-wait queue (propsol §3.2/§4.1). Not fatal — retry later.
    #[error("busy (proposal or read queue full)")]
    Busy,
    /// An argument violated a constraint (key/value size). Rejected **before**
    /// propose, so it never enters the log (fail-stop discipline, propsol §3.2).
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// The session expired or its cached result was evicted. Result **unknown**
    /// (propsol N2); the client must accept possible duplicate execution.
    /// (Session dedup is M3; M1 does not yet surface this from the happy path.)
    #[error("session expired (result unknown)")]
    SessionExpired,
    /// The session table is full; new sessions are rejected (existing sessions
    /// are unaffected). (M3.)
    #[error("session table full")]
    SessionTableFull,
    /// A configuration change is already pending (single-step ConfChange
    /// discipline, propsol §5.3). (M3.)
    #[error("configuration change pending")]
    ConfChangePending,
    /// Removing the current leader requires an automatic `transfer_leader`,
    /// which was not authorized. (M3.)
    #[error("leader removal requires transfer")]
    LeaderRemovalRequiresTransfer,
    /// A learner cannot be promoted yet: it is behind by more entries than
    /// `promote_lag_entries` allows, or it has never answered the leader
    /// (propsol §5.3 hard constraint 2, rev S). Promoting it anyway would put a
    /// voter with an incomplete log into the quorum. (M3.)
    #[error("learner not caught up (behind {behind} entries, threshold {threshold})")]
    LearnerNotCaughtUp {
        /// How many entries behind the leader the learner is.
        behind: u64,
        /// The configured `promote_lag_entries` bound.
        threshold: u64,
    },
    /// A peer reported a `cluster_id` mismatch during the transport handshake.
    #[error("cluster id mismatch")]
    ClusterIdMismatch,
    /// The node is shutting down.
    #[error("node is shutting down")]
    ShuttingDown,
    /// The data directory is locked by another process.
    #[error("data directory is locked")]
    DataDirLocked,
    /// An unrecoverable condition (storage corruption, state-machine invariant
    /// violation, …). The process should fail-stop; all subsequent calls return
    /// this.
    #[error("unrecoverable: {0}")]
    Unrecoverable(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant must be a `core::error::Error` and render a non-empty,
    /// descriptive `Display` string (the fail-loud discipline).
    #[test]
    fn every_variant_is_a_std_error_with_display() {
        fn check(e: &ArachneError) {
            let _ = (|| -> Box<dyn core::error::Error + Send + Sync> { Box::new(e.clone()) })();
            assert!(!e.to_string().is_empty(), "{e:?} must display");
        }
        let leader = (NodeId::from("n2"), "127.0.0.1:7002".parse().expect("addr"));
        for e in [
            ArachneError::NotLeader {
                leader_hint: Some(leader),
            },
            ArachneError::NotLeader { leader_hint: None },
            ArachneError::QuorumUnavailable,
            ArachneError::Timeout,
            ArachneError::Busy,
            ArachneError::InvalidArgument("too big".into()),
            ArachneError::SessionExpired,
            ArachneError::SessionTableFull,
            ArachneError::ConfChangePending,
            ArachneError::LeaderRemovalRequiresTransfer,
            ArachneError::ClusterIdMismatch,
            ArachneError::ShuttingDown,
            ArachneError::DataDirLocked,
            ArachneError::Unrecoverable("corrupt".into()),
        ] {
            check(&e);
        }
    }

    #[test]
    fn variant_names_are_distinct() {
        // The full §3.2 variant list is present (13 variants).
        let variants = vec![
            ArachneError::NotLeader { leader_hint: None },
            ArachneError::QuorumUnavailable,
            ArachneError::Timeout,
            ArachneError::Busy,
            ArachneError::InvalidArgument(String::new()),
            ArachneError::SessionExpired,
            ArachneError::SessionTableFull,
            ArachneError::ConfChangePending,
            ArachneError::LeaderRemovalRequiresTransfer,
            ArachneError::ClusterIdMismatch,
            ArachneError::ShuttingDown,
            ArachneError::DataDirLocked,
            ArachneError::Unrecoverable(String::new()),
        ];
        let mut seen = std::collections::BTreeSet::new();
        for v in &variants {
            assert!(seen.insert(format!("{v:?}")), "duplicate variant name");
        }
        assert_eq!(seen.len(), 13, "the full §3.2 variant list must be present");
    }
}
