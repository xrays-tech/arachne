//! Arachne core: the transport-agnostic, deterministic product core.
//!
//! This crate is intentionally transport-agnostic. The core logic never
//! references concrete transport types directly; transport is provided
//! *optionally* through the `transport-tonic` feature, which enables
//! `arachne-transport-tonic` — the only crate allowed to touch tonic/rustls.
//!
//! Building with `--no-default-features` yields a "lean" core with no transport
//! dependency at all (see `scripts/check-deps.sh` Gate B).
//!
//! The core is built around a set of **seam traits** (see [`seam`]) that
//! isolate all environment interaction — time, randomness, the network, and the
//! data store — behind injectable interfaces. The core itself contains no
//! concrete clock, RNG, transport, or storage: those are supplied by the caller
//! (production or test) and are fully swappable.
//!
//! The seam traits and shared value types now live in the leaf crate
//! [`arachne_seam`]; this crate **re-exports** them at the same public paths
//! (`arachne::seam::*`, `arachne::types::*`, and the root re-exports) so the
//! public API is unchanged.
//!
//! # Consistency semantics (propsol §2)
//!
//! Arachne is a **CP** (consistency/availability → consistency) embedded KV
//! store: at most one leader at any time, commits require a quorum, and when
//! the quorum is lost **every linearizable operation fails** — the system
//! never degrades to a read-mostly mode or splits the brain.
//!
//! | Operation | Semantics | Implementation |
//! |---|---|---|
//! | `put` / `delete` | linearizable write | Raft log; committed after quorum persistence, replied after apply |
//! | `get` | linearizable read (default) | ReadIndex (propsol §5.4); a non-leader node redirects / returns `NotLeader` |
//! | `get_stale` | arbitrary stale read allowed | direct local state-machine read; **not guaranteed monotone** across calls (declared prominently, propsol §2.1/N1) |
//! | loss of quorum | writes and linearizable reads return `QuorumUnavailable`; `get_stale` still works | CheckQuorum + automatic leader step-down |
//!
//! The **error matrix** (propsol §3.3) — per operation × condition:
//!
//! | Condition | `put` / `delete` | `get` (ReadIndex) | `get_stale` |
//! |---|---|---|---|
//! | invalid argument | `InvalidArgument` (rejected before propose) | `InvalidArgument` | `InvalidArgument` |
//! | this node is not the leader | `NotLeader{hint}` | `NotLeader{hint}` | **succeeds** (local read) |
//! | quorum lost | `QuorumUnavailable` | `QuorumUnavailable` (quorum heartbeat round fails) | succeeds (may be stale) |
//! | proposal / read queue full | `Busy` | `Busy` | `Busy` (read queue only) |
//! | bounded wait timeout | `Timeout` (result unknown) | `Timeout` | succeeds |
//! | session table full / expired | `SessionTableFull` / `SessionExpired` | n/a (reads use no session) | n/a |
//! | shutting down | `ShuttingDown` | `ShuttingDown` | `ShuttingDown` |
//! | fatal storage error | `Unrecoverable` | `Unrecoverable` | `Unrecoverable` |
//!
//! # Configuration presets (propsol §7)
//!
//! [`Profile::Lan`] and [`Profile::Wan`] map to a concrete [`ProfileConfig`]
//! holding the full §7 field table; individual fields are overridable and the
//! result is validated with [`ProfileConfig::validate`] (see `profile`).

pub mod client;
pub mod consensus;
/// Test-only crash injection (propsol v0.2.9 L). Compiled only with the
/// `fault-injection` feature; see the module docs.
#[cfg(feature = "fault-injection")]
pub mod fault_injection;
pub mod metrics;
pub mod profile;
pub mod runtime;
pub mod state_machine;
pub mod storage;

pub use client::ArachneError;
pub use metrics::Metrics;
pub use runtime::{Command, Runtime, RuntimeConfig, RuntimeThread};

pub use arachne_seam::seam;
pub use arachne_seam::types;
pub use arachne_seam::{
    ApplyOutcome, Clock, ConfState, EntryType, FsyncObserver, HardState, LogIndex, LogEntry,
    NodeId, NodeIdError, RaftId, RaftState, Rng, Snapshot, SnapshotMeta, StateMachine, Storage,
    StorageError, Term, Timestamp, Transport, TransportFactory, TransportMessage, TransportRx,
};
pub use profile::{Profile, ProfileConfig, ProfileError};

/// Return the current crate version (from `CARGO_PKG_VERSION`).
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Re-export the active transport's name, available only when the
/// `transport-tonic` feature is enabled (the default).
///
/// This is the minimal glue that lets a downstream binary (e.g. `arachne-node`)
/// pull the transport "via arachne's default feature" and use it without
/// naming `arachne-transport-tonic` directly.
#[cfg(feature = "transport-tonic")]
pub use arachne_transport_tonic::transport_name;

#[cfg(test)]
mod tests {
    use super::version;

    #[test]
    fn version_matches_pkg_version() {
        // Tests may use free-form assertions (project convention).
        assert_eq!(version(), env!("CARGO_PKG_VERSION"));
        assert!(!version().is_empty());
    }
}
