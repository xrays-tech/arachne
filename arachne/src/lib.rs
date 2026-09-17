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
//! [`arachne_seam`]: https://docs.rs/arachne-seam

pub mod storage;

pub use arachne_seam::seam;
pub use arachne_seam::types;
pub use arachne_seam::{
    ApplyOutcome, Clock, ConfState, EntryType, HardState, LogIndex, LogEntry, NodeId,
    NodeIdError, RaftId, RaftState, Rng, Snapshot, SnapshotMeta, StateMachine, Storage,
    StorageError, Term, Timestamp, Transport, TransportFactory, TransportMessage, TransportRx,
};

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
