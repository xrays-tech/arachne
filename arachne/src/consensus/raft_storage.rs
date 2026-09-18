//! Adapter: implements `raft::storage::Storage` over `arachne_seam::Storage`.
//!
//! The adapter is a thin, single-instance wrapper around the caller's durable
//! [`SeamStorage`]. There is no second cache: the `RawNode` reads through this
//! adapter and the `RaftNode` persists through it (via `RawNode::mut_store`),
//! so the durable state has exactly one home and can never diverge.
//!
//! # Error mapping
//!
//! Our seam's error taxonomy is mapped to raft 0.7's actual taxonomy:
//!
//! | seam                          | raft 0.7                                  |
//! |-------------------------------|-------------------------------------------|
//! | `StorageError::Compacted`     | `Store(StorageError::Compacted)`          |
//! | `StorageError::Corruption`    | `Store(StorageError::Unavailable)`        |
//! | `StorageError::Unrecoverable` | `Store(StorageError::Unavailable)`        |
//! | `StorageError::Io(e)`         | `Io(e)`                                   |
//!
//! `Compacted` is the *normal* signal that a log range is gone (raft switches
//! a lagging follower to snapshot transfer). `Corruption` and `Unrecoverable`
//! are **fail-start** conditions: raft's `Unavailable` is exactly "the storage
//! is broken, the node must stop participating", so that is where they map.

use raft::eraftpb::{
    ConfState, Entry, EntryType as RaftEntryType, HardState, Snapshot,
};
use raft::storage::{GetEntriesContext, RaftState, Storage as RaftStorageTrait};
use raft::{Error as RaftError, Result as RaftResult, StorageError as RaftStorageError};

use arachne_seam::storage::{
    ConfState as SeamConfState, EntryType as SeamEntryType, HardState as SeamHardState,
    LogEntry, Snapshot as SeamSnapshot, Storage as SeamStorage,
    StorageError as SeamStorageError,
};

/// A `raft::storage::Storage` adapter over an `arachne_seam::Storage`.
pub struct RaftStorage<S: SeamStorage> {
    inner: S,
    /// Static initial cluster configuration used when the durable store has no
    /// persisted `ConfState` (M0: membership is static; ConfChange lands M3).
    /// Without a voter set containing this node, raft has no quorum and can
    /// never elect a leader.
    bootstrap_conf_state: SeamConfState,
}

impl<S: SeamStorage> RaftStorage<S> {
    /// Wrap a seam storage in a raft-compatible adapter with an **empty**
    /// bootstrap voter set.
    ///
    /// A store built this way reports no voters until the durable `ConfState`
    /// carries one, so a node with no persisted membership can never elect
    /// itself. Use [`RaftStorage::with_conf_state`] to bootstrap a cluster;
    /// this constructor is a convenience for unit tests of the adapter.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            bootstrap_conf_state: SeamConfState::default(),
        }
    }

    /// Wrap a seam storage and declare the initial voter set (`initial_cluster`
    /// bootstrap, propsol §5.7). Used when the store has no persisted
    /// `ConfState` yet.
    pub fn with_conf_state(inner: S, bootstrap_conf_state: SeamConfState) -> Self {
        Self {
            inner,
            bootstrap_conf_state,
        }
    }

    // ---- write side (drives persistence from the `RaftNode`) ----

    /// Append entries to the durable log (buffered; not yet fsynced).
    pub fn append(&mut self, entries: &[LogEntry]) -> RaftResult<()> {
        self.inner.append(entries).map_err(map_error)
    }

    /// Durability barrier: after this returns, all previously appended entries
    /// are durable (invariant I2/I4).
    pub fn sync_entries(&mut self) -> RaftResult<()> {
        self.inner.sync_entries().map_err(map_error)
    }

    /// Persist the hard state. The seam guarantees this fsyncs on every call
    /// (invariant I1).
    pub fn set_hard_state(&mut self, hs: &SeamHardState) -> RaftResult<()> {
        self.inner.set_hard_state(hs).map_err(map_error)
    }

    // ---- type conversions (pure functions of their inputs) ----

    /// Convert a seam [`LogEntry`] to a raft [`Entry`].
    pub fn to_raft_entry(e: &LogEntry) -> Entry {
        let mut entry = Entry::default();
        entry.set_index(e.index);
        entry.set_term(e.term);
        entry.set_entry_type(match e.entry_type {
            SeamEntryType::Entry => RaftEntryType::EntryNormal,
            SeamEntryType::ConfChange => RaftEntryType::EntryConfChange,
            SeamEntryType::ConfChangeV2 => RaftEntryType::EntryConfChangeV2,
        });
        entry.set_data(e.data.clone().into());
        entry
    }

    /// Convert a raft [`Entry`] to a seam [`LogEntry`].
    pub fn from_raft_entry(e: &Entry) -> LogEntry {
        LogEntry {
            index: e.get_index(),
            term: e.get_term(),
            entry_type: match e.get_entry_type() {
                RaftEntryType::EntryNormal => SeamEntryType::Entry,
                RaftEntryType::EntryConfChange => SeamEntryType::ConfChange,
                RaftEntryType::EntryConfChangeV2 => SeamEntryType::ConfChangeV2,
            },
            data: e.get_data().to_vec(),
        }
    }

    /// Convert a seam [`HardState`] to a raft [`HardState`].
    ///
    /// The seam expresses "no vote" as `vote = None`; raft expresses it as
    /// `vote = 0` (the invalid node id).
    pub fn to_raft_hard_state(hs: &SeamHardState) -> HardState {
        let mut raft_hs = HardState::default();
        raft_hs.set_term(hs.term);
        raft_hs.set_vote(hs.vote.unwrap_or(0));
        raft_hs.set_commit(hs.commit);
        raft_hs
    }

    /// Convert a raft [`HardState`] to a seam [`HardState`].
    pub fn to_seam_hard_state(hs: &HardState) -> SeamHardState {
        SeamHardState {
            term: hs.get_term(),
            vote: if hs.get_vote() == 0 { None } else { Some(hs.get_vote()) },
            commit: hs.get_commit(),
        }
    }

    /// Convert a seam [`ConfState`] to a raft [`ConfState`].
    pub fn to_raft_conf_state(cs: &SeamConfState) -> ConfState {
        let mut raft_cs = ConfState::default();
        raft_cs.set_voters(cs.voters.clone());
        raft_cs.set_learners(cs.learners.clone());
        raft_cs
    }

    /// Convert a seam [`Snapshot`] to a raft [`Snapshot`].
    pub fn to_raft_snapshot(snap: &SeamSnapshot) -> Snapshot {
        let mut raft_snap = Snapshot::default();
        raft_snap.set_data(snap.data.clone().into());
        let meta = raft_snap.mut_metadata();
        meta.set_index(snap.meta.index);
        meta.set_term(snap.meta.term);
        let mut cs = ConfState::default();
        cs.set_voters(snap.meta.conf_state.voters.clone());
        cs.set_learners(snap.meta.conf_state.learners.clone());
        meta.set_conf_state(cs);
        raft_snap
    }
}

impl<S: SeamStorage> RaftStorageTrait for RaftStorage<S> {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let state = self.inner.initial_state().map_err(map_error)?;
        // A fresh store has no persisted membership: fall back to the declared
        // bootstrap voter set. Once membership is persisted (M3 ConfChange),
        // the durable copy wins.
        let conf_state = if state.conf_state.voters.is_empty() && state.conf_state.learners.is_empty()
        {
            self.bootstrap_conf_state.clone()
        } else {
            state.conf_state
        };
        Ok(RaftState {
            hard_state: Self::to_raft_hard_state(&state.hard_state),
            conf_state: Self::to_raft_conf_state(&conf_state),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let entries = self
            .inner
            .entries(low, high, max_size.into())
            .map_err(map_error)?;
        Ok(entries.iter().map(Self::to_raft_entry).collect())
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        // raft convention: index 0 is the "no entry" sentinel with term 0.
        // (M2: once snapshots exist, the snapshot's own index must return the
        // snapshot's term here, exactly as raft-rs `MemoryStorage` does.)
        if idx == 0 {
            return Ok(0);
        }
        // raft distinguishes these two out-of-range directions:
        //   * below the retained window -> `Compacted` (switch to a snapshot)
        //   * above the last index       -> `Unavailable` (not yet replicated)
        let last = self.inner.last_index().map_err(map_error)?;
        if idx > last {
            return Err(RaftError::Store(RaftStorageError::Unavailable));
        }
        self.inner.term(idx).map_err(map_error)
    }

    fn first_index(&self) -> RaftResult<u64> {
        self.inner.first_index().map_err(map_error)
    }

    fn last_index(&self) -> RaftResult<u64> {
        self.inner.last_index().map_err(map_error)
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        match self.inner.snapshot().map_err(map_error)? {
            Some(snap) => Ok(Self::to_raft_snapshot(&snap)),
            // No snapshot available: tell raft to retry later. A single-node
            // cluster never reaches this path (no lagging follower to send a
            // snapshot to).
            None => Err(RaftError::Store(RaftStorageError::SnapshotTemporarilyUnavailable)),
        }
    }
}

/// Map a seam [`SeamStorageError`] to the raft 0.7 error taxonomy.
fn map_error(e: SeamStorageError) -> RaftError {
    match e {
        SeamStorageError::Compacted => RaftError::Store(RaftStorageError::Compacted),
        // Fail-start conditions: the node must stop participating.
        SeamStorageError::Corruption { .. } | SeamStorageError::Unrecoverable { .. } => {
            RaftError::Store(RaftStorageError::Unavailable)
        }
        SeamStorageError::Io(io_err) => RaftError::Io(io_err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arachne_seam::storage::{EntryType, RaftState};
    use arachne_seam::types::LogIndex;

    /// A small in-memory seam storage double that can be pre-populated and
    /// optionally forced to report a specific error. Test-only.
    struct Double {
        entries: Vec<LogEntry>,
        hard_state: SeamHardState,
        conf_state: SeamConfState,
        /// When set, reads of the matching kind return this error.
        forced: Option<ForcedError>,
    }

    /// The real `StorageError` is not `Clone` (it carries `std::io::Error`),
    /// so the test double records a small copyable kind and reconstructs the
    /// error on demand.
    #[derive(Clone, Copy)]
    enum ForcedError {
        Corruption,
        Unrecoverable,
        Io,
    }

    impl ForcedError {
        fn to_storage_error(self) -> SeamStorageError {
            match self {
                ForcedError::Corruption => SeamStorageError::Corruption {
                    detail: "bad crc".into(),
                },
                ForcedError::Unrecoverable => SeamStorageError::Unrecoverable {
                    detail: "unrecoverable".into(),
                },
                ForcedError::Io => SeamStorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "disk full",
                )),
            }
        }
    }

    impl Double {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                hard_state: SeamHardState::default(),
                conf_state: SeamConfState::default(),
                forced: None,
            }
        }

        fn set_forced(&mut self, e: ForcedError) {
            self.forced = Some(e);
        }
    }

    impl SeamStorage for Double {
        fn initial_state(&self) -> Result<RaftState, SeamStorageError> {
            if let Some(f) = self.forced {
                return Err(f.to_storage_error());
            }
            Ok(RaftState {
                hard_state: self.hard_state.clone(),
                conf_state: self.conf_state.clone(),
            })
        }

        fn entries(
            &self,
            low: LogIndex,
            high: LogIndex,
            max_size: Option<u64>,
        ) -> Result<Vec<LogEntry>, SeamStorageError> {
            if let Some(f) = self.forced {
                return Err(f.to_storage_error());
            }
            if low >= high {
                return Ok(Vec::new());
            }
            let first = self.entries.first().map_or(1, |e| e.index);
            let last = self.entries.last().map_or(0, |e| e.index);
            if low < first || high > last + 1 {
                return Err(SeamStorageError::Compacted);
            }
            let start = (low - first) as usize;
            let end = (high - first) as usize;
            let mut out = self.entries[start..end].to_vec();
            if let Some(limit) = max_size {
                let mut size = 0u64;
                let mut keep = 0;
                while keep < out.len() {
                    size += out[keep].data.len() as u64;
                    keep += 1;
                    if size > limit {
                        out.truncate(keep - 1);
                        break;
                    }
                }
            }
            Ok(out)
        }

        fn term(&self, index: LogIndex) -> Result<u64, SeamStorageError> {
            if let Some(f) = self.forced {
                return Err(f.to_storage_error());
            }
            let first = self.entries.first().map_or(1, |e| e.index);
            let last = self.entries.last().map_or(0, |e| e.index);
            if index < first || index > last {
                return Err(SeamStorageError::Compacted);
            }
            Ok(self.entries[(index - first) as usize].term)
        }

        fn first_index(&self) -> Result<LogIndex, SeamStorageError> {
            Ok(self.entries.first().map_or(1, |e| e.index))
        }

        fn last_index(&self) -> Result<LogIndex, SeamStorageError> {
            Ok(self.entries.last().map_or(0, |e| e.index))
        }

        fn snapshot(&self) -> Result<Option<SeamSnapshot>, SeamStorageError> {
            Ok(None)
        }

        fn append(&mut self, entries: &[LogEntry]) -> Result<(), SeamStorageError> {
            self.entries.extend_from_slice(entries);
            Ok(())
        }

        fn set_hard_state(&mut self, hs: &SeamHardState) -> Result<(), SeamStorageError> {
            self.hard_state = hs.clone();
            Ok(())
        }

        fn sync_entries(&mut self) -> Result<(), SeamStorageError> {
            Ok(())
        }

        fn compact(&mut self, _to: LogIndex) -> Result<(), SeamStorageError> {
            Ok(())
        }
    }

    fn entry(index: LogIndex, term: u64, data: &[u8]) -> LogEntry {
        LogEntry {
            index,
            term,
            entry_type: EntryType::Entry,
            data: data.to_vec(),
        }
    }

    /// Round-trip: append seam entries, read them back through the raft trait,
    /// and confirm the fields survive the conversion.
    #[test]
    fn entry_round_trip_through_raft_view() {
        let mut inner = Double::new();
        inner
            .append(&[entry(1, 1, b"a"), entry(2, 2, b"b")])
            .expect("append must succeed");
        let store = RaftStorage::new(inner);

        let got = store
            .entries(1, 3, u64::MAX, GetEntriesContext::empty(false))
            .expect("range [1,3) is retained");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].get_index(), 1);
        assert_eq!(got[0].get_term(), 1);
        assert_eq!(got[0].get_data(), b"a");
        assert_eq!(got[0].get_entry_type(), RaftEntryType::EntryNormal);
        assert_eq!(got[1].get_index(), 2);
        assert_eq!(got[1].get_term(), 2);
        assert_eq!(got[1].get_data(), b"b");

        // Index/term consistency via the other read accessors.
        assert_eq!(store.first_index().unwrap(), 1);
        assert_eq!(store.last_index().unwrap(), 2);
        assert_eq!(store.term(1).unwrap(), 1);
        assert_eq!(store.term(2).unwrap(), 2);
    }

    /// Hard-state and conf-state round-trip through `initial_state`.
    #[test]
    fn hard_state_and_conf_state_round_trip() {
        let mut inner = Double::new();
        inner
            .set_hard_state(&SeamHardState {
                term: 5,
                vote: Some(7),
                commit: 3,
            })
            .expect("set_hard_state must succeed");
        inner.conf_state = SeamConfState {
            voters: vec![1, 2, 3],
            learners: vec![4],
        };
        let store = RaftStorage::new(inner);

        let state = store.initial_state().expect("fresh double has a valid state");
        assert_eq!(state.hard_state.get_term(), 5);
        assert_eq!(state.hard_state.get_vote(), 7);
        assert_eq!(state.hard_state.get_commit(), 3);
        assert_eq!(state.conf_state.get_voters(), &[1, 2, 3]);
        assert_eq!(state.conf_state.get_learners(), &[4]);
    }

    /// `vote = None` maps to raft's invalid id `0` and back.
    #[test]
    fn hard_state_vote_none_maps_to_zero() {
        let raft_hs = RaftStorage::<Double>::to_raft_hard_state(&SeamHardState {
            term: 1,
            vote: None,
            commit: 0,
        });
        assert_eq!(raft_hs.get_vote(), 0);
        let seam_hs = RaftStorage::<Double>::to_seam_hard_state(&raft_hs);
        assert_eq!(seam_hs.vote, None);
    }

    /// A compacted range surfaces as raft's `Store(Compacted)`.
    #[test]
    fn compacted_range_maps_to_raft_compacted() {
        let store = RaftStorage::new(Double::new());
        // Empty store: first=1, last=0, so [1,2) is outside the retained window.
        let err = store
            .entries(1, 2, u64::MAX, GetEntriesContext::empty(false))
            .expect_err("out-of-range must be an error");
        assert_eq!(
            err,
            RaftError::Store(RaftStorageError::Compacted)
        );
    }

    /// `Corruption` / `Unrecoverable` (fail-start) map to raft `Unavailable`.
    #[test]
    fn fail_start_errors_map_to_unavailable() {
        let mut inner = Double::new();
        inner.set_forced(ForcedError::Corruption);
        let store = RaftStorage::new(inner);
        let err = store
            .entries(1, 2, u64::MAX, GetEntriesContext::empty(false))
            .expect_err("forced error must surface");
        assert_eq!(err, RaftError::Store(RaftStorageError::Unavailable));
    }

    /// `Io` errors pass through as `raft::Error::Io`.
    #[test]
    fn io_error_maps_to_raft_io() {
        let mut inner = Double::new();
        inner.set_forced(ForcedError::Io);
        let store = RaftStorage::new(inner);
        let err = store
            .entries(1, 2, u64::MAX, GetEntriesContext::empty(false))
            .expect_err("forced error must surface");
        assert!(matches!(err, RaftError::Io(_)));
    }

    /// `None` snapshot maps to raft's temporary-unavailable (retry later).
    #[test]
    fn no_snapshot_maps_to_temporarily_unavailable() {
        let store = RaftStorage::new(Double::new());
        let err = store
            .snapshot(0, 0)
            .expect_err("no snapshot must be an error");
        assert_eq!(
            err,
            RaftError::Store(RaftStorageError::SnapshotTemporarilyUnavailable)
        );
    }

    /// A populated seam snapshot converts with its metadata intact.
    #[test]
    fn snapshot_conversion_preserves_metadata() {
        let snap = SeamSnapshot {
            meta: arachne_seam::storage::SnapshotMeta {
                index: 9,
                term: 4,
                conf_state: SeamConfState {
                    voters: vec![1, 2],
                    learners: vec![],
                },
            },
            data: b"payload".to_vec(),
        };
        let raft_snap = RaftStorage::<Double>::to_raft_snapshot(&snap);
        assert_eq!(raft_snap.get_metadata().get_index(), 9);
        assert_eq!(raft_snap.get_metadata().get_term(), 4);
        assert_eq!(raft_snap.get_metadata().get_conf_state().get_voters(), &[1, 2]);
        assert_eq!(raft_snap.get_data(), b"payload");
    }
}
