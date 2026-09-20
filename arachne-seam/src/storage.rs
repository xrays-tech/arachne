//! The storage seam: the durable log and durable state for one raft node.
//!
//! Raft needs a durable backing store for two kinds of state:
//!
//! * the **log** — an append-only sequence of [`LogEntry`]s, and
//! * the **state** — the current [`HardState`] (term, vote, commit) and
//!   [`ConfState`] (membership), plus optional [`Snapshot`]s.
//!
//! This module defines the **interface** ([`Storage`]) and the value types
//! shared across the consensus core. It does **not** implement a WAL, recovery,
//! an fsync policy, snapshot-on-disk, or any I/O — those are later tasks. A
//! concrete `Storage` (a real WAL, or an in-memory test double) is supplied by
//! the caller, exactly like the other seams (`Clock`, `Transport`, …).
//!
//! # Durability invariants
//!
//! * **I1** — [`Storage::set_hard_state`] MUST be durable before it returns.
//!   The term/vote/commit state is the one thing a node must never lose; it is
//!   never batched with other writes and never deferred.
//! * **I2 / I4** — entries appended via [`Storage::append`] need not be durable
//!   immediately, but [`Storage::sync_entries`] is the durability barrier:
//!   after it returns, all previously appended entries are durable.
//!
//! # Error discipline
//!
//! [`StorageError::Compacted`] is the *normal* signal that a requested log
//! range is no longer available as entries — raft uses it to switch a lagging
//! follower to snapshot transfer. [`StorageError::Corruption`] and
//! [`StorageError::Unrecoverable`] are **fail-start** conditions: they must
//! never be silently ignored or retried; the node must abort.

use crate::types::{LogIndex, Term};

/// The raft node id as required by the consensus core.
///
/// The public API uses [`NodeId`](crate::types::NodeId) (a `String`); the
/// `String` ↔ `u64` mapping is established at bootstrap and persisted as
/// replicated membership state (a later milestone). `RaftId` MUST NOT appear in
/// WAL records — the WAL stores only this raw `u64` and opaque payloads, so it
/// stays decoupled from the human-facing `NodeId`.
pub type RaftId = u64;

/// The kind of a log entry.
///
/// The discriminants are part of the wire/on-disk format and are fixed:
/// `Entry = 0`, `ConfChange = 1`, `ConfChangeV2 = 2`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryType {
    /// A normal replicated log entry (an opaque command payload).
    Entry = 0,
    /// A single-node configuration change (v1).
    ConfChange = 1,
    /// A joint (two-phase) configuration change (v2).
    ConfChangeV2 = 2,
}

/// A single entry in the replicated log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    /// The entry's position in the log (1-based, strictly increasing).
    pub index: LogIndex,
    /// The term in which the entry was created.
    pub term: Term,
    /// The kind of entry.
    pub entry_type: EntryType,
    /// The opaque command/payload bytes.
    pub data: Vec<u8>,
}

/// The durable "hard" state of a raft node: the values that must survive a
/// crash. All three are always written durably together.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct HardState {
    /// The node's current term.
    pub term: Term,
    /// The `RaftId` this node voted for in the current term, if any.
    pub vote: Option<RaftId>,
    /// The highest log index known to be committed.
    pub commit: LogIndex,
}

/// The cluster membership configuration.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ConfState {
    /// The voting members.
    pub voters: Vec<RaftId>,
    /// The non-voting (learner) members.
    pub learners: Vec<RaftId>,
}

/// Metadata describing a snapshot: its position and the membership it carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// The log index the snapshot was taken at.
    pub index: LogIndex,
    /// The term the snapshot was taken at.
    pub term: Term,
    /// The membership configuration the snapshot carries.
    pub conf_state: ConfState,
}

/// A snapshot of the replicated state, used to bootstrap a lagging follower.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// The snapshot's position/membership metadata.
    pub meta: SnapshotMeta,
    /// The opaque serialized state-machine state.
    pub data: Vec<u8>,
}

/// A pending **off-thread flush** of records a storage has already written.
///
/// Opaque to the caller: only the storage that minted it knows what it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushToken(pub u64);

/// What [`Storage::persist_ready_records`] did with a `Ready`'s records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistSubmit {
    /// The records are durable: a synchronous storage, or nothing to persist.
    Durable,
    /// The records are written and readable but not yet durable; the token
    /// reports completion through [`Storage::poll_flush`].
    Offloaded(FlushToken),
}

/// A callback a storage invokes when an offloaded flush completes.
///
/// The runtime installs one so it can wake as soon as durability lands instead
/// of waiting for its next tick (propsol v0.2.13 P). Kept as a plain closure so
/// this leaf crate stays dependency-free.
#[derive(Clone, Default)]
pub struct FlushWaker(Option<std::sync::Arc<dyn Fn() + Send + Sync>>);

impl FlushWaker {
    /// Wrap a callback.
    pub fn new(wake: impl Fn() + Send + Sync + 'static) -> Self {
        Self(Some(std::sync::Arc::new(wake)))
    }

    /// Signal completion. Cheap and callable from any thread.
    pub fn wake(&self) {
        if let Some(wake) = &self.0 {
            wake();
        }
    }
}

impl core::fmt::Debug for FlushWaker {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("FlushWaker").field(&self.0.is_some()).finish()
    }
}

/// The durable raft state: the hard state plus the membership configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftState {
    /// The durable term/vote/commit state.
    pub hard_state: HardState,
    /// The membership configuration.
    pub conf_state: ConfState,
}

/// Errors reported by a [`Storage`].
///
/// [`Compacted`](StorageError::Compacted) is the *normal* signal for a missing
/// log range (raft switches to snapshot transfer). [`Corruption`](StorageError::Corruption)
/// and [`Unrecoverable`](StorageError::Unrecoverable) are **fail-start**
/// conditions that must never be silently ignored — the node must abort.
#[derive(Debug)]
pub enum StorageError {
    /// The requested log range is not available as entries (outside the
    /// retained window). Normal; not a fault of the node.
    Compacted,
    /// Durable data is corrupted. Fail-start: abort, do not retry.
    Corruption { detail: String },
    /// A state from which the node cannot recover. Fail-start: abort.
    Unrecoverable { detail: String },
    /// A low-level I/O failure; the underlying error is its `source()`.
    Io(std::io::Error),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Compacted => write!(f, "log range has been compacted"),
            StorageError::Corruption { detail } => write!(f, "storage corruption: {detail}"),
            StorageError::Unrecoverable { detail } => {
                write!(f, "unrecoverable storage error: {detail}")
            }
            StorageError::Io(err) => write!(f, "storage I/O error: {err}"),
        }
    }
}

impl core::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        // Only the I/O wrapper has an underlying source error.
        match self {
            StorageError::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// The durable storage seam for one raft node.
///
/// Implementations may be a real WAL or an in-memory test double. The trait is
/// `Send` (not `Sync`): a single task owns and mutates the storage.
pub trait Storage: Send + 'static {
    // ---- read side (raft-rs semantics; `Compacted` for compacted ranges) ----

    /// Return the durable [`RaftState`] (hard state + membership) at startup.
    fn initial_state(&self) -> Result<RaftState, StorageError>;

    /// Return the entries whose indices lie in the half-open range `[low, high)`.
    ///
    /// `max_size` optionally bounds the total serialized size (sum of `data`
    /// lengths) of the returned entries; the implementation returns the longest
    /// prefix of the range that fits.
    ///
    /// A degenerate range (`low >= high`) returns `Ok(vec![])` — an empty range
    /// is not an error (raft-rs treats `low == high` as "no entries requested").
    ///
    /// `StorageError::Compacted` is returned when the requested range is not
    /// available as entries — a range that falls outside the retained window
    /// `[first_index, last_index]`. This is the normal signal for raft to fall
    /// back to snapshot transfer.
    // NOTE: the exact error taxonomy for `low < first_index` vs
    // `high > last_index + 1` (both currently `Compacted`) will be reconciled
    // against the real `raft` crate at P4.
    fn entries(
        &self,
        low: LogIndex,
        high: LogIndex,
        max_size: Option<u64>,
    ) -> Result<Vec<LogEntry>, StorageError>;

    /// Return the term of the entry at `index`.
    ///
    /// `Compacted` if the index is outside the retained window
    /// `[first_index, last_index]`.
    fn term(&self, index: LogIndex) -> Result<Term, StorageError>;

    /// The index of the first log entry retained in storage (1 on a fresh,
    /// empty store).
    fn first_index(&self) -> Result<LogIndex, StorageError>;

    /// The index of the last log entry retained in storage (0 on a fresh,
    /// empty store).
    fn last_index(&self) -> Result<LogIndex, StorageError>;

    /// Return the latest snapshot if one exists. P2 always returns `Ok(None)`.
    fn snapshot(&self) -> Result<Option<Snapshot>, StorageError>;

    // ---- write side ----------------------------------------------------------

    /// Append entries to the durable log. May buffer; durability is provided
    /// by [`sync_entries`](Storage::sync_entries).
    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError>;

    /// Persist the given [`HardState`].
    ///
    /// **Invariant I1:** implementations MUST make this durable before
    /// returning. It is never batched with other writes and never deferred.
    fn set_hard_state(&mut self, hs: &HardState) -> Result<(), StorageError>;

    /// Durability barrier for previously appended entries.
    ///
    /// **Invariants I2/I4:** after this returns, all entries appended before
    /// the call are durable. It is the only point at which `append`ed entries
    /// are guaranteed on disk.
    fn sync_entries(&mut self) -> Result<(), StorageError>;

    /// Drop entries/segments strictly before `compact_to`.
    ///
    /// P2 may return [`StorageError::Unrecoverable`] or be a no-op until real
    /// compaction lands (M2).
    fn compact(&mut self, compact_to: LogIndex) -> Result<(), StorageError>;

    /// Persist one `Ready`'s records, entries first and then the optional hard
    /// state, in the order raft requires.
    ///
    /// The default is the synchronous sequence every storage already
    /// implements. A storage that can flush off-thread overrides it to write
    /// both records and return [`PersistSubmit::Offloaded`], so the caller can
    /// keep serving reads while the device catches up (propsol v0.2.13 P).
    fn persist_ready_records(
        &mut self,
        entries: &[LogEntry],
        hard_state: Option<&HardState>,
    ) -> Result<PersistSubmit, StorageError> {
        if !entries.is_empty() {
            self.append(entries)?;
            // I2/I4: the entries are durable once this returns.
            self.sync_entries()?;
        }
        if let Some(hs) = hard_state {
            // I1: always fsynced, never batched.
            self.set_hard_state(hs)?;
        }
        Ok(PersistSubmit::Durable)
    }

    /// Non-blocking completion check for a token from
    /// [`persist_ready_records`](Storage::persist_ready_records).
    ///
    /// `Ok(None)` means "still in flight, ask again"; `Ok(Some(result))`
    /// reports the outcome exactly once. Storages that never return a token
    /// inherit a "already complete" answer.
    fn poll_flush(
        &mut self,
        token: &FlushToken,
    ) -> Result<Option<Result<(), String>>, StorageError> {
        let _ = token;
        Ok(Some(Ok(())))
    }

    /// Install the callback to wake when an offloaded flush completes.
    fn set_flush_waker(&mut self, waker: FlushWaker) {
        let _ = waker;
    }

    /// Persist `snapshot` durably and make it this store's latest snapshot.
    ///
    /// **Invariant I3:** once this returns, the snapshot's position and
    /// membership are durable. Only then may the snapshot be advertised to
    /// peers, and only then may the log at or below `snapshot.meta.index` be
    /// released with [`compact`](Storage::compact).
    ///
    /// Moving the snapshot backwards is never allowed: an implementation that
    /// already holds a snapshot at or beyond `snapshot.meta.index` must leave
    /// its state unchanged and report success.
    ///
    /// The default implementation fails loudly rather than silently dropping
    /// the snapshot: only storages that can persist snapshots override it.
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        let _ = snapshot;
        Err(StorageError::Unrecoverable {
            detail: "this storage implementation does not support snapshots".into(),
        })
    }

    /// Adopt `snapshot` as this store's complete log: persist it, then discard
    /// **every** locally retained entry.
    ///
    /// This is the follower side of snapshot transfer, and it is deliberately
    /// stronger than `save_snapshot` + `compact`: an installed snapshot is
    /// authoritative, and any entry this node holds from a diverged branch
    /// must not survive it. raft only installs a snapshot at or above the
    /// local commit index, so the discarded entries are uncommitted by
    /// construction and the leader re-sends whatever is still needed.
    ///
    /// The default implementation fails loudly; storages that can persist
    /// snapshots override it.
    fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        let _ = snapshot;
        Err(StorageError::Unrecoverable {
            detail: "this storage implementation does not support snapshots".into(),
        })
    }
}

/// Observes per-segment fsync events from a WAL implementation.
///
/// The WAL calls [`on_segment_fsynced`](FsyncObserver::on_segment_fsynced)
/// after **every real fsync** of a segment file. This allows external
/// verification machinery (e.g., a [`FsyncLedger`]) to track which segments'
/// bytes are durably on disk, enabling detection of cross-segment durability
/// gaps (the N2 class of bug).
///
/// # Implementation requirements
///
/// Observers must be **cheap and non-blocking**: the call happens on the hot
/// path of the WAL's fsync. Do not perform I/O, allocation, or locking in the
/// observer implementation.
pub trait FsyncObserver: Send + Sync + 'static {
    /// Called AFTER a segment's bytes have been durably fsynced.
    ///
    /// `segment_first_index` identifies the segment (its filename index);
    /// `durable_through_index` is the highest log index whose bytes in that
    /// segment are now durable (0 if the segment holds no Entry yet).
    fn on_segment_fsynced(&self, segment_first_index: LogIndex, durable_through_index: LogIndex);
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::error::Error;

    /// A minimal in-memory `Storage` double over a `Vec<LogEntry>` and a
    /// `HardState`. Test-only; not production code.
    struct MemStorage {
        entries: Vec<LogEntry>,
        hard_state: HardState,
    }

    impl MemStorage {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                hard_state: HardState::default(),
            }
        }

        fn entry(index: LogIndex, term: Term, data: Vec<u8>) -> LogEntry {
            LogEntry {
                index,
                term,
                entry_type: EntryType::Entry,
                data,
            }
        }

        fn first(&self) -> LogIndex {
            self.entries.first().map_or(1, |e| e.index)
        }
        fn last(&self) -> LogIndex {
            self.entries.last().map_or(0, |e| e.index)
        }
    }

    impl Storage for MemStorage {
        fn initial_state(&self) -> Result<RaftState, StorageError> {
            Ok(RaftState {
                hard_state: self.hard_state.clone(),
                conf_state: ConfState::default(),
            })
        }

        fn entries(
            &self,
            low: LogIndex,
            high: LogIndex,
            max_size: Option<u64>,
        ) -> Result<Vec<LogEntry>, StorageError> {
            // Chosen semantics (documented on the trait):
            //   - a degenerate range (low >= high) returns an empty vec;
            //   - a range outside the retained window yields `Compacted`.
            if low >= high {
                return Ok(Vec::new());
            }
            if low < self.first() {
                return Err(StorageError::Compacted);
            }
            if high > self.last() + 1 {
                return Err(StorageError::Compacted);
            }
            let start = (low - self.first()) as usize;
            let end = (high - self.first()) as usize;
            let mut out: Vec<LogEntry> = self.entries[start..end].to_vec();
            if let Some(limit) = max_size {
                let mut size: u64 = 0;
                let mut keep = 0;
                while keep < out.len() {
                    let add = out[keep].data.len() as u64;
                    if size + add > limit {
                        break;
                    }
                    size += add;
                    keep += 1;
                }
                out.truncate(keep);
            }
            Ok(out)
        }

        fn term(&self, index: LogIndex) -> Result<Term, StorageError> {
            if index < self.first() || index > self.last() {
                return Err(StorageError::Compacted);
            }
            Ok(self.entries[(index - self.first()) as usize].term)
        }

        fn first_index(&self) -> Result<LogIndex, StorageError> {
            Ok(self.first())
        }

        fn last_index(&self) -> Result<LogIndex, StorageError> {
            Ok(self.last())
        }

        fn snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
            Ok(None)
        }

        fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
            self.entries.extend_from_slice(entries);
            Ok(())
        }

        fn set_hard_state(&mut self, hs: &HardState) -> Result<(), StorageError> {
            self.hard_state = hs.clone();
            Ok(())
        }

        fn sync_entries(&mut self) -> Result<(), StorageError> {
            Ok(())
        }

        fn compact(&mut self, _compact_to: LogIndex) -> Result<(), StorageError> {
            // No-op in the P2 double; real compaction lands in M2.
            Ok(())
        }
    }

    /// A store with entries at indices 1..=3 (terms 1,1,2; data "a","b","c").
    fn populated() -> MemStorage {
        let mut s = MemStorage::new();
        s.append(&[
            MemStorage::entry(1, 1, b"a".to_vec()),
            MemStorage::entry(2, 1, b"b".to_vec()),
            MemStorage::entry(3, 2, b"c".to_vec()),
        ])
        .expect("append to a fresh in-memory store must succeed");
        s
    }

    #[test]
    fn initial_state_is_default_when_fresh() {
        let s = MemStorage::new();
        let state = s.initial_state().expect("fresh store must have a valid state");
        assert_eq!(state.hard_state, HardState::default());
        assert_eq!(state.conf_state, ConfState::default());
    }

    #[test]
    fn hard_state_default_is_zero() {
        assert_eq!(
            HardState::default(),
            HardState {
                term: 0,
                vote: None,
                commit: 0
            }
        );
    }

    #[test]
    fn fresh_store_first_is_one_last_is_zero() {
        let s = MemStorage::new();
        assert_eq!(s.first_index().unwrap(), 1);
        assert_eq!(s.last_index().unwrap(), 0);
    }

    #[test]
    fn append_then_first_last_and_term() {
        let mut s = MemStorage::new();
        s.append(&[
            MemStorage::entry(1, 7, b"x".to_vec()),
            MemStorage::entry(2, 7, b"y".to_vec()),
            MemStorage::entry(3, 9, b"z".to_vec()),
        ])
        .unwrap();
        assert_eq!(s.first_index().unwrap(), 1);
        assert_eq!(s.last_index().unwrap(), 3);
        assert_eq!(s.term(1).unwrap(), 7);
        assert_eq!(s.term(2).unwrap(), 7);
        assert_eq!(s.term(3).unwrap(), 9);
    }

    #[test]
    fn entries_return_the_half_open_range() {
        let s = populated();
        assert_eq!(
            s.entries(1, 4, None).unwrap().iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            s.entries(2, 4, None).unwrap().iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            s.entries(2, 3, None).unwrap().iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn entries_degenerate_range_returns_empty() {
        let s = populated();
        // low == high is a degenerate range: returns an empty vec, not an error.
        assert_eq!(s.entries(2, 2, None).unwrap(), Vec::<LogEntry>::new());
        // low > high is also degenerate.
        assert_eq!(s.entries(3, 1, None).unwrap(), Vec::<LogEntry>::new());
    }

    #[test]
    fn entries_below_first_is_compacted() {
        let s = populated();
        // index 0 is below the first retained index (1).
        assert!(matches!(s.entries(0, 2, None).unwrap_err(), StorageError::Compacted));
    }

    #[test]
    fn entries_above_last_is_compacted() {
        let s = populated();
        // high = 5 is beyond last_index + 1 (= 4).
        assert!(matches!(s.entries(1, 5, None).unwrap_err(), StorageError::Compacted));
    }

    #[test]
    fn entries_on_empty_store_is_compacted() {
        let s = MemStorage::new();
        // No entries retained: first=1, last=0, so high=2 > last+1=1.
        assert!(matches!(s.entries(1, 2, None).unwrap_err(), StorageError::Compacted));
    }

    #[test]
    fn term_out_of_range_is_compacted() {
        let s = populated();
        assert!(matches!(s.term(0).unwrap_err(), StorageError::Compacted));
        assert!(matches!(s.term(4).unwrap_err(), StorageError::Compacted));
    }

    #[test]
    fn set_hard_state_persists() {
        let mut s = MemStorage::new();
        let hs = HardState {
            term: 5,
            vote: Some(42),
            commit: 3,
        };
        s.set_hard_state(&hs).unwrap();
        let read_back = s.initial_state().unwrap().hard_state;
        assert_eq!(read_back, hs);
    }

    #[test]
    fn snapshot_returns_none_in_p2() {
        let s = populated();
        assert_eq!(s.snapshot().unwrap(), None);
    }

    #[test]
    fn compact_is_a_no_op_in_p2() {
        let mut s = populated();
        s.compact(2).unwrap();
        // Nothing is dropped in the P2 double.
        assert_eq!(s.first_index().unwrap(), 1);
        assert_eq!(s.last_index().unwrap(), 3);
    }

    #[test]
    fn max_size_bounds_the_returned_prefix() {
        let s = populated(); // data sizes: a=1, b=1, c=1
        assert_eq!(s.entries(1, 4, Some(0)).unwrap(), Vec::<LogEntry>::new());
        // limit 2 -> entries 1,2 (total size 2); entry 3 would make it 3 > 2.
        assert_eq!(
            s.entries(1, 4, Some(2)).unwrap().iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            s.entries(1, 4, Some(3)).unwrap().iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            s.entries(1, 4, Some(100)).unwrap().iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn entry_type_discriminants_are_fixed() {
        assert_eq!(EntryType::Entry as u8, 0);
        assert_eq!(EntryType::ConfChange as u8, 1);
        assert_eq!(EntryType::ConfChangeV2 as u8, 2);
    }

    #[test]
    fn storage_error_is_a_std_error() {
        fn assert_is_error<E: core::error::Error + Send + Sync + 'static>(e: E) -> String {
            e.to_string()
        }
        assert!(!assert_is_error(StorageError::Compacted).is_empty());
        assert!(!assert_is_error(StorageError::Corruption { detail: "x".into() }).is_empty());
    }

    #[test]
    fn storage_error_display_output() {
        assert_eq!(
            StorageError::Compacted.to_string(),
            "log range has been compacted"
        );
        assert_eq!(
            StorageError::Corruption {
                detail: "bad checksum".into()
            }
            .to_string(),
            "storage corruption: bad checksum"
        );
        assert_eq!(
            StorageError::Unrecoverable {
                detail: "fs gone".into()
            }
            .to_string(),
            "unrecoverable storage error: fs gone"
        );
        let io = StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "disk full",
        ));
        assert_eq!(io.to_string(), "storage I/O error: disk full");
    }

    #[test]
    fn storage_error_source() {
        // The Io wrapper exposes its inner error as the source.
        let err = StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "disk full",
        ));
        let src = err
            .source()
            .expect("Io error must expose its inner error as source");
        assert_eq!(src.to_string(), "disk full");
        // The other variants have no underlying source.
        assert!(StorageError::Compacted.source().is_none());
        assert!(StorageError::Corruption { detail: "x".into() }.source().is_none());
        assert!(StorageError::Unrecoverable { detail: "x".into() }.source().is_none());
    }
}
