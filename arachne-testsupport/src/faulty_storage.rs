//! A fault-injecting `Storage` wrapper for testing error paths and
//! verifying that the fsync ledger does not advance on failure.
//!
//! [`FaultyStorage`] wraps any [`Storage`] implementation and delegates all
//! calls to the inner storage, optionally injecting deterministic faults:
//!
//! * Fail the **n-th** `sync_entries` call with a [`StorageError`].
//! * Fail the **n-th** `set_hard_state` call with a [`StorageError`].
//!
//! Each operation is recorded in an [`OpRecord`] log so tests can assert
//! call ordering and counts.
//!
//! # What this does NOT cover
//!
//! Byte-level faults (torn writes, partial writes, bit flips) cannot be
//! injected through the logical `Storage` seam — the seam operates on
//! `LogEntry` values, not raw bytes. Those are applied **offline to WAL
//! files** in P3c (the fuzz/recovery battery). `FaultyStorage` covers
//! fsync-failure / error-injection / slow-markers at the logical layer.

use std::sync::Arc;

use arachne_seam::storage::{
    HardState, LogEntry, RaftState, Snapshot, Storage, StorageError,
};
use arachne_seam::types::{LogIndex, Term};

use crate::durability::DurabilityLedger;

/// A deterministic fault schedule (no RNG).
#[derive(Clone, Debug, Default)]
pub struct FaultSchedule {
    /// If `Some(n)`, the n-th (1-based) `sync_entries` call returns an error.
    pub fail_sync_entries_at: Option<u64>,
    /// If `Some(n)`, every n-th (1-based) `sync_entries` call returns an error
    /// (the first at `n`, then `2n`, ...). Takes effect only when
    /// `fail_sync_entries_at` is not the matching call.
    pub fail_sync_entries_every: Option<u64>,
    /// If `Some(n)`, the n-th (1-based) `set_hard_state` call returns an error.
    pub fail_set_hard_state_at: Option<u64>,
    /// If `Some(n)`, the n-th (1-based) `append` call returns an error.
    pub fail_append_at: Option<u64>,
}

/// A record of a storage operation (for test assertions).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    Append,
    SetHardState,
    SyncEntries,
    Compact,
    InitialState,
    Entries,
    Term,
    FirstIndex,
    LastIndex,
    Snapshot,
}

/// A logged operation with its kind and a sequence number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpRecord {
    /// Monotonically increasing sequence number (1-based).
    pub seq: u64,
    /// The kind of operation.
    pub kind: OpKind,
}

/// A fault-injecting `Storage` wrapper.
///
/// Delegates all calls to the inner `S`, injecting deterministic faults per
/// the [`FaultSchedule`] and logging every operation.
pub struct FaultyStorage<S: Storage> {
    inner: S,
    schedule: FaultSchedule,
    ops: Vec<OpRecord>,
    append_count: u64,
    sync_entries_count: u64,
    set_hard_state_count: u64,
    /// Optional durability ledger: every successful `set_hard_state` is recorded
    /// as a durable HardState (I1 — the WAL fsyncs inside), so tests can
    /// reconcile outbound messages against what the node has actually persisted.
    durability: Option<Arc<DurabilityLedger>>,
}

impl<S: Storage> FaultyStorage<S> {
    /// Wrap an inner storage with a fault schedule.
    pub fn new(inner: S, schedule: FaultSchedule) -> Self {
        Self {
            inner,
            schedule,
            ops: Vec::new(),
            append_count: 0,
            sync_entries_count: 0,
            set_hard_state_count: 0,
            durability: None,
        }
    }

    /// Wrap an inner storage with a fault schedule and a durability ledger.
    ///
    /// Every successful `set_hard_state` is recorded into `durability` as a
    /// durable HardState (I1), which is what lets a test reconcile an outbound
    /// message's `term` against the node's persisted term.
    pub fn with_ledger(
        inner: S,
        schedule: FaultSchedule,
        durability: Arc<DurabilityLedger>,
    ) -> Self {
        Self {
            durability: Some(durability),
            ..Self::new(inner, schedule)
        }
    }

    /// Return the number of `append` calls made (including failed ones).
    pub fn append_count(&self) -> u64 {
        self.append_count
    }

    /// Return a snapshot of the operation log.
    pub fn ops(&self) -> &[OpRecord] {
        &self.ops
    }

    /// Return the number of `sync_entries` calls made (including failed ones).
    pub fn sync_entries_count(&self) -> u64 {
        self.sync_entries_count
    }

    /// Return the number of `set_hard_state` calls made (including failed ones).
    pub fn set_hard_state_count(&self) -> u64 {
        self.set_hard_state_count
    }

    /// Record an operation and return its sequence number.
    fn log_op(&mut self, kind: OpKind) -> u64 {
        let seq = self.ops.len() as u64 + 1;
        self.ops.push(OpRecord { seq, kind });
        seq
    }
}

impl<S: Storage> Storage for FaultyStorage<S> {
    fn initial_state(&self) -> Result<RaftState, StorageError> {
        self.inner.initial_state()
    }

    fn entries(
        &self,
        low: LogIndex,
        high: LogIndex,
        max_size: Option<u64>,
    ) -> Result<Vec<LogEntry>, StorageError> {
        self.inner.entries(low, high, max_size)
    }

    fn term(&self, index: LogIndex) -> Result<Term, StorageError> {
        self.inner.term(index)
    }

    fn first_index(&self) -> Result<LogIndex, StorageError> {
        self.inner.first_index()
    }

    fn last_index(&self) -> Result<LogIndex, StorageError> {
        self.inner.last_index()
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        self.inner.snapshot()
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        self.log_op(OpKind::Append);
        self.append_count += 1;
        if let Some(fail_at) = self.schedule.fail_append_at
            && self.append_count == fail_at
        {
            return Err(StorageError::Unrecoverable {
                detail: "injected append failure".into(),
            });
        }
        self.inner.append(entries)
    }

    fn set_hard_state(&mut self, hs: &HardState) -> Result<(), StorageError> {
        self.log_op(OpKind::SetHardState);
        self.set_hard_state_count += 1;
        if let Some(fail_at) = self.schedule.fail_set_hard_state_at
            && self.set_hard_state_count == fail_at
        {
            return Err(StorageError::Unrecoverable {
                detail: "injected set_hard_state failure".into(),
            });
        }
        self.inner.set_hard_state(hs)?;
        // I1: the WAL fsyncs inside `set_hard_state`; a successful call is a
        // durable HardState. Record it for INV1's term/commit reconciliation.
        if let Some(ledger) = &self.durability {
            ledger.record_hard_state(hs.term, hs.vote, hs.commit);
        }
        Ok(())
    }

    fn sync_entries(&mut self) -> Result<(), StorageError> {
        self.log_op(OpKind::SyncEntries);
        self.sync_entries_count += 1;
        if let Some(fail_at) = self.schedule.fail_sync_entries_at
            && self.sync_entries_count == fail_at
        {
            return Err(StorageError::Unrecoverable {
                detail: "injected sync_entries failure".into(),
            });
        }
        if let Some(every) = self.schedule.fail_sync_entries_every
            && every > 0
            && self.sync_entries_count % every == 0
        {
            return Err(StorageError::Unrecoverable {
                detail: "injected periodic sync_entries failure".into(),
            });
        }
        self.inner.sync_entries()
    }

    fn compact(&mut self, compact_to: LogIndex) -> Result<(), StorageError> {
        self.log_op(OpKind::Compact);
        self.inner.compact(compact_to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arachne_seam::storage::{ConfState, EntryType};

    /// A minimal in-memory storage double for testing.
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
            _max_size: Option<u64>,
        ) -> Result<Vec<LogEntry>, StorageError> {
            if low >= high {
                return Ok(Vec::new());
            }
            let first = self.entries.first().map_or(1, |e| e.index);
            let last = self.entries.last().map_or(0, |e| e.index);
            if low < first || high > last + 1 {
                return Err(StorageError::Compacted);
            }
            Ok(self.entries[(low - first) as usize..(high - first) as usize].to_vec())
        }
        fn term(&self, index: LogIndex) -> Result<Term, StorageError> {
            let first = self.entries.first().map_or(1, |e| e.index);
            let last = self.entries.last().map_or(0, |e| e.index);
            if index < first || index > last {
                return Err(StorageError::Compacted);
            }
            Ok(self.entries[(index - first) as usize].term)
        }
        fn first_index(&self) -> Result<LogIndex, StorageError> {
            Ok(self.entries.first().map_or(1, |e| e.index))
        }
        fn last_index(&self) -> Result<LogIndex, StorageError> {
            Ok(self.entries.last().map_or(0, |e| e.index))
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
            Ok(())
        }
    }

    fn entry(index: u64, term: u64, data: &[u8]) -> LogEntry {
        LogEntry {
            index,
            term,
            entry_type: EntryType::Entry,
            data: data.to_vec(),
        }
    }

    #[test]
    fn injected_sync_failure_surfaces_as_error() {
        let inner = MemStorage::new();
        let schedule = FaultSchedule {
            fail_sync_entries_at: Some(1),
            ..Default::default()
        };
        let mut faulty = FaultyStorage::new(inner, schedule);
        faulty.append(&[entry(1, 1, b"x")]).unwrap();
        let result = faulty.sync_entries();
        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::Unrecoverable { .. })));
        // The op was logged.
        assert_eq!(faulty.ops().len(), 2); // append + sync_entries
        assert_eq!(faulty.ops()[1].kind, OpKind::SyncEntries);
        assert_eq!(faulty.sync_entries_count(), 1);
    }

    #[test]
    fn injected_hard_state_failure_surfaces_as_error() {
        let inner = MemStorage::new();
        let schedule = FaultSchedule {
            fail_set_hard_state_at: Some(1),
            ..Default::default()
        };
        let mut faulty = FaultyStorage::new(inner, schedule);
        let result = faulty.set_hard_state(&HardState {
            term: 1,
            vote: Some(1),
            commit: 0,
        });
        assert!(result.is_err());
        assert_eq!(faulty.set_hard_state_count(), 1);
    }

    #[test]
    fn successful_run_advances_and_logs() {
        let inner = MemStorage::new();
        let faulty = FaultyStorage::new(inner, FaultSchedule::default());
        let mut faulty = faulty;
        faulty.append(&[entry(1, 1, b"a")]).unwrap();
        faulty.sync_entries().unwrap();
        faulty.set_hard_state(&HardState {
            term: 1,
            vote: Some(1),
            commit: 1,
        }).unwrap();
        assert_eq!(faulty.sync_entries_count(), 1);
        assert_eq!(faulty.set_hard_state_count(), 1);
        assert_eq!(faulty.ops().len(), 3);
        assert_eq!(faulty.ops()[0].kind, OpKind::Append);
        assert_eq!(faulty.ops()[1].kind, OpKind::SyncEntries);
        assert_eq!(faulty.ops()[2].kind, OpKind::SetHardState);
    }

    #[test]
    fn second_sync_succeeds_after_first_failure() {
        let inner = MemStorage::new();
        let schedule = FaultSchedule {
            fail_sync_entries_at: Some(1),
            ..Default::default()
        };
        let mut faulty = FaultyStorage::new(inner, schedule);
        faulty.append(&[entry(1, 1, b"x")]).unwrap();
        assert!(faulty.sync_entries().is_err());
        // Second call succeeds.
        assert!(faulty.sync_entries().is_ok());
        assert_eq!(faulty.sync_entries_count(), 2);
    }
    #[test]
    fn injected_append_failure_surfaces_as_error() {
        let inner = MemStorage::new();
        let schedule = FaultSchedule {
            fail_append_at: Some(1),
            ..Default::default()
        };
        let mut faulty = FaultyStorage::new(inner, schedule);
        assert!(faulty.append(&[entry(1, 1, b"x")]).is_err());
        assert_eq!(faulty.append_count(), 1);
        // The next append succeeds (deterministic schedule).
        assert!(faulty.append(&[entry(1, 1, b"x")]).is_ok());
        assert_eq!(faulty.append_count(), 2);
    }

    #[test]
    fn periodic_sync_failure_fires_every_nth_call() {
        let inner = MemStorage::new();
        let schedule = FaultSchedule {
            fail_sync_entries_every: Some(2),
            ..Default::default()
        };
        let mut faulty = FaultyStorage::new(inner, schedule);
        assert!(faulty.sync_entries().is_ok(), "1st call (1 % 2 != 0) succeeds");
        assert!(faulty.sync_entries().is_err(), "2nd call fails");
        assert!(faulty.sync_entries().is_ok(), "3rd call succeeds");
        assert!(faulty.sync_entries().is_err(), "4th call fails");
        assert_eq!(faulty.sync_entries_count(), 4);
    }

    /// Only *successful* HardStates reach the ledger — a failed fsync must not
    /// be recorded as durable (INV1's negative half at the storage layer).
    #[test]
    fn ledger_records_only_successful_hard_states() {
        let inner = MemStorage::new();
        let ledger = Arc::new(DurabilityLedger::new());
        let schedule = FaultSchedule {
            fail_set_hard_state_at: Some(2),
            ..Default::default()
        };
        let mut faulty = FaultyStorage::with_ledger(inner, schedule, ledger.clone());

        faulty.set_hard_state(&HardState { term: 1, vote: Some(1), commit: 0 }).unwrap();
        assert!(faulty.set_hard_state(&HardState { term: 2, vote: Some(1), commit: 0 }).is_err());
        faulty.set_hard_state(&HardState { term: 3, vote: Some(2), commit: 4 }).unwrap();

        let states = ledger.hard_states();
        assert_eq!(states.len(), 2, "the injected failure must not be recorded");
        assert_eq!(states[0].term, 1);
        assert_eq!(states[1].term, 3);
        assert_eq!(ledger.max_persisted_term(), 3);
        assert_eq!(ledger.persisted_commit(), 4);
    }
}
