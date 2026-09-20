//! A per-node **durability ledger**: what a node has actually made durable.
//!
//! INV1 has two halves (test-plan §7):
//!
//! * **I2/I4** — no log entry is sent before it is fsynced. The entry half comes
//!   from the WAL's [`FsyncObserver`] events (delegated to an embedded
//!   [`FsyncLedger`]).
//! * **I1** — no term/vote change is propagated before its `HardState` is
//!   fsynced. The `Storage` seam has no HardState-fsync callback, but
//!   [`FaultyStorage`](crate::FaultyStorage) sits *between* raft and the WAL and
//!   therefore observes every `set_hard_state` call; `WalStorage::set_hard_state`
//!   fsyncs before returning (I1), so a recorded call *is* a durable HardState.
//!
//! A test can therefore reconcile **every outbound message** against one
//! ledger: its carried entries must be covered by the entry watermark, and its
//! `term` must not exceed the highest persisted term.
//!
//! The ledger is deliberately independent of the node's own bookkeeping — it is
//! fed by the storage wrapper and the WAL observer, not by the code under test.

use std::sync::Mutex;

use arachne_seam::RaftId;
use arachne_seam::storage::FsyncObserver;
use arachne_seam::types::{LogIndex, Term};

use crate::fsync_ledger::{FsyncEvent, FsyncLedger};

/// One `set_hard_state` that returned successfully (hence fsynced, I1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PersistedHardState {
    /// Monotonically increasing sequence number (1-based).
    pub seq: u64,
    /// The persisted term.
    pub term: Term,
    /// The persisted vote, if any.
    pub vote: Option<RaftId>,
    /// The persisted commit index.
    pub commit: LogIndex,
}

/// What one node has made durable: entry fsyncs plus persisted HardStates.
#[derive(Default)]
pub struct DurabilityLedger {
    entries: FsyncLedger,
    hard_states: Mutex<Vec<PersistedHardState>>,
}

impl DurabilityLedger {
    /// Create an empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    // ---- entry durability (I2/I4) ------------------------------------------

    /// Whether the recorded entry fsyncs cover `[first_index, last_index]`.
    pub fn entries_cover(&self, first_index: LogIndex, last_index: LogIndex) -> bool {
        self.entries.union_covers(first_index, last_index)
    }

    /// A snapshot of the recorded entry-fsync events.
    pub fn entry_events(&self) -> Vec<FsyncEvent> {
        self.entries.events()
    }

    // ---- HardState durability (I1) -----------------------------------------

    /// Record a successfully persisted HardState. Called by the storage wrapper
    /// for every `set_hard_state` that returned `Ok` (I1: the WAL fsyncs inside).
    pub fn record_hard_state(&self, term: Term, vote: Option<RaftId>, commit: LogIndex) {
        let mut states = self.hard_states.lock().expect("ledger mutex poisoned");
        let seq = states.len() as u64 + 1;
        states.push(PersistedHardState {
            seq,
            term,
            vote,
            commit,
        });
    }

    /// The highest term this node has durably persisted (0 if none yet).
    ///
    /// INV1 (I1): no message may carry a term higher than this.
    pub fn max_persisted_term(&self) -> Term {
        self.hard_states
            .lock()
            .expect("ledger mutex poisoned")
            .iter()
            .map(|s| s.term)
            .max()
            .unwrap_or(0)
    }

    /// The highest commit index this node has durably persisted.
    pub fn persisted_commit(&self) -> LogIndex {
        self.hard_states
            .lock()
            .expect("ledger mutex poisoned")
            .iter()
            .map(|s| s.commit)
            .max()
            .unwrap_or(0)
    }

    /// A snapshot of the recorded HardState persists.
    pub fn hard_states(&self) -> Vec<PersistedHardState> {
        self.hard_states
            .lock()
            .expect("ledger mutex poisoned")
            .clone()
    }

    /// How many HardStates have been persisted.
    pub fn hard_state_count(&self) -> u64 {
        self.hard_states
            .lock()
            .expect("ledger mutex poisoned")
            .len() as u64
    }
}

impl FsyncObserver for DurabilityLedger {
    fn on_segment_fsynced(&self, segment_first_index: LogIndex, durable_through_index: LogIndex) {
        self.entries
            .on_segment_fsynced(segment_first_index, durable_through_index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_durability_is_delegated() {
        let ledger = DurabilityLedger::new();
        assert!(!ledger.entries_cover(1, 1));
        ledger.on_segment_fsynced(1, 5);
        assert!(ledger.entries_cover(1, 5));
        assert!(!ledger.entries_cover(1, 6));
        assert_eq!(ledger.entry_events().len(), 1);
    }

    #[test]
    fn hard_state_watermarks_track_the_maximum() {
        let ledger = DurabilityLedger::new();
        assert_eq!(ledger.max_persisted_term(), 0);
        ledger.record_hard_state(1, Some(1), 0);
        ledger.record_hard_state(3, Some(2), 7);
        ledger.record_hard_state(2, None, 4);
        assert_eq!(ledger.max_persisted_term(), 3);
        assert_eq!(ledger.persisted_commit(), 7);
        assert_eq!(ledger.hard_state_count(), 3);
        assert_eq!(ledger.hard_states()[0].seq, 1);
        assert_eq!(ledger.hard_states()[1].vote, Some(2));
    }
}
