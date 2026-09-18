//! A deterministic per-segment fsync ledger for verifying WAL durability.
//!
//! [`FsyncLedger`] implements [`FsyncObserver`](arachne_seam::FsyncObserver)
//! and records every segment fsync event. The key verification property is
//! [`union_covers`](FsyncLedger::union_covers): the union of all recorded
//! segment ranges must cover `[first_index, last_index]` for the WAL to be
//! considered fully durable.
//!
//! This is the verification machinery that would have caught the N2 bug:
//! if a segment's fsync is missed (e.g., a rolled-over segment that was never
//! fsynced), `union_covers` returns `false`.

use std::sync::Mutex;

use arachne_seam::storage::FsyncObserver;
use arachne_seam::types::LogIndex;

/// A single recorded fsync event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FsyncEvent {
    /// Monotonically increasing sequence number (1-based).
    pub seq: u64,
    /// The first log index of the fsynced segment (from its filename).
    pub segment_first_index: LogIndex,
    /// The highest log index whose bytes in that segment are durable.
    pub durable_through_index: LogIndex,
}

/// A thread-safe ledger that records per-segment fsync events.
///
/// Implements [`FsyncObserver`] so it can be passed to a [`WalStorage`] via
/// `WalOptions::fsync_observer`.
#[derive(Default)]
pub struct FsyncLedger {
    events: Mutex<Vec<FsyncEvent>>,
}

impl FsyncLedger {
    /// Create an empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a snapshot of all recorded events.
    pub fn events(&self) -> Vec<FsyncEvent> {
        self.events.lock().expect("ledger mutex poisoned").clone()
    }

    /// Check whether the union of all recorded segment ranges covers
    /// `[first_index, last_index]`.
    ///
    /// Returns `true` iff for every index `i` in `[first_index, last_index]`,
    /// there exists at least one recorded event where
    /// `event.segment_first_index <= i <= event.durable_through_index`.
    ///
    /// This is the property that would have caught N2: if a rolled-over
    /// segment was never fsynced, its range is missing from the union.
    pub fn union_covers(&self, first_index: LogIndex, last_index: LogIndex) -> bool {
        if first_index > last_index {
            return true;
        }
        let events = self.events.lock().expect("ledger mutex poisoned");
        // Build the union of covered ranges.
        let mut covered: Vec<(LogIndex, LogIndex)> = Vec::new();
        for e in events.iter() {
            if e.durable_through_index < e.segment_first_index {
                continue;
            }
            covered.push((e.segment_first_index, e.durable_through_index));
        }
        // Sort by start for efficient coverage check.
        covered.sort();
        // Walk through the range [first_index, last_index] and check that
        // every point is covered by at least one segment range.
        let mut pos = first_index;
        for (start, end) in &covered {
            if *end < pos {
                continue;
            }
            if *start > pos {
                // Gap: the next covered range starts after where we need
                // coverage → not fully covered.
                return false;
            }
            if *end >= last_index {
                // This range covers the rest of the requested range.
                return true;
            }
            pos = end.saturating_add(1);
        }
        // If we exhausted all ranges and pos > last_index, we covered
        // everything. Otherwise there's a trailing gap.
        pos > last_index
    }
}

impl FsyncObserver for FsyncLedger {
    fn on_segment_fsynced(&self, segment_first_index: LogIndex, durable_through_index: LogIndex) {
        let mut events = self.events.lock().expect("ledger mutex poisoned");
        let seq = events.len() as u64 + 1;
        events.push(FsyncEvent {
            seq,
            segment_first_index,
            durable_through_index,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ledger_covers_empty_range() {
        let ledger = FsyncLedger::new();
        // Degenerate range: first > last → true.
        assert!(ledger.union_covers(5, 3));
    }

    #[test]
    fn single_event_covers_its_range() {
        let ledger = FsyncLedger::new();
        ledger.on_segment_fsynced(1, 10);
        assert!(ledger.union_covers(1, 10));
        assert!(ledger.union_covers(3, 7));
        // Range extending beyond the covered area → false.
        assert!(!ledger.union_covers(1, 11));
        assert!(!ledger.union_covers(11, 15));
    }

    #[test]
    fn multiple_events_union_covers() {
        let ledger = FsyncLedger::new();
        ledger.on_segment_fsynced(1, 5);
        ledger.on_segment_fsynced(6, 10);
        ledger.on_segment_fsynced(11, 15);
        assert!(ledger.union_covers(1, 15));
        assert!(ledger.union_covers(3, 12));
    }

    #[test]
    fn missing_middle_segment_detected() {
        // This is the N2 detection property: a gap in the middle means
        // union_covers must return false.
        let ledger = FsyncLedger::new();
        ledger.on_segment_fsynced(1, 5);
        // Segment 6-10 is MISSING (not fsynced).
        ledger.on_segment_fsynced(11, 15);
        // Full range 1..15 is NOT covered.
        assert!(!ledger.union_covers(1, 15));
        // But 1..5 IS covered.
        assert!(ledger.union_covers(1, 5));
        // And 11..15 IS covered.
        assert!(ledger.union_covers(11, 15));
        // 5..11 straddles the gap → not covered.
        assert!(!ledger.union_covers(5, 11));
    }

    #[test]
    fn overlapping_events_union() {
        let ledger = FsyncLedger::new();
        ledger.on_segment_fsynced(1, 10);
        ledger.on_segment_fsynced(5, 15); // overlaps
        assert!(ledger.union_covers(1, 15));
    }

    #[test]
    fn events_are_sequenced() {
        let ledger = FsyncLedger::new();
        ledger.on_segment_fsynced(1, 5);
        ledger.on_segment_fsynced(6, 10);
        let events = ledger.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].seq, 1);
        assert_eq!(events[1].seq, 2);
        assert_eq!(events[0].segment_first_index, 1);
        assert_eq!(events[1].segment_first_index, 6);
    }

    #[test]
    fn zero_durable_through_is_noop() {
        let ledger = FsyncLedger::new();
        // A segment with no entries (durable_through = 0) covers nothing.
        ledger.on_segment_fsynced(1, 0);
        assert!(!ledger.union_covers(1, 1));
    }
}
