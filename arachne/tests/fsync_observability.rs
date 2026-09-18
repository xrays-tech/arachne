//! Integration test: per-segment fsync observability.
//!
//! Builds a real `WalStorage` with a `FsyncLedger` observer, appends a batch
//! straddling a rollover, calls `sync_entries`, and asserts the ledger's
//! `union_covers` is true (every segment was fsynced).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::storage::{WalConfig, WalOptions, WalStorage, FsyncPolicy};
use arachne::{EntryType, FsyncObserver, LogEntry, Storage};
use arachne_testsupport::FsyncLedger;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-integration-{}-{}",
        std::process::id(),
        n
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn make_entry(index: u64, term: u64, data: &[u8]) -> LogEntry {
    LogEntry {
        index,
        term,
        entry_type: EntryType::Entry,
        data: data.to_vec(),
    }
}

#[test]
fn straddling_rollover_ledger_covers_all() {
    let dir = temp_dir();
    let ledger = Arc::new(FsyncLedger::new());

    // Tiny segment to force rollover within a single batch.
    let opts = WalOptions {
        cluster_id: "test".into(),
        node_id: "n1".into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 60,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: Some(ledger.clone()),
    };

    {
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        // Append 4 entries in one batch. With segment_bytes=60, the batch
        // straddles a rollover.
        storage
            .append(&[
                make_entry(1, 1, b"aaaa"),
                make_entry(2, 1, b"bbbb"),
                make_entry(3, 1, b"cccc"),
                make_entry(4, 1, b"dddd"),
            ])
            .unwrap();
        // sync_entries is the durability barrier: it must fsync ALL segments
        // (including the outgoing one from the rollover).
        storage.sync_entries().unwrap();
    }

    // The ledger must cover the full range [1, 4].
    assert!(
        ledger.union_covers(1, 4),
        "ledger does not cover [1, 4]; events: {:?}",
        ledger.events()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn missing_segment_event_detected() {
    // Negative control: if we only record events for segment 1 but NOT for
    // segment 2, union_covers must return false.
    let ledger = FsyncLedger::new();
    ledger.on_segment_fsynced(1, 2);
    // Segment 3-4 is missing.
    assert!(!ledger.union_covers(1, 4));
    // But the covered range is correct.
    assert!(ledger.union_covers(1, 2));
}

/// F4: restart-reconciliation test — validates that the ledger's claimed
/// durable ranges correspond to real on-disk durability.
///
/// Build a `WalStorage` with an `FsyncLedger` observer, append + sync, record
/// the ledger's claimed durable ranges, **drop** the storage, **reopen** it,
/// and assert every log index the ledger claimed durable is actually present
/// on disk with identical content.
#[test]
fn restart_reconciliation_validates_ledger() {
    let dir = temp_dir();
    let ledger = Arc::new(FsyncLedger::new());

    let opts = WalOptions {
        cluster_id: "recon".into(),
        node_id: "n1".into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 60, // tiny → forces rollover
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: Some(ledger.clone()),
    };

    // Original entries for content verification.
    let original_entries: Vec<LogEntry> = (1..=6u64)
        .map(|i| make_entry(i, 1, &[b'x'; 4]))
        .collect();

    {
        let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
        storage.append(&original_entries).unwrap();
        storage.sync_entries().unwrap();
    }

    // Record what the ledger claims is durable.
    let events = ledger.events();
    assert!(!events.is_empty(), "ledger must have recorded fsync events");
    let max_durable = events
        .iter()
        .map(|e| e.durable_through_index)
        .max()
        .unwrap_or(0);
    assert_eq!(
        max_durable, 6,
        "ledger claims durable_through={max_durable}, expected 6"
    );

    // Reopen: every index the ledger claimed durable must be present with
    // identical content.
    let storage = WalStorage::open(&dir, opts).unwrap();
    for i in 1..=max_durable {
        let entries = storage.entries(i, i + 1, None).unwrap();
        assert_eq!(entries.len(), 1, "entry {i} missing after reopen");
        assert_eq!(
            entries[0], original_entries[(i - 1) as usize],
            "entry {i} content mismatch after reopen"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
