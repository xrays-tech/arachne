//! T6 storage conformance suite: run against a real `WalStorage` and
//! `FaultyStorage<WalStorage>` (proving transparency of the fault wrapper).
//!
//! See `arachne-testsupport::store_suite` for the check definitions.

use std::sync::atomic::{AtomicU64, Ordering};

use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_testsupport::{FaultSchedule, FaultyStorage, run_storage_suite};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Create a unique base temp directory for the test. The caller must clean it
/// up (removes all subdirs created by the suite).
fn base_dir() -> std::path::PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-suite-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn wal_opts() -> WalOptions {
    WalOptions {
        cluster_id: "suite".into(),
        node_id: "n1".into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1024 * 1024, // 1 MiB: no rollover during the suite.
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// A counter shared with the `fresh` closure to create unique subdirs.
struct DirMaker {
    base: std::path::PathBuf,
    next: std::cell::Cell<u64>,
}

impl DirMaker {
    fn new(base: std::path::PathBuf) -> Self {
        Self {
            base,
            next: std::cell::Cell::new(0),
        }
    }

    fn next_dir(&self) -> std::path::PathBuf {
        let n = self.next.get();
        self.next.set(n + 1);
        let dir = self.base.join(format!("check-{n}"));
        std::fs::create_dir_all(&dir).expect("failed to create subdir");
        dir
    }
}

/// Run the suite against a real `WalStorage` (fresh temp dir per check).
#[test]
fn wal_storage_passes_suite() {
    let base = base_dir();
    let maker = DirMaker::new(base.clone());

    let report = run_storage_suite("WalStorage", || {
        let dir = maker.next_dir();
        WalStorage::open(&dir, wal_opts()).expect("open failed")
    });

    assert!(
        report.all_passed(),
        "WalStorage failed T6 suite; failures: {:?}",
        report.failures()
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// Run the suite against `FaultyStorage<WalStorage>` with a no-fault schedule.
/// This proves the fault wrapper is transparent (delegates correctly).
#[test]
fn faulty_storage_transparent_passes_suite() {
    let base = base_dir();
    let maker = DirMaker::new(base.clone());

    let report = run_storage_suite("FaultyStorage<WalStorage>", || {
        let dir = maker.next_dir();
        let inner = WalStorage::open(&dir, wal_opts()).expect("open failed");
        FaultyStorage::new(inner, FaultSchedule::default())
    });

    assert!(
        report.all_passed(),
        "FaultyStorage (no faults) failed T6 suite; failures: {:?}",
        report.failures()
    );
    let _ = std::fs::remove_dir_all(&base);
}

/// The suite report contains exactly 7 checks.
#[test]
fn suite_has_seven_checks() {
    let base = base_dir();
    let maker = DirMaker::new(base.clone());

    let report = run_storage_suite("count", || {
        let dir = maker.next_dir();
        WalStorage::open(&dir, wal_opts()).expect("open failed")
    });

    assert_eq!(report.checks.len(), 7);
    assert!(report.all_passed());
    let _ = std::fs::remove_dir_all(&base);
}
