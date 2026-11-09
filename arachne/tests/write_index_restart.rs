//! Phase 4: the effective index and session dedup survive a **real disk
//! snapshot** and a restart.
//!
//! Phase 2 proved the in-memory snapshot v3 round-trip. This drives the durable
//! path through the real actor: a write's original index is captured, enough
//! data is written to trigger a local snapshot (so the original entry is
//! covered by the snapshot and can be compacted out of the WAL), the runtime is
//! aborted, and a new runtime is opened on the same data dir.
//!
//! After restart:
//! * `get_stale_with_index` still reports the original index for the key (the
//!   state, including the session record's index, came from the disk snapshot);
//! * replaying the same `(client_id, seq_no)` returns the **original** index and
//!   does not re-apply (the v3 session record survived the snapshot);
//! * a fresh write after restart has a greater index.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time imports there), so this file uses
//! `core::time::Duration`, never `std::time`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::state_machine::KvStateMachine;
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{ArachneError, Metrics, NodeId, Profile, ProfileConfig, TransportFactory};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

/// Small segments so compaction can actually drop the original entry.
const SEGMENT_BYTES: u64 = 8 * 1024;
/// Small trigger: a handful of ~200 B writes crosses it.
const SNAPSHOT_THRESHOLD_BYTES: u64 = 4 * 1024;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-write-index-restart-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 5,
        election_timeout_ms: 100,
        rpc_timeout_ms: 50,
        snapshot_threshold_bytes: SNAPSHOT_THRESHOLD_BYTES,
        ..Profile::Lan.config()
    }
}

fn wal_opts() -> WalOptions {
    WalOptions {
        cluster_id: "write-index-restart".into(),
        node_id: "n1".into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: SEGMENT_BYTES,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// Open the WAL, waiting for a killed predecessor's data-dir lock to release.
async fn open_wal(dir: &Path) -> WalStorage {
    let mut last = String::new();
    for _ in 0..300 {
        match WalStorage::open(dir, wal_opts()) {
            Ok(wal) => return wal,
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(10)).await;
            }
        }
    }
    panic!("could not reopen the WAL: {last}");
}

/// Start (or restart) the single node on `dir` and wait until it elects itself.
async fn spawn(dir: &Path) -> (tokio::task::JoinHandle<()>, Handle, Arc<Metrics>) {
    let wal = open_wal(dir).await;
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(NodeId::from("n1"));
    let metrics = Arc::new(Metrics::new());
    let profile = profile();
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from("n1"),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: RaftNodeConfig::from_profile(&profile),
        profile,
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
    let task = tokio::spawn(runtime.run());
    for _ in 0..400 {
        if metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(metrics.is_leader(), "single node must elect itself");
    (task, handle, metrics)
}

/// Snapshot indexes present in a data directory.
fn snapshot_files(dir: &Path) -> Vec<u64> {
    let mut found: Vec<u64> = std::fs::read_dir(dir)
        .expect("read data dir")
        .flatten()
        .filter_map(|entry| {
            arachne_kv::storage::snapshot::parse_snapshot_file_name(
                &entry.file_name().to_string_lossy(),
            )
        })
        .map(|(index, _term)| index)
        .collect();
    found.sort_unstable();
    found
}

/// Wait until a durable snapshot strictly newer than `min_index` exists (and
/// its creation has been recorded, implying the metadata pointer was saved).
async fn wait_for_snapshot(dir: &Path, metrics: &Metrics, min_index: u64) -> u64 {
    for _ in 0..1000 {
        if metrics.snapshots_created_total() > 0
            && let Some(&newest) = snapshot_files(dir).last()
            && newest > min_index
        {
            return newest;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    panic!(
        "no durable snapshot newer than the anchor index {min_index} appeared \
         (created={})",
        metrics.snapshots_created_total()
    );
}

/// The original effective index and the session dedup record must both survive
/// a disk snapshot + restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_index_survives_snapshot_and_restart() {
    const CLIENT: u64 = 1_234;
    const SEQ: u64 = 99;

    let dir = temp_dir();
    let (task, handle, metrics) = spawn(&dir).await;

    // The anchor: an explicit session whose original index we will track.
    let anchor = handle
        .propose_raw_with_index(
            KvStateMachine::encode_put(CLIENT, SEQ, b"anchor", b"v1"),
            CLIENT,
            SEQ,
        )
        .await
        .expect("anchor write");
    assert!(anchor >= 1, "a present value never reports index 0");

    // Write enough data to cross the snapshot trigger (each value ~200 B).
    let filler = vec![0xABu8; 200];
    for i in 0..128u64 {
        let key = format!("fill{i:03}").into_bytes();
        handle.put(&key, &filler).await.expect("filler write");
    }
    let snapshot = wait_for_snapshot(&dir, &metrics, anchor).await;
    assert!(
        snapshot > anchor,
        "the snapshot must cover the anchor entry ({snapshot} > {anchor})"
    );

    // Stop the actor and reopen the same data dir.
    task.abort();
    let _ = task.await;
    drop(handle);
    let (task, handle, metrics) = spawn(&dir).await;
    assert!(
        metrics.applied_index() >= snapshot,
        "the restarted node must resume from the snapshot at {snapshot}, got {}",
        metrics.applied_index()
    );

    // The value and its origin came from the disk snapshot (the WAL no longer
    // needs the original entry: raft resumes at the snapshot index).
    let (value, origin) = handle
        .get_stale_with_index(b"anchor")
        .await
        .expect("stale read after restart")
        .expect("the anchor must be restored from the snapshot");
    assert_eq!(value, b"v1".to_vec(), "the restored value is intact");
    assert_eq!(
        origin, anchor,
        "the original effective index survived the disk snapshot"
    );

    // The v4 digest survived the disk snapshot: replaying the same session with
    // a DIFFERENT payload is rejected as a session conflict (never deduped into
    // a stale result), and the restored value is untouched.
    match handle
        .propose_raw_with_index(
            KvStateMachine::encode_put(CLIENT, SEQ, b"anchor", b"v2"),
            CLIENT,
            SEQ,
        )
        .await
    {
        Err(ArachneError::SessionConflict { client_id, seq_no }) => {
            assert_eq!(client_id, CLIENT);
            assert_eq!(seq_no, SEQ);
        }
        other => panic!("expected SessionConflict after restart, got {other:?}"),
    }
    assert_eq!(
        handle.get_stale(b"anchor").await.expect("stale read"),
        Some(b"v1".to_vec()),
        "a conflicting replay must not re-apply its payload"
    );

    // Replaying the SAME payload is deduped and reports the ORIGINAL index (the
    // recorded digest matches, so this is a genuine idempotent retry).
    let replay = handle
        .propose_raw_with_index(
            KvStateMachine::encode_put(CLIENT, SEQ, b"anchor", b"v1"),
            CLIENT,
            SEQ,
        )
        .await
        .expect("same-payload replay after restart");
    assert_eq!(
        replay, anchor,
        "the v4 session record survived the disk snapshot (replay reports the original index)"
    );

    // A fresh write after restart is ordered strictly after the original.
    let fresh = handle
        .put_with_index(b"after", b"restart")
        .await
        .expect("fresh write after restart");
    assert!(
        fresh > anchor,
        "a fresh write after restart ({fresh}) must exceed the original index ({anchor})"
    );

    task.abort();
    let _ = task.await;
    let _ = std::fs::remove_dir_all(&dir);
}
