//! Gate-6 MF-A: regression protection for the `client_id` seq-continuation.
//!
//! The P0 fix has two load-bearing halves: a node-scoped `client_id`, and an
//! `initial_seq` that starts **above** every session that id already committed.
//! In a normal test process two `Runtime::new`s get different client ids, so
//! the seq-continuation half is never exercised — deleting it would still pass.
//!
//! This test pins the client id with the `fault-injection`-only override and
//! restarts on the same data dir twice:
//! * **snapshot variant**: a small snapshot threshold puts the old sessions in
//!   a durable snapshot before the restart;
//! * **tail variant**: a large threshold leaves them only in the WAL tail (the
//!   part the actor replays after `Runtime::new`), exercising the tail scan.
//!
//! Without the seq continuation the restarted handle starts at seq 1, collides
//! with the old session 1, is silently deduped, and the new key never appears.
//!
//! Run:
//! `cargo test -p arachne-kv --features fault-injection --test write_index_client_id`
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time imports there), so this file uses
//! `core::time::Duration`, never `std::time`.

#![cfg(feature = "fault-injection")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig, set_client_id_override};
use arachne_kv::state_machine::KvStateMachine;
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{Metrics, NodeId, Profile, ProfileConfig, TransportFactory};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

/// The fixed client id the test pins across both runtimes.
const FIXED_CLIENT_ID: u64 = 42;
/// Filler sessions use a separate client so they cannot collide with 42's seqs.
const FILLER_CLIENT_ID: u64 = 1000;
/// Small enough that the filler writes trigger a snapshot quickly.
const SMALL_SNAPSHOT_THRESHOLD: u64 = 4 * 1024;
/// Large enough that no snapshot exists for the tail variant.
const LARGE_SNAPSHOT_THRESHOLD: u64 = 64 * 1024 * 1024;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-write-index-client-id-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn wal_opts() -> WalOptions {
    WalOptions {
        cluster_id: "write-index-client-id".into(),
        node_id: "n1".into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: WalConfig::default().segment_bytes,
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

/// Start (or restart) the single node on `dir` and wait until it self-elects.
async fn spawn(
    dir: &Path,
    snapshot_threshold_bytes: u64,
) -> (tokio::task::JoinHandle<()>, Handle, Arc<Metrics>) {
    let wal = open_wal(dir).await;
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(NodeId::from("n1"));
    let metrics = Arc::new(Metrics::new());
    let profile = ProfileConfig {
        heartbeat_interval_ms: 5,
        election_timeout_ms: 100,
        rpc_timeout_ms: 50,
        snapshot_threshold_bytes,
        ..Profile::Lan.config()
    };
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

/// Snapshot indexes present in the data dir.
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

/// Wait until a durable snapshot at or above `min_index` exists (its creation
/// being recorded implies the metadata pointer was saved).
async fn wait_for_snapshot(dir: &Path, metrics: &Metrics, min_index: u64) -> u64 {
    for _ in 0..1000 {
        if metrics.snapshots_created_total() > 0
            && let Some(&newest) = snapshot_files(dir).last()
            && newest >= min_index
        {
            return newest;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    panic!("no durable snapshot at or above index {min_index} appeared");
}

/// One restart scenario. `expect_snapshot` selects the snapshot vs WAL-tail
/// coverage of the old sessions.
async fn scenario(tag: &str, snapshot_threshold_bytes: u64, expect_snapshot: bool) {
    // Must be set before the first runtime is constructed.
    set_client_id_override(Some(FIXED_CLIENT_ID));

    let dir = temp_dir(tag);
    let (task, handle, metrics) = spawn(&dir, snapshot_threshold_bytes).await;

    // Seven old sessions under the pinned client id. `propose_raw_with_index`
    // bypasses the handle's own sequence, so these are exactly seqs 1..=7.
    let mut max_old_index = 0u64;
    for seq in 1..=7u64 {
        let key = format!("old{seq}");
        let value = format!("val{seq}");
        let index = handle
            .propose_raw_with_index(
                KvStateMachine::encode_put(FIXED_CLIENT_ID, seq, key.as_bytes(), value.as_bytes()),
                FIXED_CLIENT_ID,
                seq,
            )
            .await
            .expect("old session must apply");
        max_old_index = max_old_index.max(index);
    }
    assert!(max_old_index >= 1);

    if expect_snapshot {
        // Filler under a different client id: it triggers the snapshot without
        // colliding with client 42's sequences.
        for seq in 1..=64u64 {
            let key = format!("fill{seq:02}");
            let value = vec![0xABu8; 200];
            handle
                .propose_raw_with_index(
                    KvStateMachine::encode_put(
                        FILLER_CLIENT_ID,
                        seq,
                        key.as_bytes(),
                        &value,
                    ),
                    FILLER_CLIENT_ID,
                    seq,
                )
                .await
                .expect("filler session must apply");
        }
        let snapshot = wait_for_snapshot(&dir, &metrics, max_old_index).await;
        assert!(
            snapshot >= max_old_index,
            "the snapshot must cover the old sessions ({snapshot} >= {max_old_index})"
        );
    } else {
        assert!(
            snapshot_files(&dir).is_empty(),
            "the tail variant must not take a snapshot"
        );
    }

    // Restart on the same dir with the override still pinned.
    task.abort();
    let _ = task.await;
    drop(handle);
    let (task, handle, _metrics) = spawn(&dir, snapshot_threshold_bytes).await;

    // The public write uses the handle's restored sequence. Without the
    // continuation it is 1 and collides with old session 1, so this returns the
    // old index and `new-key` is never applied.
    let new_index = handle
        .put_with_index(b"new-key", b"new-value")
        .await
        .expect("the first write after restart must apply");
    assert!(
        new_index > max_old_index,
        "restart reused an old session: new index {new_index} <= old max {max_old_index}"
    );
    assert_eq!(
        handle.get_stale(b"new-key").await.expect("stale read"),
        Some(b"new-value".to_vec()),
        "the new value must be applied, not silently deduped"
    );

    task.abort();
    let _ = task.await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A single test so the process-global override cannot race another runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_with_fixed_client_id_continues_its_sequence() {
    scenario("snapshot", SMALL_SNAPSHOT_THRESHOLD, true).await;
    scenario("tail", LARGE_SNAPSHOT_THRESHOLD, false).await;
    set_client_id_override(None);
}
