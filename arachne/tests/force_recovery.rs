//! Integration: force-recovery against a WAL written by the **real runtime
//! actor**.
//!
//! The point of force-recovery is "reset this node to a single-voter cluster at
//! its committed (== applied) point". That is only possible if the durable WAL
//! actually records the committed index. This test writes through the runtime,
//! stops it, and asserts `WalStorage::force_recovery` recovers the committed
//! point (and therefore discards nothing).
//!
//! NOTE (entropy gates): `tests/` dirs are scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time/network imports there), so this file uses
//! `core::time::Duration` and no tokio select macros.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{Metrics, NodeId, Profile, ProfileConfig};
use arachne_testsupport::InMemoryTransportFactory;
use slog::Drain;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-force-recovery-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn wal_opts(node: &str, cluster: &str) -> WalOptions {
    WalOptions {
        cluster_id: cluster.into(),
        node_id: node.into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: WalConfig::default().segment_bytes,
        },
        created_at_millis: 0,
        fsync_observer: None,
    }
}

/// The durable committed index must be recoverable after the runtime commits a
/// write — otherwise force-recovery cannot know the recovery point and would
/// discard committed data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_persists_the_committed_index_for_force_recovery() {
    let dir = temp_dir("commit");
    let cluster = "force-recovery-it";
    let wal = WalStorage::open(&dir, wal_opts("n1", cluster)).expect("open wal");

    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = {
        use arachne::TransportFactory;
        factory.create(NodeId::from("n1"))
    };
    let metrics = Arc::new(Metrics::new());
    let profile = ProfileConfig {
        heartbeat_interval_ms: 5,
        election_timeout_ms: 100,
        rpc_timeout_ms: 50,
        ..Profile::Lan.config()
    };
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from("n1"),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: RaftNodeConfig::from_profile(&profile),
        profile: profile.clone(),
        metrics: Arc::clone(&metrics),
    };
    let logger = slog::Logger::root(slog::Discard.fuse(), slog::o!());
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger).expect("build runtime");
    let task = tokio::spawn(runtime.run());

    // Wait for self-election, then commit+apply one write.
    for _ in 0..400 {
        if metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(metrics.is_leader(), "the single node must elect itself");
    handle.put(b"k", b"v").await.expect("put must commit+apply");
    // A second write forces a *second* commit advance, so the test covers
    // repeated advances (not just the first one).
    handle.put(b"k2", b"v2").await.expect("second put must commit+apply");

    // Stop the actor: it owns the WAL, so this releases the data-dir lock.
    task.abort();
    let _ = task.await;

    let report = WalStorage::force_recovery(
        &dir,
        "n1",
        Some("force-recovery-it-rotated".into()),
        WalConfig::default(),
        1_700_000_000_000,
    )
    .expect("force-recovery must succeed");

    assert!(
        report.commit >= 2,
        "every committed index must be durable before force-recovery (got commit={}, \
         discarded={}); a stale commit would discard committed data",
        report.commit,
        report.discarded_entries
    );
    assert_eq!(
        report.discarded_entries, 0,
        "a fully committed log must not be truncated by force-recovery"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
