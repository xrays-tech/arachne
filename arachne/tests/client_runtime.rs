//! Integration: the node runtime actor + client `Handle` over the in-memory
//! transport (single node).
//!
//! Proves the read/write path end to end: `Handle::put` proposes through the
//! actor and waits for commit+apply; `Handle::get` is a ReadIndex linearizable
//! read (propsol §5.4) and `get_stale` a local read, both of which observe the
//! committed write.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{Metrics, NodeId, Profile, ProfileConfig};
use arachne_testsupport::InMemoryTransportFactory;
use slog::Drain;

static DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let n = DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-client-rt-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn single_node_put_then_read() {
    let dir = temp_dir();
    let wal = WalStorage::open(
        &dir,
        WalOptions {
            cluster_id: "t".into(),
            node_id: "n1".into(),
            config: WalConfig {
                fsync_policy: FsyncPolicy::Always,
                segment_bytes: WalConfig::default().segment_bytes,
            },
            created_at_millis: 0,
            fsync_observer: None,
        },
    )
    .expect("open wal");

    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = {
        use arachne::TransportFactory;
        factory.create(NodeId::from("n1"))
    };

    let metrics = Arc::new(Metrics::new());
    // Fast timings so the single node elects itself quickly in the test.
    let profile = ProfileConfig {
        heartbeat_interval_ms: 5,
        election_timeout_ms: 100,
        rpc_timeout_ms: 50,
        ..Profile::Lan.config()
    };
    let raft = RaftNodeConfig::from_profile(&profile);
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from("n1"),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft,
        profile: profile.clone(),
        metrics: Arc::clone(&metrics),
    };
    let logger = slog::Logger::root(slog::Discard.fuse(), slog::o!());
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger).expect("build runtime");
    let task = tokio::spawn(runtime.run());

    // Wait for self-election (single node, no peers).
    for _ in 0..400 {
        if metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(metrics.is_leader(), "single node must elect itself");

    handle.put(b"k", b"v").await.expect("put");
    assert_eq!(
        handle.get_stale(b"k").await.expect("get_stale"),
        Some(b"v".to_vec())
    );
    assert_eq!(handle.get(b"k").await.expect("get"), Some(b"v".to_vec()));

    // A write over the profile limit is rejected before proposing.
    let big = vec![0u8; (profile.max_value_bytes as usize) + 1];
    assert!(matches!(
        handle.put(b"k", &big).await,
        Err(arachne::ArachneError::InvalidArgument(_))
    ));

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
