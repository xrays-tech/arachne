//! Integration (M2): atomic multi-put + consistent prefix/range reads over the
//! in-memory transport (single node).
//!
//! Proves the M2 contracts end to end through the real actor + handle:
//! - `multi_put` sets a whole batch atomically under one session (J2: every
//!   key in one batch reports the **same** origin index via
//!   `get_stale_with_index`, and the batch is all-or-nothing at one applied
//!   watermark);
//! - `get_stale_prefix` returns a coherent segment + its applied index (J1: no
//!   torn mix across the range), including the hydra "head + entity" shape
//!   where the head key is the prefix's first key;
//! - over-limit batches are rejected before propose.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{Metrics, NodeId, Profile, ProfileConfig};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::Drain;

static DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let n = DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-m2-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

async fn single_node(
) -> (
    tokio::task::JoinHandle<()>,
    arachne_kv::client::Handle,
    Arc<Metrics>,
    std::path::PathBuf,
) {
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
        use arachne_kv::TransportFactory;
        factory.create(NodeId::from("n1"))
    };

    let metrics = Arc::new(Metrics::new());
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
        profile,
        metrics: Arc::clone(&metrics),
    };
    let logger = slog::Logger::root(slog::Discard.fuse(), slog::o!());
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger).expect("build runtime");
    let task = tokio::spawn(runtime.run());

    for _ in 0..400 {
        if metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(metrics.is_leader(), "single node must elect itself");

    (task, handle, metrics, dir)
}

/// A `multi_put` of a whole batch is atomic: every key lands with the **same**
/// origin index (J2 observable through `get_stale_with_index`).
#[tokio::test]
async fn multi_put_all_keys_share_one_origin_index() {
    let (task, handle, _metrics, dir) = single_node().await;

    let entries: &[(&[u8], &[u8])] = &[(b"head", b"h"), (b"e1", b"v1"), (b"e2", b"v2")];
    handle.multi_put(entries).await.expect("multi_put");

    // Every key in the batch reports the same origin index (>= 1).
    let (_, i1) = handle.get_stale_with_index(b"head").await.expect("head").expect("present");
    let (_, i2) = handle.get_stale_with_index(b"e1").await.expect("e1").expect("present");
    let (_, i3) = handle.get_stale_with_index(b"e2").await.expect("e2").expect("present");
    assert_eq!(i1, i2, "all batch keys share one origin index");
    assert_eq!(i1, i3, "all batch keys share one origin index");
    assert!(i1 >= 1);

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// hydra shape: `prefix` = the head key, `[prefix, successor)` covers the head
/// and the entity keys in one coherent read with a **single applied index**.
#[tokio::test]
async fn prefix_read_returns_head_and_entities_with_one_index() {
    let (task, handle, _metrics, dir) = single_node().await;

    // hydra's tree layout: head = "t", entities under "t\x00…".
    handle
        .multi_put(&[(b"t", b"head"), (b"t\x00e1", b"v1"), (b"t\x00e2", b"v2")])
        .await
        .expect("multi_put");

    let (entries, index, truncated) = handle
        .get_stale_prefix(b"t", 4)
        .await
        .expect("prefix read");
    assert!(!truncated);
    assert_eq!(
        entries,
        vec![
            (b"t".to_vec(), b"head".to_vec()),
            (b"t\x00e1".to_vec(), b"v1".to_vec()),
            (b"t\x00e2".to_vec(), b"v2".to_vec()),
        ]
    );
    // The whole segment belongs to one applied watermark: the batch's index.
    assert!(index >= 1);
    // And each key still reports the same individual origin — aligned with the
    // range read's unified index (single-writer: origin == the batch index).
    assert_eq!(
        handle.get_stale_with_index(b"t").await.expect("head").expect("present").1,
        index,
        "single-writer batch origin must equal the segment's applied index"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// An over-limit / malformed batch is rejected before propose: `InvalidArgument
/// ` and the state is untouched.
#[tokio::test]
async fn multi_put_rejects_over_limit_batch() {
    let (task, handle, _metrics, dir) = single_node().await;

    // A value beyond max_value_bytes.
    let big = vec![0u8; 1024 * 1024 + 1]; // > max_value_bytes (1 MiB)
    let err = handle
        .multi_put(&[(b"k", big.as_slice())])
        .await
        .expect_err("over-limit value must be rejected");
    assert!(matches!(err, arachne_kv::ArachneError::InvalidArgument(_)));

    // Empty batch.
    let err = handle
        .multi_put(&[])
        .await
        .expect_err("empty batch must be rejected");
    assert!(matches!(err, arachne_kv::ArachneError::InvalidArgument(_)));

    // Nothing was proposed or applied.
    assert_eq!(handle.get_stale(b"k").await.expect("get_stale"), None);

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A range read is bounded by `limit` and reports truncation.
#[tokio::test]
async fn range_read_honors_truncation() {
    let (task, handle, _metrics, dir) = single_node().await;

    for i in 0..10u64 {
        handle
            .put(format!("k{i:02}").as_bytes(), b"v")
            .await
            .expect("put");
    }

    let (entries, _, truncated) = handle.get_stale_range(b"", b"", 3).await.expect("range");
    assert_eq!(entries.len(), 3);
    assert!(truncated, "more keys existed past the limit");

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A clearly-reversed `[start, end)` (start > end) is rejected with
/// `InvalidArgument` before reaching the state machine — it must never panic
/// the read-reply task (`BTreeMap::range` would panic on `start > end`, which
/// under abort-builds would let a remote caller crash the node).
#[tokio::test]
async fn reversed_range_is_rejected_as_invalid() {
    let (task, handle, _metrics, dir) = single_node().await;

    let err = handle
        .get_stale_range(b"z", b"a", 10)
        .await
        .expect_err("reversed range must be rejected");
    assert!(matches!(err, arachne_kv::ArachneError::InvalidArgument(_)));

    // A degenerate [x, x) is a legal empty range, not an error.
    let (entries, _, truncated) = handle
        .get_stale_range(b"k", b"k", 10)
        .await
        .expect("equal endpoints are a legal empty range");
    assert!(entries.is_empty());
    assert!(!truncated);

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- 3-node: multi-put replicates atomically (acceptance §5.4) --------------

use arachne_kv::client::Handle;
use arachne_kv::{ArachneError, TransportFactory};

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn temp3_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("arachne-m2-3n-{}-{}", std::process::id(), DIR.fetch_add(1, Ordering::Relaxed)))
}

/// Spawn a 3-node in-process cluster (in-memory transport) with client-side
/// redirect enabled, wait for a stable leader, and return `(handles, leader,
/// follower)`.
async fn spawn3() -> (Vec<Handle>, usize, usize) {
    let profile = ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    };
    let factory = InMemoryTransportFactory::new();
    let addrs: HashMap<NodeId, std::net::SocketAddr> = (1..=3u64)
        .map(|i| (node_id(i), format!("127.0.0.1:800{i}").parse().expect("addr")))
        .collect();
    let metrics: Vec<Arc<Metrics>> = (0..3).map(|_| Arc::new(Metrics::new())).collect();
    let mut handles = Vec::new();
    let mut tasks = Vec::new();
    let mut dirs = Vec::new();

    for i in 1..=3u64 {
        let dir = temp3_dir();
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let wal = WalStorage::open(
            &dir,
            WalOptions {
                cluster_id: "m2".into(),
                node_id: format!("n{i}"),
                config: WalConfig {
                    fsync_policy: FsyncPolicy::Always,
                    segment_bytes: WalConfig::default().segment_bytes,
                },
                created_at_millis: 0,
                fsync_observer: None,
            },
        )
        .expect("open wal");
        let me = node_id(i);
        let (tx, rx) = factory.create(me.clone());
        let peers: HashMap<u64, NodeId> = (1..=3u64)
            .filter(|j| *j != i)
            .map(|j| (j, node_id(j)))
            .collect();
        let config = RuntimeConfig {
            self_raft_id: i,
            self_node_id: me,
            peers,
            addresses: addrs.clone(),
            raft: RaftNodeConfig::from_profile(&profile),
            profile: profile.clone(),
            metrics: Arc::clone(&metrics[(i - 1) as usize]),
        };
        let (runtime, handle) = Runtime::new(config, wal, tx, rx, &slog::Logger::root(slog::Discard.fuse(), slog::o!())).expect("build runtime");
        tasks.push(tokio::spawn(runtime.run()));
        handles.push(handle);
        dirs.push(dir);
    }
    for i in 0..3 {
        for j in 0..3 {
            if i != j {
                handles[i].register_peer(handles[j].clone());
            }
        }
    }

    let mut leader = None;
    for _ in 0..800 {
        let ids: Vec<u64> = metrics.iter().map(|m| m.leader_id()).collect();
        if ids[0] != 0 && ids.iter().all(|i| *i == ids[0]) {
            leader = Some((ids[0] - 1) as usize);
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    let leader = leader.expect("the cluster must agree on a leader");
    let follower = (0..3usize).find(|i| *i != leader).expect("a follower exists");

    // Keep the tasks/dirs alive for the test; they leak the abort handles via
    // the caller's task guard. We store them in a leaked Vec to avoid the abort
    // after the handles are consumed.
    let _ = tasks;
    let _ = dirs;

    (handles, leader, follower)
}

/// M2 acceptance §5.4: a `multi_put` (whole-tree replace) issued through a
/// **follower** is redirected to the leader, commits as one entry, and every
/// node converges on the same applied index for each batch key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_put_replicates_across_three_nodes() {
    let (handles, leader, follower) = spawn3().await;
    assert_ne!(leader, follower);

    let entries: &[(&[u8], &[u8])] = &[(b"head", b"h"), (b"e1", b"v1"), (b"e2", b"v2")];
    // Retry transient "not ready yet" errors (redirect/leadership settling).
    let mut done = false;
    for _ in 0..400 {
        match handles[follower].multi_put(entries).await {
            Ok(()) => {
                done = true;
                break;
            }
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. }) => {
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(e) => panic!("follower multi_put failed with a non-transient error: {e}"),
        }
    }
    assert!(done, "follower multi_put must redirect and succeed");

    // Every node converges: each batch key is present, and on each node the
    // head key's origin index equals the applied index (single-writer batch).
    for _ in 0..800 {
        let mut all_present = true;
        for h in &handles {
            let Some((_, idx)) = h.get_stale_with_index(b"head").await.expect("head read") else {
                all_present = false;
                break;
            };
            if idx < 1 {
                all_present = false;
                break;
            }
        }
        if all_present {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }

    let leader_idx = handles[leader]
        .get_stale_with_index(b"head")
        .await
        .expect("leader head read")
        .expect("head present")
        .1;
    let mut follower_same_idx = Vec::new();
    for i in 0..3usize {
        if i == leader {
            continue;
        }
        let idx = handles[i]
            .get_stale_with_index(b"head")
            .await
            .expect("head read")
            .expect("head present")
            .1;
        follower_same_idx.push(idx);
    }
    assert!(
        follower_same_idx.iter().all(|i| *i == leader_idx),
        "all nodes must agree on the batch's applied index (leader {leader_idx}, followers {follower_same_idx:?})"
    );

    // The batch keys are all visible on the leader (whole tree replaced).
    let keys: [&[u8]; 3] = [b"head", b"e1", b"e2"];
    let expect: [&[u8]; 3] = [b"h", b"v1", b"v2"];
    for (key, expected) in keys.into_iter().zip(expect) {
        assert_eq!(
            handles[leader]
                .get_stale_with_index(key)
                .await
                .expect("read")
                .map(|(v, _)| v),
            Some(expected.to_vec()),
            "batch key {key:?} must be present on the leader"
        );
    }
}
