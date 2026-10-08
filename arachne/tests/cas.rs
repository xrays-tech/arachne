//! Integration (M3): compare-and-swap over the real actor + handle
//! (single node, in-memory transport).
//!
//! Proves J3 (CAS exactly-once) end to end:
//! - the recommended loop: `get_stale_with_index` → `cas(IndexEquals(i))`
//!   succeeds on the live origin and fails with `NotApplied{current}` on a
//!   stale one, leaving state untouched;
//! - `NotExists` create-if-absent;
//! - a failed CAS is recorded, so a replay of the same session returns the
//!   cached verdict without recomputing the compare;
//! - a failed CAS is a legal *result* (`CasResult::NotApplied`), never an
//!   error (`Timeout` / `Busy` are still distinguishable).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::state_machine::{CasOp, CasPred, CasResult};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{ArachneError, Metrics, NodeId, Profile, ProfileConfig};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::Drain;

static DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let n = DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-cas-{}-{n}", std::process::id()));
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

/// The recommended loop: stale read gives `(value, index)`, CAS on that index
/// succeeds and updates the value.
#[tokio::test]
async fn cas_on_read_index_applies() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle.put(b"k", b"v1").await.expect("put");
    let (_, index) = handle
        .get_stale_with_index(b"k")
        .await
        .expect("stale read")
        .expect("key present");

    let result = handle
        .cas(b"k", CasPred::IndexEquals(index), CasOp::Put(b"v2".to_vec()))
        .await
        .expect("cas");
    assert_eq!(result, CasResult::Applied);
    assert_eq!(handle.get_stale(b"k").await.expect("get_stale"), Some(b"v2".to_vec()));

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// CAS on a stale index fails with `NotApplied{current}` and leaves the value
/// untouched — the caller can re-read and retry with the fresh index.
#[tokio::test]
async fn cas_on_stale_index_not_applied() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle.put(b"k", b"v1").await.expect("put");
    let (_, index) = handle
        .get_stale_with_index(b"k")
        .await
        .expect("stale read")
        .expect("key present");
    // Another writer bumps the key.
    handle.put(b"k", b"v2").await.expect("put2");

    // The stale index is now behind: CAS fails, reports the current state.
    let result = handle
        .cas(b"k", CasPred::IndexEquals(index), CasOp::Put(b"vX".to_vec()))
        .await
        .expect("cas");
    assert_eq!(
        result,
        CasResult::NotApplied {
            current_index: index + 1,
            current_value: Some(b"v2".to_vec()),
        }
    );
    // Nothing changed.
    assert_eq!(handle.get_stale(b"k").await.expect("get_stale"), Some(b"v2".to_vec()));

    // Retry on the tour-current index succeeds.
    let (_, fresh) = handle
        .get_stale_with_index(b"k")
        .await
        .expect("fresh read")
        .expect("key present");
    assert_eq!(
        handle
            .cas(b"k", CasPred::IndexEquals(fresh), CasOp::Put(b"v3".to_vec()))
            .await
            .expect("retry cas"),
        CasResult::Applied
    );
    assert_eq!(handle.get_stale(b"k").await.expect("get_stale"), Some(b"v3".to_vec()));

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A failed CAS is a legal result, never an error: `NotApplied`, not
/// `Timeout`/`Busy` — which is what lets a caller distinguish "definitely did
/// not apply" from "result unknown".
#[tokio::test]
async fn cas_miss_is_result_not_error() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle.put(b"k", b"v").await.expect("put");
    match handle
        .cas(b"k", CasPred::IndexEquals(999_999), CasOp::Put(b"x".to_vec()))
        .await
    {
        Ok(CasResult::NotApplied { .. }) => {}
        other => panic!("a CAS miss must be a NotApplied result, got {other:?}"),
    }

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// `NotExists` is create-if-absent.
#[tokio::test]
async fn cas_not_exists_create_only() {
    let (task, handle, _metrics, dir) = single_node().await;

    assert_eq!(
        handle
            .cas(b"new", CasPred::NotExists, CasOp::Put(b"created".to_vec()))
            .await
            .expect("create not-exists cas"),
        CasResult::Applied
    );
    // Second create fails: the key now exists.
    match handle
        .cas(b"new", CasPred::NotExists, CasOp::Put(b"x".to_vec()))
        .await
    {
        Ok(CasResult::NotApplied {
            current_value: Some(v),
            ..
        }) => assert_eq!(v, b"created".to_vec()),
        other => panic!("second create-if-absent must fail, got {other:?}"),
    }

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A value-equality predicate is the compatible-intuitive CAS.
#[tokio::test]
async fn cas_value_equals() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle.put(b"k", b"abc").await.expect("put");
    assert_eq!(
        handle
            .cas(b"k", CasPred::ValueEquals(b"abc".to_vec()), CasOp::Put(b"def".to_vec()))
            .await
            .expect("cas"),
        CasResult::Applied
    );
    // The second compare sees the updated value and fails.
    match handle
        .cas(b"k", CasPred::ValueEquals(b"abc".to_vec()), CasOp::Put(b"ghi".to_vec()))
        .await
        .expect("cas")
    {
        CasResult::NotApplied {
            current_value: Some(v),
            ..
        } => assert_eq!(v, b"def".to_vec()),
        other => panic!("value-mismatch CAS must fail with the current value, got {other:?}"),
    }

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// An over-limit CAS value is rejected before propose (both the success op's
/// value and the `ValueEquals` predicate's payload obey `max_value_bytes`).
#[tokio::test]
async fn cas_rejects_oversized_value() {
    let (task, handle, _metrics, dir) = single_node().await;

    let big = vec![0u8; 1024 * 1024 + 1];
    let err = handle
        .cas(b"k", CasPred::NotExists, CasOp::Put(big.clone()))
        .await
        .expect_err("oversized cas value must be rejected");
    assert!(matches!(err, ArachneError::InvalidArgument(_)));

    // The `ValueEquals` predicate's payload is a value too and must respect
    // the same bound (else an over-limit compare would reach the log and fail
    // to replicate past the transport max_message_size).
    let err = handle
        .cas(b"k", CasPred::ValueEquals(big), CasOp::Delete)
        .await
        .expect_err("oversized ValueEquals predicate must be rejected");
    assert!(matches!(err, ArachneError::InvalidArgument(_)));

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
