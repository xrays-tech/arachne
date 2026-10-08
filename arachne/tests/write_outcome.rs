//! Integration (M1): the per-command outcome write-back pipeline.
//!
//! `put`/`delete` discard the outcome they receive, so their regressions cannot
//! prove the pipeline exists: the apply task writes each command's
//! [`ApplyOutcome`] back through a per-entry channel to the actor, which replies
//! with it. These tests use the `#[doc(hidden)]` M1 observability seam
//! ([`Handle::propose_with_outcome`]) to see the outcome end to end on a single
//! node over the in-memory transport, and to assert the session-dedup replay
//! returns the *cached* outcome (invariant: each `(client_id, seq_no)` has at
//! most one outcome and replays never recompute).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::state_machine::KvStateMachine;
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{ApplyOutcome, Metrics, NodeId, Profile, ProfileConfig};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::Drain;

static DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let n = DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-m1-outcome-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Build a single-node runtime + handle over the in-memory transport and wait
/// for self-election. Returns the spawned actor task so each test can abort it.
async fn single_node(
) -> (tokio::task::JoinHandle<()>, arachne_kv::client::Handle, Arc<Metrics>, std::path::PathBuf) {
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

/// A `put` comes back as `Ok(Value(v))`: the outcome the apply task wrote back
/// through the per-entry channel, not a bare `Ok(())`.
#[tokio::test]
async fn put_outcome_is_the_stored_value() {
    let (task, handle, _metrics, dir) = single_node().await;

    let outcome = handle
        .propose_with_outcome(
            KvStateMachine::encode_put(1, 1, b"k", b"v"),
            1,
            1,
        )
        .await
        .expect("propose");
    assert_eq!(outcome, ApplyOutcome::Value(b"v".to_vec()));

    // The value is really there.
    assert_eq!(handle.get_stale(b"k").await.expect("get_stale"), Some(b"v".to_vec()));

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A `delete` (no stored value for the key) comes back as `Ok(None)` — distinct
/// from the `Ok(Value(..))` of a put, and still a *success* result, not an
/// error.
#[tokio::test]
async fn delete_outcome_is_none() {
    let (task, handle, _metrics, dir) = single_node().await;

    // Key absent → delete succeeds with outcome `None`.
    let outcome = handle
        .propose_with_outcome(
            KvStateMachine::encode_delete(1, 1, b"absent"),
            1,
            1,
        )
        .await
        .expect("propose");
    assert_eq!(outcome, ApplyOutcome::None);

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Replaying the same `(client_id, seq_no)` returns the **cached** outcome from
/// the session table — the apply task does not re-apply and does not recompute
/// (invariant: at most one outcome per session; replay returns the cache).
#[tokio::test]
async fn replay_of_same_session_returns_cached_outcome() {
    let (task, handle, _metrics, dir) = single_node().await;

    let first = handle
        .propose_with_outcome(
            KvStateMachine::encode_put(42, 7, b"k", b"first"),
            42,
            7,
        )
        .await
        .expect("propose");
    assert_eq!(first, ApplyOutcome::Value(b"first".to_vec()));

    // Same session, a *different* payload than the first: dedup must win — the
    // cached outcome from the original apply is returned, and the key keeps the
    // first write.
    let replay = handle
        .propose_with_outcome(
            KvStateMachine::encode_put(42, 7, b"k", b"second"),
            42,
            7,
        )
        .await
        .expect("replay");
    assert_eq!(replay, ApplyOutcome::Value(b"first".to_vec()));
    assert_eq!(
        handle.get_stale(b"k").await.expect("get_stale"),
        Some(b"first".to_vec()),
        "the replayed session must not re-apply the second payload"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The pipeline carries an outcome that *differs from the command's input*: a
/// put of an empty value yields `Ok(Value(vec![]))` (presence, not `None`), so
/// the reply truly reflects the apply outcome and not a rewritten ack.
#[tokio::test]
async fn empty_value_put_is_value_not_none() {
    let (task, handle, _metrics, dir) = single_node().await;

    let outcome = handle
        .propose_with_outcome(
            KvStateMachine::encode_put(2, 1, b"k", b""),
            2,
            1,
        )
        .await
        .expect("propose");
    assert_eq!(
        outcome,
        ApplyOutcome::Value(vec![]),
        "an empty stored value is `Value(vec![])`, not `None`"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
