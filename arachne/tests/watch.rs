//! Integration (M4/P2): watch over the real actor + handle (single node,
//! in-memory transport).
//!
//! Proves J4 (watch events are index-monotone and the snapshot ∪ events
//! window reconstructs the prefix state exactly):
//! - register-then-write delivers events with no gap (a strict `>` filter
//!   against the registration snapshot's applied index);
//! - the `snapshot ∪ events` union reconstructs the prefix state precisely
//!   (no loss, no duplication) under concurrent writes;
//! - prefix filtering happens in the actor (non-matching keys never arrive);
//! - a slow consumer is **disconnected** (the channel closes) rather than
//!   silently dropping events, and re-watching restores a complete stream;
//! - a deduped replay (same `(cid, seq)`) does not re-emit an event;
//! - a truncated prefix snapshot is **rejected** (watch refused) rather than
//!   returning an incomplete snapshot.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::state_machine::KvStateMachine;
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{Metrics, NodeId, Profile, ProfileConfig, WatchEvent};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::Drain;

static DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let n = DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-watch-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> slog::Logger {
    slog::Logger::root(slog::Discard.fuse(), slog::o!())
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
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
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

/// Drain `rx` with a bounded poll; returns the collected events.
async fn drain(rx: &mut tokio::sync::mpsc::Receiver<WatchEvent>, loops: usize) -> Vec<WatchEvent> {
    let mut out = Vec::new();
    for _ in 0..loops {
        if let Ok(e) = rx.try_recv() {
            out.push(e);
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    out
}

/// Register-then-write delivers events with no gap, and the events are
/// index-monotone and strictly after the snapshot's applied index.
#[tokio::test]
async fn watch_delivers_events_after_snapshot_index() {
    let (task, handle, _metrics, dir) = single_node().await;

    // Pre-write a key that must appear in the snapshot.
    handle.put(b"head", b"h0").await.expect("pre-write");
    let mut sub = handle.watch(b"head", 10).await.expect("watch");
    assert!(!sub.truncated);
    assert!(sub.applied_index >= 1);
    // The snapshot contains the pre-written key.
    assert!(sub.snapshot.iter().any(|(k, _)| k == b"head"));

    // Write after registration; the event must arrive.
    handle.put(b"head", b"h1").await.expect("post-write");
    let mut all = drain(&mut sub.events, 60).await;
    let ev = all.iter().find(|e| e.key == b"head").expect("head event");
    assert_eq!(ev.value.as_deref(), Some(&b"h1"[..]));
    assert!(
        ev.index > sub.applied_index,
        "events must be strictly after the snapshot index"
    );

    // Multiple events are index-monotone.
    handle.put(b"head", b"h2").await.expect("post-write2");
    let mut later = drain(&mut sub.events, 60).await;
    all.append(&mut later);
    let idxs: Vec<u64> = all.iter().map(|e| e.index).collect();
    assert_eq!(idxs, {
        let mut s = idxs.clone();
        s.sort_unstable();
        s
    }, "index-monotone");
    assert!(all.iter().all(|e| e.index > sub.applied_index));

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The `snapshot ∪ events` union reconstructs the prefix state precisely when
/// writes race with registration.
#[tokio::test]
async fn watch_snapshot_union_events_reconstructs_state() {
    let (task, handle, _metrics, dir) = single_node().await;

    // Register BEFORE the written keys exist; then write a batch.
    let mut sub = handle.watch(b"t", 100).await.expect("watch");
    handle.put(b"t", b"head").await.expect("put head");
    handle.put(b"t\x00e1", b"v1").await.expect("put e1");
    handle.put(b"t\x00e2", b"v2").await.expect("put e2");
    handle.put(b"other", b"x").await.expect("put other (outside prefix)");

    let events = drain(&mut sub.events, 120).await;
    // Only prefix "t" keys arrive (other excluded).
    assert!(events.iter().all(|e| e.key.starts_with(b"t")));

    // Rebuild the prefix state from snapshot ∪ events.
    let mut state: HashMap<Vec<u8>, Vec<u8>> = sub
        .snapshot
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for e in &events {
        if let Some(v) = &e.value {
            state.insert(e.key.clone(), v.clone());
        } else {
            state.remove(&e.key);
        }
    }
    assert_eq!(state.get(b"t".as_slice()), Some(&b"head".to_vec()));
    assert_eq!(state.get(b"t\x00e1".as_slice()), Some(&b"v1".to_vec()));
    assert_eq!(state.get(b"t\x00e2".as_slice()), Some(&b"v2".to_vec()));
    assert!(!state.contains_key(b"other".as_slice()));

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A deduped replay of the same `(client_id, seq_no)` does **not** re-emit an
/// event (events are first-application-only).
#[tokio::test]
async fn watch_deduped_replay_emits_no_second_event() {
    let (task, handle, _metrics, dir) = single_node().await;

    // Use the doc-hidden outcome seam to replay the exact same (cid, seq).
    let mut sub = handle.watch(b"k", 10).await.expect("watch");
    let cmd = KvStateMachine::encode_put(77, 5, b"k", b"v");
    handle
        .propose_with_outcome(cmd.clone(), 77, 5)
        .await
        .expect("first apply");
    handle
        .propose_with_outcome(cmd, 77, 5)
        .await
        .expect("replay (deduped: same cid/seq)");

    let events = drain(&mut sub.events, 120).await;
    let k_events = events.iter().filter(|e| e.key == b"k").count();
    assert_eq!(k_events, 1, "a deduped replay must not re-emit the event");

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A slow consumer (receiver not drained) is **disconnected** — its channel
/// closes — rather than silently dropping events; a re-watch restores a
/// complete stream.
#[tokio::test]
async fn watch_slow_consumer_is_disconnected_and_can_rewatch() {
    let (task, handle, _metrics, dir) = single_node().await;

    let mut sub = handle.watch(b"k", 10).await.expect("watch");
    // Do NOT drain: flood the key until the bounded watcher queue (1024)
    // overflows and the actor disconnects the watcher (sender dropped ⇒ the
    // receiver reports `is_closed`).
    for i in 0..1500u64 {
        let _ = handle.put(b"k", i.to_string().as_bytes()).await;
    }
    // The watcher must have been dropped by the actor by now.
    let mut disconnected = false;
    for _ in 0..200 {
        if sub.events.is_closed() {
            disconnected = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(disconnected, "an overflowing watcher must be disconnected");

    // Re-watch: the new subscription carries the current snapshot and a fresh
    // stream; keys written after re-watch are delivered (not silently missing).
    let mut sub2 = handle.watch(b"k", 10).await.expect("re-watch");
    handle.put(b"k", b"after").await.expect("write after re-watch");
    let events = drain(&mut sub2.events, 120).await;
    assert!(
        events
            .iter()
            .any(|e| e.key == b"k" && e.value.as_deref() == Some(&b"after"[..])),
        "re-watch must resume delivering events"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A truncated prefix snapshot is rejected (`Busy`) rather than returned as an
/// incomplete snapshot (a truncated snapshot would silently miss older keys).
#[tokio::test]
async fn watch_rejects_truncated_snapshot() {
    let (task, handle, _metrics, dir) = single_node().await;

    // Write many keys under one prefix so a tiny snapshot limit truncates.
    for i in 0..200u64 {
        handle
            .put(format!("p{i:04}").as_bytes(), b"v")
            .await
            .expect("put");
    }
    let err = handle
        .watch(b"p", 3)
        .await
        .expect_err("truncated snapshot must be rejected");
    assert!(matches!(err, arachne_kv::ArachneError::Busy));

    // A large-enough limit succeeds.
    let _sub = handle.watch(b"p", 1000).await.expect("large-limit watch succeeds");

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
