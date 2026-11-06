//! Integration (Phase 2): index-returning writes.
//!
//! `put_with_index` / `multi_put_with_index` / `delete_with_index` return the
//! log index of the entry that **actually established** the write. The index
//! must come from the apply pipeline, never a post-apply re-read: two writes
//! to the same key in one apply batch would otherwise both report the final
//! value's origin (design §5.3 / commit-index design §9.1).
//!
//! Dedup is the other half: replaying the same `(client_id, seq_no)` reports
//! the **original** index, which is also why the session record (and its
//! index) is persisted in the KV snapshot v3.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::state_machine::KvStateMachine;
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{Metrics, NodeId, Profile, ProfileConfig, StateMachine};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::Drain;

static DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let n = DIR.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("arachne-write-index-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Build a single-node runtime + handle over the in-memory transport and wait
/// for self-election. Returns the spawned actor task so each test can abort it.
async fn single_node() -> (
    tokio::task::JoinHandle<()>,
    arachne_kv::client::Handle,
    Arc<Metrics>,
    std::path::PathBuf,
) {
    let dir = temp_dir();
    let wal = WalStorage::open(
        &dir,
        WalOptions {
            cluster_id: "write-index".into(),
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

/// `put_with_index` returns exactly the origin index a later stale read reports
/// for the key.
#[tokio::test]
async fn put_with_index_matches_stale_origin_index() {
    let (task, handle, _metrics, dir) = single_node().await;

    let written = handle
        .put_with_index(b"k", b"v")
        .await
        .expect("put_with_index");
    let (value, origin) = handle
        .get_stale_with_index(b"k")
        .await
        .expect("stale read")
        .expect("key present");
    assert_eq!(value, b"v".to_vec());
    assert_eq!(origin, written, "returned index == value origin");
    assert!(written >= 1, "a present key never reports index 0");

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// One atomic `multi_put_with_index` entry gives every key the same index.
#[tokio::test]
async fn multi_put_with_index_is_one_shared_index() {
    let (task, handle, _metrics, dir) = single_node().await;

    let index = handle
        .multi_put_with_index(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")])
        .await
        .expect("multi_put_with_index");

    for (key, expected) in [
        (b"a".as_slice(), b"1".as_slice()),
        (b"b".as_slice(), b"2".as_slice()),
        (b"c".as_slice(), b"3".as_slice()),
    ] {
        let (value, origin) = handle
            .get_stale_with_index(key)
            .await
            .expect("stale read")
            .expect("key present");
        assert_eq!(value, expected.to_vec());
        assert_eq!(origin, index, "every key shares the batch index");
    }

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// `delete_with_index` returns the delete entry's index even though the key
/// disappears; a later put has a strictly greater index.
#[tokio::test]
async fn delete_with_index_returns_delete_entry_and_next_put_is_greater() {
    let (task, handle, _metrics, dir) = single_node().await;

    let put_index = handle.put_with_index(b"k", b"v").await.expect("put");
    let delete_index = handle
        .delete_with_index(b"k")
        .await
        .expect("delete_with_index");
    assert_eq!(
        handle.get_stale_with_index(b"k").await.expect("stale read"),
        None,
        "the key is gone"
    );
    assert!(
        delete_index > put_index,
        "the delete entry has a later index ({delete_index} > {put_index})"
    );

    let re_put_index = handle
        .put_with_index(b"k", b"v2")
        .await
        .expect("re-put");
    assert!(
        re_put_index > delete_index,
        "a re-put after delete has a greater index"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Replaying the same `(client_id, seq_no)` returns the **original** index,
/// not the replay entry's index (and does not re-apply the new payload).
#[tokio::test]
async fn dedup_replay_returns_original_index() {
    let (task, handle, _metrics, dir) = single_node().await;

    const CLIENT: u64 = 42;
    const SEQ: u64 = 7;

    let first = handle
        .propose_raw_with_index(KvStateMachine::encode_put(CLIENT, SEQ, b"k", b"v1"), CLIENT, SEQ)
        .await
        .expect("first apply");
    let replay = handle
        .propose_raw_with_index(KvStateMachine::encode_put(CLIENT, SEQ, b"k", b"v2"), CLIENT, SEQ)
        .await
        .expect("replay");

    assert_eq!(
        replay, first,
        "a deduped replay reports the original entry index"
    );
    assert_eq!(
        handle.get_stale(b"k").await.expect("stale read"),
        Some(b"v1".to_vec()),
        "dedup wins: the replay's payload is not applied"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two concurrent writes to the **same key** must each report their own entry
/// index. A post-apply re-read would return the final value's origin for both
/// (the §9.1 trap).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_same_key_writes_report_distinct_entry_indexes() {
    let (task, handle, _metrics, dir) = single_node().await;

    let h1 = handle.clone();
    let h2 = handle.clone();
    let (i1, i2) = tokio::join!(
        h1.put_with_index(b"k", b"a"),
        h2.put_with_index(b"k", b"b"),
    );
    let i1 = i1.expect("first put");
    let i2 = i2.expect("second put");

    assert_ne!(
        i1, i2,
        "two winning writes must report distinct entry indexes"
    );

    // The final value is the one whose entry applied last (larger index), and
    // the stale origin must agree with that write — proving each reply carried
    // its own entry's index, not a re-read of the final value.
    let (value, origin) = handle
        .get_stale_with_index(b"k")
        .await
        .expect("stale read")
        .expect("key present");
    let (last_index, last_value) = if i1 > i2 {
        (i1, b"a".as_slice())
    } else {
        (i2, b"b".as_slice())
    };
    assert_eq!(origin, last_index);
    assert_eq!(value, last_value.to_vec());

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Snapshot v3 round-trip preserves the session's original index, so a replay
/// after restore still reports the index of the entry that established it.
#[test]
fn snapshot_v3_roundtrip_preserves_session_index() {
    let sm = KvStateMachine::new();
    let cmd = KvStateMachine::encode_put(7, 1, b"k", b"v");
    let (_, first, _) = sm
        .apply_with_events_and_index(1, &cmd)
        .expect("first apply");
    assert_eq!(first, 1);

    let snap = sm.snapshot().expect("snapshot");
    let restored = KvStateMachine::new();
    restored.restore(&snap).expect("restore");

    let (outcome, replay, events) = restored
        .apply_with_events_and_index(2, &cmd)
        .expect("replay");
    assert_eq!(outcome, arachne_kv::ApplyOutcome::Value(b"v".to_vec()));
    assert_eq!(replay, 1, "the original index survived the snapshot");
    assert!(events.is_empty());
}

/// A v2 KV snapshot payload has no per-session index, so it must fail-stop
/// rather than fabricate `index 0`.
#[test]
fn v2_snapshot_payload_is_rejected() {
    let mut v2 = Vec::new();
    v2.push(2u8); // old KV_SNAPSHOT_VERSION
    v2.extend_from_slice(&0u64.to_le_bytes()); // applied
    v2.extend_from_slice(&0u32.to_le_bytes()); // kv count
    v2.extend_from_slice(&0u32.to_le_bytes()); // session count

    let sm = KvStateMachine::new();
    assert!(
        sm.restore(&v2).is_err(),
        "a v2 payload must be rejected (fail-stop, no index-0 migration)"
    );
}
