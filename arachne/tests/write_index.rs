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
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::state_machine::KvStateMachine;
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{
    ArachneError, Metrics, NodeId, Profile, ProfileConfig, StateMachine, TransportFactory,
};
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
    let (tx, rx) = factory.create(NodeId::from("n1"));

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
        .propose_raw_with_index(KvStateMachine::encode_put(CLIENT, SEQ, b"k", b"v1"), CLIENT, SEQ)
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

// ---- 3-node leader churn: no index rollback on the public path -------------

fn three_node_profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    }
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

/// A 3-node in-memory cluster. Nodes can be killed (actor aborted).
struct Cluster3 {
    handles: Vec<Handle>,
    metrics: Vec<Arc<Metrics>>,
    tasks: Vec<Option<tokio::task::JoinHandle<()>>>,
    dirs: Vec<PathBuf>,
}

impl Cluster3 {
    async fn spawn() -> Self {
        let profile = three_node_profile();
        let factory = InMemoryTransportFactory::new();
        let addrs: HashMap<NodeId, SocketAddr> = (1..=3u64)
            .map(|i| (node_id(i), format!("127.0.0.1:74{i:02}").parse().expect("addr")))
            .collect();
        let metrics: Vec<Arc<Metrics>> = (0..3).map(|_| Arc::new(Metrics::new())).collect();
        let mut handles = Vec::new();
        let mut tasks = Vec::new();
        let mut dirs = Vec::new();

        for i in 1..=3u64 {
            let dir = temp_dir();
            let wal = WalStorage::open(
                &dir,
                WalOptions {
                    cluster_id: "write-index-churn".into(),
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
            let peers = (1..=3u64)
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
            let (runtime, handle) =
                Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
            tasks.push(Some(tokio::spawn(runtime.run())));
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
        Self {
            handles,
            metrics,
            tasks,
            dirs,
        }
    }

    async fn leader(&self) -> usize {
        for _ in 0..1000 {
            let ids: Vec<u64> = self.metrics.iter().map(|m| m.leader_id()).collect();
            if ids[0] != 0 && ids.iter().all(|id| *id == ids[0]) {
                return (ids[0] - 1) as usize;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        panic!("the cluster never agreed on a leader");
    }

    async fn kill(&mut self, node: usize) {
        if let Some(task) = self.tasks[node].take() {
            task.abort();
            let _ = task.await;
        }
    }

    async fn shutdown(mut self) {
        for node in 0..3 {
            self.kill(node).await;
        }
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn logger() -> slog::Logger {
    slog::Logger::root(slog::Discard.fuse(), slog::o!())
}

async fn until_multi_put_with_index(handle: &Handle, entries: &[(&[u8], &[u8])]) -> u64 {
    for _ in 0..800 {
        match handle.multi_put_with_index(entries).await {
            Ok(index) => return index,
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. }) => {
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(e) => panic!("multi_put_with_index failed: {e}"),
        }
    }
    panic!("multi_put_with_index never committed");
}

/// Killing the leader must not let a later `multi_put_with_index` return an
/// index at or below the old head: the new head is a new entry and must be
/// visible on every surviving node. The dead leader's old handle must not fake
/// a successful index either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_churn_does_not_roll_back_multi_put_with_index() {
    let mut cluster = Cluster3::spawn().await;
    let leader = cluster.leader().await;

    // A known old head after a run of entries (the churn baseline).
    for i in 0..50u64 {
        cluster.handles[leader]
            .put(format!("seed{i:02}").as_bytes(), b"v")
            .await
            .expect("seed write");
    }
    let old_head = cluster.handles[leader]
        .put_with_index(b"head", b"old")
        .await
        .expect("old head");
    assert!(old_head >= 50, "the old head is a real committed index");

    let dead_handle = cluster.handles[leader].clone();
    cluster.kill(leader).await;

    // Wait for one of the survivors to win the election.
    let alive: Vec<usize> = (0..3).filter(|i| *i != leader).collect();
    let mut new_leader = None;
    for _ in 0..1000 {
        if let Some(pos) = alive
            .iter()
            .copied()
            .find(|i| cluster.metrics[*i].is_leader())
        {
            new_leader = Some(pos);
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    let new_leader = new_leader.expect("the surviving majority must elect a leader");
    let surviving_follower = alive
        .iter()
        .copied()
        .find(|i| *i != new_leader)
        .expect("one surviving follower");

    // Write the new head through the surviving follower (redirects to the new
    // leader). The returned index must be a fresh, greater entry index.
    let entries: [(&[u8], &[u8]); 2] = [(b"head", b"new"), (b"generation", b"1")];
    let new_head = until_multi_put_with_index(&cluster.handles[surviving_follower], &entries).await;
    assert!(
        new_head > old_head,
        "multi_put_with_index returned {new_head}, not above the old head {old_head}"
    );

    // The new head and its batch peer must be visible on both survivors.
    for &node in &alive {
        let mut seen = false;
        for _ in 0..600 {
            if cluster
                .handles[node]
                .get_stale(b"head")
                .await
                .expect("stale head read")
                .as_deref()
                == Some(b"new".as_slice())
            {
                seen = true;
                break;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        assert!(seen, "node {node} never observed the new head");
        assert_eq!(
            cluster.handles[node]
                .get_stale(b"generation")
                .await
                .expect("stale generation read"),
            Some(b"1".to_vec()),
            "node {node} lost the batch peer"
        );
    }

    // The killed leader's old handle cannot report a successful index.
    let ghost = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        dead_handle.put_with_index(b"ghost", b"write"),
    )
    .await
    .expect("a write through the dead leader must not hang");
    match ghost {
        Ok(index) => panic!("the dead leader's handle reported a successful index {index}"),
        Err(ArachneError::ShuttingDown)
        | Err(ArachneError::QuorumUnavailable)
        | Err(ArachneError::NotLeader { .. })
        | Err(ArachneError::Timeout) => {}
        Err(e) => panic!("unexpected error from the dead leader's handle: {e}"),
    }

    cluster.shutdown().await;
}
