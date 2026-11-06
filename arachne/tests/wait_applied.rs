//! Integration (Phase 3): `Handle::wait_applied` — a local, quorum-free wait
//! for the state machine to apply a log index.
//!
//! Covers, over the in-memory transport (no tonic):
//! * `index == 0` and an already-applied index return immediately;
//! * a waiter is woken by a concurrent write reaching its index (event driven,
//!   no poll interval);
//! * an unreachable index times out cleanly (`Timeout`, never a hang);
//! * the wait is **local**: on a minority follower it times out instead of
//!   redirecting / requiring a quorum, while a stale `multi_get` still works;
//! * dropping the future (cancellation) leaves no permanent waiter and the
//!   runtime keeps serving;
//! * a three-node follower converges to a write's returned index.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time imports there), so this file uses
//! `core::time::Duration` and never `std::time`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{
    ArachneError, Metrics, NodeId, Profile, ProfileConfig, ReadConsistency, TransportFactory,
};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-wait-applied-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn peers_of(self_id: u64, n: u64) -> HashMap<u64, NodeId> {
    (1..=n)
        .filter(|j| *j != self_id)
        .map(|j| (j, node_id(j)))
        .collect()
}

/// Fake loopback addrs per node. Never dialed by the in-memory transport; they
/// only populate `NotLeader{leader_hint}`.
fn addresses(n: u64) -> HashMap<NodeId, SocketAddr> {
    (1..=n)
        .map(|i| (node_id(i), format!("127.0.0.1:720{i}").parse().expect("addr")))
        .collect()
}

fn profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    }
}

/// Poll the current applied index through an empty stale batch read.
async fn applied(handle: &Handle) -> u64 {
    handle
        .multi_get(&[], ReadConsistency::Stale)
        .await
        .expect("empty multi_get")
        .applied_index
}

/// A single-node runtime over the in-memory transport.
async fn single_node() -> (
    tokio::task::JoinHandle<()>,
    Handle,
    Arc<Metrics>,
    PathBuf,
) {
    let dir = temp_dir("single");
    let wal = WalStorage::open(
        &dir,
        WalOptions {
            cluster_id: "wait-applied".into(),
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
    (task, handle, metrics, dir)
}

// ---- 1. index 0 and already-applied index are immediate ---------------------

#[tokio::test]
async fn index_zero_and_caught_up_return_immediately() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle.put(b"k", b"v").await.expect("put");
    let current = applied(&handle).await;
    assert!(current >= 1, "the write must have applied");

    let zero = handle
        .wait_applied(0, core::time::Duration::from_secs(1))
        .await
        .expect("index 0 is a no-op");
    assert!(
        zero >= 1,
        "index 0 returns the current applied index (the write applied)"
    );

    let caught_up = handle
        .wait_applied(current, core::time::Duration::from_secs(1))
        .await
        .expect("already applied returns immediately");
    assert!(
        caught_up >= current,
        "caught-up wait must report an applied index >= the requested one"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- zero timeout precedes the already-applied / index-0 no-op -------------

#[tokio::test]
async fn zero_timeout_takes_precedence_over_already_applied() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle.put(b"k", b"v").await.expect("put");
    let current = applied(&handle).await;
    assert!(current >= 1, "the write must have applied");

    // Even though `current` is already applied, a zero budget means the caller
    // gave no time for a round trip: no command is sent and Timeout wins over
    // the `index == 0` / caught-up no-op.
    let result = handle
        .wait_applied(current, core::time::Duration::ZERO)
        .await;
    match result {
        Err(ArachneError::Timeout) => {}
        other => panic!(
            "a zero timeout must return Timeout immediately, without sending; got {other:?}"
        ),
    }

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- 2. woken by a concurrent write ---------------------------------------

#[tokio::test]
async fn wakes_when_a_concurrent_write_reaches_the_index() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle.put(b"seed", b"0").await.expect("seed");
    let current = applied(&handle).await;
    let future = current + 1;

    let started = tokio::time::Instant::now();
    let waiter = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .wait_applied(future, core::time::Duration::from_secs(3))
                .await
        })
    };
    // Give the waiter a chance to register before the write (otherwise this
    // would test only the immediate path).
    tokio::time::sleep(core::time::Duration::from_millis(20)).await;

    let written = handle
        .put_with_index(b"k", b"v")
        .await
        .expect("put_with_index");
    assert!(
        written >= future,
        "the write must land at or above the awaited index"
    );

    let observed = waiter.await.expect("waiter task").expect("wait must succeed");
    assert!(
        observed >= future,
        "wake must report an applied index >= the awaited one"
    );
    assert!(observed >= written);
    assert!(
        started.elapsed() < core::time::Duration::from_secs(2),
        "the wait must be woken by progress, not by a slow poll"
    );

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- 2b. multiple waiters on the same index all resolve --------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_waiters_on_the_same_index_all_resolve() {
    let (task, handle, _metrics, dir) = single_node().await;

    handle.put(b"seed", b"0").await.expect("seed");
    let future = applied(&handle).await + 1;

    let mut waiters = Vec::new();
    for _ in 0..8 {
        let handle = handle.clone();
        waiters.push(tokio::spawn(async move {
            handle
                .wait_applied(future, core::time::Duration::from_secs(3))
                .await
        }));
    }
    // Let every waiter register before the write that satisfies them.
    tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    let written = handle
        .put_with_index(b"k", b"v")
        .await
        .expect("put_with_index");
    assert!(written >= future);

    for waiter in waiters {
        let observed = waiter
            .await
            .expect("waiter task")
            .expect("every waiter must resolve");
        assert!(
            observed >= future && observed >= written,
            "waiter observed {observed}, below index {future} / write {written}"
        );
    }

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- 3. unreachable index times out cleanly --------------------------------
#[tokio::test]
async fn times_out_cleanly() {
    let (task, handle, _metrics, dir) = single_node().await;

    let result = tokio::time::timeout(
        core::time::Duration::from_secs(2),
        handle.wait_applied(u64::MAX, core::time::Duration::from_millis(50)),
    )
    .await
    .expect("wait_applied must never hang past its own timeout");

    match result {
        Err(ArachneError::Timeout) => {}
        other => panic!("expected Timeout, got {other:?}"),
    }

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- 5. cancellation is safe ----------------------------------------------

#[tokio::test]
async fn cancellation_is_safe() {
    let (task, handle, _metrics, dir) = single_node().await;

    // Start a long wait and drop its future after ~10 ms (cancellation).
    let dropped = tokio::time::timeout(
        core::time::Duration::from_millis(10),
        handle.wait_applied(u64::MAX, core::time::Duration::from_secs(5)),
    )
    .await;
    assert!(dropped.is_err(), "the long wait must still be pending when dropped");

    // The runtime keeps serving: a fresh wait succeeds, and so does a write.
    let now = applied(&handle).await;
    let observed = handle
        .wait_applied(now, core::time::Duration::from_secs(1))
        .await
        .expect("a fresh wait still succeeds");
    assert!(observed >= now);

    handle.put(b"k", b"v").await.expect("write after cancellation");
    let caught_up = handle
        .wait_applied(applied(&handle).await, core::time::Duration::from_secs(1))
        .await
        .expect("wait after write");
    assert!(caught_up >= 1);

    task.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- three-node cluster harness -------------------------------------------

struct Cluster {
    factory: InMemoryTransportFactory,
    handles: Vec<Handle>,
    metrics: Vec<Arc<Metrics>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    dirs: Vec<PathBuf>,
}

impl Cluster {
    fn spawn(n: u64) -> Self {
        let profile = profile();
        let factory = InMemoryTransportFactory::new();
        let addrs = addresses(n);
        let metrics: Vec<Arc<Metrics>> = (0..n).map(|_| Arc::new(Metrics::new())).collect();
        let mut handles = Vec::new();
        let mut tasks = Vec::new();
        let mut dirs = Vec::new();

        for i in 1..=n {
            let dir = temp_dir(&format!("n{i}"));
            let wal = WalStorage::open(
                &dir,
                WalOptions {
                    cluster_id: "wait-applied".into(),
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
            let config = RuntimeConfig {
                self_raft_id: i,
                self_node_id: me,
                peers: peers_of(i, n),
                addresses: addrs.clone(),
                raft: RaftNodeConfig::from_profile(&profile),
                profile: profile.clone(),
                metrics: Arc::clone(&metrics[(i - 1) as usize]),
            };
            let (runtime, handle) =
                Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
            tasks.push(tokio::spawn(runtime.run()));
            handles.push(handle);
            dirs.push(dir);
        }
        for i in 0..n as usize {
            for j in 0..n as usize {
                if i != j {
                    handles[i].register_peer(handles[j].clone());
                }
            }
        }
        Self {
            factory,
            handles,
            metrics,
            tasks,
            dirs,
        }
    }

    /// Wait until every node agrees on one leader; return its 0-based index.
    async fn leader(&self) -> usize {
        for _ in 0..800 {
            let ids: Vec<u64> = self.metrics.iter().map(|m| m.leader_id()).collect();
            if ids[0] != 0 && ids.iter().all(|i| *i == ids[0]) {
                return (ids[0] - 1) as usize;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        panic!("the cluster never agreed on a leader");
    }

    async fn wait_value(&self, handle: &Handle, key: &[u8], expected: &[u8]) {
        for _ in 0..600 {
            if handle.get_stale(key).await.expect("stale read").as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        panic!("node never served {key:?}={expected:?}");
    }

    async fn shutdown(self) {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks {
            let _ = task.await;
        }
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Poll a write until it commits, retrying transient "not ready yet" errors.
async fn until_put_with_index(handle: &Handle, key: &[u8], value: &[u8]) -> u64 {
    for _ in 0..600 {
        match handle.put_with_index(key, value).await {
            Ok(index) => return index,
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. }) => {
                tokio::time::sleep(core::time::Duration::from_millis(2)).await;
            }
            Err(e) => panic!("put_with_index failed: {e}"),
        }
    }
    panic!("put_with_index never committed");
}

// ---- 4. local on a minority follower ---------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn is_local_on_a_minority_follower() {
    let cluster = Cluster::spawn(3);
    let leader = cluster.leader().await;

    // A pre-partition value every node has applied, so the stale read has
    // something to serve after isolation.
    cluster.handles[leader]
        .put(b"k", b"before")
        .await
        .expect("pre-partition put");
    for handle in &cluster.handles {
        cluster.wait_value(handle, b"k", b"before").await;
    }

    // Isolate one follower completely (both directions): it is a minority of
    // one and can neither lead nor learn a new committed index.
    let follower = (0..3usize).find(|i| *i != leader).expect("a follower exists");
    let fid = (follower + 1) as u64;
    for j in 1..=3u64 {
        if j != fid {
            cluster.factory.firewall(node_id(fid), node_id(j));
            cluster.factory.firewall(node_id(j), node_id(fid));
        }
    }

    // Non-vacuous guard: the firewall actually drops traffic.
    let mut dropped = false;
    for _ in 0..1000 {
        if cluster.factory.firewall_drop_count() > 0 {
            dropped = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(
        dropped,
        "firewall never dropped traffic — the minority test would be vacuous"
    );

    // A write on the surviving majority produces an index the isolated follower
    // cannot apply. (Redirects keep this working even if leadership moved
    // inside the majority.)
    let unreachable = until_put_with_index(&cluster.handles[leader], b"k2", b"v2").await;
    assert!(unreachable >= 1);

    // `wait_applied` is local: no redirect, no quorum. The only acceptable
    // outcome is a clean `Timeout` — not `NotLeader`/`QuorumUnavailable`.
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        core::time::Duration::from_secs(2),
        cluster.handles[follower].wait_applied(unreachable, core::time::Duration::from_millis(100)),
    )
    .await
    .expect("a local wait must not hang");
    match result {
        Err(ArachneError::Timeout) => {}
        other => panic!(
            "minority follower wait must time out locally, got {other:?} \
             (a redirect or quorum error would mean it is not local)"
        ),
    }
    assert!(started.elapsed() < core::time::Duration::from_secs(2));

    // The weak read still works on the minority follower (no quorum).
    let stale = cluster.handles[follower]
        .multi_get(&[b"k", b"k2"], ReadConsistency::Stale)
        .await
        .expect("stale multi_get on a minority follower");
    assert_eq!(
        stale.values,
        vec![Some(b"before".to_vec()), None],
        "the isolated follower must still serve its local applied state"
    );

    cluster.shutdown().await;
}

// ---- 6. follower converges to the head index -------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_follower_waits_for_head_index() {
    let cluster = Cluster::spawn(3);
    let leader = cluster.leader().await;
    let followers: Vec<usize> = (0..3usize).filter(|i| *i != leader).collect();
    assert_eq!(followers.len(), 2);

    // Write through one follower (it redirects to the leader) and take the
    // effective index the leader returned.
    let index = until_put_with_index(&cluster.handles[followers[0]], b"k", b"v").await;

    // Another follower must converge to at least that index within the
    // propagation budget.
    let observed = cluster.handles[followers[1]]
        .wait_applied(index, core::time::Duration::from_secs(2))
        .await
        .expect("the follower must apply the head index");
    assert!(
        observed >= index,
        "follower reported {observed}, below the awaited index {index}"
    );

    // ---- failover cycle: kill the leader, re-elect, repeat ------------------
    // Abort the current leader's actor. `Cluster::shutdown` aborts again, which
    // is idempotent, and awaiting an already-aborted task is harmless.
    cluster.tasks[leader].abort();

    // Ignore the dead node's stale metrics: only the surviving nodes may elect.
    let alive: Vec<usize> = (0..3usize).filter(|i| *i != leader).collect();
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

    // With one node dead, the surviving majority is the new leader + exactly
    // one follower. Write via the new leader and wait on that follower.
    let alive_follower = alive
        .iter()
        .copied()
        .find(|i| *i != new_leader)
        .expect("the surviving majority has one follower");
    let failover_index = until_put_with_index(&cluster.handles[new_leader], b"k2", b"v2").await;
    let observed_after_failover = cluster.handles[alive_follower]
        .wait_applied(failover_index, core::time::Duration::from_secs(2))
        .await
        .expect("the surviving follower must apply the post-failover index");
    assert!(
        observed_after_failover >= failover_index,
        "surviving follower reported {observed_after_failover}, below the post-failover index \
         {failover_index}"
    );

    cluster.shutdown().await;
}
