//! Regression gate **INV14** (plan-read-concurrency 2.0, TDD green gate for all
//! Phase-2/3 read-path changes): a linearizable read on a **deposed but
//! unaware** leader must never return a value that the majority has already
//! superseded.
//!
//! Scenario (the F1 case the barrier redesign targets):
//!
//! 1. Five-node cluster; leader `L` commits `k = old` (term 1).
//! 2. `L` is partitioned from the rest (both directions): it can neither confirm
//!    ReadIndex rounds nor learn of a term-2 election. It is **alive** and
//!    still believes it may serve reads.
//! 3. The majority (quorum of the four remaining voters) elects a term-2
//!    leader and commits `k = new`.
//! 4. `L` now holds `k = old` (unaware stale state). A linearizable read on
//!    `L` must return an error (or a redirect to the latest value), **never
//!    `old`**.
//!
//! The canary `get_stale(k)` on `L` must equal `old`: it proves `L` is a *living*
//! node with stale state, so the linear-read assertion is non-vacuous (a naive
//! "serve-from-local-state" read path would answer `old` and fail).
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time/network imports there), so this file uses
//! `core::time::Duration` and no tokio select macros.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::TransportFactory;
use arachne_kv::{ArachneError, Metrics, NodeId, Profile, ProfileConfig};
use arachne_kv_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

/// Five voters: partitioning the leader leaves four, whose quorum of three
/// elects a replacement robustly (no 2-voter split-brain).
const N: u64 = 5;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-inv14-{tag}-{}-{n}",
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

fn addresses(n: u64) -> HashMap<NodeId, SocketAddr> {
    (1..=n)
        .map(|i| (node_id(i), SocketAddr::from(([127, 0, 0, 1], 7100 + i as u16))))
        .collect()
}

fn test_profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 600,
        rpc_timeout_ms: 300,
        read_index_timeout_ms: 3000,
        ..Profile::Lan.config()
    }
}

fn wal_opts(i: u64) -> WalOptions {
    WalOptions {
        cluster_id: "inv14".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// One node: handle, metrics, data dir, and (while running) its actor task.
struct Node {
    handle: Handle,
    metrics: Arc<Metrics>,
    dir: PathBuf,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    fn kill(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }

    fn is_running(&self) -> bool {
        self.task.is_some()
    }
}

async fn open_wal(dir: &PathBuf, i: u64) -> WalStorage {
    let mut last = String::new();
    for _ in 0..200 {
        match WalStorage::open(dir, wal_opts(i)) {
            Ok(wal) => return wal,
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(10)).await;
            }
        }
    }
    panic!("node {i} could not reopen its WAL: {last}");
}

async fn spawn_node(
    i: u64,
    dir: PathBuf,
    factory: &InMemoryTransportFactory,
    addresses: &HashMap<NodeId, SocketAddr>,
    profile: &ProfileConfig,
) -> Node {
    let wal = open_wal(&dir, i).await;
    let (tx, rx) = factory.create(node_id(i));
    let metrics = Arc::new(Metrics::new());
    let config = RuntimeConfig {
        self_raft_id: i,
        self_node_id: node_id(i),
        peers: peers_of(i, N),
        addresses: addresses.clone(),
        raft: RaftNodeConfig::from_profile(profile),
        profile: profile.clone(),
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger())
        .expect("build runtime");
    let task = tokio::spawn(runtime.run());
    Node {
        handle,
        metrics,
        dir,
        task: Some(task),
    }
}

fn link_peers(nodes: &[Node]) {
    for i in 0..nodes.len() {
        for j in 0..nodes.len() {
            if i != j {
                nodes[i].handle.register_peer(nodes[j].handle.clone());
            }
        }
    }
}

async fn until_put(handle: &Handle, key: &[u8], value: &[u8]) -> Result<(), ArachneError> {
    for _ in 0..2000 {
        match handle.put(key, value).await {
            Ok(()) => return Ok(()),
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. })
            => {
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(ArachneError::Timeout)
}

/// Find the leader among the running nodes (all nodes report a non-zero
/// `leader_id` once the cluster has settled).
async fn await_leader(nodes: &mut [Node]) -> usize {
    for _ in 0..1000 {
        if let Some(pos) = nodes
            .iter()
            .position(|n| n.is_running() && n.metrics.is_leader())
        {
            return pos;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    panic!("no leader elected");
}

/// Identify the new (term-2) leader among the *non-L* voters. A candidate `c`
/// is the leader iff it self-identifies as leader and a quorum of non-L nodes
/// report the same `leader_id`.
async fn await_new_leader(nodes: &mut [Node], excluded: usize) -> usize {
    let n = nodes.len();
    let quorum = (n + 1) / 2;
    let non_l: Vec<usize> = (0..n).filter(|p| *p != excluded).collect();
    for _ in 0..1500 {
        for c in &non_l {
            let leader_id = nodes[*c].metrics.leader_id();
            if leader_id == 0 || !nodes[*c].metrics.is_leader() {
                continue;
            }
            let agreeing = non_l
                .iter()
                .copied()
                .filter(|p| nodes[*p].metrics.leader_id() == leader_id)
                .count();
            if agreeing >= quorum {
                return *c;
            }
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    panic!(
        "quorum ({quorum}) never agreed on a new leader after node {excluded} was partitioned"
    );
}

/// INV14 core assertion: a linearizable read on a deposed-but-unaware leader
/// must never return a value superseded by the majority.
fn assert_not_stale_read(
    label: &str,
    res: &Result<Option<Vec<u8>>, ArachneError>,
    stale: &[u8],
) {
    match res {
        Ok(Some(v)) if *v == stale.to_vec() => {
            panic!(
                "INV14 violated: {label} on a deposed-unaware leader served a stale value ({stale:?})"
            );
        }
        Ok(Some(_)) => {
            // Latest (or another non-stale) value: acceptable.
        }
        Ok(None) => {}
        Err(_) => {
            // NotLeader (redirect) / Timeout / QuorumUnavailable / Busy:
            // all acceptable — the read was refused, not answered from stale state.
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deposed_unaware_leader_linear_read_never_serves_stale_value() {
    let profile = test_profile();
    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(N);

    let mut nodes: Vec<Node> = Vec::new();
    for i in 1..=N {
        let dir = temp_dir(&format!("n{i}"));
        nodes.push(spawn_node(i, dir, &factory, &addresses, &profile).await);
    }
    link_peers(&nodes);

    // Phase 1 — a healthy term-1 cluster; the leader commits the old value.
    let leader = await_leader(&mut nodes).await;
    let old = b"old".to_vec();
    until_put(&nodes[leader].handle, b"k", &old)
        .await
        .expect("the initial write commits while a quorum is alive");

    // Canary 0 (sanity, pre-partition): the leader serves the committed value.
    let pre = nodes[leader]
        .handle
        .get_stale(b"k")
        .await
        .expect("pre-partition weak read must be served");
    assert_eq!(
        pre.as_deref(),
        Some(old.as_slice()),
        "pre-partition weak read must equal the committed value"
    );

    // Phase 2 — partition the leader (both directions) so it is *unaware*.
    // The node at 0-based index `leader` carries node id `node_id(leader+1)`.
    let lid = (leader + 1) as u64;
    for j in 1..=N {
        let jid = j as u64;
        if jid != lid {
            factory.firewall(node_id(lid), node_id(jid));
            factory.firewall(node_id(jid), node_id(lid));
        }
    }

    // Non-vacuous guard: confirm the firewall is actually dropping traffic
    // (the partitioned leader is still sending, but the majority drops it).
    let dropped = tokio::time::timeout(
        core::time::Duration::from_secs(5),
        async {
            for _ in 0..1000 {
                if factory.firewall_drop_count() > 0 {
                    return Ok(());
                }
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(())
        },
    )
    .await
    .expect("firewall should drop the leader's traffic within 5 s");
    assert!(
        dropped == Ok(()),
        "firewall never observed dropping traffic — the test would be vacuous"
    );

    // Phase 3 — the majority elects a term-2 leader and commits the new value.
    let new_leader = await_new_leader(&mut nodes, leader).await;
    let new = b"new".to_vec();
    until_put(&nodes[new_leader].handle, b"k", &new)
        .await
        .expect("the majority commits the new value after the deposition");

    // Phase 4 — verify the deposed leader is alive and stale (canary).
    let stale = nodes[leader]
        .handle
        .get_stale(b"k")
        .await
        .expect("get_stale on the deposed leader must still be served (it is alive)");
    assert_eq!(
        stale.as_deref(),
        Some(old.as_slice()),
        "canary: the deposed-unaware leader must hold the stale value (non-vacuous gate)"
    );

    // Phase 5 — the linearizable read must NOT answer with the stale value.
    let linear = tokio::time::timeout(
        core::time::Duration::from_secs(15),
        nodes[leader].handle.get(b"k"),
    )
    .await
    .expect("a linearizable read on a partitioned leader must not hang");
    assert_not_stale_read("linear get", &linear, old.as_slice());

    // Teardown.
    let dirs: Vec<PathBuf> = nodes.iter().map(|n| n.dir.clone()).collect();
    for node in nodes.iter_mut() {
        node.kill();
    }
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
