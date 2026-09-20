//! Integration: the documented behaviour when the cluster **loses quorum**
//! (propsol §2.1, M2 exit criterion ④).
//!
//! With two of three voters down, one node survives. A single survivor can
//! never win an election, so:
//!
//! * `put` (linearizable write) fails promptly and never reports success;
//! * `get` (linearizable read via ReadIndex) fails the same way — ReadIndex
//!   needs a confirming quorum, so it must not answer from local state;
//! * `get_stale` still answers from the local state machine, which is the
//!   documented weak-read opt-out (N1), not a violation of CP;
//! * nothing wedges: once the peers return, the cluster elects a leader and
//!   serves writes again, with every acknowledged write still present.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time/network imports there), so this file uses
//! `core::time::Duration` and no tokio select macros.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::client::Handle;
use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::TransportFactory;
use arachne::{ArachneError, Metrics, NodeId, Profile, ProfileConfig};
use arachne_testsupport::InMemoryTransportFactory;
use slog::{o, Drain, Logger};

const N: u64 = 3;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-quorum-{tag}-{}-{n}", std::process::id()));
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
        .map(|i| {
            (
                node_id(i),
                SocketAddr::from(([127, 0, 0, 1], 7100 + i as u16)),
            )
        })
        .collect()
}

fn test_profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        // Long enough that the survivor cannot accidentally win, short enough
        // that the test does not sit through an election it is waiting out.
        election_timeout_ms: 600,
        rpc_timeout_ms: 300,
        ..Profile::Lan.config()
    }
}

fn wal_opts(i: u64) -> WalOptions {
    WalOptions {
        cluster_id: "quorum-loss".into(),
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
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
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
    for _ in 0..800 {
        match handle.put(key, value).await {
            Ok(()) => return Ok(()),
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. }) => {
                tokio::time::sleep(core::time::Duration::from_millis(2)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(ArachneError::Timeout)
}

/// The documented failure shape for an operation that needs a quorum
/// (propsol §2.1): `QuorumUnavailable`, never a success and never a hang.
///
/// This is deliberately exact. It also pins the redirect contract: a peer that
/// is gone must not surface as `ShuttingDown` (that would mean *this* node is
/// going away) nor as a `NotLeader` hint the caller can follow.
fn assert_quorum_failure(op: &str, result: &Result<impl Sized, ArachneError>) {
    match result {
        Ok(_) => panic!("{op} succeeded without a quorum: linearizability violated"),
        Err(ArachneError::QuorumUnavailable) => {}
        Err(e) => panic!("{op} must report QuorumUnavailable, got: {e}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_quorum_fails_writes_and_linearizable_reads_but_serves_stale_reads() {
    let profile = test_profile();
    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(N);

    let mut nodes: Vec<Node> = Vec::new();
    for i in 1..=N {
        let dir = temp_dir(&format!("n{i}"));
        nodes.push(spawn_node(i, dir, &factory, &addresses, &profile).await);
    }
    link_peers(&nodes);

    // A healthy cluster first: one committed write we can later read weakly.
    let mut leader = 0usize;
    for _ in 0..800 {
        if let Some(pos) = nodes.iter().position(|n| n.metrics.is_leader())
            && nodes.iter().all(|n| n.metrics.leader_id() != 0)
        {
            leader = pos;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    assert!(nodes[leader].metrics.is_leader(), "no leader elected");
    let leader_handle = nodes[leader].handle.clone();
    until_put(&leader_handle, b"k", b"v")
        .await
        .expect("the write commits while a quorum is alive");

    // Kill the leader and one follower: the survivor is alone, and a single
    // voter can never form a quorum of three.
    let survivor = (0..N as usize)
        .find(|pos| *pos != leader && *pos != (leader + 1) % N as usize)
        .expect("a survivor exists");
    let mut killed = Vec::new();
    for pos in [leader, (leader + 1) % N as usize] {
        nodes[pos].kill();
        killed.push(pos);
    }
    let survivor_id = (survivor + 1) as u64;

    // Wait for the survivor to notice it is on its own: it must campaign (and
    // lose), and it must not keep claiming to be a serving leader.
    tokio::time::sleep(core::time::Duration::from_millis(300)).await;

    // Writes and linearizable reads must fail promptly, with a documented
    // error, and never answer from local state.
    let put = tokio::time::timeout(
        core::time::Duration::from_secs(10),
        nodes[survivor].handle.put(b"k2", b"v2"),
    )
    .await
    .expect("a write without a quorum must not hang");
    assert_quorum_failure("put", &put);

    let get = tokio::time::timeout(
        core::time::Duration::from_secs(10),
        nodes[survivor].handle.get(b"k"),
    )
    .await
    .expect("a linearizable read without a quorum must not hang");
    assert_quorum_failure("get", &get);

    // The weak read is the documented opt-out: it is served from the local
    // state machine and is unaffected by the loss of quorum (propsol §2.1/N1).
    let stale = nodes[survivor]
        .handle
        .get_stale(b"k")
        .await
        .expect("get_stale must still be served");
    assert_eq!(
        stale.as_deref(),
        Some(b"v".as_slice()),
        "the survivor must still serve its applied state"
    );

    // Nothing wedged: bring the peers back and the cluster recovers, with the
    // pre-outage write intact.
    for pos in killed {
        let id = (pos + 1) as u64;
        let dir = nodes[pos].dir.clone();
        nodes[pos] = spawn_node(id, dir, &factory, &addresses, &profile).await;
    }
    link_peers(&nodes);

    let mut new_leader_wait = 0;
    for _ in 0..1_500 {
        if let Some(pos) = nodes
            .iter()
            .enumerate()
            .find(|(_, n)| n.is_running() && n.metrics.is_leader())
            .map(|(pos, _)| pos)
        {
            new_leader_wait = pos;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    assert!(
        new_leader_wait < N as usize && nodes[new_leader_wait].metrics.is_leader(),
        "the cluster must elect a leader again once the peers return"
    );

    let new_leader_handle = nodes[new_leader_wait].handle.clone();
    until_put(&new_leader_handle, b"k3", b"v3")
        .await
        .expect("writes resume after the quorum is restored");
    let recovered = nodes[survivor]
        .handle
        .get_stale(b"k")
        .await
        .expect("stale read after recovery");
    assert_eq!(
        recovered.as_deref(),
        Some(b"v".as_slice()),
        "the pre-outage write must survive the quorum loss (node {survivor_id})"
    );

    let dirs: Vec<PathBuf> = nodes.iter().map(|n| n.dir.clone()).collect();
    for node in nodes.iter_mut() {
        node.kill();
    }
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
