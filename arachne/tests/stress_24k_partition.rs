//! Phase 4 stress: 24k-key materialization on a 3-node in-memory cluster
//! across a minority partition (Fault A) and quorum loss (Fault B).
//!
//! This is the heavy integration stressed variant of the Phase 4 acceptance:
//! seed 24,000 entity keys (~200 B values) in 512-key `multi_put` batches,
//! publish a `head` token/index, then run concurrent materializers on every
//! node (2 tasks per node) that follow the head -> `wait_applied` ->
//! `multi_get(Stale)` pipeline.
//!
//! Fault A isolates one follower with `InMemoryTransportFactory::firewall`:
//! the majority keeps materializing a new head, while the minority still serves
//! its local `multi_get(Stale)` state and its `wait_applied` times out locally.
//! Heal the firewall and all three nodes must converge again with every entity
//! intact.
//!
//! Fault B kills two nodes (leader + one follower), leaving a minority of one:
//! writes and linearizable reads must fail with a documented error (never serve
//! stale local state), `multi_get(Stale)` still works, and `wait_applied` times
//! out locally. Restart the killed nodes and assert full recovery.
//!
//! Run explicitly (heavy, `#[ignore]`d):
//! `cargo test -p arachne-kv --test stress_24k_partition -- --ignored --nocapture`
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time imports there), so this file uses
//! `core::time::Duration` and `tokio::time::Instant`, never `std::time`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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

const N: usize = 3;
const ENTITY_KEYS: usize = 24_000;
/// Seed/read batch (256–512 per the acceptance request).
const BATCH: usize = 512;
const VALUE_BYTES: usize = 200;
/// Concurrent materializers per node.
const TASKS_PER_NODE: usize = 2;
/// One materialization issues one read command per batch (47 for 24k/512).
const READS_PER_MATERIALIZATION: u64 = ENTITY_KEYS.div_ceil(BATCH) as u64;
/// The acceptance bound for one materialization (≈50, never 24,000).
const MAX_READ_COMMANDS_PER_MATERIALIZATION: u64 = 50;
/// Majority materialization rounds after the partition.
const AFTER_FAULT_ROUNDS: usize = 2;

const HEAD: &[u8] = b"head";

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-stress24k-{tag}-{}-{n}",
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
    (0..n)
        .map(|i| {
            (
                node_id(i + 1),
                format!("127.0.0.1:73{:02}", i + 1).parse().expect("addr"),
            )
        })
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

fn wal_opts(i: u64) -> WalOptions {
    WalOptions {
        cluster_id: "stress-24k".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: WalConfig::default().segment_bytes,
        },
        created_at_millis: 0,
        fsync_observer: None,
    }
}

fn entity_key(i: usize) -> Vec<u8> {
    format!("entity{i:05}").into_bytes()
}

fn entity_value(i: usize) -> Vec<u8> {
    let mut v = format!("value-{i:05}:").into_bytes();
    assert!(v.len() <= VALUE_BYTES);
    v.resize(VALUE_BYTES, b'x');
    v
}

fn token(generation: u64) -> Vec<u8> {
    format!("gen-{generation}").into_bytes()
}

// ---- cluster harness -------------------------------------------------------

struct Cluster {
    factory: InMemoryTransportFactory,
    handles: Vec<Handle>,
    metrics: Vec<Arc<Metrics>>,
    /// `None` while the node is intentionally down (Fault B).
    tasks: Vec<Option<tokio::task::JoinHandle<()>>>,
    dirs: Vec<PathBuf>,
}

/// Build one node on `dir` and return `(task, handle, metrics)`.
async fn build_node(
    dir: &Path,
    i: u64,
    factory: &InMemoryTransportFactory,
    addrs: &HashMap<NodeId, SocketAddr>,
) -> (tokio::task::JoinHandle<()>, Handle, Arc<Metrics>) {
    let wal = open_wal(dir, i).await;
    let me = node_id(i);
    let (tx, rx) = factory.create(me.clone());
    let metrics = Arc::new(Metrics::new());
    let profile = profile();
    let config = RuntimeConfig {
        self_raft_id: i,
        self_node_id: me,
        peers: peers_of(i, N as u64),
        addresses: addrs.clone(),
        raft: RaftNodeConfig::from_profile(&profile),
        profile,
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
    (tokio::spawn(runtime.run()), handle, metrics)
}

/// Open a node's WAL, waiting for a killed predecessor's lock to release.
async fn open_wal(dir: &Path, i: u64) -> WalStorage {
    let mut last = String::new();
    for _ in 0..300 {
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

impl Cluster {
    async fn spawn() -> Self {
        let factory = InMemoryTransportFactory::new();
        let addrs = addresses(N as u64);
        let mut handles = Vec::new();
        let mut tasks = Vec::new();
        let mut dirs = Vec::new();
        let mut metrics = Vec::new();

        for i in 1..=N as u64 {
            let dir = temp_dir(&format!("n{i}"));
            let (task, handle, m) = build_node(&dir, i, &factory, &addrs).await;
            tasks.push(Some(task));
            handles.push(handle);
            dirs.push(dir);
            metrics.push(m);
        }
        link_peers(&handles);
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
        for _ in 0..2000 {
            let ids: Vec<u64> = self.metrics.iter().map(|m| m.leader_id()).collect();
            if ids[0] != 0 && ids.iter().all(|id| *id == ids[0]) {
                return (ids[0] - 1) as usize;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        panic!("the cluster never agreed on a leader");
    }

    /// Abort a node's actor and wait until its WAL lock is released.
    async fn kill(&mut self, node: usize) {
        if let Some(task) = self.tasks[node].take() {
            task.abort();
            let _ = task.await;
        }
    }

    /// Restart a previously killed node on its existing data dir and re-link it.
    async fn restart(&mut self, node: usize) {
        assert!(self.tasks[node].is_none(), "restart of a running node");
        let i = (node + 1) as u64;
        let dir = self.dirs[node].clone();
        let addrs = addresses(N as u64);
        let (task, handle, metrics) = build_node(&dir, i, &self.factory, &addrs).await;

        let others: Vec<Handle> = self
            .handles
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != node)
            .map(|(_, h)| h.clone())
            .collect();
        for other in &others {
            other.register_peer(handle.clone());
            handle.register_peer(other.clone());
        }

        self.metrics[node] = metrics;
        self.handles[node] = handle;
        self.tasks[node] = Some(task);
    }

    async fn shutdown(mut self) {
        for node in 0..N {
            self.kill(node).await;
        }
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn link_peers(handles: &[Handle]) {
    for i in 0..handles.len() {
        for j in 0..handles.len() {
            if i != j {
                handles[i].register_peer(handles[j].clone());
            }
        }
    }
}

// ---- convergence / verification helpers -----------------------------------

/// Wait until each node serves the expected head token/index and a sample of
/// entities (head convergence already implies the earlier entities were
/// applied in log order; the sample catches torn/incorrect state cheaply).
async fn wait_convergence(
    cluster: &Cluster,
    nodes: &[usize],
    token: &[u8],
    head_index: u64,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
) {
    let sample: Vec<usize> = (0..keys.len()).step_by(997).collect();

    for &node in nodes {
        let handle = &cluster.handles[node];

        let mut head_ok = false;
        for _ in 0..4000 {
            match handle.get_stale_with_index(HEAD).await {
                Ok(Some((value, index))) if value.as_slice() == token && index == head_index => {
                    head_ok = true;
                    break;
                }
                _ => tokio::time::sleep(core::time::Duration::from_millis(5)).await,
            }
        }
        assert!(
            head_ok,
            "node {node} never converged to head token {token:?} at index {head_index}"
        );

        let refs: Vec<&[u8]> = sample.iter().map(|i| keys[*i].as_slice()).collect();
        let mut sample_ok = false;
        for _ in 0..2000 {
            match handle.multi_get(&refs, ReadConsistency::Stale).await {
                Ok(result)
                    if result.values.len() == sample.len()
                        && sample.iter().enumerate().all(|(j, &idx)| {
                            result.values[j].as_deref() == Some(values[idx].as_slice())
                        }) =>
                {
                    sample_ok = true;
                    break;
                }
                _ => tokio::time::sleep(core::time::Duration::from_millis(5)).await,
            }
        }
        assert!(
            sample_ok,
            "node {node} never served the sampled entities correctly"
        );
    }
}

/// Read every entity through `multi_get(Stale)` and verify every value. Returns
/// the number of read commands issued.
async fn verify_all_entities(handle: &Handle, keys: &[Vec<u8>], values: &[Vec<u8>]) -> u64 {
    let mut calls = 0u64;
    for start in (0..keys.len()).step_by(BATCH) {
        let end = (start + BATCH).min(keys.len());
        let refs: Vec<&[u8]> = (start..end).map(|i| keys[i].as_slice()).collect();
        let result = handle
            .multi_get(&refs, ReadConsistency::Stale)
            .await
            .expect("multi_get batch");
        calls += 1;
        assert_eq!(
            result.values.len(),
            end - start,
            "a batch must return one value per requested key"
        );
        for (offset, got) in result.values.iter().enumerate() {
            assert_eq!(
                got.as_deref(),
                Some(values[start + offset].as_slice()),
                "wrong value for key {:?}",
                keys[start + offset]
            );
        }
    }
    calls
}

/// Verify a node's full local state (head token + every entity) via stale reads.
async fn verify_local_state(
    handle: &Handle,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    expected_token: &[u8],
    label: &str,
) {
    let head = handle
        .get_stale(HEAD)
        .await
        .unwrap_or_else(|e| panic!("{label}: head stale read failed: {e}"))
        .unwrap_or_else(|| panic!("{label}: head is absent"));
    assert_eq!(head, expected_token, "{label}: wrong head token");

    let calls = verify_all_entities(handle, keys, values).await;
    assert_eq!(
        calls, READS_PER_MATERIALIZATION,
        "{label}: one read command per batch"
    );
}

// ---- writes ----------------------------------------------------------------

async fn until_multi_put(handle: &Handle, entries: &[(&[u8], &[u8])]) {
    for _ in 0..800 {
        match handle.multi_put(entries).await {
            Ok(()) => return,
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. }) => {
                tokio::time::sleep(core::time::Duration::from_millis(2)).await;
            }
            Err(e) => panic!("multi_put failed: {e}"),
        }
    }
    panic!("multi_put never committed");
}

async fn until_put_with_index(handle: &Handle, key: &[u8], value: &[u8]) -> u64 {
    for _ in 0..800 {
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

// ---- materializers ---------------------------------------------------------

struct Materialization {
    node: usize,
    calls: u64,
    final_applied: u64,
    elapsed: core::time::Duration,
}

/// One materialization: head -> `wait_applied` -> batched stale reads, verifying
/// every entity value and that the observed applied index is monotone.
async fn materialize(
    node: usize,
    handle: Handle,
    expected_token: Vec<u8>,
    expected_head_index: u64,
    keys: Arc<Vec<Vec<u8>>>,
    values: Arc<Vec<Vec<u8>>>,
) -> Materialization {
    let started = tokio::time::Instant::now();

    // 1. The head the materializer is expected to materialize.
    let (head, head_index) = handle
        .get_stale_with_index(HEAD)
        .await
        .expect("head stale read")
        .expect("head must be present");
    assert_eq!(
        head, expected_token,
        "node {node}: materializer saw the wrong head token"
    );
    assert_eq!(
        head_index, expected_head_index,
        "node {node}: head origin index drifted"
    );

    // 2. Wait (locally) for the state machine to apply the head.
    let waited = handle
        .wait_applied(head_index, core::time::Duration::from_secs(2))
        .await
        .expect("wait_applied must resolve");
    assert!(
        waited >= head_index,
        "node {node}: wait_applied returned {waited} < {head_index}"
    );

    // 3. Read all entities in batches; one command per batch, values verified.
    let mut calls = 0u64;
    let mut applied = waited;
    for start in (0..keys.len()).step_by(BATCH) {
        let end = (start + BATCH).min(keys.len());
        let refs: Vec<&[u8]> = (start..end).map(|i| keys[i].as_slice()).collect();
        let result = handle
            .multi_get(&refs, ReadConsistency::Stale)
            .await
            .expect("multi_get batch");
        calls += 1;

        assert_eq!(result.values.len(), end - start);
        for (offset, got) in result.values.iter().enumerate() {
            assert_eq!(
                got.as_deref(),
                Some(values[start + offset].as_slice()),
                "node {node}: wrong value for key {:?}",
                keys[start + offset]
            );
        }
        assert!(
            result.applied_index >= head_index,
            "node {node}: batch applied {} < head {head_index}",
            result.applied_index
        );
        assert!(
            result.applied_index >= applied,
            "node {node}: applied index regressed ({} < {applied})",
            result.applied_index
        );
        applied = result.applied_index;
    }

    assert!(
        calls <= MAX_READ_COMMANDS_PER_MATERIALIZATION,
        "node {node}: one materialization issued {calls} read commands (> \
         {MAX_READ_COMMANDS_PER_MATERIALIZATION}); per-key fan-out?"
    );

    Materialization {
        node,
        calls,
        final_applied: applied,
        elapsed: started.elapsed(),
    }
}

/// Spawn `TASKS_PER_NODE` materializers on each node, join them, and assert the
/// `multi_get_commands` delta is exactly one command per batch per task.
async fn run_round(
    name: &str,
    cluster: &Cluster,
    nodes: &[usize],
    token: &[u8],
    head_index: u64,
    keys: &Arc<Vec<Vec<u8>>>,
    values: &Arc<Vec<Vec<u8>>>,
) {
    let before: Vec<u64> = nodes
        .iter()
        .map(|&node| cluster.metrics[node].multi_get_commands())
        .collect();

    let started = tokio::time::Instant::now();
    let mut tasks = Vec::new();
    for &node in nodes {
        for _ in 0..TASKS_PER_NODE {
            tasks.push(tokio::spawn(materialize(
                node,
                cluster.handles[node].clone(),
                token.to_vec(),
                head_index,
                Arc::clone(keys),
                Arc::clone(values),
            )));
        }
    }

    let mut stats = Vec::new();
    for task in tasks {
        stats.push(task.await.expect("materializer task panicked"));
    }
    let elapsed = started.elapsed();

    assert_eq!(
        stats.len(),
        nodes.len() * TASKS_PER_NODE,
        "{name}: wrong materializer count"
    );
    let max_applied = stats.iter().map(|s| s.final_applied).max().unwrap_or(0);
    let max_materialization = stats
        .iter()
        .map(|s| s.elapsed)
        .max()
        .unwrap_or_default();
    for stat in &stats {
        assert!(
            nodes.contains(&stat.node),
            "{name}: materializer reported an unexpected node {}",
            stat.node
        );
    }
    let calls: Vec<u64> = stats.iter().map(|s| s.calls).collect();

    // One command per batch per materialization.
    for &calls in &calls {
        assert_eq!(
            calls, READS_PER_MATERIALIZATION,
            "{name}: a materialization issued {calls} read commands, expected \
             {READS_PER_MATERIALIZATION}"
        );
    }

    // The actor counter must advance by exactly the materializers' calls: no
    // per-key fan-out and no hidden reads. Each node runs TASKS_PER_NODE
    // concurrent tasks, so its delta is 2 × 47 here (the per-materialization
    // bound is the 47 above, well under 50).
    let expected_per_node = TASKS_PER_NODE as u64 * READS_PER_MATERIALIZATION;
    let mut deltas = Vec::new();
    for (idx, &node) in nodes.iter().enumerate() {
        let after = cluster.metrics[node].multi_get_commands();
        let delta = after - before[idx];
        assert_eq!(
            delta, expected_per_node,
            "{name}: node {node} handled {delta} multi_get commands, expected \
             {expected_per_node} ({TASKS_PER_NODE} tasks × {READS_PER_MATERIALIZATION} batches)"
        );
        assert!(
            delta / TASKS_PER_NODE as u64 <= MAX_READ_COMMANDS_PER_MATERIALIZATION,
            "{name}: per-materialization command count exceeds the ~50 budget"
        );
        deltas.push((node, delta));
    }

    println!(
        "[{name}] nodes={nodes:?} tasks={} elapsed={elapsed:?} \
         max_materialization={max_materialization:?} \
         per_materialization_calls={READS_PER_MATERIALIZATION} node_deltas={deltas:?} \
         max_applied={max_applied}",
        stats.len()
    );
}

// ---- the stress test -------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "24k-key 3-node partition/quorum-loss stress (run with --ignored --nocapture)"]
async fn three_node_24k_partition_and_quorum_loss() {
    let keys = Arc::new((0..ENTITY_KEYS).map(entity_key).collect::<Vec<_>>());
    let values = Arc::new((0..ENTITY_KEYS).map(entity_value).collect::<Vec<_>>());

    let mut cluster = Cluster::spawn().await;
    let leader = cluster.leader().await;

    // ---- seed --------------------------------------------------------------
    let seed_started = tokio::time::Instant::now();
    for start in (0..keys.len()).step_by(BATCH) {
        let end = (start + BATCH).min(keys.len());
        let entries: Vec<(&[u8], &[u8])> = (start..end)
            .map(|i| (keys[i].as_slice(), values[i].as_slice()))
            .collect();
        until_multi_put(&cluster.handles[leader], &entries).await;
    }
    let token0 = token(0);
    let head0 = until_put_with_index(&cluster.handles[leader], HEAD, &token0).await;
    println!(
        "seed: {ENTITY_KEYS} keys ({} batches of {BATCH}) + head in {:?}",
        ENTITY_KEYS.div_ceil(BATCH),
        seed_started.elapsed()
    );

    let all: Vec<usize> = (0..N).collect();
    wait_convergence(&cluster, &all, &token0, head0, &keys, &values).await;
    println!("converged: all {N} nodes at head index {head0}");

    // ---- round 1: healthy --------------------------------------------------
    run_round("round-1 healthy", &cluster, &all, &token0, head0, &keys, &values).await;

    // ---- Fault A: isolate one follower -------------------------------------
    let isolated = (0..N).find(|i| *i != leader).expect("a follower exists");
    let majority: Vec<usize> = (0..N).filter(|i| *i != isolated).collect();
    assert_eq!(majority.len(), 2);

    let iid = (isolated + 1) as u64;
    for j in 1..=N as u64 {
        if j != iid {
            cluster.factory.firewall(node_id(iid), node_id(j));
            cluster.factory.firewall(node_id(j), node_id(iid));
        }
    }
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
        "Fault A: firewall never dropped traffic — the partition would be vacuous"
    );

    // Majority writes a new head generation.
    let token1 = token(1);
    let head1 = until_put_with_index(&cluster.handles[leader], HEAD, &token1).await;
    assert!(head1 > head0, "the new head must have a later index");
    wait_convergence(&cluster, &majority, &token1, head1, &keys, &values).await;

    for round in 0..AFTER_FAULT_ROUNDS {
        run_round(
            &format!("round-{} majority", round + 2),
            &cluster,
            &majority,
            &token1,
            head1,
            &keys,
            &values,
        )
        .await;
    }

    // Minority: local stale reads still work; local wait times out.
    verify_local_state(
        &cluster.handles[isolated],
        &keys,
        &values,
        &token0,
        "Fault A minority",
    )
    .await;
    let wait = tokio::time::timeout(
        core::time::Duration::from_secs(2),
        cluster.handles[isolated]
            .wait_applied(head1, core::time::Duration::from_millis(100)),
    )
    .await
    .expect("Fault A: minority wait_applied must not hang");
    match wait {
        Err(ArachneError::Timeout) => {}
        other => panic!(
            "Fault A: minority wait_applied must be a local Timeout, got {other:?} \
             (NotLeader/QuorumUnavailable would mean it is not local)"
        ),
    }
    println!("[Fault A] minority node {isolated}: local stale reads OK, wait_applied timed out");

    // Heal the partition and converge all three again.
    for j in 1..=N as u64 {
        if j != iid {
            cluster.factory.heal(node_id(iid), node_id(j));
            cluster.factory.heal(node_id(j), node_id(iid));
        }
    }
    wait_convergence(&cluster, &all, &token1, head1, &keys, &values).await;
    for &node in &all {
        verify_local_state(
            &cluster.handles[node],
            &keys,
            &values,
            &token1,
            &format!("Fault A recovery node {node}"),
        )
        .await;
    }
    println!("[Fault A] healed: all {N} nodes converged to head index {head1}");

    // ---- Fault B: quorum loss (kill two, survivor is a minority of one) ----
    let leader_now = cluster.leader().await;
    let survivor = (0..N)
        .find(|i| *i != leader_now)
        .expect("a follower survives");
    let killed: Vec<usize> = (0..N).filter(|i| *i != survivor).collect();
    assert_eq!(killed.len(), 2);
    for &node in &killed {
        cluster.kill(node).await;
    }
    // Give the survivor time to notice the loss of quorum.
    tokio::time::sleep(core::time::Duration::from_millis(300)).await;

    let survivor_handle = cluster.handles[survivor].clone();

    let put = tokio::time::timeout(
        core::time::Duration::from_secs(10),
        survivor_handle.put(b"blocked", b"write"),
    )
    .await
    .expect("Fault B: a write must not hang without a quorum");
    match put {
        Err(ArachneError::QuorumUnavailable) => {}
        other => panic!(
            "Fault B: write on a minority must report QuorumUnavailable, got {other:?}"
        ),
    }

    let get = tokio::time::timeout(
        core::time::Duration::from_secs(10),
        survivor_handle.get(HEAD),
    )
    .await
    .expect("Fault B: a linearizable read must not hang without a quorum");
    match get {
        Err(ArachneError::QuorumUnavailable) => {}
        other => panic!(
            "Fault B: linearizable read on a minority must report \
             QuorumUnavailable, got {other:?}"
        ),
    }

    // Weak reads still serve the survivor's local applied state.
    verify_local_state(
        &survivor_handle,
        &keys,
        &values,
        &token1,
        "Fault B survivor",
    )
    .await;

    let wait = tokio::time::timeout(
        core::time::Duration::from_secs(2),
        survivor_handle.wait_applied(head1 + 1, core::time::Duration::from_millis(100)),
    )
    .await
    .expect("Fault B: survivor wait_applied must not hang");
    match wait {
        Err(ArachneError::Timeout) => {}
        other => panic!("Fault B: survivor wait_applied must time out locally, got {other:?}"),
    }
    println!(
        "[Fault B] survivor node {survivor}: writes/linear reads QuorumUnavailable, \
         stale reads OK, wait_applied timed out"
    );

    // Restore the killed nodes and assert full recovery.
    for &node in &killed {
        cluster.restart(node).await;
    }
    let _ = cluster.leader().await;
    wait_convergence(&cluster, &all, &token1, head1, &keys, &values).await;
    for &node in &all {
        verify_local_state(
            &cluster.handles[node],
            &keys,
            &values,
            &token1,
            &format!("Fault B recovery node {node}"),
        )
        .await;
    }
    println!("[Fault B] restored: all {N} nodes recovered at head index {head1}");

    cluster.shutdown().await;
}
