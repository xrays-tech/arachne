//! Integration: ConfChange routing and durable membership
//! (propsol v0.2.16 rev S, S1a+S1b; M3 ①③ groundwork).
//!
//! Before rev S two things were true, and both are checked here:
//!
//! 1. A committed ConfChange entry was fed to the KV state machine like any
//!    other entry. Its payload is a ConfChange protobuf, so decoding it as a
//!    command fail-stops the node — the first membership change would have
//!    killed the cluster.
//! 2. Membership had no durable home at all, so a change was lost on restart
//!    and the node fell back to `initial_cluster` (here: to no voters at all,
//!    since these tests bootstrap a single node by declaration).
//!
//! The changes are proposed through `Handle::propose_conf_change_raw`
//! (feature `fault-injection`): the public `add_learner`/`promote_learner`/
//! `remove_member` surface with its single-flight gate is S2.
#![cfg(feature = "fault-injection")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::runtime::{Runtime, RuntimeConfig, RuntimeThread};
use arachne::storage::{FsyncPolicy, Storage, WalConfig, WalOptions, WalStorage};
use arachne::{ArachneError, Metrics, NodeId, Profile, ProfileConfig, TransportFactory};
use arachne_testsupport::InMemoryTransportFactory;
use raft::eraftpb::ConfChangeType;
use slog::{o, Drain, Logger};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-confchange-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 600,
        rpc_timeout_ms: 300,
        ..Profile::Lan.config()
    }
}

fn wal_options() -> WalOptions {
    wal_options_for(1)
}

fn wal_options_for(i: u64) -> WalOptions {
    WalOptions {
        cluster_id: "conf-change".into(),
        node_id: format!("n{i}"),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 0,
        fsync_observer: None,
    }
}

/// Start a single-voter cluster on `dir`, returning the running parts.
fn start(dir: &PathBuf) -> (Arc<Metrics>, arachne::client::Handle, RuntimeThread) {
    start_with_profile(dir, profile())
}

fn start_with_profile(
    dir: &PathBuf,
    profile: ProfileConfig,
) -> (Arc<Metrics>, arachne::client::Handle, RuntimeThread) {
    let wal = WalStorage::open(dir, wal_options()).expect("open wal");
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(NodeId::from("n1"));
    let metrics = Arc::new(Metrics::new());
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from("n1"),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: arachne::consensus::RaftNodeConfig::from_profile(&profile),
        profile: profile.clone(),
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
    let thread = runtime.spawn_dedicated().expect("spawn");
    (metrics, handle, thread)
}

async fn until_leader(metrics: &Metrics) {
    for _ in 0..400 {
        if metrics.is_leader() {
            return;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    panic!("the single node must elect itself");
}

/// The durable membership as it would be recovered on the next start.
fn recovered_conf_state(dir: &PathBuf) -> (u64, Vec<u64>, Vec<u64>) {
    recovered_conf_state_for(dir, 1)
}

fn recovered_conf_state_for(dir: &PathBuf, i: u64) -> (u64, Vec<u64>, Vec<u64>) {
    let wal = WalStorage::open(dir, wal_options_for(i)).expect("reopen wal");
    let state = wal.initial_state().expect("initial state");
    (
        wal.conf_change_index(),
        state.conf_state.voters,
        state.conf_state.learners,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_conf_change_is_routed_applied_and_survives_restart() {
    let dir = temp_dir("durable");
    let (metrics, handle, thread) = start(&dir);
    until_leader(&metrics).await;

    // Negative control, on a store no runtime owns: a log with entries but no
    // ConfChange recovers **no** membership, which is what makes the assertion
    // below about the change rather than about defaults.
    {
        let plain = temp_dir("control");
        let wal = WalStorage::open(&plain, wal_options()).expect("open");
        assert_eq!(wal.conf_change_index(), 0);
        assert!(wal.initial_state().expect("state").conf_state.voters.is_empty());
        drop(wal);
        let _ = std::fs::remove_dir_all(&plain);
    }

    // A normal write works before the change...
    handle.put(b"before", b"v1").await.expect("put before");

    // ...and the change is accepted, applied, and answered only once the new
    // configuration is durable.
    handle
        .propose_conf_change_raw(ConfChangeType::AddLearnerNode, 2)
        .await
        .expect("the membership change must commit and apply");

    // The real proof that the entry was routed around the state machine: a
    // write after it still works. Before rev S the ConfChange entry would have
    // reached the KV state machine and fail-stopped the node.
    handle
        .put(b"after", b"v2")
        .await
        .expect("the cluster must survive a ConfChange");
    assert_eq!(
        handle.get_stale(b"before").await.expect("read"),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        handle.get_stale(b"after").await.expect("read"),
        Some(b"v2".to_vec())
    );

    // Stop the actor so the storage lock is released, then look at what a
    // restart would recover.
    drop(handle);
    thread.shutdown();
    let (index, voters, learners) = recovered_conf_state(&dir);
    assert!(index > 0, "the ConfChange index must be durable");
    assert_eq!(voters, vec![1], "this node stays the only voter");
    assert_eq!(learners, vec![2], "node 2 was added as a learner");

    // Restart on the same directory: membership comes back from disk and the
    // replayed ConfChange entry is skipped rather than re-applied (S1b), so the
    // node elects, serves, and keeps the same configuration.
    let (metrics, handle, thread) = start(&dir);
    until_leader(&metrics).await;
    handle.put(b"restarted", b"v3").await.expect("put after restart");
    assert_eq!(
        handle.get_stale(b"restarted").await.expect("read"),
        Some(b"v3".to_vec())
    );
    drop(handle);
    thread.shutdown();
    assert_eq!(
        recovered_conf_state(&dir),
        (index, vec![1], vec![2]),
        "membership must be identical after a restart"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_conf_change_is_applied_between_the_commands_around_it() {
    // The ordering rule from S1b: nothing behind a ConfChange may be handed to
    // the state machine before the ConfChange itself is applied, or the KV view
    // and the membership view would disagree with the log order. Firing the
    // change and the following write concurrently makes it likely both land in
    // the same `Ready`, which is exactly the case the barrier exists for.
    let dir = temp_dir("ordering");
    let (metrics, handle, thread) = start(&dir);
    until_leader(&metrics).await;

    handle.put(b"k1", b"v1").await.expect("first write");
    let index_before = metrics.commit_index();

    let change = handle.propose_conf_change_raw(ConfChangeType::AddLearnerNode, 2);
    let write = handle.put(b"k2", b"v2");
    let (change, write) = tokio::join!(change, write);
    change.expect("the change applies");
    write.expect("the write behind it applies too");

    // Both writes are visible, so the entry behind the change was not lost.
    assert_eq!(
        handle.get_stale(b"k1").await.expect("read"),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        handle.get_stale(b"k2").await.expect("read"),
        Some(b"v2".to_vec())
    );

    drop(handle);
    thread.shutdown();

    // The change sits exactly between the two writes in the log, and the
    // durable membership records that index — which is only possible if the
    // change was applied after `k1` and before `k2`.
    let (index, voters, learners) = recovered_conf_state(&dir);
    assert_eq!(
        index,
        index_before + 1,
        "the change is the entry right after the first write"
    );
    assert_eq!(voters, vec![1]);
    assert_eq!(learners, vec![2]);
    assert!(
        metrics.applied_index() > index,
        "the write behind the change was applied after it"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// S2: the public membership API — single-flight gate, transfer, removal
// ---------------------------------------------------------------------------

/// A simulated slow disk widens the window in which a membership change is
/// "in flight" (proposed, committed, not yet applied) so the gate can be
/// exercised without racing the actor. Large enough to be unmissable, small
/// enough that the change still applies well inside `propose_timeout`.
const SLOW_FLUSH_MS: u64 = 120;

/// Start a single-voter cluster whose disk is slow, returning the handle.
fn start_slow(dir: &PathBuf) -> (Arc<Metrics>, arachne::client::Handle, RuntimeThread) {
    let profile = profile();
    let mut wal = WalStorage::open(dir, wal_options()).expect("open wal");
    wal.set_flush_delay_ms(SLOW_FLUSH_MS);
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(NodeId::from("n1"));
    let metrics = Arc::new(Metrics::new());
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from("n1"),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: arachne::consensus::RaftNodeConfig::from_profile(&profile),
        profile: profile.clone(),
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
    let thread = runtime.spawn_dedicated().expect("spawn");
    (metrics, handle, thread)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_membership_change_is_rejected_while_one_is_in_flight() {
    // propsol §5.3 hard constraint 1: at most one outstanding ConfChange, and
    // ordinary writes are explicitly unaffected by it.
    let dir = temp_dir("gate");
    let (metrics, handle, thread) = start_slow(&dir);
    until_leader(&metrics).await;

    // Fire both changes at once: the first holds the gate for at least
    // SLOW_FLUSH_MS, so the second must be rejected rather than queued. The
    // write in the same batch is there to prove the gate does not shed normal
    // traffic.
    let (first, second, write) = tokio::join!(
        handle.add_learner(2),
        handle.add_learner(3),
        handle.put(b"k", b"v")
    );
    write.expect("a normal write must not be affected by a pending ConfChange");

    let (accepted, rejected) = match (first, second) {
        (Ok(()), Err(e)) => (2, e),
        (Err(e), Ok(())) => (3, e),
        (Ok(()), Ok(())) => panic!("both membership changes were accepted: the gate is missing"),
        (Err(a), Err(b)) => panic!("both were rejected: {a} / {b}"),
    };
    assert!(
        matches!(rejected, ArachneError::ConfChangePending),
        "the loser must be told a change is pending, got {rejected:?}"
    );
    assert!(accepted == 2 || accepted == 3);

    // Once the change has applied, the gate is free again.
    handle
        .add_learner(4)
        .await
        .expect("a change is accepted once nothing is in flight");

    drop(handle);
    thread.shutdown();
    let (_, voters, learners) = recovered_conf_state(&dir);
    assert_eq!(voters, vec![1]);
    assert_eq!(
        learners.len(),
        2,
        "exactly two learners were added (one first, one after the gate cleared): {learners:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- A real 3-node cluster, for transfer and leader removal ---------------

const N: u64 = 3;

fn nid(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

struct ClusterNode {
    raft_id: u64,
    handle: arachne::client::Handle,
    metrics: Arc<Metrics>,
    dir: PathBuf,
    task: Option<tokio::task::JoinHandle<()>>,
}

/// Spawn one node of an `n`-node cluster.
///
/// `bootstrap_voters` is the configuration the node should *start* with; it is
/// separate from the transport peers (`1..=n` minus self) because an existing
/// node has to be able to send raft messages to a joiner long before that
/// joiner is a voter (rev S S5).
async fn spawn_cluster_node_with(
    i: u64,
    n: u64,
    bootstrap_voters: Vec<u64>,
    join_as_learner: bool,
    factory: &InMemoryTransportFactory,
    addresses: &HashMap<NodeId, SocketAddr>,
    profile: &ProfileConfig,
) -> ClusterNode {
    let dir = temp_dir(&format!("n{i}"));
    let mut last = String::new();
    let mut opened = None;
    for _ in 0..200 {
        match WalStorage::open(&dir, wal_options_for(i)) {
            Ok(wal) => {
                opened = Some(wal);
                break;
            }
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(10)).await;
            }
        }
    }
    let wal = opened.unwrap_or_else(|| panic!("node {i} could not open its WAL: {last}"));

    let (tx, rx) = factory.create(nid(i));
    let metrics = Arc::new(Metrics::new());
    let peers = (1..=n).filter(|j| *j != i).map(|j| (j, nid(j))).collect();
    let mut raft = arachne::consensus::RaftNodeConfig::from_profile(profile);
    raft.bootstrap_voters = Some(bootstrap_voters);
    raft.join_as_learner = join_as_learner;
    let config = RuntimeConfig {
        self_raft_id: i,
        self_node_id: nid(i),
        peers,
        addresses: addresses.clone(),
        raft,
        profile: profile.clone(),
        metrics: Arc::clone(&metrics),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
    ClusterNode {
        raft_id: i,
        handle,
        metrics,
        dir,
        task: Some(tokio::spawn(runtime.run())),
    }
}

fn link_peers(nodes: &[ClusterNode]) {
    for i in 0..nodes.len() {
        for j in 0..nodes.len() {
            if i != j {
                nodes[i].handle.register_peer(nodes[j].handle.clone());
            }
        }
    }
}

/// Wait until every node agrees on one leader, and return its raft id.
async fn wait_for_leader(nodes: &[ClusterNode]) -> u64 {
    for _ in 0..800 {
        let leaders: Vec<u64> = nodes
            .iter()
            .filter(|n| n.metrics.is_leader())
            .map(|n| n.raft_id)
            .collect();
        if leaders.len() == 1 && nodes.iter().all(|n| n.metrics.leader_id() == leaders[0]) {
            return leaders[0];
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    panic!("the cluster must elect one leader");
}

/// Wait until every listed node reports `leader` — a liveness property, not an
/// instant: a node that just handed leadership away clears its own view of the
/// leader until the new leader's first message reaches it.
async fn wait_until_all_agree_on(nodes: &[ClusterNode], leader: u64) {
    for _ in 0..800 {
        if nodes.iter().all(|n| n.metrics.leader_id() == leader) {
            return;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    panic!("the cluster must converge on leader {leader}");
}

async fn until_put(handle: &arachne::client::Handle, key: &[u8], value: &[u8]) {
    for _ in 0..800 {
        match handle.put(key, value).await {
            Ok(()) => return,
            Err(ArachneError::Timeout)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. }) => {
                tokio::time::sleep(core::time::Duration::from_millis(2)).await;
            }
            Err(e) => panic!("a write must not fail like this: {e}"),
        }
    }
    panic!("the write never went through");
}

// Two worker threads, not four: these tests already run several actor threads
// of their own, and CI runs this file's tests in parallel on a small runner.
async fn spawn_cluster_node(
    i: u64,
    factory: &InMemoryTransportFactory,
    addresses: &HashMap<NodeId, SocketAddr>,
    profile: &ProfileConfig,
) -> ClusterNode {
    spawn_cluster_node_with(i, N, (1..=N).collect(), false, factory, addresses, profile).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leadership_moves_and_a_leader_can_be_removed() {
    let profile = profile();
    let factory = InMemoryTransportFactory::new();
    let addresses: HashMap<NodeId, SocketAddr> = (1..=N)
        .map(|i| (nid(i), SocketAddr::from(([127, 0, 0, 1], 7200 + i as u16))))
        .collect();

    let mut nodes: Vec<ClusterNode> = Vec::new();
    for i in 1..=N {
        nodes.push(spawn_cluster_node(i, &factory, &addresses, &profile).await);
    }
    link_peers(&nodes);
    let leader = wait_for_leader(&nodes).await;

    // 1. An explicit transfer to a chosen voter. The reply must mean the
    //    leadership *actually* moved, not that raft accepted a message.
    let target = (1..=N).find(|i| *i != leader).expect("a second voter exists");
    nodes[(leader - 1) as usize]
        .handle
        .transfer_leader(target)
        .await
        .expect("the transfer must complete");
    assert!(
        nodes[(target - 1) as usize].metrics.is_leader(),
        "the target must be leading once the transfer resolves"
    );
    wait_until_all_agree_on(&nodes, target).await;

    // 2. Removing the current leader is the two-step sequence: transfer, then
    //    propose the removal (propsol §5.3 hard constraint 3).
    nodes[(target - 1) as usize]
        .handle
        .remove_member(target)
        .await
        .expect("removing a leader must hand over first and then succeed");

    // 3. Two voters still form a quorum, so the cluster keeps serving.
    let survivor = (1..=N).find(|i| *i != target).expect("a survivor exists");
    until_put(&nodes[(survivor - 1) as usize].handle, b"after", b"removal").await;

    // 4. The removed node is out: it stops receiving commits. It is also
    //    decommissioned here (an out-of-configuration node still running would
    //    keep starting elections at ever-higher terms, which no test needs).
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    let removed_commit = nodes[(target - 1) as usize].metrics.commit_index();
    if let Some(task) = nodes[(target - 1) as usize].task.take() {
        task.abort();
    }
    for _ in 0..5 {
        until_put(&nodes[(survivor - 1) as usize].handle, b"more", b"writes").await;
    }
    assert_eq!(
        nodes[(target - 1) as usize].metrics.commit_index(),
        removed_commit,
        "a removed member must not see new commits"
    );

    // 5. Durable post-mortem: the surviving configuration really has two
    //    voters and no longer contains the removed node.
    for n in nodes.iter_mut() {
        if let Some(task) = n.task.take() {
            task.abort();
        }
    }
    let dir = nodes[(survivor - 1) as usize].dir.clone();
    let (_, voters, _) = recovered_conf_state_for(&dir, survivor);
    assert_eq!(voters.len(), 2, "the configuration shrank to two voters");
    assert!(
        !voters.contains(&target),
        "the removed node must be gone from the durable configuration: {voters:?}"
    );

    for n in &nodes {
        let _ = std::fs::remove_dir_all(&n.dir);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_learner_that_never_answered_cannot_be_promoted() {
    // propsol §5.3 hard constraint 2: promote only a learner that is online and
    // caught up. A learner that does not exist can never satisfy either half,
    // which makes the gate observable without standing up a fourth node (the
    // positive path lands with the real joiner in S5).
    let dir = temp_dir("promote-gate");
    let (metrics, handle, thread) = start(&dir);
    until_leader(&metrics).await;

    handle
        .add_learner(2)
        .await
        .expect("a learner can always be added");

    match handle.promote_learner(2).await {
        Err(ArachneError::LearnerNotCaughtUp { behind, threshold }) => {
            assert_eq!(threshold, profile().promote_lag_entries);
            assert!(
                behind > 0,
                "a learner that never answered is behind by the whole log"
            );
        }
        other => panic!("expected LearnerNotCaughtUp, got {other:?}"),
    }

    // The other half of the discipline: promoting something that is not a
    // learner at all is refused, so a brand-new voter cannot skip the learner
    // phase.
    match handle.promote_learner(9).await {
        Err(ArachneError::InvalidArgument(message)) => {
            assert!(message.contains("not a learner"), "unexpected message: {message}");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    // The cluster is untouched: writes still work, and the durable membership
    // still has node 2 as a learner only.
    handle.put(b"k", b"v").await.expect("writes still work");
    drop(handle);
    thread.shutdown();
    let (_, voters, learners) = recovered_conf_state(&dir);
    assert_eq!(voters, vec![1], "nothing was promoted");
    assert_eq!(learners, vec![2], "the learner is still a learner");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_snapshot_carries_the_live_membership() {
    // rev S S4. A locally created snapshot is what rebuilds a node later, so it
    // must record the configuration the cluster agreed on — learners included.
    // Before this, it recorded the static bootstrap voters (and no learners at
    // all), so restoring from it would silently forget every membership change.
    let dir = temp_dir("snapshot-membership");
    let profile = ProfileConfig {
        // Small enough that a couple of writes trigger a local snapshot.
        snapshot_threshold_bytes: 256,
        ..profile()
    };
    let (metrics, handle, thread) = start_with_profile(&dir, profile);
    until_leader(&metrics).await;

    handle
        .add_learner(2)
        .await
        .expect("the learner must be added before the snapshot");

    // Cross the snapshot threshold.
    let payload = vec![b'x'; 128];
    for i in 0..8u8 {
        handle
            .put(b"k", &payload)
            .await
            .unwrap_or_else(|e| panic!("write {i} failed: {e}"));
    }

    // Wait for the snapshot file to appear.
    let mut found = false;
    for _ in 0..400 {
        if std::fs::read_dir(&dir)
            .expect("read data dir")
            .filter_map(|e| e.ok())
            .any(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with("snapshot-") && name.ends_with(".snap")
            })
        {
            found = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(found, "the snapshot trigger must fire");

    drop(handle);
    thread.shutdown();

    // The snapshot on disk carries the membership, not the bootstrap set.
    let wal = WalStorage::open(&dir, wal_options()).expect("reopen wal");
    let snapshot = wal
        .snapshot()
        .expect("read snapshot")
        .expect("a snapshot exists");
    assert_eq!(
        snapshot.meta.conf_state.learners,
        vec![2],
        "the snapshot must carry the learner"
    );
    assert_eq!(snapshot.meta.conf_state.voters, vec![1]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Wait for a condition that polls the nodes' published state.
async fn wait_until(mut cond: impl FnMut() -> bool, what: &str) {
    for _ in 0..800 {
        if cond() {
            return;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_node_joins_as_a_learner_is_promoted_and_a_member_is_removed() {
    // M3 acceptance ①: the whole node-replacement flow — add_learner → catch up
    // → promote → remove — must keep quorum alive and service uninterrupted.
    const TOTAL: u64 = 4;
    let profile = profile();
    let factory = InMemoryTransportFactory::new();
    let addresses: HashMap<NodeId, SocketAddr> = (1..=TOTAL)
        .map(|i| (nid(i), SocketAddr::from(([127, 0, 0, 1], 7300 + i as u16))))
        .collect();
    let voters: Vec<u64> = (1..=3).collect();

    // Three voters, plus a fourth node that starts as a *learner*: it is
    // reachable (its address is configured everywhere) but not in the voting
    // configuration, which is what `join_as_learner` declares.
    let mut nodes: Vec<ClusterNode> = Vec::new();
    for i in 1..=3 {
        nodes.push(
            spawn_cluster_node_with(i, TOTAL, voters.clone(), false, &factory, &addresses, &profile)
                .await,
        );
    }
    nodes.push(
        spawn_cluster_node_with(4, TOTAL, voters.clone(), true, &factory, &addresses, &profile).await,
    );
    link_peers(&nodes);

    // The voting cluster elects a leader; the joiner is not part of it yet.
    let leader = wait_for_leader(&nodes[..3]).await;
    let leader_handle = nodes[(leader - 1) as usize].handle.clone();
    until_put(&leader_handle, b"before", b"join").await;

    // 1. Add it as a learner. It does not vote, so quorum is unchanged.
    leader_handle
        .add_learner(4)
        .await
        .expect("the learner must be added");

    // 2. It receives the log and catches up while the cluster keeps serving.
    for i in 0..20u64 {
        until_put(&leader_handle, b"load", format!("v{i}").as_bytes()).await;
    }
    let target = nodes[(leader - 1) as usize].metrics.commit_index();
    wait_until(
        || nodes[3].metrics.applied_index() >= target,
        "the learner to catch up",
    )
    .await;

    // 3. Promote it: it has answered and is no longer behind, which is exactly
    //    what the promotion gate requires.
    leader_handle
        .promote_learner(4)
        .await
        .expect("a caught-up learner must be promotable");

    // 4. Service continues with four voters.
    until_put(&leader_handle, b"after", b"promote").await;

    // 5. Remove the current leader: the transfer-then-remove sequence must work
    //    and leave the cluster serving.
    let current = wait_for_leader(&nodes).await;
    nodes[(current - 1) as usize]
        .handle
        .remove_member(current)
        .await
        .expect("removing a leader must hand over first and then succeed");
    let survivor = (1..=TOTAL).find(|i| *i != current).expect("a survivor exists");
    until_put(&nodes[(survivor - 1) as usize].handle, b"after", b"removal").await;

    // 6. Durable post-mortem: three voters, no learners, removed node gone.
    for n in nodes.iter_mut() {
        if let Some(task) = n.task.take() {
            task.abort();
        }
    }
    let dir = nodes[(survivor - 1) as usize].dir.clone();
    let (_, voters_now, learners_now) = recovered_conf_state_for(&dir, survivor);
    assert_eq!(voters_now.len(), 3, "three voters remain: {voters_now:?}");
    assert!(
        !voters_now.contains(&current),
        "the removed node is gone: {voters_now:?}"
    );
    assert!(
        learners_now.is_empty(),
        "node 4 was promoted, so no learners remain: {learners_now:?}"
    );

    for n in &nodes {
        let _ = std::fs::remove_dir_all(&n.dir);
    }
}
