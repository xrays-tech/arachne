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
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::runtime::{Runtime, RuntimeConfig, RuntimeThread};
use arachne::storage::{FsyncPolicy, Storage, WalConfig, WalOptions, WalStorage};
use arachne::{Metrics, NodeId, Profile, ProfileConfig, TransportFactory};
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
    WalOptions {
        cluster_id: "conf-change".into(),
        node_id: "n1".into(),
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
    let profile = profile();
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
    let wal = WalStorage::open(dir, wal_options()).expect("reopen wal");
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
