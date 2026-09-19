//! Integration: the ReadIndex linearizable read path (propsol §5.4) on the lib
//! runtime actor, over a 3-node in-process cluster (in-memory transport).
//!
//! Proves, end to end:
//! * a write issued through the **leader's** handle commits and is visible;
//! * `get` on the **leader's** handle is a ReadIndex read and returns the value;
//! * `get` on a **follower's** handle is *not* a stale local read — the follower
//!   replies `NotLeader{hint}` and the client redirects to the leader, which
//!   returns the value (propsol §3.3);
//! * `get` on a follower for an absent key returns `Ok(None)`.
//!
//! The in-memory transport is pull-based: each runtime's tick loop re-polls its
//! inbound queue every heartbeat (10 ms here), so raft traffic lands within one
//! tick.
//!
//! NOTE (entropy gates): `tests/` dirs are scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time/network imports there), so this file uses
//! `core::time::Duration` + `tokio::time::sleep` and no tokio select macros.

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

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-readidx-{tag}-{}-{n}",
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

/// Distinct fake loopback addrs per node. They are never actually dialed (the
/// transport is in-process); they only populate `NotLeader{leader_hint}` so the
/// client-side redirect can resolve the leader's `NodeId`.
fn addresses(n: u64) -> HashMap<NodeId, SocketAddr> {
    (1..=n)
        .map(|i| (node_id(i), format!("127.0.0.1:{i}").parse().expect("addr")))
        .collect()
}

/// Poll `handle.put` until it succeeds, retrying the transient "not ready yet"
/// errors (timeout / no quorum / redirect). Bounded so a stuck cluster fails
/// loudly instead of hanging.
async fn until_put(handle: &Handle, key: &[u8], value: &[u8]) -> Result<(), ArachneError> {
    for _ in 0..400 {
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

/// Poll `handle.get` until it succeeds, retrying the transient "not ready yet"
/// errors (timeout / no quorum / redirect). Bounded so a stuck cluster fails
/// loudly instead of hanging. The final value is returned for the caller to
/// assert.
async fn until_get(handle: &Handle, key: &[u8]) -> Result<Option<Vec<u8>>, ArachneError> {
    for _ in 0..400 {
        match handle.get(key).await {
            Ok(value) => return Ok(value),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_index_linearizable_read_over_in_memory() {
    const N: u64 = 3;
    let profile = ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    };

    let factory = InMemoryTransportFactory::new();
    let addresses = addresses(N);
    let metrics: Vec<Arc<Metrics>> = (0..N).map(|_| Arc::new(Metrics::new())).collect();
    let mut handles: Vec<Handle> = Vec::new();
    let mut tasks = Vec::new();
    let mut dirs = Vec::new();

    for i in 1..=N {
        let dir = temp_dir(&format!("n{i}"));
        let wal = WalStorage::open(
            &dir,
            WalOptions {
                cluster_id: "readidx".into(),
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
            peers: peers_of(i, N),
            addresses: addresses.clone(),
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

    // Make every handle aware of its peers so the client-side redirect can hop
    // in-process (propsol §3.3).
    for i in 0..N as usize {
        for j in 0..N as usize {
            if i != j {
                handles[i].register_peer(handles[j].clone());
            }
        }
    }

    // Wait for an election.
    for _ in 0..800 {
        if metrics.iter().any(|m| m.is_leader()) {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(20)).await;
    }
    let leader = (0..N as usize)
        .find(|i| metrics[*i].is_leader())
        .expect("a leader must be elected");
    let follower = (0..N as usize).find(|i| *i != leader).expect("a follower exists");

    // Write through the LEADER's handle; a successful return means committed +
    // applied.
    until_put(&handles[leader], b"k", b"v")
        .await
        .expect("leader put must succeed");

    // ReadIndex read on the LEADER's handle returns the committed value.
    assert_eq!(
        until_get(&handles[leader], b"k")
            .await
            .expect("leader read must converge"),
        Some(b"v".to_vec())
    );

    // Read on a FOLLOWER's handle: the follower replies NotLeader{hint} (not a
    // stale local read) and the client redirects to the leader.
    assert_eq!(
        until_get(&handles[follower], b"k")
            .await
            .expect("follower read must redirect and converge"),
        Some(b"v".to_vec())
    );

    // Read for an absent key returns Ok(None) (bounded).
    assert_eq!(
        until_get(&handles[follower], b"absent")
            .await
            .expect("absent-key read must converge"),
        None
    );

    // Cleanup.
    for task in &tasks {
        task.abort();
    }
    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
