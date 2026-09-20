//! Integration: the client-side redirect contract (propsol §3.3) on the lib
//! runtime actor, over a 3-node in-process cluster (in-memory transport).
//!
//! Proves:
//! * a **follower's** ordinary `Handle` follows the leader hint and the write
//!   commits on all three nodes — the in-process redirect path;
//! * a follower's **single-shot** handle ([`Handle::without_redirect`]) returns
//!   `NotLeader{hint}` verbatim — never a stale local value and never a collapse
//!   to `QuorumUnavailable`. This is the contract the node's HTTP surface turns
//!   into `409` + leader hint, which is what makes the documented
//!   `NotLeader → 409` mapping reachable in a **multi-process** deployment
//!   (M1 acceptance ③). A normal handle cannot demonstrate this because it
//!   swallows the hint and retries in-process.
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
        "arachne-redirect-{tag}-{}-{n}",
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

/// Distinct fake loopback addrs per node. They are never dialed (the transport
/// is in-process); they only populate `NotLeader{leader_hint}` so the client
/// can resolve the leader's `NodeId`/address.
fn addresses(n: u64) -> HashMap<NodeId, SocketAddr> {
    (1..=n)
        .map(|i| (node_id(i), format!("127.0.0.1:700{i}").parse().expect("addr")))
        .collect()
}

/// Poll `handle.put` until it succeeds, retrying the transient "not ready yet"
/// errors. Bounded so a stuck cluster fails loudly instead of hanging.
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

/// A 3-node in-process cluster over the in-memory transport, with every handle
/// registered as a peer of every other (client-side redirect enabled).
struct Cluster {
    handles: Vec<Handle>,
    metrics: Vec<Arc<Metrics>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    dirs: Vec<PathBuf>,
}

impl Cluster {
    fn spawn(n: u64) -> Self {
        let profile = ProfileConfig {
            heartbeat_interval_ms: 10,
            election_timeout_ms: 200,
            rpc_timeout_ms: 100,
            ..Profile::Lan.config()
        };
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
                    cluster_id: "redirect".into(),
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

        // Make every handle aware of its peers so the client-side redirect can
        // hop in-process (propsol §3.3).
        for i in 0..n as usize {
            for j in 0..n as usize {
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

    /// Wait until **every** node agrees on one leader; return
    /// `(leader index, follower index)`.
    async fn leader_and_follower(&self) -> (usize, usize) {
        for _ in 0..800 {
            let ids: Vec<u64> = self.metrics.iter().map(|m| m.leader_id()).collect();
            if ids[0] != 0 && ids.iter().all(|i| *i == ids[0]) {
                let leader = (ids[0] - 1) as usize;
                let follower = (0..self.handles.len())
                    .find(|i| *i != leader)
                    .expect("there must be a non-leader");
                return (leader, follower);
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        panic!("the cluster never agreed on a leader");
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

/// A follower's ordinary handle follows the hint: the write is redirected to
/// the leader and applied on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_handle_redirects_to_leader() {
    let cluster = Cluster::spawn(3);
    let (leader, follower) = cluster.leader_and_follower().await;
    assert_ne!(leader, follower);

    until_put(&cluster.handles[follower], b"k", b"v")
        .await
        .expect("redirected put must succeed");

    // Every node must hold the value *eventually*. Follower visibility is
    // asynchronous by design: the commit index reaches it on the leader's next
    // message, and its state machine is applied by its own task, so the honest
    // assertion is a bounded poll (a weak read may legitimately be stale — N1).
    for h in &cluster.handles {
        let mut seen = None;
        for _ in 0..400 {
            if let Ok(Some(v)) = h.get_stale(b"k").await {
                seen = Some(v);
                break;
            }
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
        assert_eq!(
            seen,
            Some(b"v".to_vec()),
            "every node's state machine must hold k => v"
        );
    }

    cluster.shutdown().await;
}

/// A single-shot handle (`without_redirect`) must return `NotLeader{hint}`
/// verbatim: a follower can neither accept a write nor serve a linearizable
/// read locally, and the hint must name the real leader and its address.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_shot_handle_returns_not_leader_hint() {
    let cluster = Cluster::spawn(3);
    let (leader, follower) = cluster.leader_and_follower().await;
    let single = cluster.handles[follower].without_redirect();

    let err = single
        .put(b"k", b"v")
        .await
        .expect_err("a follower cannot accept a write");
    match err {
        ArachneError::NotLeader {
            leader_hint: Some((id, addr)),
        } => {
            let leader_id = node_id((leader + 1) as u64);
            assert_eq!(id, leader_id, "the hint must name the current leader");
            assert_eq!(
                addr,
                addresses(3)[&leader_id],
                "the hint must carry the leader's configured address"
            );
        }
        other => panic!("expected NotLeader with a hint, got {other:?}"),
    }

    // A linearizable read is refused the same way (never a stale local value).
    let err = single
        .get(b"k")
        .await
        .expect_err("a follower cannot serve a linearizable read");
    assert!(
        matches!(err, ArachneError::NotLeader { .. }),
        "expected NotLeader, got {err:?}"
    );

    // The single-shot clone shares the inner state but not the redirect budget:
    // the ordinary handle still redirects and commits.
    until_put(&cluster.handles[follower], b"k", b"v")
        .await
        .expect("redirected put must still succeed");

    cluster.shutdown().await;
}
