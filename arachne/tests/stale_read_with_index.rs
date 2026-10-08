//! Integration: `Handle::get_stale_with_index` — the weak read that also
//! reports the value's **origin index** (the log index of the entry that wrote
//! it). Design doc `dev-docs/arachne-kv-commit-index-design.md` §6 (I2/I6/I8):
//!
//! * I2 — once a write is applied, every node's `get_stale_with_index`
//!   reports the *same* origin index (cross-node comparable; raft log index is
//!   the same everywhere);
//! * I6 — a deleted key is absent (`Ok(None)`), and a re-put carries an origin
//!   index above the whole history;
//! * I8 — `get_stale_with_index(key).0` equals `get_stale(key)` (same local
//!   read, additive API; existing semantics unchanged).
//!
//! Built on the in-memory transport (no tonic), mirroring `client_redirect.rs`.

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

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-index-{tag}-{}-{n}",
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
/// is in-process); they only populate `NotLeader{leader_hint}`.
fn addresses(n: u64) -> HashMap<NodeId, SocketAddr> {
    (1..=n)
        .map(|i| (node_id(i), format!("127.0.0.1:700{i}").parse().expect("addr")))
        .collect()
}

/// Poll `handle.put` until it succeeds, retrying transient "not ready yet"
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

/// Poll `handle.delete` until it succeeds (same transient-error retry policy).
async fn until_delete(handle: &Handle, key: &[u8]) -> Result<(), ArachneError> {
    for _ in 0..400 {
        match handle.delete(key).await {
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

/// A 3-node in-process cluster over the in-memory transport, every handle
/// registered as a peer of every other (client-side redirect enabled).
/// Mirror of `client_redirect.rs::Cluster`.
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
                    cluster_id: "index".into(),
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
            handles,
            metrics,
            tasks,
            dirs,
        }
    }

    /// Wait until **every** node agrees on one leader; return its index.
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

    /// Poll a node's `get_stale_with_index` until it returns `Some((value, _))`
    /// with the given value, then return it. `get_stale` may be arbitrarily
    /// stale, so a bounded poll is the test-fashioned await (design doc §8).
    async fn wait_stale_with_index(
        &self,
        h: &Handle,
        key: &[u8],
        expected: &[u8],
    ) -> (Vec<u8>, u64) {
        for _ in 0..400 {
            match h.get_stale_with_index(key).await {
                Ok(Some((v, i))) if v.as_slice() == expected => return (v, i),
                _ => {
                    tokio::time::sleep(core::time::Duration::from_millis(5)).await;
                }
            }
        }
        panic!("node never served {key:?}={expected:?} via get_stale_with_index");
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

/// I2 + I8: a write lands on every node with the **same** origin index, and
/// the value side equals what `get_stale` returns on the same node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn origin_index_agrees_across_nodes() {
    let cluster = Cluster::spawn(3);
    let leader = cluster.leader().await;

    until_put(&cluster.handles[leader], b"k", b"v")
        .await
        .expect("put must succeed");

    let leader_origin = {
        let (v, i) = cluster
            .wait_stale_with_index(&cluster.handles[leader], b"k", b"v")
            .await;
        assert!(i >= 1, "present value must carry origin index >= 1");
        assert_eq!(v, vec![b'v']);
        i
    };

    // Every follower must converge to the exact same origin index.
    for (idx, node) in cluster.handles.iter().enumerate() {
        let (v, i) = cluster.wait_stale_with_index(node, b"k", b"v").await;
        assert_eq!(
            i, leader_origin,
            "node {idx} must report the same origin index as the leader"
        );
        assert_eq!(v, vec![b'v']);
    }

    // I8: the value side is identical to the plain stale read on the same node.
    let plain = cluster.handles[leader].get_stale(b"k").await.expect("get_stale");
    assert_eq!(plain, Some(vec![b'v']), "value must not drift from get_stale");

    cluster.shutdown().await;
}

/// I6: a deleted key is absent (`Ok(None)`), and a re-put carries an origin
/// index strictly above the one captured before the delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_then_reput_raises_origin_index() {
    let cluster = Cluster::spawn(3);
    let leader = cluster.leader().await;
    let handle = &cluster.handles[leader];

    until_put(handle, b"k", b"v1").await.expect("put v1");
    let (_, first_origin) = cluster.wait_stale_with_index(handle, b"k", b"v1").await;

    until_delete(handle, b"k").await.expect("delete");
    // The delete is linearizable → applied on the leader before it returns.
    assert_eq!(
        handle.get_stale_with_index(b"k").await.expect("read"),
        None,
        "a deleted key must read as absent"
    );

    until_put(handle, b"k", b"v2").await.expect("put v2");
    let (_, second_origin) = cluster.wait_stale_with_index(handle, b"k", b"v2").await;
    assert!(
        second_origin > first_origin,
        "re-put origin ({second_origin}) must exceed the pre-delete origin ({first_origin})"
    );

    cluster.shutdown().await;
}
