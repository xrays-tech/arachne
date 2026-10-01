//! C3 (application-level read coalescing) — same-cycle linear reads share one
//! ReadIndex quorum round.
//!
//! Proves, end to end over the in-memory transport (single node, quorum = 1):
//! * `read_batch_shares_one_read_index_round`: four concurrent linear reads issued
//!   in the same drive cycle produce exactly **one** `node.read_index` call (one
//!   quorum round), and all four resolve with the committed value.
//! * `concurrent_linear_reads_resolve_correctly`: N concurrent linear reads all
//!   resolve correctly once the applied index advances (the `read_index <=
//!   applied` discipline is preserved under coalescing).
//!
//! The read-index round count is observed through the node's shared
//! `Metrics` registry (the thread-safe gauge the runtime refreshes each drive
//! cycle). C3 bumps it by one per *coalesced batch*, not per read.

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
use tokio::task::JoinSet;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-readbatch-{tag}-{}-{n}",
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
        .map(|i| (node_id(i), format!("127.0.0.1:{i}").parse().expect("addr")))
        .collect()
}

/// Poll `put` until it commits/applies (a successful return is committed + applied
/// through the leader's handle).
async fn until_put(handle: &Handle, key: &[u8], value: &[u8]) -> Result<(), ArachneError> {
    for _ in 0..400 {
        match handle.put(key, value).await {
            Ok(()) => return Ok(()),
            Err(ArachneError::Timeout) | Err(ArachneError::QuorumUnavailable)
                | Err(ArachneError::NotLeader { .. })
            => {
                tokio::time::sleep(core::time::Duration::from_millis(2)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(ArachneError::Timeout)
}

/// Poll `get` until it resolves, retrying transient not-ready errors.
async fn until_get(handle: &Handle, key: &[u8]) -> Result<Option<Vec<u8>>, ArachneError> {
    for _ in 0..400 {
        match handle.get(key).await {
            Ok(value) => return Ok(value),
            Err(ArachneError::Timeout) | Err(ArachneError::QuorumUnavailable)
                | Err(ArachneError::NotLeader { .. })
            => {
                tokio::time::sleep(core::time::Duration::from_millis(2)).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(ArachneError::Timeout)
}

struct Cluster {
    handles: Vec<Handle>,
    metrics: Vec<Arc<Metrics>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    dirs: Vec<PathBuf>,
}

impl Cluster {
    fn new(n: u64) -> Self {
        let profile = ProfileConfig {
            heartbeat_interval_ms: 10,
            election_timeout_ms: 200,
            rpc_timeout_ms: 100,
            ..Profile::Lan.config()
        };
        let factory = InMemoryTransportFactory::new();
        let addresses = addresses(n);
        let mut handles = Vec::new();
        let mut metrics = Vec::new();
        let mut tasks = Vec::new();
        let mut dirs = Vec::new();
        for i in 1..=n {
            let dir = temp_dir(&format!("n{i}"));
            let wal = WalStorage::open(
                &dir,
                WalOptions {
                    cluster_id: "readbatch".into(),
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
            let m = Arc::new(Metrics::new());
            let config = RuntimeConfig {
                self_raft_id: i,
                self_node_id: me,
                peers: peers_of(i, n),
                addresses: addresses.clone(),
                raft: RaftNodeConfig::from_profile(&profile),
                profile: profile.clone(),
                metrics: Arc::clone(&m),
            };
            let (runtime, handle) =
                Runtime::new(config, wal, tx, rx, &logger()).expect("build runtime");
            let task = tokio::spawn(runtime.run());
            tasks.push(task);
            handles.push(handle);
            metrics.push(m);
            dirs.push(dir);
        }
        // Client-side redirect support (no-op for a single node).
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

    /// Wait until at least one node reports itself leader.
    async fn wait_leader(&self) -> usize {
        for _ in 0..2_000 {
            if let Some(pos) = self.metrics.iter().position(|m| m.is_leader()) {
                return pos;
            }
            tokio::time::sleep(core::time::Duration::from_millis(2)).await;
        }
        panic!("no leader elected");
    }

    /// Abort the actor tasks and wipe the temp dirs.
    fn shutdown(&mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
        }
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

// A **single-threaded** (`current_thread`) runtime is deliberate here: it
// guarantees that the four reads are enqueued into the command channel in one
// poll before the consensus actor (which runs on the same thread) gets a
// chance to process the first one. The actor's burst drain then sees all four
// in a single command event and coalesces them into one ReadIndex round. On a
// multi-threaded runtime the actor can wake between any two sends, which is
// exactly what the pre-C3 per-read `read_index` used to tolerate, and would
// defeat the "same cycle" assertion.
#[tokio::test(flavor = "current_thread")]
async fn read_batch_shares_one_read_index_round() {
    let mut c = Cluster::new(1);
    let leader = c.wait_leader().await;
    let handle = c.handles[leader].clone();
    let metrics = c.metrics[leader].clone();

    // Seed a committed value to read back.
    until_put(&handle, b"seed", b"v").await.expect("seed put commits");

    let before = metrics.read_index_rounds();

    // Fire four linear reads concurrently (join! awaits them as one concurrent set).
    let (r0, r1, r2, r3) = tokio::join!(
        until_get(&handle, b"seed"),
        until_get(&handle, b"seed"),
        until_get(&handle, b"seed"),
        until_get(&handle, b"seed"),
    );
    for r in [r0, r1, r2, r3] {
        let value = r.expect("read must resolve");
        assert_eq!(value, Some(b"v".to_vec()), "a coalesced read must return the committed value");
    }

    // C3: all four reads issued in the same cycle share **one** ReadIndex round.
    let after = metrics.read_index_rounds();
    assert_eq!(
        after - before,
        1,
        "four same-cycle reads must share a single ReadIndex quorum round (got {} rounds)",
        after - before
    );

    c.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_linear_reads_resolve_correctly() {
    let mut c = Cluster::new(1);
    let leader = c.wait_leader().await;
    let handle = c.handles[leader].clone();

    const N: usize = 8;
    // Seed N distinct committed values.
    for i in 0..N {
        let key = format!("k{i}");
        let value = format!("v{i}");
        until_put(&handle, key.as_bytes(), value.as_bytes()).await.expect("seed put commits");
    }

    // Fire N concurrent linear reads, each tagged with its key index, and await
    // all of them.
    type ReadResult = (usize, Option<Vec<u8>>);
    let mut set: JoinSet<ReadResult> = JoinSet::new();
    for i in 0..N {
        let key = format!("k{i}").as_bytes().to_vec();
        let h = handle.clone();
        let idx = i;
        set.spawn(async move {
            let value = until_get(&h, &key).await.unwrap_or_default();
            (idx, value)
        });
    }
    let mut results: Vec<ReadResult> = Vec::new();
    while let Some(res) = set.join_next().await {
        match res {
            Ok(v) => results.push(v),
            Err(e) => panic!("read task failed: {e}"),
        }
    }
    results.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    for (idx, r) in &results {
        assert_eq!(
            *r,
            Some(format!("v{idx}").into()),
            "read k{idx} must return the committed value v{idx}"
        );
    }

    c.shutdown();
}
