//! TDD for 4.2: peer outbound messages are routed through per-peer **bounded
//! queues** driven by a background **sender task** — the actor tick no longer
//! `await`s each peer send inline (which, on the tonic edge, serializes every
//! heartbeat/replication message behind the slowest peer).
//!
//! This file exercises two *new* properties the inline-send design never had:
//!
//! 1. **Overflow degrades gracefully.** A slow outbound transport on the leader
//!    backs up its per-peer queue; excess messages are dropped and counted in
//!    `metrics.dropped_sends()`, and raft retransmits so the follower still
//!    converges. (The inline-send design has no queue and no drop accounting,
//!    so this test cannot pass on unmodified code.)
//! 2. **Delivery is unchanged.** The sender-task path must reproduce the old
//!    inline-send behaviour: all peers reached, follower catches up to the
//!    committed value. (A regression gate.)
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids the std clock as a literal substring). This file times with
//! `core::time::Duration` and `tokio::time` only.

use std::path::PathBuf;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::TransportFactory;
use arachne_kv::{Metrics, NodeId, Profile, ProfileConfig};
use arachne_kv_seam::seam::{ForwardTransport, Transport, TransportMessage};
use arachne_kv_testsupport::{
    InMemoryRx, InMemoryTransportFactory, InMemoryTx, TransportError,
};
use slog::{Discard, Drain, Logger, o};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-oq-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(Discard.fuse(), o!())
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

/// A transport that delays every send by `delay_ms` after wrapping an in-memory
/// sender half. The delay is what backs up the leader's bounded outbound queue
/// under a write burst, forcing overflow and drop accounting.
#[derive(Clone, Debug)]
struct SlowTx {
    inner: InMemoryTx,
    delay_ms: u64,
}

impl Transport for SlowTx {
    type Error = TransportError;

    fn send(
        &self,
        to: NodeId,
        msg: TransportMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = self.inner.clone();
        let delay = core::time::Duration::from_millis(self.delay_ms);
        async move {
            tokio::time::sleep(delay).await;
            inner.send(to, msg).await
        }
    }
}

// No remote forwarding support: this test double only models slow delivery.
impl ForwardTransport for SlowTx {}

/// Build one node's runtime + handle from a pre-built outbound transport and its
/// receiving half. Generic over `T` so the caller can pass either a fast
/// `InMemoryTx` or a slow `SlowTx` without an object-safe upcast.
fn make_node<T: Transport + Clone + ForwardTransport + std::fmt::Debug>(
    i: u64,
    n: u64,
    tx: T,
    rx: InMemoryRx,
) -> (Arc<Metrics>, Handle, arachne_kv::RuntimeThread) {
    let dir = temp_dir(&format!("n{i}"));
    let wal = WalStorage::open(
        &dir,
        WalOptions {
            cluster_id: "outbound-queue".into(),
            node_id: format!("n{i}"),
            config: WalConfig {
                fsync_policy: FsyncPolicy::Always,
                segment_bytes: 1 << 20,
            },
            created_at_millis: 1_700_000_000_000,
            fsync_observer: None,
        },
    )
    .expect("open wal");

    let m = Arc::new(Metrics::new());
    let profile = ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 1000,
        rpc_timeout_ms: 300,
        ..Profile::Lan.config()
    };
    let raft = RaftNodeConfig::from_profile(&profile);
    let config = RuntimeConfig {
        self_raft_id: i,
        self_node_id: node_id(i),
        peers: (1..=n).filter(|j| *j != i).map(|j| (j, node_id(j))).collect(),
        addresses: (1..=n)
            .map(|j| (
                node_id(j),
                std::net::SocketAddr::from(([127, 0, 0, 1], 7400 + j as u16)),
            ))
            .collect(),
        raft,
        profile: profile.clone(),
        metrics: Arc::clone(&m),
    };
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
    let thread = runtime.spawn_dedicated().expect("spawn the consensus thread");
    (m, handle, thread)
}

/// Build a 3-node cluster. `slow` wraps every node's outbound in a transport that
/// delays each send by `delay_ms` (the queue-overflow case); `false` uses the
/// plain in-memory sender (fast path). Returns (metrics, handles, live threads).
fn build_cluster(slow: bool, delay_ms: u64) -> (Vec<Arc<Metrics>>, Vec<Handle>, Vec<arachne_kv::RuntimeThread>) {
    const N: u64 = 3;
    let mut metrics: Vec<Arc<Metrics>> = Vec::with_capacity(N as usize);
    let mut handles: Vec<Handle> = Vec::with_capacity(N as usize);
    let mut threads: Vec<arachne_kv::RuntimeThread> = Vec::with_capacity(N as usize);
    let factory = InMemoryTransportFactory::new();
    for i in 1..=N {
        let (tx, rx) = factory.create(node_id(i));
        let (m, h, t) = if slow {
            make_node(i, N, SlowTx { inner: tx, delay_ms }, rx)
        } else {
            make_node(i, N, tx, rx)
        };
        metrics.push(m);
        handles.push(h);
        threads.push(t);
    }
    for i in 0..N as usize {
        for j in 0..N as usize {
            if i != j {
                handles[i].register_peer(handles[j].clone());
            }
        }
    }
    (metrics, handles, threads)
}

/// Wait until at least one node reports itself as leader.
async fn wait_for_leader(metricses: &[Arc<Metrics>]) -> usize {
    for _ in 0..800 {
        if let Some(pos) = metricses.iter().position(|m| m.is_leader())
        {
            return pos;
        }
        tokio::time::sleep(core::time::Duration::from_millis(10)).await;
    }
    assert!(metricses.iter().any(|m| m.is_leader()), "no leader elected");
    panic!("no leader elected");
}

/// Drive `writes` puts on the leader (all the same key/value), then wait until
/// `follower` observes the final value (the leader retransmits any message dropped
/// by the overflowing queue).
async fn drive_and_converge(
    leader: &Handle,
    follower: &Handle,
    writes: usize,
    key: &[u8],
    value: &[u8],
) {
    for _ in 0..writes {
        match leader.put(key, value).await {
            Ok(_) => {}
            // Under the slow-transport overload a put may legitimately exceed
            // its deadline; docs define Timeout as "result unknown" and raft
            // retransmits, so the convergence poll below is the real check.
            Err(e) if matches!(e, arachne_kv::ArachneError::Timeout) => {}
            Err(e) => panic!("put failed (unexpected): {e}"),
        }
    }
    let mut converged = false;
    for _ in 0..1200 {
        if follower
            .get_stale(key)
            .await
            .expect("get_stale")
            == Some(value.to_vec())
        {
            converged = true;
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(10)).await;
    }
    assert!(converged, "follower did not converge to the final value");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_queue_delivers_and_follower_catches_up() {
    // 3-node in-memory cluster; all outbound is the FAST in-memory path. The
    // NEW sender-task path must replicate the old inline-send behaviour: a write
    // committed by the leader must be observed by every follower.
    let (metrics, handles, _threads) = build_cluster(false, 0);
    let leader = wait_for_leader(&metrics).await;
    let leader_handle = handles[leader].clone();
    let value = b"caught-up-via-outbound-queue";
    let key = b"oq:seed";
    leader_handle.put(key, value).await.expect("put");
    // Every follower (non-leader) must observe the value via the new path.
    for i in 0..metrics.len() {
        if i != leader {
            drive_and_converge(&leader_handle, &handles[i], 1, key, value).await;
        }
    }
    assert_eq!(
        leader_handle.get(key).await.expect("leader get"),
        Some(value.to_vec())
    );
    let follower_idx = if leader == 0 { 1 } else { 0 };
    assert_eq!(
        handles[follower_idx].get(key).await.expect("follower get"),
        Some(value.to_vec())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_outbound_fills_queue_and_counts_drops() {
    // 3-node cluster where **every** node's outbound is deliberately SLOW (a
    // 25ms per-send delay). Whichever node is the leader carries the write
    // replication, so its per-peer queue overflows under a write burst:
    // messages are dropped and counted in `metrics.dropped_sends()`. Raft
    // retransmits the missing entries (it resends each `next_index` on every
    // tick regardless of whether the send "succeeded"), so the followers still
    // converge.
    //
    // The inline-send design (pre-4.2) has no queue and no drop accounting, so
    // this test is guaranteed to fail on unmodified code and is the TDD
    // regression gate for the new path.
    let (metrics, handles, _threads) = build_cluster(true, 25);
    let leader = wait_for_leader(&metrics).await;
    let leader_handle = handles[leader].clone();

    // Record the pre-burst drop count for every node (should be 0; only the
    // burst should add drops, and only on the node that leads during the burst).
    let pre_drops: Vec<u64> = metrics.iter().map(|m| m.dropped_sends()).collect();

    // A write burst fast enough to outrun the 25ms per-send delay on the leader's
    // outbound, backing up and overflowing its per-peer queue.
    let value = b"burst-value";
    let key = b"oq:burst";
    const WRITES: usize = 25;
    let mut timed_out = 0usize;
    for _ in 0..WRITES {
        match leader_handle.put(key, value).await {
            Ok(_) => {}
            // Under the very slow transport a put may rightfully exceed its
            // apply deadline: docs define `Timeout` as "result unknown", and the
            // burst goal is to overflow the queue (asserted by `drops_added`
            // below) while raft retransmits so followers converge (asserted by
            // `drive_and_converge`). A `Timeout` is the expected outcome here,
            // not a defect.
            Err(e) if matches!(e, arachne_kv::ArachneError::Timeout) => {
                timed_out += 1;
            }
            Err(e) => {
                eprintln!("[OQ-TEST] put failed: {e}");
                for (i, m) in metrics.iter().enumerate() {
                    eprintln!(
                        "[OQ-TEST] node {i}: dropped_sends={} applied={:?}",
                        m.dropped_sends(),
                        m.applied_index()
                    );
                }
                panic!("put failed: {e}");
            }
        }
    }
    // The burst just overflowed the leader's outbound queue; retransmissions may
    // still be in flight. Drive the followers to converge so the final value is
    // durable before asserting drop accounting.
    for i in 0..metrics.len() {
        if i != leader {
            drive_and_converge(&leader_handle, &handles[i], WRITES, key, value).await;
        }
    }

    // At least one node (the one that led during the burst) must have recorded
    // the overflow drops.
    let post_drops: Vec<u64> = metrics.iter().map(|m| m.dropped_sends()).collect();
    let drops_added: u64 = post_drops
        .iter()
        .zip(pre_drops.iter())
        .map(|(after, before)| after.saturating_sub(*before))
        .sum();
    assert!(
        drops_added > 0,
        "expected outbound-queue overflow drops under the slow transport, but no node reported a drop (pre: {pre_drops:?}, post: {post_drops:?})"
    );
}
