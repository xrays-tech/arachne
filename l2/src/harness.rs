//! The 3-node in-sim cluster over REAL tonic (M1 stage 3b).
//!
//! Builds three `turmoil` hosts (`n1/n2/n3`); each opens a real `WalStorage`,
//! a `TonicTransportFactory` driven by the deterministic [`TurmoilIo`] seam, and
//! a real Arachne `Runtime`. A driver client waits for a leader, `put`s one key
//! through the leader's [`Handle`], and reads it back from every node
//! (`get_stale`), recording a per-node observation. `run_three_node(seed)`
//! returns the recorded observations; two runs with the same seed must be
//! byte-identical.
//!
//! Everything runs on turmoil's seeded simulated network + clock, and the raft
//! election RNG is seeded via the stage-2 `raft::set_election_rng_seed` hook.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arachne::client::Handle;
use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{Metrics, NodeId, ProfileConfig, RaftId, TransportFactory};
use arachne_transport_tonic::TonicTransportFactory;
use slog::{o, Drain, Logger};

use crate::io::TurmoilIo;
use crate::net::SimNetwork;

/// The cluster identity every node and the transport agree on.
const CLUSTER_ID: &str = "l2";

/// One node's observed state after the scenario driver converged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeObservation {
    /// The node's raft id (1..=3).
    pub raft_id: RaftId,
    /// The leader this node reported when the observation was taken.
    pub leader_id: u64,
    /// The value this node returned for the test key (`None` if never seen).
    pub value: Option<Vec<u8>>,
}

/// State shared across all hosts (they are all one process).
#[derive(Default)]
struct Shared {
    /// Per-node metrics, for leader detection by the driver.
    metrics: Mutex<Vec<(RaftId, Arc<Metrics>)>>,
    /// Per-node `Handle`, so the driver can issue/read commands.
    handles: Mutex<HashMap<RaftId, Handle>>,
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn node_id(i: RaftId) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn peers_of(self_id: RaftId, n: RaftId) -> HashMap<RaftId, NodeId> {
    (1..=n)
        .filter(|j| *j != self_id)
        .map(|j| (j, node_id(j)))
        .collect()
}

fn profile() -> ProfileConfig {
    let mut p = ProfileConfig::lan();
    p.heartbeat_interval_ms = 50;
    p.election_timeout_ms = 600;
    p.rpc_timeout_ms = 300;
    p.read_index_timeout_ms = 2 * p.election_timeout_ms;
    p
}

fn wal_opts(node: &str) -> WalOptions {
    WalOptions {
        cluster_id: CLUSTER_ID.into(),
        node_id: node.into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// Box an error for `turmoil::Result` (which is `Box<dyn Error>`).
fn box_err<E: std::fmt::Display>(e: E) -> Box<dyn std::error::Error> {
    e.to_string().into()
}

/// A unique temp dir for one node of one run.
fn unique_dir(seed: u64, i: RaftId) -> PathBuf {
    static RUN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let run = RUN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "arachne-l2-{seed}-{}-{run}-{i}",
        std::process::id()
    ))
}

/// `raft id -> dialable address`, filled after turmoil has assigned host IPs.
type AddrMap = HashMap<NodeId, SocketAddr>;

/// Run the 3-node scenario once with `seed` and return the per-node observations.
///
/// Deterministic: fixed turmoil seed + fixed raft election seed; the driver only
/// uses simulated time. Two calls with the same seed return identical results.
pub fn run_three_node(seed: u64) -> Vec<NodeObservation> {
    let shared = Arc::new(Shared::default());
    let sink: Arc<Mutex<Vec<NodeObservation>>> = Arc::new(Mutex::new(Vec::new()));
    let dirs: Arc<Mutex<Vec<PathBuf>>> = Arc::new(Mutex::new(Vec::new()));
    // Filled with dialable `host_ip:port` addresses after host registration.
    let addrs_cell: Arc<Mutex<Option<AddrMap>>> = Arc::new(Mutex::new(None));

    // Seed the patched raft election RNG on this (single) sim thread.
    raft::set_election_rng_seed(seed);

    let mut sim = SimNetwork::with_seed(seed);
    for i in 1..=3u64 {
        let shared = Arc::clone(&shared);
        let dirs = Arc::clone(&dirs);
        let addrs_cell = Arc::clone(&addrs_cell);
        sim.host(format!("n{i}"), move || {
            let shared = Arc::clone(&shared);
            let dirs = Arc::clone(&dirs);
            let addrs_cell = Arc::clone(&addrs_cell);
            async move {
                let addrs = addrs_cell
                    .lock()
                    .expect("addrs cell")
                    .clone()
                    .expect("addresses assigned before run");
                let name = format!("n{i}");
                let dir = unique_dir(seed, i);
                std::fs::create_dir_all(&dir).map_err(box_err)?;
                dirs.lock().expect("dirs").push(dir.clone());
                let wal = WalStorage::open(&dir, wal_opts(&name))
                    .map_err(|e| { eprintln!("[{name}] wal open failed: {e}"); box_err(e) })?;
                eprintln!("[{name}] wal open ok");

                let factory = TonicTransportFactory::with_io(
                    TurmoilIo,
                    CLUSTER_ID,
                    1,
                    0,
                    Vec::new(),
                    addrs.clone(),
                );
                // Match the transport's per-request / connect deadlines to the
                // node's **simulated** RPC timeout. The factory defaults (5s
                // request, 2s connect) are wall-clock-shaped: under the simulator
                // a stalled cached channel would hold the single-task runtime
                // actor for 5s of simulated time — far past the election window —
                // so the cluster looks wedged (no ticks, no inbound, no commit).
                // Bounding the send at `rpc_timeout` keeps the actor responsive
                // and lets raft retry on a fresh connection.
                let rpc_timeout = Duration::from_millis(profile().rpc_timeout_ms.max(1));
                let _ = factory.request_timeout(rpc_timeout);
                let _ = factory.connect_timeout(rpc_timeout);
                // Bind `0.0.0.0:port` (turmoil only allows unspecified/loopback
                // binds); peers dial this node at its `host_ip:port` entry.
                let bind = SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 7000 + i as u16);
                factory
                    .start_with_bind(node_id(i), bind)
                    .await
                    .map_err(|e| { eprintln!("[{name}] start_with_bind failed: {e}"); box_err(e) })?;
                eprintln!("[{name}] listening on {bind}");
                let (tx, rx) = factory.create(node_id(i));

                let metrics = Arc::new(Metrics::new());
                let cfg = RuntimeConfig {
                    self_raft_id: i,
                    self_node_id: node_id(i),
                    peers: peers_of(i, 3),
                    addresses: addrs.clone(),
                    raft: RaftNodeConfig::from_profile(&profile()),
                    profile: profile(),
                    metrics: Arc::clone(&metrics),
                };
                let (runtime, handle) =
                    Runtime::new(cfg, wal, tx, rx, &logger())
                        .map_err(|e| { eprintln!("[{name}] Runtime::new failed: {e}"); box_err(e) })?;
                eprintln!("[{name}] runtime up");
                shared.metrics.lock().expect("metrics").push((i, metrics));
                shared.handles.lock().expect("handles").insert(i, handle);
                tokio::spawn(runtime.run());

                // Keep the host alive for the whole simulation.
                std::future::pending::<()>().await;
                Ok(())
            }
        });
    }

    // Assign dialable addresses now that turmoil knows every host's IP, and
    // publish them to the hosts before `run`.
    let mut addrs: AddrMap = HashMap::new();
    for i in 1..=3u64 {
        let ip = sim.host_ip(format!("n{i}"));
        addrs.insert(node_id(i), SocketAddr::new(ip, 7000 + i as u16));
    }
    *addrs_cell.lock().expect("addrs cell") = Some(addrs);

    // Pin an explicit, small per-link latency. Turmoil's **default** link
    // latency is large and jittery — a single gRPC round-trip costs tens to
    // ~100ms of simulated time (measured with `l2/tests/transport_echo.rs`) —
    // which exceeds the node's election timeout and made the cluster flap
    // (check-quorum step-down, terms 1->2->3...). With an explicit latency the
    // same probe is crisp (p50 == p99 == 2ms at 1ms latency, zero timeouts),
    // and 1ms is the realistic LAN figure for a single-process sim. This is a
    // harness configuration fix, not a product change.
    let link_latency = Duration::from_millis(1);
    for (a, b) in [("n1", "n2"), ("n1", "n3"), ("n2", "n3")] {
        sim.set_link_latency(a, b, link_latency);
    }

    let dshared = Arc::clone(&shared);
    let dsink = Arc::clone(&sink);
    sim.client("driver", async move {
        drive(&dshared, &dsink).await.map_err(box_err)?;
        Ok(())
    });

    sim.run().expect("sim runs to the driver's completion");
    drop(sim);

    for dir in dirs.lock().expect("dirs").iter() {
        let _ = std::fs::remove_dir_all(dir);
    }
    let out = sink.lock().expect("sink").clone();
    out
}

/// Wait for a leader, `put` one key through it, then read it back from every
/// node. Records one [`NodeObservation`] per node into `sink`.
async fn drive(shared: &Shared, sink: &Mutex<Vec<NodeObservation>>) -> Result<(), String> {
    // 1. Wait until all three nodes have registered their handles.
    for _ in 0..2000 {
        if shared.handles.lock().expect("handles").len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let handle_count = shared.handles.lock().expect("handles").len();
    eprintln!("[driver] handles registered: {handle_count}");

    // 2. Wait for a leader.
    let mut leader = 0u64;
    for step in 0..300 {
        let metrics = shared.metrics.lock().expect("metrics").clone();
        if let Some((id, _)) = metrics.iter().find(|(_, m)| m.is_leader()) {
            leader = *id;
            break;
        }
        if step % 50 == 0 {
            let states: Vec<(RaftId, bool, u64)> = metrics
                .iter()
                .map(|(id, m)| (*id, m.is_leader(), m.leader_id()))
                .collect();
            eprintln!("[driver] waiting for leader, states(id,is_leader,leader_id)={states:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if leader == 0 {
        return Err("no leader elected within the driver's wait budget".into());
    }
    eprintln!("[driver] leader = {leader}");
    for step in 0..60 {
        if step % 5 == 0 {
            let metrics = shared.metrics.lock().expect("metrics").clone();
            let states: Vec<(RaftId, bool, u64)> = metrics
                .iter()
                .map(|(id, m)| (*id, m.is_leader(), m.leader_id()))
                .collect();
            eprintln!("[driver] post-leader states(id,is_leader,leader_id)={states:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // 3. `put` through the leader (bounded retries for transient errors).
    let leader_handle = shared
        .handles
        .lock()
        .expect("handles")
        .get(&leader)
        .cloned()
        .ok_or_else(|| format!("leader {leader} has no handle"))?;
    let mut put_ok = false;
    let mut last_err = String::new();
    for attempt in 0..5 {
        match leader_handle.put(b"k", b"v").await {
            Ok(()) => {
                put_ok = true;
                break;
            }
            Err(e) => {
                last_err = format!("{e}");
                eprintln!("[driver] put attempt {attempt} err: {last_err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    eprintln!("[driver] put_ok={put_ok} last_err={last_err}");
    if !put_ok {
        return Err("put did not commit within the driver's wait budget".into());
    }

    // 4. Read back from every node; record observations in raft-id order.
    for i in 1..=3u64 {
        let handle = shared
            .handles
            .lock()
            .expect("handles")
            .get(&i)
            .cloned()
            .ok_or_else(|| format!("node {i} has no handle"))?;
        let mut value = None;
        for _ in 0..500 {
            if let Ok(seen) = handle.get_stale(b"k").await {
                if seen.is_some() {
                    value = seen;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let leader_id = shared
            .metrics
            .lock()
            .expect("metrics")
            .iter()
            .find(|(id, _)| *id == i)
            .map(|(_, m)| m.leader_id())
            .unwrap_or(0);
        sink.lock().expect("sink").push(NodeObservation {
            raft_id: i,
            leader_id,
            value,
        });
    }

    Ok(())
}

