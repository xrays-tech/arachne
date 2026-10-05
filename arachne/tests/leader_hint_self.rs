//! Regression test for the **defect C** fix: after a leadership transfer, the
//! new leader must be able to name *itself* in its own leader hint.
//!
//! The hint is built in `Runtime::hint()` as
//! `raft_to_node.get(&node.leader_id())`, where `raft_to_node` (raft id → node
//! id) is a map that must include the node's **own** entry. A rewrite that
//! built `raft_to_node` from `config.peers` alone (which excludes self) lost the
//! self entry, so a node that *became* leader via `transfer_leader` could not
//! find itself in that map: `leader_hint()` returned `None` (or the old leader)
//! for a long window instead of `(own node, own address)`.
//!
//! `Metrics::leader_id()` is immune to the defect-C bug (raft tracks its own
//! leader regardless of the hint map), so it is the reliable way to find the
//! current leader and to confirm the transfer actually moved leadership.
//!
//! Three independent in-process nodes over the real tonic transport, each with
//! its **own** `TonicTransportFactory` (per-node `start_with_bind`, exactly as
//! `assemble_cluster`/the `l2` harness do), which keeps each node's
//! `Arc<Metrics>` so we can drive the cluster from outside.
#[cfg(feature = "transport-tonic")]
mod hint_self {
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::task::JoinHandle;

    use arachne_kv::client::Handle;
    use arachne_kv::consensus::RaftNodeConfig;
    use arachne_kv::runtime::{Runtime, RuntimeConfig};
    use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
    use arachne_kv::{Metrics, NodeId, ProfileConfig, RaftId, TransportFactory};
    use arachne_kv_transport_tonic::TonicTransportFactory;
    use slog::{Drain, o, Logger};

    static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    const MAX_ATTEMPTS: u32 = 120; // 120 × 50ms = 6s budget

    async fn poll() {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    /// A fresh, unique temp dir for a single node.
    fn temp_dir(tag: &str) -> PathBuf {
        let n = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = env::temp_dir().join(format!(
            "arachne-hint-self-{tag}-{0}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn logger() -> Logger {
        slog::Logger::root(slog::Discard.fuse(), o!())
    }

    fn node_id(i: RaftId) -> NodeId {
        NodeId::new(format!("n{i}"))
    }

    /// Bind `n` fresh, unique TCP ports on 127.0.0.1. The listener is dropped
    /// each iteration (freeing the port), so this *detects* concrete, free
    /// addresses the factories can later re-bind.
    fn prebind_ports(n: u32) -> Vec<SocketAddr> {
        let mut addrs = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("cannot bind a free loopback port");
            addrs.push(listener.local_addr().expect("listener has a local addr"));
        }
        addrs
    }

    fn profile() -> ProfileConfig {
        let mut p = ProfileConfig::lan();
        p.heartbeat_interval_ms = 50;
        p.election_timeout_ms = 600;
        p.rpc_timeout_ms = 300;
        p.read_index_timeout_ms = 2 * p.election_timeout_ms;
        p
    }

    fn peers_of(self_id: RaftId) -> HashMap<RaftId, NodeId> {
        (1..=3u64)
            .filter(|j| *j != self_id)
            .map(|j| (j, node_id(j)))
            .collect()
    }

    fn wal_opts(node: &str) -> WalOptions {
        WalOptions {
            cluster_id: "hint-self".to_string(),
            node_id: node.into(),
            config: WalConfig {
                fsync_policy: FsyncPolicy::Always,
                segment_bytes: 1 << 20,
            },
            created_at_millis: 1_700_000_000_000,
            fsync_observer: None,
        }
    }

    /// Assemble 3 independent in-process nodes, each with its own factory
    /// bound to its own pre-detected port (the per-node `start_with_bind`
    /// pattern of `assemble_cluster`). Returns `(metrics, handles, tasks, dirs,
    /// addrs)`; `addrs` is the shared `node_id -> dialable address` map the
    /// runtimes use (and the same map a resolved `leader_hint()` addresses).
    async fn three_nodes() -> (
        Vec<Arc<Metrics>>,
        Vec<Handle>,
        Vec<JoinHandle<()>>,
        Vec<PathBuf>,
        HashMap<NodeId, SocketAddr>,
    ) {
        let ports = prebind_ports(3);
        let ids: Vec<NodeId> = (1..=3u64).map(node_id).collect();

        // Concrete, dialable address map (all real, pre-detected ports).
        let mut addrs: HashMap<NodeId, SocketAddr> = HashMap::new();
        for (id, addr) in ids.iter().zip(ports.iter()) {
            addrs.insert(id.clone(), *addr);
        }

        let metrics: Vec<Arc<Metrics>> = (0..3).map(|_| Arc::new(Metrics::new())).collect();
        let mut handles: Vec<Handle> = Vec::new();
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        let mut dirs: Vec<PathBuf> = Vec::new();
        let prof = profile();

        for i in 1..=3u64 {
            let dir = temp_dir(&format!("n{i}"));
            dirs.push(dir.clone());
            let name = format!("n{i}");
            let wal = WalStorage::open(&dir, wal_opts(&name)).unwrap();

            // Each node has its OWN factory; it binds its own listener.
            let factory = TonicTransportFactory::new(
                "hint-self-cluster",
                1,
                0,
                Vec::new(),
                addrs.clone(),
            );
            // Bound the factory's request/connect deadlines to the node's RPC
            // timeout so a stalled channel can't hold the actor past the election
            // window (mirrors the l2 harness; under real TCP this is belt-and-
            // suspenders but makes the test robust).
            let rpc = core::time::Duration::from_millis(prof.rpc_timeout_ms.max(1));
            factory.request_timeout(rpc);
            factory.connect_timeout(rpc);
            let bind = ports[(i - 1) as usize];
            factory
                .start_with_bind(node_id(i), bind)
                .await
                .unwrap_or_else(|e| panic!("node {name} start_with_bind: {e}"));
            let (tx, rx) = factory.create(node_id(i));

            let cfg = RuntimeConfig {
                self_raft_id: i,
                self_node_id: node_id(i).clone(),
                peers: peers_of(i),
                addresses: addrs.clone(),
                raft: RaftNodeConfig::from_profile(&prof),
                profile: prof.clone(),
                metrics: Arc::clone(&metrics[(i - 1) as usize]),
            };
            let (runtime, handle) =
                Runtime::new(cfg, wal, tx, rx, &logger())
                    .unwrap_or_else(|e| panic!("node {name} Runtime::new: {e}"));
            // Register the node's command sink with its transport so its gRPC
            // `Forward` RPC can run forwarded client commands (propsol v0.2.19).
            factory
                .set_command_sink(node_id(i), runtime.command_sink());
            tasks.push(tokio::spawn(runtime.run()));
            handles.push(handle);
        }

        // In-process peer registration so a follower's redirect can hop locally
        // (the transfer command must reach the current leader's runtime).
        for a in 0..3 {
            for b in 0..3 {
                if a != b {
                    handles[a].register_peer(handles[b].clone());
                }
            }
        }

        (metrics, handles, tasks, dirs, addrs)
    }

    /// Find the current leader via the bug-immune metrics. Only the *actual*
    /// leader reports `is_leader() == true` (a follower knows a leader but is
    /// not one), so exactly one node is true once an election has settled.
    /// Returns the leader's raft id (1-based) or `None` on a missed bound.
    async fn find_leader(metrics: &[Arc<Metrics>]) -> Option<RaftId> {
        let mut leaders: Vec<RaftId> = Vec::new();
        for _ in 0..MAX_ATTEMPTS {
            leaders.clear();
            for i in 0..3u32 {
                if metrics[i as usize].is_leader() {
                    leaders.push(i as RaftId + 1);
                }
            }
            eprintln!("[hint-self] leader poll: {leaders:?}");
            if leaders.len() == 1 {
                return Some(leaders[0]);
            }
            poll().await;
        }
        eprintln!("[hint-self] gave up; last {leaders:?}");
        None
    }

    /// Poll until the given node is a leader (bug-immune: `leader_id == self`).
    async fn wait_is_leader(metrics: &[Arc<Metrics>], node: RaftId) -> bool {
        for _ in 0..MAX_ATTEMPTS {
            if metrics[(node - 1) as usize].is_leader() {
                return true;
            }
            eprintln!(
                "[hint-self] is_leader poll: n{}={:?}",
                node,
                metrics[(node - 1) as usize].leader_id()
            );
            poll().await;
        }
        false
    }

    fn teardown(tasks: &[JoinHandle<()>], dirs: &[PathBuf]) {
        for t in tasks {
            t.abort();
        }
        for d in dirs {
            let _ = fs::remove_dir_all(d);
        }
    }

    /// Defect C: after a leadership transfer, the **new leader** names itself in
    /// its own `leader_hint()`; the old (now-following) leader hints at the new
    /// leader.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn new_leader_names_itself_in_hint() {
        let (metrics, handles, tasks, dirs, addrs) = three_nodes().await;

        let leader = find_leader(&metrics)
            .await
            .expect("a 3-node cluster must elect a leader via Metrics::leader_id/is_leader");

        let follower = (1..=3u64)
            .find(|i| *i != leader)
            .expect("with 3 nodes there is a follower distinct from the leader");
        eprintln!("[hint-self] leader={leader} follower={follower}");

        handles[(leader - 1) as usize]
            .transfer_leader(follower)
            .await
            .expect("transfer_leader to a follower must succeed");

        assert!(
            wait_is_leader(&metrics, follower).await,
            "after transfer, node {follower} must be the leader"
        );

        let self_node = node_id(follower);
        let self_addr = addrs[&self_node];
        let hint = handles[(follower - 1) as usize].leader_hint().await;
        eprintln!(
            "[hint-self] new leader {follower}: self_node={self_node} addr={self_addr} hint={hint:?}"
        );
        assert_eq!(
            hint,
            Some((self_node.clone(), self_addr)),
            "new leader's leader_hint() must name itself (node + its listen address); got {hint:?}"
        );

        let old_hint = handles[(leader - 1) as usize].leader_hint().await;
        assert_eq!(
            old_hint,
            Some((self_node, self_addr)),
            "old (now-following) leader must hint at the new leader; got {old_hint:?}"
        );

        teardown(&tasks, &dirs);
    }
}

#[cfg(not(feature = "transport-tonic"))]
mod no_tonic {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use arachne_kv::server::{Arachne, ArachneError, ClusterConfig};
    use arachne_kv::NodeId;

    #[tokio::test]
    async fn multi_node_requires_tonic_feature() {
        let n1 = NodeId::new("n1");
        let n2 = NodeId::new("n2");
        let cfg = ClusterConfig::member(
            "lean-cluster".to_string(),
            n1.clone(),
            "127.0.0.1:1".parse().expect("addr"),
            PathBuf::new(),
            vec![n1.clone(), n2.clone()],
            {
                let mut m = HashMap::new();
                m.insert(n1, "127.0.0.1:1".parse().unwrap());
                m.insert(n2, "127.0.0.1:2".parse().unwrap());
                m
            },
        );
        match Arachne::start(cfg) {
            Err(ArachneError::Unrecoverable(m)) => {
                assert_eq!(m, "multi-node requires the transport-tonic feature");
            }
            e => panic!("multi-node without tonic: expected Unrecoverable, got {e:?}"),
        }
    }
}