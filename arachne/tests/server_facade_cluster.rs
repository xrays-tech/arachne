//! Single-process 3-node cluster test for the multi-node embedding façade.
//!
//! **Why a single process?** The façade is a *library* — external HTTP/network
//! wrappers belong to calling projects, not to this crate. A node participates
//! in a **real** tonic raft cluster (election, replication, failover) by being
//! assembled in-process, so this suite hosts three independent nodes via
//! `assemble_cluster × 3` rather than subprocesses or an out-of-band command
//! channel.
//!
//! **How leadership is detected / how writes work.** Each assembled node has
//! its *own* `Handle`; independent handles do **not** share peer handles, so a
//! follower's linearizable `get`/`put` cannot redirect across separately
//! assembled nodes (redirect is in-process only). Therefore:
//!
//!   1. find the current leader by polling each node's `handle.get` until one
//!      returns `Ok` (a leader serves a ReadIndex read; non-leaders return
//!      `NotLeader`);
//!   2. perform `put` and linearizable `get` through the leader's handle;
//!   3. verify cross-node replication through `handle.get_stale` on all three
//!      nodes (stale reads are quorum-free and always work).
//!
//! **Gate C (no real time in tests):** this file avoids `std::time`.
//! Unique temp dirs come from `std::process::id()` plus a process-local sequence
//! counter (no `SystemTime`). Port pre-detection uses `std::net::TcpListener`
//! (Gate C only forbids `std::time` and `tokio::net` in tests). Every polling
//! loop is bounded by a fixed attempt count and a `tokio::time::sleep` interval
//! — no wall-clock `Instant`.
//!
//! `assemble_cluster` is `async` (the gRPC servers run on the caller's tokio
//! runtime); each node's raft actor runs on its own dedicated OS thread.

#[cfg(feature = "transport-tonic")]
mod cluster {
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use arachne::server::{AssembledClusterNode, ClusterConfig};
    use arachne::NodeId;

    // Unique per-process sequence so `process::id()` (shared across all tests in
    // one binary) is disambiguated without reaching for real time.
    static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    /// A fresh, unique temp dir for a single node/test.
    fn unique_tempdir() -> PathBuf {
        let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
        PathBuf::from(env::temp_dir())
            .join(format!("arachne-cluster-{0}-{1}", std::process::id(), seq))
    }

    /// Bind `n` fresh, unique TCP ports on 127.0.0.1 (Gate C: no real time; the
    /// kernel supplies the concrete values). Uses `std::net` (sync) — Gate C
    /// only forbids `std::time` and `tokio::net` in tests — and is called
    /// synchronously from the async setup (a one-shot, cheap bind).
    fn prebind_ports(n: u32) -> Vec<SocketAddr> {
        let mut addrs = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("cannot bind a free loopback port");
            addrs.push(listener.local_addr().expect("listener has a local addr"));
        }
        addrs
    }

    const MAX_ATTEMPTS: u32 = 80; // 80 × 50ms = 4s budget (elections are ~0.3s)

    async fn poll() {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    /// Poll the given (alive) nodes until one serves a linearizable read
    /// (`get == Ok`); that node is the current leader. Returns its **original
    ///** index (the `idx` we passed in), or None if the bound is hit.
    async fn find_leader(alive: &[(usize, &AssembledClusterNode)]) -> Option<usize> {
        let mut last_err = String::new();
        for _ in 0..MAX_ATTEMPTS {
            for (idx, node) in alive.iter() {
                match node.handle.get(b"health").await {
                    Ok(_) => return Some(*idx),
                    Err(e) => last_err = format!("[{idx}] {e:?}"),
                }
            }
            poll().await;
        }
        eprintln!(
            "no live node served a read in {MAX_ATTEMPTS} attempts; last: {last_err}"
        );
        None
    }

    /// Poll a node's stale read until it equals `expected`.
    async fn wait_stale(node: &AssembledClusterNode, key: &[u8], expected: &[u8]) -> bool {
        for _ in 0..MAX_ATTEMPTS {
            match node.handle.get_stale(key).await {
                Ok(Some(v)) if v == expected => return true,
                _ => {}
            }
            poll().await;
        }
        false
    }

    /// Assemble 3 nodes with concrete pre-detected ports and unique temp dirs.
    /// Returns the nodes and their data dirs (same order) so tests can clean up.
    async fn three_nodes() -> (Vec<AssembledClusterNode>, Vec<PathBuf>) {
        let ids: Vec<NodeId> = vec![NodeId::new("n1"), NodeId::new("n2"), NodeId::new("n3")];
        let ports = prebind_ports(3);
        let initial_cluster = ids.clone();

        let mut nodes = Vec::with_capacity(3);
        let mut dirs = Vec::with_capacity(3);
        for (i, id) in ids.iter().enumerate() {
            let dir = unique_tempdir();
            dirs.push(dir.clone());
            let cfg = ClusterConfig::member(
                "facade-cluster".to_string(),
                id.clone(),
                ports[i],
                dir,
                initial_cluster.clone(),
                {
                    let mut m = HashMap::new();
                    for (j, nid) in ids.iter().enumerate() {
                        m.insert(nid.clone(), ports[j]);
                    }
                    m
                },
            );
            let node = arachne::server::assemble_cluster(cfg)
                .await
                .unwrap_or_else(|e| panic!("assemble node {id}: {e:?}"));
            nodes.push(node);
        }
        (nodes, dirs)
    }

    /// Shutdown each node: stop the gRPC (idempotent, `&self`), then stop the
    /// actor thread (which *consumes* the `RuntimeThread` — so this takes the
    /// nodes by value and moves each thread exactly once). The gRPC-first,
    /// thread-second order matches `Arachne::shutdown`.
    async fn shutdown_nodes(nodes: Vec<AssembledClusterNode>) {
        for node in nodes {
            node.tonic.shutdown().await;
            node.thread.shutdown();
        }
    }

    #[tokio::test]
    async fn three_node_cluster_elects_and_serves() {
        let (nodes, dirs) = three_nodes().await;
        let alive: Vec<(usize, &AssembledClusterNode)> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (i, n))
            .collect();
        let leader = find_leader(&alive)
            .await
            .expect("a leader must be elected in the 3-node cluster");

        // Write through the leader, then confirm via the leader's stale read.
        nodes[leader].handle.put(b"hello", b"world").await.expect("put through leader");
        assert!(
            nodes[leader].handle.get_stale(b"hello").await == Ok(Some(b"world".to_vec())),
            "leader should serve the written value"
        );

        // Cross-node replication via stale reads on all 3 nodes.
        for (i, node) in nodes.iter().enumerate() {
            assert!(
                wait_stale(node, b"hello", b"world").await,
                "node {i} should see the replicated value via get_stale"
            );
        }
        println!("[3-node] leader index {leader}; all 3 nodes replicated 'hello' via get_stale");

        shutdown_nodes(nodes).await;
        for d in &dirs {
            let _ = fs::remove_dir_all(d);
        }
    }

    #[tokio::test]
    async fn leader_failure_fails_over_and_reads_recover() {
        let (mut nodes, dirs) = three_nodes().await;
        let alive: Vec<(usize, &AssembledClusterNode)> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (i, n))
            .collect();
        let leader = find_leader(&alive)
            .await
            .expect("initial leader elected");

        // Commit a value before killing the leader.
        nodes[leader].handle.put(b"key", b"value-1").await.expect("commit value-1");
        assert!(
            nodes[leader]
                .handle
                .get_stale(b"key")
                .await
                .is_ok(),
            "leader committed value-1"
        );

        // Kill the leader: remove it from `nodes` (so we own it and can move
        // its `RuntimeThread` exactly once) and stop the actor thread — the
        // leader can no longer send heartbeats, so the followers detect the loss
        // of quorum and elect a new leader.
        let leader_node = nodes.remove(leader);
        leader_node.thread.shutdown();
        let _ = leader_node.tonic.shutdown().await;

        // `nodes` now holds only the survivors (in their original relative
        // order); poll them for the new leader.
        let survivors: Vec<(usize, &AssembledClusterNode)> = nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (i, n))
            .collect();
        let new_leader = find_leader(&survivors)
            .await
            .expect("a survivor must be elected after failover");
        // Map the survivor's *reduced* position back to its *original* index:
        // removing the failed leader shifts every survivor that sat at or after
        // it left by one. So original = k when k < leader, else k + 1.
        let original_new_leader = if new_leader < leader { new_leader } else { new_leader + 1 };
        assert_ne!(
            original_new_leader, leader,
            "new leader must differ from the failed one"
        );
        println!(
            "[failover] failed leader index {leader}; new leader original index {original_new_leader}"
        );

        // The new leader serves the pre-failover value and can write a new one.
        assert_eq!(
            nodes[new_leader].handle.get(b"key").await,
            Ok(Some(b"value-1".to_vec())),
            "new leader should serve the pre-failover value"
        );
        nodes[new_leader]
            .handle
            .put(b"key", b"value-2")
            .await
            .expect("write through new leader");
        assert!(
            nodes[new_leader]
                .handle
                .get_stale(b"key")
                .await
                == Ok(Some(b"value-2".to_vec())),
            "new leader should serve the post-failover value"
        );

        shutdown_nodes(nodes).await;
        for d in &dirs {
            let _ = fs::remove_dir_all(d);
        }
    }
}

#[cfg(not(feature = "transport-tonic"))]
mod no_tonic {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use arachne::server::{Arachne, ArachneError, ClusterConfig};
    use arachne::NodeId;

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