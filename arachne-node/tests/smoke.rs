//! Smoke test: assemble a real single-node Arachne node on a temp data dir,
//! drive it to leadership, propose a write, and observe it applied.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::state_machine::KvStateMachine;
use arachne_node::config::parse_config;
use arachne_node::metrics::Metrics;
use arachne_node::node::Arachne;
use slog::Drain;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-node-smoke-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[test]
fn single_node_commits_and_applies_a_write() {
    let dir = temp_dir();
    let toml = format!(
        "cluster_id = \"smoke\"\n\
         node_id = \"n1\"\n\
         listen = \"127.0.0.1:7000\"\n\
         data_dir = \"{dir}\"\n\
         http_listen = \"127.0.0.1:0\"\n\
         initial_cluster = [\"n1\"]\n\
         heartbeat_interval_ms = 10\n",
        dir = dir.display()
    );
    let config = parse_config(&toml).expect("valid config");
    let metrics = Arc::new(Metrics::new());
    let logger = slog::Logger::root(slog::Discard.fuse(), slog::o!());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build runtime");

    runtime.block_on(async {
        let mut node = Arachne::open(&config, Arc::clone(&metrics), &logger).expect("open node");

        for _ in 0..300 {
            node.tick().await.expect("tick");
            if node.is_ready() {
                break;
            }
        }
        assert!(node.is_ready(), "single node must elect itself leader");
        assert!(metrics.is_leader(), "metrics must report leadership");

        let command = KvStateMachine::encode_put(1, 1, b"k", b"v");
        node.propose(&command).expect("propose on the leader");

        let mut applied = false;
        for _ in 0..300 {
            node.tick().await.expect("tick");
            if node.applied_index() >= 1 {
                applied = true;
                break;
            }
        }
        assert!(applied, "the proposed write must be applied");
        assert_eq!(
            node.get(b"k").expect("get"),
            Some(b"v".to_vec()),
            "the applied state machine must hold the written value"
        );
    });

    let _ = std::fs::remove_dir_all(&dir);
}
