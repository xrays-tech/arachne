//! Smoke test: assemble a node via the lib runtime, wait for leadership, write
//! through the client `Handle`, and read the value back.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne_node::config::parse_config;
use arachne_node::metrics::Metrics;
use arachne_node::node::Arachne;
use slog::Drain;

static DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let n = DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-node-smoke-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn node_serves_writes_after_election() {
    let dir = temp_dir();
    let toml = format!(
        "cluster_id = \"smoke\"\n\
         node_id = \"n1\"\n\
          listen = \"127.0.0.1:0\"\n\
         data_dir = \"{dir}\"\n\
         http_listen = \"127.0.0.1:0\"\n\
         initial_cluster = [\"n1\"]\n\
         heartbeat_interval_ms = 5\n\
         election_timeout_ms = 100\n\
         rpc_timeout_ms = 50\n",
        dir = dir.display()
    );
    let config = parse_config(&toml).expect("valid config");
    let metrics = Arc::new(Metrics::new());
    let logger = slog::Logger::root(slog::Discard.fuse(), slog::o!());

    let node = Arachne::open(&config, Arc::clone(&metrics), &logger)
        .await
        .expect("open node");
    let kv = node.handle();

    for _ in 0..400 {
        if metrics.is_ready() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(metrics.is_ready(), "single node must elect itself");

    kv.put(b"k", b"v").await.expect("put");
    assert_eq!(
        kv.get_stale(b"k").await.expect("get_stale"),
        Some(b"v".to_vec())
    );

    node.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
