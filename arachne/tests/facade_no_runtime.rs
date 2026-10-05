//! Regression test for the **defect A** fix: the peerless facade path must be
//! buildable **without** any tokio runtime on the caller thread.
//!
//! `Arachne::start(ClusterConfig::single_node)` assembles the node synchronously
//! (the "zero-tokio, peerless path" per the façade docs) and then spawns the
//! actor on a *dedicated* OS thread with its own current-thread tokio runtime.
//! The caller thread — a plain `std` thread with **no** tokio context — must be
//! able to drive the whole assembly.
//!
//! The regression this guards: a rewrite that built `tokio::time::Interval`
//! eagerly inside `Runtime::new` (via `tokio::time::interval(period)`) panics in
//! exactly this context, because `interval()` requires a running tokio runtime.
//! The fix stores a plain `Duration` on the node and builds the `Interval`
//! lazily at the top of `run()` — which always executes inside a runtime (the
//! actor thread's, or an embedder's).
//!
//! This is a **synchronous** `#[test]` on purpose: `#[tokio::test]` would give
//! the test a running runtime and hide the regression.
mod peerless {
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use arachne_kv::server::{Arachne, ArachneError, ClusterConfig, WalConfig};

    // Unique per-process sequence so `process::id()` (shared across all tests
    // in one binary) is disambiguated without reaching for real time.
    static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    /// A fresh, unique temp dir for a single test.
    fn unique_tempdir() -> PathBuf {
        let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
        PathBuf::from(env::temp_dir())
            .join(format!("arachne-facade-no-rt-{0}-{1}", std::process::id(), seq))
    }

    /// The peerless node must assemble (and be shut down) on a thread with
    /// **no** tokio runtime at all. This is a plain sync `#[test]`.
    #[test]
    fn single_node_start_without_tokio_context() {
        let dir = unique_tempdir();

        // The whole point of this test: a fully synchronous assembly on a
        // thread that has no tokio runtime. Before the defect-A fix this
        // panic (eager `tokio::time::interval` inside `Runtime::new`).
        Arachne::start(ClusterConfig::single_node(1, &dir, WalConfig::default()))
            .expect("start(ClusterConfig::single_node) must not panic on a thread with no tokio runtime (defect A)");

        // The node is alive and in the 'initialized' state.

        // A second init must still fail with AlreadyInitialized (state is intact,
        // the runtime is not clobbered).
        match Arachne::start(ClusterConfig::single_node(2, &dir, WalConfig::default())) {
            Err(ArachneError::AlreadyInitialized) => {}
            Err(e) => panic!("second start: expected AlreadyInitialized, got {e}"),
            Ok(_) => panic!("second start: expected AlreadyInitialized, got Ok"),
        }

        // Tear down: synchronous, joins the actor thread and releases the WAL.
        Arachne::shutdown().expect("shutdown after a no-runtime assembly");

        // Cleanup.
        let _ = fs::remove_dir_all(&dir);
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