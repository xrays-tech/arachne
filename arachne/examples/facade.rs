//! A minimal embedding example: one in-process Arachne node driven through the
//! `arachne_kv::server` facade. No peers, no network, no external runtime
//! ownership — the facade owns its own dedicated-thread runtime. The example
//! only `block`s the facade's public async API on its own (trivial) tokio
//! runtime.
//!
//! ```sh
//! cargo run --example facade
//! ```

use std::time::{Duration, Instant};

use arachne_kv::server::{Arachne, ArachneError, ClusterConfig, WalConfig};

fn main() {
    let dir = std::env::temp_dir().join(format!(
        "arachne-facade-example-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");

    let output: Result<(), ArachneError> = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime")
        .block_on(async {
            // (1) One node per process — start an N=1 cluster (single_node).
            Arachne::start(ClusterConfig::single_node(1, &dir, WalConfig::default()))?;

            // (2) Wait until the node has elected itself leader. Pre-election
            //     reads fail fast (fast non-leader redirect); once the node is
            //     leader the same read resolves to `None` (key not yet set).
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match Arachne::get(b"hello").await {
                    Ok(v) => {
                        assert!(v.is_none(), "unexpected value before write");
                        break;
                    }
                    Err(ArachneError::NotLeader { .. })
                    | Err(ArachneError::QuorumUnavailable) => {
                        if Instant::now() >= deadline {
                            panic!("node never became ready within 5s");
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(e) => panic!("unexpected read error: {e}"),
                }
            }

            // (3) A linearizable write, then a linearizable + local read.
            Arachne::set(b"hello", b"world").await?;
            let value = Arachne::get(b"hello").await?;
            assert_eq!(value, Some(b"world".to_vec()));
            println!("get(b\"hello\")          -> {value:?}");

            let stale = Arachne::get_stale(b"hello").await?;
            assert_eq!(stale, Some(b"world".to_vec()));
            println!("get_stale(b\"hello\")    -> {stale:?}");

            // (4) Delete and confirm absence.
    Arachne::delete(b"hello").await?;
    assert_eq!(Arachne::get(b"hello").await, Ok(None));
    println!("get(b\"hello\") after del -> None");

            // (5) Shut down: frees the static slot and releases the WAL lock.
            Arachne::shutdown()?;
            Ok(())
        });

    match output {
        Ok(()) => println!("facade lifecycle complete"),
        Err(e) => eprintln!("example failed: {e}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
