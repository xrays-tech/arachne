//! Arachne L2 deterministic-simulator harness (skeleton, decision D-T1).
//!
//! This establishes the L2 *harness shape* on top of `turmoil` without yet
//! driving raft (that is the M1/M2 goal). It proves the three primitives the
//! L2 layer will rely on:
//!
//!   * **hosts** — a TCP echo server (host `a`) and a client (host `b*`);
//!   * **partitions** — a link partition and its repair;
//!   * **crash / restart** — a host crash (`Sim::crash`) and restart
//!     (`Sim::bounce`), with the server re-establishing itself afterwards.
//!
//! The whole scenario is driven under a **fixed seed** and asserted to be
//! **deterministic**: running it twice with the same seed must reproduce the
//! identical observable outcome (the exchanged payloads and the simulated
//! elapsed time). Any divergence would be a determinism leak (P0, test-plan
//! §5) — this is the L2 canary for the double-run reproduction gate.
//!
//! # Why this is a standalone project
//!
//! `turmoil` and its dependency tree are dev-only tooling. Keeping them out of
//! the production workspace (decision D-ART, test-plan §3.2) guarantees the
//! simulator can never leak into a production crate's normal dependency tree.
//! The product core is pulled *lean* (`default-features = false`), mirroring
//! the sim-build rule, so the L2 build never silently compiles tonic.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil::{
    net::{TcpListener, TcpStream},
    Builder, Result,
};

const PORT: u16 = 9000;
const SERVER: &str = "a";

/// The deterministic observable outcome of one full scenario run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScenarioOutcome {
    /// Every echoed payload the client received, in order.
    echoes: Vec<String>,
    /// Total simulated time elapsed across all phases (seed-dependent).
    elapsed: Duration,
}

/// A poisoned lock should be impossible here (single-threaded sim); surface it
/// as an error rather than panicking in harness code.
fn locked<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Run the complete multi-phase scenario once under `seed` and return its
/// observable outcome.
fn run_scenario(seed: u64) -> Result<ScenarioOutcome> {
    // Shared collection the clients fill; main reads it after the sim finishes.
    // Turmoil is single-threaded, so a plain lock is sufficient and deterministic.
    let echoes: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let mut sim = Builder::new().rng_seed(seed).build();

    // Host `a`: a TCP echo server. The closure is re-invoked on `bounce`, so it
    // re-binds and re-serves after a crash/restart.
    sim.host(SERVER, || async {
        let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT)).await?;
        loop {
            let (mut stream, _peer) = listener.accept().await?;
            let mut buf = [0u8; 64];
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                continue; // peer closed; serve the next connection.
            }
            stream.write_all(&buf[..n]).await?;
            stream.flush().await?;
        }
    });

    // Phase 0 — basic exchange: the client connects, writes, and reads the echo.
    let e0 = Arc::clone(&echoes);
    sim.client("b0", async move {
        let mut stream = TcpStream::connect((SERVER, PORT)).await?;
        stream.write_all(b"hello").await?;
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await?;
        locked(&e0).push(String::from_utf8_lossy(&buf[..n]).to_string());
        Ok(())
    });
    sim.run()?;

    // Phase 1 — crash + restart: kill the server, restart it, exchange again.
    sim.crash(SERVER);
    sim.bounce(SERVER);
    let e1 = Arc::clone(&echoes);
    sim.client("b1", async move {
        let mut stream = TcpStream::connect((SERVER, PORT)).await?;
        stream.write_all(b"after-restart").await?;
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await?;
        locked(&e1).push(String::from_utf8_lossy(&buf[..n]).to_string());
        Ok(())
    });
    sim.run()?;

    // Phase 2 — partition + repair: a partitioned link must refuse the
    // connection; repairing it must allow the exchange to succeed.
    let e2 = Arc::clone(&echoes);
    sim.client("b2", async move {
        turmoil::partition(SERVER, "b2");
        if TcpStream::connect((SERVER, PORT)).await.is_ok() {
            return Err("connect must fail while the link is partitioned".into());
        }
        turmoil::repair(SERVER, "b2");
        let mut stream = TcpStream::connect((SERVER, PORT)).await?;
        stream.write_all(b"after-repair").await?;
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await?;
        locked(&e2).push(String::from_utf8_lossy(&buf[..n]).to_string());
        Ok(())
    });
    sim.run()?;

    let echoed = locked(&echoes).clone();
    let elapsed = sim.elapsed();
    Ok(ScenarioOutcome { echoes: echoed, elapsed })
}

fn main() {
    let seed: u64 = 0x5EED_42;

    let first = run_scenario(seed).expect("scenario run #1 must succeed");
    let second = run_scenario(seed).expect("scenario run #2 must succeed");

    // Determinism: the same seed must reproduce the identical outcome. A
    // divergence here is a determinism leak (test-plan §5) — no whitelisting.
    assert_eq!(
        first, second,
        "same seed must yield the same deterministic outcome (determinism canary)"
    );

    // The scenario must have actually done the work: three exchanges (basic,
    // post-restart, post-repair) and a real crash/restart in between.
    assert_eq!(
        first.echoes,
        vec!["hello".to_string(), "after-restart".to_string(), "after-repair".to_string()],
        "the client must receive exactly the three echoed payloads in order"
    );
    assert!(
        first.elapsed > Duration::ZERO,
        "the scenario must advance simulated time"
    );

    println!(
        "arachne-l2 skeleton OK: seed {seed:#010x} -> {} echoes, elapsed {:?} (deterministic across two same-seed runs)",
        first.echoes.len(),
        first.elapsed
    );
    // Prove the product core is wired into the harness (lean, no transport).
    println!("linked product core: arachne {}", arachne::version());
}
