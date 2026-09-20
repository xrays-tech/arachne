//! Transport-layer echo over turmoil with no raft and no WAL: a latency probe
//! and a gate for the stage 3b finding.
//!
//! Two hosts run the real production `TonicTransportFactory<TurmoilIo>`. Each
//! sends `N` messages to the other over a cached channel, each send bounded by a
//! simulated timeout, and drains its own inbound. It measures the **send
//! latency distribution** (simulated milliseconds) so the tail can be quantified
//! and hypotheses tested by changing one knob.
//!
//! # What it established (the stage 3b root cause)
//!
//! With turmoil's **default** link latency (`ECHO_LINK_LATENCY_US=0`), a gRPC
//! round-trip costs tens of milliseconds of simulated time and jitters past
//! 100ms (`p50 ~40-50ms`, `p99 ~100ms`, a few sends missing a 100ms bound). That
//! is neither a rare tail nor a deadlock: it is the default network model, and
//! it exceeds the cluster harness's election timeout — which is why the leader
//! flapped (check-quorum step-down, terms 1->2->3...), not any raft/WAL defect.
//! Pinning **any** explicit latency makes the same probe crisp: 1ms ⇒
//! `p50 == p99 == 2ms` (one RTT) with zero timeouts; 10ms ⇒ `p50 == p99 ==
//! 20ms`, zero timeouts. Keep-alive, warm-up, pacing and one-way traffic change
//! nothing.
//!
//! As a gate, this test pins an explicit 1ms link latency and asserts that every
//! send gets its reply within the bound. Set `ECHO_LINK_LATENCY_US=0` to
//! reproduce the default-latency stall.
//!
//! # Knobs (all optional environment variables)
//!
//! * `ECHO_N` — sends per host (default 50)
//! * `ECHO_TIMEOUT_MS` — per-send bound (default 100)
//! * `ECHO_SLEEP_MS` — spacing between sends (default 5)
//! * `ECHO_WARMUP` — unmeasured sends before measuring (tests connection setup)
//! * `ECHO_KEEPALIVE_MS` — HTTP/2 keep-alive interval; `0` disables it
//! * `ECHO_SENDER_ONLY` — `1` = only host n1 sends (one-way traffic)
//! * `ECHO_LINK_LATENCY_US` — explicit per-link simulated latency; `0` leaves
//!   turmoil's (large, jittery) default in place
//!
//! # Turmoil budget
//!
//! Turmoil's default run budget is ~10s of simulated time, so keep
//! `N * (TIMEOUT_MS + SLEEP_MS)` comfortably under it.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arachne::seam::TransportMessage;
use arachne::{NodeId, Transport, TransportFactory, TransportRx};
use arachne_transport_tonic::TonicTransportFactory;

use arachne_l2::io::TurmoilIo;
use arachne_l2::net::SimNetwork;

const CLUSTER_ID: &str = "echo";

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[derive(Clone, Default, Debug)]
struct NodeReport {
    ok: u64,
    err: u64,
    received: u64,
    /// Measured send latency in simulated milliseconds (the bound itself when a
    /// send timed out).
    latencies: Vec<u64>,
}

fn percentile(sorted: &[u64], p: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as u64 - 1) * p / 100) as usize;
    sorted[idx]
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn box_err<E: std::fmt::Display>(e: E) -> Box<dyn std::error::Error> {
    e.to_string().into()
}

#[test]
fn transport_echo_over_turmoil() {
    let n = env_u64("ECHO_N", 50);
    let timeout_ms = env_u64("ECHO_TIMEOUT_MS", 100);
    let sleep_ms = env_u64("ECHO_SLEEP_MS", 5);
    let warmup = env_u64("ECHO_WARMUP", 0);
    let keepalive_ms = env_u64("ECHO_KEEPALIVE_MS", 30_000);
    let sender_only = env_u64("ECHO_SENDER_ONLY", 0) == 1;
    // Explicit 1ms (a realistic LAN figure) by default; `0` = leave turmoil's
    // default in place, which reproduces the stage 3b stall.
    let link_latency_us = env_u64("ECHO_LINK_LATENCY_US", 1000);

    let send_timeout = Duration::from_millis(timeout_ms.max(1));
    let send_sleep = Duration::from_millis(sleep_ms);
    // `0` means "disable": a keep-alive interval far beyond the run budget is
    // equivalent for this diagnostic.
    let keepalive = Duration::from_millis(if keepalive_ms == 0 { 3_600_000 } else { keepalive_ms });

    let shared: Arc<Mutex<HashMap<u64, NodeReport>>> = Arc::new(Mutex::new(HashMap::new()));
    let addrs_cell: Arc<Mutex<Option<HashMap<NodeId, SocketAddr>>>> = Arc::new(Mutex::new(None));

    let mut sim = SimNetwork::with_seed(0x0_3B0);
    for i in 1..=2u64 {
        let shared = Arc::clone(&shared);
        let addrs_cell = Arc::clone(&addrs_cell);
        sim.host(format!("n{i}"), move || {
            let shared = Arc::clone(&shared);
            let addrs_cell = Arc::clone(&addrs_cell);
            async move {
                let addrs = addrs_cell
                    .lock()
                    .expect("addrs cell")
                    .clone()
                    .expect("addresses assigned before run");
                let me = node_id(i);
                let peer = node_id(if i == 1 { 2 } else { 1 });
                let factory =
                    TonicTransportFactory::with_io(TurmoilIo, CLUSTER_ID, 1, 0, Vec::new(), addrs);
                let _ = factory.request_timeout(send_timeout);
                let _ = factory.connect_timeout(send_timeout);
                let _ = factory.keep_alive_interval(keepalive);
                let bind = SocketAddr::new(IpAddr::from([0, 0, 0, 0]), 7000 + i as u16);
                factory
                    .start_with_bind(me.clone(), bind)
                    .await
                    .map_err(box_err)?;
                let (tx, mut rx) = factory.create(me.clone());

                // Drain inbound so the server's bounded queue never fills.
                let drain = Arc::clone(&shared);
                tokio::spawn(async move {
                    while (rx.recv().await).is_some() {
                        drain.lock().expect("reports").entry(i).or_default().received += 1;
                    }
                });

                if sender_only && i != 1 {
                    return std::future::pending::<turmoil::Result>().await;
                }

                let report = Arc::clone(&shared);
                tokio::spawn(async move {
                    // Unmeasured warm-up (isolates connection establishment).
                    for _ in 0..warmup {
                        let _ = tokio::time::timeout(
                            Duration::from_millis(500),
                            tx.send(peer.clone(), TransportMessage::Raft(vec![0])),
                        )
                        .await;
                        tokio::time::sleep(send_sleep).await;
                    }

                    for _ in 0..n {
                        let start = tokio::time::Instant::now();
                        let outcome = tokio::time::timeout(
                            send_timeout,
                            tx.send(peer.clone(), TransportMessage::Raft(vec![1, 2, 3])),
                        )
                        .await;
                        let elapsed_ms = start.elapsed().as_millis() as u64;
                        // Scoped so the (non-`Send`) guard is dropped before the
                        // await below.
                        {
                            let mut reports = report.lock().expect("reports");
                            let entry = reports.entry(i).or_default();
                            match outcome {
                                Ok(Ok(())) => {
                                    entry.ok += 1;
                                    entry.latencies.push(elapsed_ms);
                                }
                                _ => {
                                    entry.err += 1;
                                    entry.latencies.push(timeout_ms);
                                }
                            }
                        }
                        tokio::time::sleep(send_sleep).await;
                    }
                });

                std::future::pending::<turmoil::Result>().await
            }
        });
    }

    let mut addrs: HashMap<NodeId, SocketAddr> = HashMap::new();
    for i in 1..=2u64 {
        let ip = sim.host_ip(format!("n{i}"));
        addrs.insert(node_id(i), SocketAddr::new(ip, 7000 + i as u16));
    }
    *addrs_cell.lock().expect("addrs cell") = Some(addrs);
    if link_latency_us > 0 {
        sim.set_link_latency(
            "n1",
            "n2",
            Duration::from_micros(link_latency_us),
        );
    }

    // Only the hosts that actually send are waited for.
    let expected_senders: Vec<u64> = if sender_only { vec![1] } else { vec![1, 2] };
    let driver_shared = Arc::clone(&shared);
    sim.client("driver", async move {
        for _ in 0..400 {
            let done = {
                let reports = driver_shared.lock().expect("reports");
                expected_senders.iter().all(|i| {
                    reports
                        .get(i)
                        .map(|r| r.ok + r.err >= n)
                        .unwrap_or(false)
                })
            };
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let mut reports = driver_shared.lock().expect("reports").clone();
        let mut total_err = 0u64;
        for i in 1..=2u64 {
            let Some(report) = reports.get_mut(&i) else {
                continue;
            };
            report.latencies.sort_unstable();
            let l = &report.latencies;
            let over_10 = l.iter().filter(|v| **v > 10).count();
            let over_50 = l.iter().filter(|v| **v > 50).count();
            eprintln!(
                "[echo] n{i}: ok={} err={} received={} latency_ms(min/p50/p90/p99/max)={}/{}/{}/{}/{} >10ms={} >50ms={}",
                report.ok,
                report.err,
                report.received,
                percentile(l, 0),
                percentile(l, 50),
                percentile(l, 90),
                percentile(l, 99),
                percentile(l, 100),
                over_10,
                over_50,
            );
            total_err += report.err;
        }
        assert_eq!(
            total_err, 0,
            "every send must get its reply within {timeout_ms}ms of simulated time"
        );
        Ok(())
    });

    sim.run().expect("sim runs");
}
