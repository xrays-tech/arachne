//! Minimal reproducible comparison for the stage 3b spike: a **transport-layer
//! echo over turmoil with no raft and no WAL**.
//!
//! Two hosts run the real production `TonicTransportFactory<TurmoilIo>`. Each
//! sends `N` messages to the other, each send bounded by a simulated timeout,
//! and drains its own inbound. This separates the layers:
//!
//! * if this test stalls the way the 3-node cluster does, the defect is in the
//!   transport seam + tonic/hyper under turmoil, independent of consensus;
//! * if it passes, the cluster failure is a raft/WAL/timing interaction.
//!
//! Every send is wrapped in `tokio::time::timeout`, so a hang surfaces as a
//! counted failure and a failing assertion rather than an unbounded test.
//!
//! # Observed (deterministic, same seed)
//!
//! This test **currently fails**, which is the point: all 50 requests arrive at
//! the peer (`received = 50` on both), but 1 send on n1 and 4 on n2 do not get
//! their reply within the 100ms simulated bound. Widening the bound to 1500ms
//! (with 5 sends) yields zero failures — so the replies are **delayed, not
//! lost**. That is the stage 3b root cause: occasional sub-second-to-second
//! gRPC reply latencies that exceed the node's election timeout, causing
//! check-quorum step-downs and election churn. See the stage 3b entry in
//! `dev-docs/handoff-m1.md`.
//!
//! `#[ignore]`d like the cluster spike: it is a diagnostic reproducer, not a
//! gate, until the latency source is fixed — then it should be un-ignored.

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
/// Messages each host sends to the other.
const N: u64 = 50;
/// Per-send bound (simulated time). 100ms is tight enough to catch the
/// transport's occasional slow replies; 1500ms hides them.
const SEND_TIMEOUT: Duration = Duration::from_millis(100);
/// Turmoil's default run budget is ~10s of simulated time; every send is
/// bounded, so the worst case (every send timing out) is `N * (TIME + 5ms)`,
/// which must stay under it.
const DRIVER_POLLS: usize = 400;

#[derive(Clone, Copy, Default, Debug)]
struct Counts {
    sent_ok: u64,
    sent_err: u64,
    received: u64,
}

fn node_id(i: u64) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn box_err<E: std::fmt::Display>(e: E) -> Box<dyn std::error::Error> {
    e.to_string().into()
}

#[test]
#[ignore = "stage 3b diagnostic comparison (transport echo under turmoil)"]
fn transport_echo_over_turmoil() {
    let shared: Arc<Mutex<HashMap<u64, Counts>>> = Arc::new(Mutex::new(HashMap::new()));
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
                let _ = factory.request_timeout(SEND_TIMEOUT);
                let _ = factory.connect_timeout(SEND_TIMEOUT);
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
                        drain.lock().expect("counts").entry(i).or_default().received += 1;
                    }
                });

                // Send N messages, each bounded by a simulated timeout.
                let send = Arc::clone(&shared);
                tokio::spawn(async move {
                    for _ in 0..N {
                        let outcome = tokio::time::timeout(
                            SEND_TIMEOUT,
                            tx.send(peer.clone(), TransportMessage::Raft(vec![1, 2, 3])),
                        )
                        .await;
                        {
                            let mut counts = send.lock().expect("counts");
                            let entry = counts.entry(i).or_default();
                            match outcome {
                                Ok(Ok(())) => entry.sent_ok += 1,
                                _ => entry.sent_err += 1,
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
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

    let driver_shared = Arc::clone(&shared);
    sim.client("driver", async move {
        for _ in 0..DRIVER_POLLS {
            let done = {
                let counts = driver_shared.lock().expect("counts");
                [1u64, 2].iter().all(|i| {
                    counts
                        .get(i)
                        .map(|c| c.sent_ok + c.sent_err >= N)
                        .unwrap_or(false)
                })
            };
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let counts = driver_shared.lock().expect("counts").clone();
        eprintln!("[echo] counts = {counts:?}");
        for i in 1..=2u64 {
            let c = counts.get(&i).copied().unwrap_or_default();
            assert_eq!(c.sent_err, 0, "node {i} had {} failed sends", c.sent_err);
            assert_eq!(c.sent_ok, N, "node {i} completed {} of {N} sends", c.sent_ok);
        }
        Ok(())
    });

    sim.run().expect("sim runs");
}
