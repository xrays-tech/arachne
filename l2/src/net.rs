//! `SimNetwork` — a thin, well-named wrapper over [`turmoil::Sim`].
//!
//! It exposes the fault-injection primitives (partition, crash, latency, loss, …)
//! and host addressing under stable, self-documenting names.
//!
//! # Orthogonal to the transport seam
//!
//! `SimNetwork` layers **on top of** turmoil and is **orthogonal** to the
//! transport seam ([`crate::io::TurmoilIo`]). The two answer different
//! questions:
//!
//! * the **seam** decides *how* the transport talks to the network (real TCP in
//!   production vs. simulated TCP here);
//! * `SimNetwork` decides *what happens to the network* (partitions, crashes,
//!   latency, loss).
//!
//! You can partition the sim without touching the seam, and swap the seam
//! (turmoil ↔ real TCP) without touching the fault model. The 3-node harness
//! (`crate::harness`) uses the seam to run real tonic, and `SimNetwork` to
//! address hosts and (in later M2 scenarios) inject faults.

use std::future::Future;
use std::net::IpAddr;
use std::time::Duration;

use turmoil::{Builder, Result, Sim, ToIpAddr, ToIpAddrs};

/// A thin wrapper over [`turmoil::Sim`] with well-named, self-documenting
/// access to host registration, host addressing, and fault injection.
pub struct SimNetwork<'a> {
    sim: Sim<'a>,
}

impl SimNetwork<'static> {
    /// Build a deterministic sim with a fixed RNG seed.
    ///
    /// The same seed reproduces the same scheduling, network timing, and (with
    /// the raft election seed) the same election — the foundation of the
    /// double-run determinism gate.
    pub fn with_seed(seed: u64) -> SimNetwork<'static> {
        let mut builder = Builder::new();
        builder.rng_seed(seed);
        SimNetwork {
            sim: builder.build(),
        }
    }
}

impl<'a> SimNetwork<'a> {
    /// Register a long-lived host (its closure is re-invoked on `bounce`).
    pub fn host<F, Fut>(&mut self, addr: impl ToIpAddr, host: F)
    where
        F: Fn() -> Fut + 'a,
        Fut: Future<Output = Result> + 'static,
    {
        self.sim.host(addr, host)
    }

    /// Register a client task (runs to completion; the sim finishes when all
    /// clients have).
    pub fn client<F>(&mut self, addr: impl ToIpAddr, client: F)
    where
        F: Future<Output = Result> + 'static,
    {
        self.sim.client(addr, client)
    }

    /// Run the simulation until every client has completed (hosts keep running
    /// in the background).
    pub fn run(&mut self) -> Result {
        self.sim.run()
    }

    /// Resolve a host name (or IP) to the IP address the sim assigned to it.
    ///
    /// This is how the harness learns each host's simulated address so it can
    /// build the cluster's `NodeId → SocketAddr` map.
    pub fn host_ip(&self, name: impl ToIpAddr) -> IpAddr {
        self.sim.lookup(name)
    }

    // ---- Fault injection (layers on top of the transport seam) -------------

    /// Drop all messages between two hosts (bidirectional partition).
    pub fn partition(&self, a: impl ToIpAddrs, b: impl ToIpAddrs) {
        self.sim.partition(a, b);
    }

    /// Drop messages from `from` to `to` only (one-way partition).
    pub fn partition_oneway(&self, from: impl ToIpAddrs, to: impl ToIpAddrs) {
        self.sim.partition_oneway(from, to);
    }

    /// Re-deliver messages previously dropped by a partition.
    pub fn repair(&self, a: impl ToIpAddrs, b: impl ToIpAddrs) {
        self.sim.repair(a, b);
    }

    /// Hold messages between two hosts until [`Self::release`].
    pub fn hold(&self, a: impl ToIpAddrs, b: impl ToIpAddrs) {
        self.sim.hold(a, b);
    }

    /// Release messages previously held by [`Self::hold`].
    pub fn release(&self, a: impl ToIpAddrs, b: impl ToIpAddrs) {
        self.sim.release(a, b);
    }

    /// Crash a host (its software stops until [`Self::bounce`]).
    pub fn crash(&mut self, host: impl ToIpAddrs) {
        self.sim.crash(host);
    }

    /// Restart a crashed host (re-invokes its host closure).
    pub fn bounce(&mut self, host: impl ToIpAddrs) {
        self.sim.bounce(host);
    }

    /// Set the global message drop probability in `[0.0, 1.0]`.
    pub fn set_fail_rate(&mut self, value: f64) {
        self.sim.set_fail_rate(value);
    }

    /// Add a per-link latency between two hosts.
    pub fn set_link_latency(&self, a: impl ToIpAddrs, b: impl ToIpAddrs, value: Duration) {
        self.sim.set_link_latency(a, b, value);
    }
}
