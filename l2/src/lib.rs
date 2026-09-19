//! Arachne L2 — run the REAL production transport over `turmoil` (M1 stage 3b).
//!
//! This crate proves the stage-3a `TransportIo` seam end to end: the *same*
//! production tonic transport code that runs over real TCP in production is
//! driven here over a `turmoil` simulated network + clock, with a
//! deterministic `TurmoilIo` implementation of the seam. A 3-node Arachne
//! cluster (real `RaftNode` + `WalStorage` + `Runtime`, real tonic gRPC) elects
//! a leader, commits a write, and replicates it to all three nodes — and the
//! whole trace is byte-identical across two same-seed runs.
//!
//! # Modules
//!
//! * [`io`] — `TurmoilIo`, the deterministic [`TransportIo`](arachne_transport_tonic::TransportIo)
//!   implementation (virtualized `Listener`/`Incoming`/`ClientIo`/`Connector`).
//! * [`net`] — `SimNetwork`, a thin, well-named wrapper over `turmoil::Sim`
//!   exposing the fault-injection primitives, layered on top of the transport
//!   seam (orthogonal to it).
//! * [`harness`] — the 3-node in-sim cluster and its scenario driver.
//!
//! This is a dev-only, standalone workspace (never published, never in any
//! production crate's dependency tree). See `l2/README.md` for the L2 roadmap.

pub mod harness;
pub mod io;
pub mod net;
