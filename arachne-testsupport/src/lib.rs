//! Test-only support crate.
//!
//! Must be referenced only as a dev-dependency by other crates. It depends on
//! the **seam** crate (`arachne-seam`) — and *never* on `arachne` — so its
//! normal dependency tree can never pull in the tonic transport (enforced by
//! `scripts/check-deps.sh` Gate C); production crates may only pull this crate
//! in via their `[dev-dependencies]` (enforced by `scripts/check-deps.sh`
//! Gate A).
//!
//! This crate provides the **deterministic** implementations of the seam
//! traits, so the deterministic core can be exercised in tests and the
//! simulator without any real time, network, or external dependencies:
//!
//! * [`ManualClock`] — a manually-advanced [`Clock`](arachne_seam::Clock).
//! * [`SeededRng`] — a deterministic, seeded [`Rng`](arachne_seam::Rng).
//! * [`InMemoryTransportFactory`] — an in-memory
//!   [`TransportFactory`](arachne_seam::TransportFactory) wiring up
//!   [`InMemoryTx`]/[`InMemoryRx`] over channels.
//! * [`InMemoryStateMachine`] — a deterministic byte-keyed
//!   [`StateMachine`](arachne_seam::StateMachine).
//! * [`block_on`] — a tiny no-waker executor helper for driving the in-memory
//!   transport futures in tests/sim.
//!
//! All of these are dependency-free (std/core only) and build offline.

mod clock;
mod faulty_storage;
mod fsync_ledger;
mod rng;
mod state_machine;
mod store_suite;
mod transport;

pub use clock::ManualClock;
pub use faulty_storage::{FaultSchedule, FaultyStorage, OpKind, OpRecord};
pub use fsync_ledger::{FsyncEvent, FsyncLedger};
pub use rng::SeededRng;
pub use state_machine::{InMemoryStateMachine, SmError};
pub use store_suite::{CheckResult, SuiteReport, run_storage_suite};
pub use transport::{
    block_on, InMemoryRx, InMemoryTransportFactory, InMemoryTx, TransportError,
};
