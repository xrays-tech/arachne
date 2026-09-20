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
//! * [`oracle`] — the deterministic [`ClientOracle`](oracle::History): a
//!   clock-free invariant checker over a client's recorded operation history
//!   (test-plan §6.4 / §7 / §9).
//! * [`linearizability`] — a self-built **complete** linearizability checker
//!   (a Wing–Gong–equivalent search) over the reduced history, cross-validated
//!   against stateright in tests (test-plan §4 T2 / D-T2).
//! * [`DurabilityLedger`] — per-node durability ledger (entry fsyncs plus
//!   persisted HardStates) for reconciling outbound raft messages against INV1.
//! * [`FaultyStorage`] — a deterministic fault-injecting [`Storage`] wrapper.
//! * [`FsyncLedger`] — records segment fsyncs (the WAL's `FsyncObserver`).
//!
//! [`Storage`]: arachne_seam::Storage
//!
//! All of these are dependency-free (std/core only) and build offline. The
//! only external crate is `stateright`, which is a **dev-dependency** used solely
//! in `#[cfg(test)]` to cross-validate the linearizability checker.

mod clock;
mod durability;
mod faulty_storage;
mod fsync_ledger;
mod linearizability;
mod oracle;
mod rng;
mod state_machine;
mod store_suite;
mod transport;

pub use clock::ManualClock;
pub use durability::{DurabilityLedger, PersistedHardState};
pub use faulty_storage::{FaultSchedule, FaultyStorage, OpKind, OpRecord};
pub use fsync_ledger::{FsyncEvent, FsyncLedger};
pub use linearizability::{check_linearizable, check_reduced, CheckOutcome, KvState};
pub use oracle::{
    CallId, ClientId, Event, Failure, History, Invariant, LogOp, Op, OpResult, OracleErrorKind,
    OracleReport, ReducedHistory, SeqNo, ValueId,
};
pub use rng::SeededRng;
pub use state_machine::{InMemoryStateMachine, SmError};
pub use store_suite::{CheckResult, SuiteReport, run_storage_suite};
pub use transport::{
    block_on, InMemoryClosed, InMemoryRx, InMemoryTransportFactory, InMemoryTx, TransportError,
};
