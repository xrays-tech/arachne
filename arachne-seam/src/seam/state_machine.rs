//! The state-machine seam.
//!
//! This is where the product's actual data lives. The core replicates log
//! entries and, once an entry is committed, applies it to the state machine in
//! index order. The state machine is the only place that mutates user data, and
//! it MUST be a pure, deterministic function of the applied log.

use crate::types::LogIndex;

/// The value a state machine returns after applying a single command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The command applied and produced a value.
    Value(Vec<u8>),
    /// The command applied with no value.
    None,
}

/// The deterministic data store driven by the replicated log.
///
/// # Determinism contract
///
/// * [`apply`](StateMachine::apply) MUST be deterministic: the same command at
///   the same index, starting from the same prior state, MUST yield the same
///   outcome and the same new state. It MUST NOT read the clock, any ambient
///   state, or perform I/O — the only inputs are `index` and `command`.
/// * Indices are applied in strictly increasing order; [`applied_index`](StateMachine::applied_index)
///   reports the highest index applied so far.
/// * [`snapshot`](StateMachine::snapshot) / [`restore`](StateMachine::restore) form a
///   lossless round-trip over the *entire* state (including any idempotency
///   session table), so a node can be rebuilt from a snapshot and then replay
///   the log from `applied_index`.
///
/// The trait is `Send` (not `Sync`): a state machine is owned and mutated by a
/// single apply task.
pub trait StateMachine: Send + 'static {
    /// The error type reported by this state machine.
    type Error: core::error::Error + Send + Sync + 'static;

    /// Apply the committed `command` at `index`.
    ///
    /// MUST be deterministic and MUST NOT read the clock or any ambient state.
    ///
    /// # Error semantics (fail-stop)
    ///
    /// An `Err` from `apply` signals a **state-machine invariant violation** —
    /// the machine's state and the log have diverged in a way that cannot be
    /// repaired by retrying. The caller MUST **fail-stop** (abort the node); it
    /// must never retry the entry or degrade to a fallback. Re-application is
    /// undefined because the machine is already inconsistent.
    fn apply(&mut self, index: LogIndex, command: &[u8]) -> Result<ApplyOutcome, Self::Error>;

    /// Read the value for `key` from the applied state, if present.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Produce an atomic, consistent snapshot of ALL state, including any
    /// idempotency session table.
    fn snapshot(&self) -> Result<Vec<u8>, Self::Error>;

    /// Restore state from a byte string produced by [`snapshot`](StateMachine::snapshot).
    fn restore(&mut self, bytes: &[u8]) -> Result<(), Self::Error>;

    /// The highest log index applied so far (0 if none).
    fn applied_index(&self) -> LogIndex;
}
