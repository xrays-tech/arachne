//! State machines for the replicated log.
//!
//! * [`kv`] — an in-memory KV + idempotency session-table state machine.

pub mod kv;

pub use kv::{KvError, KvStateMachine};
