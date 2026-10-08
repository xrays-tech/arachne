//! State machines for the replicated log.
//!
//! * [`kv`] — an in-memory KV + idempotency session-table state machine.

pub mod kv;

pub use kv::{
    CasOp, CasPred, CasResult, KvError, KvStateMachine, MAX_MULTI_PUT_ENTRIES,
    MAX_MULTI_PUT_TOTAL_BYTES, MAX_STALE_RANGE_ENTRIES,
};
