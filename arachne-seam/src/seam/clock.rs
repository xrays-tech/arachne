//! The time seam.
//!
//! All time in the Arachne core flows through this trait.
//!
//! # Determinism
//!
//! The **state machine** never reads the clock. Its `apply` sees only
//! timestamps that a leader has already stamped onto committed log entries;
//! those stamps travel inside the log and are part of the deterministic input
//! stream, not ambient state. Reading the clock inside `apply` would let the
//! same entry produce different state on different machines — a correctness
//! violation.
//!
//! # Where wall-clock time *is* used
//!
//! The **leader** does read real (wall-clock) time: it stamps entries with
//! wall-clock milliseconds so that sessions can be given TTLs and
//! garbage-collected (that work lands in M3). That is the one legitimate
//! source of wall-clock time in the system, and it flows *forward* in log
//! entries rather than being read by the state machine.
//!
//! **NOTE:** an additive `wall_clock_millis()` method will be added to this
//! trait before M3, alongside the monotonic `now_millis()` used for internal
//! timing. It is deliberately NOT added yet.

use crate::types::Timestamp;

/// A source of time for the Arachne core.
///
/// # Determinism contract
///
/// The state machine's `apply` MUST NOT read the clock — see the module docs
/// for the full contract and the note about the wall-clock method that lands
/// in M3. Implementations are `Send + Sync` so a single clock can be shared
/// across the many tasks that make up a node.
pub trait Clock: Send + Sync + 'static {
    /// Current time as **monotonic** milliseconds; guaranteed non-decreasing.
    ///
    /// Used for internal timing (e.g. election/heartbeat intervals). This is
    /// *not* the wall clock the leader uses to stamp session-TTL entries — that
    /// additive method lands in M3 (see the module docs).
    fn now_millis(&self) -> Timestamp;
}
