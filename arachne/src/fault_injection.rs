//! Test-only **crash injection** at `RaftNode::step`'s ready-stage boundaries
//! (propsol v0.2.9 L / INV2).
//!
//! Compiled **only** with the `fault-injection` feature, which is **not a
//! default feature** and is excluded from release/publish builds (enforced by
//! `scripts/check-release-features.sh`). With the feature off, this module and
//! every call site are compiled out: zero behaviour change, zero runtime cost.
//!
//! # What it models
//!
//! **Process death at a precise point in the Ready loop.** A test arms a one-shot
//! stage; the runtime checks it at that boundary inside `step`; the first check
//! consumes the arm and stops the node. Volatile state is discarded, and
//! whatever was fsynced stays on disk — exactly what a crash does. The harness
//! catches the stop with `catch_unwind` and restarts the node from its WAL.
//!
//! Round-granularity crashes (`crash`/`bounce` in the harness) cannot express the
//! window *between* persisting entries and propagating them, which is the point
//! of INV2's ready-stage sweep.
//!
//! # What it does NOT model
//!
//! * **Torn writes** — the byte-level mutation battery (`wal_mutation.rs`,
//!   `m2_wal_faults.rs`) owns those.
//! * **fsync failure** — [`FaultyStorage`](arachne_testsupport::FaultyStorage)
//!   owns that.
//! * **Real power-loss timing** — that is L4's job.
//!
//! The arm state is **thread-local** so tests running in parallel cannot
//! interfere, and it is one-shot so a crash fires exactly once per arm.

use std::cell::Cell;

/// A `RaftNode::step` boundary at which a test can crash the node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Entries and HardState are durable; **no** message has been sent yet.
    AfterPersist,
    /// Messages have been sent; committed entries have not been applied yet
    /// (apply happens in the caller, after `step` returns).
    AfterDeliver,
}

impl Stage {
    fn code(self) -> u8 {
        match self {
            Stage::AfterPersist => 1,
            Stage::AfterDeliver => 2,
        }
    }
}

/// A distinctive token present only in `fault-injection` builds; the release
/// gate greps for it. Keep in sync with `scripts/check-release-features.sh`.
pub const HOOK_SENTINEL: &str = "arachne-fault-injection-hook";

thread_local! {
    static ARMED: Cell<u8> = const { Cell::new(0) };
}

/// Arm a one-shot crash at `stage` on the current thread.
pub fn arm(stage: Stage) {
    ARMED.with(|a| a.set(stage.code()));
}

/// Disarm without triggering (test cleanup / defensive).
pub fn disarm() {
    ARMED.with(|a| a.set(0));
}

/// Whether a crash is currently armed on this thread.
pub fn armed() -> bool {
    ARMED.with(|a| a.get() != 0)
}

/// Called by the runtime at a stage boundary. Consumes a matching arm and stops
/// the node; a no-op when nothing is armed.
///
/// The stop is a `panic!` with a fixed payload (the sentinel), which the harness
/// catches. `panic!` is deliberate here: this module is test-only and the panic
/// *is* the modelled failure, so the project's production `panic!` ban does not
/// apply (the code is absent from release builds).
#[inline]
pub(crate) fn check(stage: Stage) {
    let hit = ARMED.with(|a| {
        if a.get() == stage.code() {
            a.set(0);
            true
        } else {
            false
        }
    });
    if hit {
        panic!("{HOOK_SENTINEL}: crash at {stage:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arm_is_one_shot_and_stage_specific() {
        disarm();
        assert!(!armed());
        // A check for a different stage does not consume the arm.
        check(Stage::AfterDeliver);
        arm(Stage::AfterPersist);
        assert!(armed());
        let caught = std::panic::catch_unwind(|| check(Stage::AfterPersist));
        assert!(caught.is_err(), "the armed stage must stop the node");
        assert!(!armed(), "the arm must be consumed by the crash");
        // The crash is not sticky.
        check(Stage::AfterPersist);
        check(Stage::AfterDeliver);
    }
}
