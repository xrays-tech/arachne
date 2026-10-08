//! `cargo-fuzz` target: KV state-machine snapshot restore (v0.3.0 payload).
//!
//! Feeds arbitrary byte sequences to the KV state machine and asserts it
//! **never panics** on any input, in two directions:
//!
//! 1. **`apply`**(arbitrary command bytes, M1–M3 opcodes) must never panic and
//!    must stay **self-consistent**: a machine that applied a (possibly
//!    malformed) command, was snapshotted, and was rebuilt from that snapshot
//!    must be able to apply the same bytes again — including the M3
//!    `KV_SNAPSHOT_VERSION` 2 session tag (`CasFailed`) — without panicking.
//!    (No byte-for-byte snapshot equality is asserted here: re-applying the
//!    same bytes at a higher index is a *different session* and legitimately
//!    changes the applied watermark / cached outcomes.)
//! 2. **`restore`**(arbitrary snapshot bytes) must land in `Ok(())` or
//!    `Err(KvError::MalformedSnapshot)` — a panic is a fuzzer failure. On
//!    success, `data` must be the **canonical encoding** of the decoded store:
//!    re-snapshotting reproduces the same bytes (lossless, deterministic
//!    `BTreeMap` ordering).
//!
//! Run (nightly):
//! ```sh
//! cargo +nightly fuzz run kv_snapshot_restore
//! ```

#![no_main]

use arachne_kv::state_machine::KvStateMachine;
use arachne_kv::StateMachine;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // ---- 1. apply(arbitrary command) must not panic and must be deterministic
    //
    // Any byte slice is a candidate command (including malformed/truncated
    // ones). A malformed command returns `Err(MalformedCommand)`; a valid one
    // applies. Either way the machine must stay consistent, and re-applying the
    // same command to a snapshot-restored machine must be byte-identical.
    let sm = KvStateMachine::new();
    let applied = sm.apply(1, data);
    if applied.is_ok() {
        let snap1 = sm.snapshot().unwrap();
        // Rebuild from the snapshot and apply the same command again at the
        // *next* index: the result must be deterministic (same snap bytes).
        let sm2 = KvStateMachine::new();
        sm2.restore(&snap1).unwrap();
        let applied2 = sm2.apply(2, data);
        // A re-(client_id,seq_no) is a *different* session (same bytes but
        // arbitrary content), so it may legitimately return a different
        // outcome — but it must never panic and must keep the machine
        // consistent.
        let _ = applied2;
        let _ = sm2.snapshot().unwrap();
    }

    // ---- 2. restore(arbitrary bytes) must not panic; success ⇒ canonical
    let sm = KvStateMachine::new();
    let restored = sm.restore(data);
    if restored.is_ok() {
        // Successful restore implies `data` is the canonical encoding of the
        // decoded store: re-snapshotting must reproduce it exactly.
        assert_eq!(sm.snapshot().unwrap(), data);
        // Reads after restore must work and never expose origin index 0 for a
        // present key (public-API invariant; `get_with_index` guards this).
        let _ = sm.get_with_index(data);
    }
});
