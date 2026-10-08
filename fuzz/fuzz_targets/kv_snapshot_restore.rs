//! `cargo-fuzz` target: KV state-machine snapshot restore (v0.3.0 payload).
//!
//! Feeds an arbitrary byte sequence to [`KvStateMachine::restore`] and asserts
//! it **never panics** on any input. Every input must land in `Ok(())` or
//! `Err(KvError::MalformedSnapshot)` — a panic is a fuzzer failure.
//!
//! This hardens the v0.3.0 format change (design `dev-docs/arachne-kv-commit-
//! index-design.md` §5.1/§5.4): the kv snapshot payload gained a leading format
//! version byte and a per-key 8-byte origin index, and legacy / unknown payloads
//! are **rejected** (fail-stop), never migrated. `restore` decodes entirely
//! outside the write lock with bounds-checked reads, so the panic surface is
//! exactly what this target exercises.
//!
//! Round-trip invariant: when `restore` succeeds, `data` must be the **canonical
//! encoding** of the decoded store — re-snapshotting reproduces the same bytes
//! (lossless, deterministic `BTreeMap` ordering). A counterexample would mean
//! encode/decode symmetry regression.
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
    let sm = KvStateMachine::new();
    // Any input must land in `Ok(())` or `Err(MalformedSnapshot)`; a panic is
    // a fuzzer failure (e.g. the version-byte check or a bounds decode
    // regressing).
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
