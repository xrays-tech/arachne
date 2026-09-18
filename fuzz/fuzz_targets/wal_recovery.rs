//! `cargo-fuzz` target: WAL recovery (M0 ② / INV6, randomized).
//!
//! Feeds an arbitrary byte sequence as a single WAL segment to
//! `WalStorage::open` and asserts it **never panics** on any input. Every input
//! must land in one of the two INV6 quadrants (a `Result` of `Ok` or `Err`); a
//! panic is a fuzzer failure.
//!
//! This is the randomized companion to the deterministic in-repo harness
//! (`arachne/tests/wal_mutation.rs`), which is the primary CI evidence.
//!
//! Run (nightly):
//! ```sh
//! cargo +nightly fuzz run wal_recovery
//! ```
//!
//! The entry is **synchronous** — there is no `#[tokio::main]` here.
//!
//! `#![no_main]`: the process entry point is the C `main` from libFuzzer's
//! `FuzzerMain.cpp` (linked via `libfuzzer-sys`'s default `link_libfuzzer`
//! feature); the `fuzz_target!` macro only defines the `extern "C"` entry points
//! (`LLVMFuzzerInitialize` / `rust_fuzzer_test_input`) that that `main` drives.

#![no_main]

use std::sync::OnceLock;

use arachne::storage::{
    segment_name, write_meta, Meta, FORMAT_VERSION, WalConfig, WalOptions, WalStorage,
};

/// A fixed temp directory reused across iterations. Creating a fresh directory
/// per iteration would be far too slow for the millions of iterations libFuzzer
/// performs. A valid `META` is written once; the fuzzed bytes always go into the
/// single `wal-1.log` segment (overwritten on each iteration).
static DIR: OnceLock<std::path::PathBuf> = OnceLock::new();

fn data_dir() -> &'static std::path::PathBuf {
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("arachne-fuzz-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create fuzz data dir");
        let meta = Meta {
            cluster_id: "fuzz-cluster".into(),
            node_id: "fuzz-node".into(),
            format_version: FORMAT_VERSION,
            created_at: 1_700_000_000_000,
        };
        let _ = write_meta(&dir, &meta);
        dir
    })
}

fn options() -> WalOptions {
    WalOptions {
        cluster_id: "fuzz-cluster".into(),
        node_id: "fuzz-node".into(),
        config: WalConfig::default(),
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let dir = data_dir();
    // Materialize the fuzzed segment as the (only) segment file, overwriting
    // any previous iteration's bytes (recovery may have truncated it).
    let seg_path = dir.join(segment_name(1));
    let _ = std::fs::write(&seg_path, data);
    // Run recovery. It must return a `Result` (Ok or Err) and MUST NOT panic.
    // The returned `WalStorage` (if any) is dropped immediately, releasing the
    // data-dir lock before the next iteration.
    let _ = WalStorage::open(dir, options());
});
