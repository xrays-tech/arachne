//! End-to-end test of the minimal single-instance embedding façade
//! (`arachne::server::Arachne`).
//!
//! The façade is a *global* single instance (exactly one node per process,
//! held in a `static`). `cargo test` runs tests concurrently in one process,
//! so multiple tests that touch the façade would race on the shared instance
//! and corrupt each other. Each test in this suite therefore acquires a
//! process-level lock ([`FACADE_LOCK`]) that serializes instance ownership.
//!
//! There are two lifecycle tests:
//! * [`facade_lifecycle_end_to_end`] — drives the peerless path via
//!   `Arachne::new` (which internally builds a peerless `ClusterConfig` and
//!   calls `Arachne::start`);
//! * [`facade_single_node_still_works_via_start`] — drives the same peerless
//!   path via the unified entry `Arachne::start(ClusterConfig::single_node)`.
//!
//!   1. pre-init: `get`/`get_stale`/`handle` all fail with `NotInitialized`;
//!   2. `new` succeeds once; a second call fails with `AlreadyInitialized`;
//!   3. the singleton self-elects (pre-election reads fast-path to
//!      `NotLeader`/`QuorumUnavailable`, then become `Ok`);
//!   4. `set` → `get`/`get_stale` round-trip (only possible once elected);
//!   5. `delete` → `get` returns `None`;
//!   6. `shutdown` → subsequent reads/handle fail with `NotInitialized`;
//!   7. `shutdown` is idempotent.
//!
//! **Gate C (no real time in tests):** this file deliberately avoids `std::time`
//! and `tokio::net`. The unique temp dir is derived from `std::process::id()`
//! plus a process-local sequence counter (no `SystemTime`), and every polling
//! loop is bounded by a fixed number of attempts (not a wall-clock `Instant`).
//!
//! `new()`/`shutdown()` are *synchronous* (synchronous assembly / join); the
//! data-plane ops (`set`/`get`/`get_stale`/`delete`/`handle`) are `async`.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use arachne::server::{Arachne, ArachneError, ClusterConfig, WalConfig};

// Unique per-process sequence so that `process::id()` (shared across all tests
// in one binary) is disambiguated without reaching for real time.
static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

// Serializes the façade tests against each other: `cargo test` runs them
// concurrently in one process, and both touch the single-instance
// `INSTANCE` static. Holding this guard for the whole test body guarantees
// exactly one test owns the instance at a time.
static FACADE_LOCK: Mutex<()> = Mutex::new(());

/// A fresh, unique temp dir for a single test, built from the process ID and
/// a process-local counter (no real time involved).
fn unique_tempdir() -> PathBuf {
    let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(env::temp_dir())
        .join(format!("arachne-facade-{0}-{1}", std::process::id(), seq))
}

/// Number of 50ms poll attempts before giving up (≈ 2s — comfortably inside a
/// default 150ms election window; a bounded count stands in for a wall-clock
/// deadline, so no real time is needed).
const MAX_ATTEMPTS: u32 = 40;

/// Sleep a fixed poll interval. `tokio::time` is allowed — Gate C only forbids
/// `std::time` and `tokio::net`.
async fn poll() {
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
}

/// Poll `get` until the singleton self-elects (leader). Pre-election linear
/// reads fast-path to `NotLeader`/`QuorumUnavailable`; once elected they return
/// `Ok(_)` for the not-yet-written key. Returns `true` if leadership was
/// observed within the attempt bound.
async fn wait_for_leader() -> bool {
    let mut last_err: String = String::new();
    for _ in 0..MAX_ATTEMPTS {
        match Arachne::get(b"hello").await {
            Ok(_val) => return true, // any Ok means we are a leader (quorum-free read).
            Err(e) => last_err = format!("{e:?}"),
        }
        poll().await;
    }
    eprintln!("warning: singleton did not self-elect in {MAX_ATTEMPTS} attempts; last error: {last_err}");
    false
}

#[tokio::test]
async fn facade_lifecycle_end_to_end() {
    // Serialize against the other façade test: both own the single-instance
    // static, so exactly one may do so at a time.
    let _guard = FACADE_LOCK.lock().unwrap();
    let dir = unique_tempdir();

    // ---- pre-init: every op fails with NotInitialized (no runtime yet) ----
    match Arachne::get(b"hello").await {
        Err(ArachneError::NotInitialized) => {}
        Err(e) => panic!("pre-init get: expected NotInitialized, got {e}"),
        Ok(_) => panic!("pre-init get: expected error, got Ok"),
    }
    match Arachne::get_stale(b"hello").await {
        Err(ArachneError::NotInitialized) => {}
        Err(e) => panic!("pre-init get_stale: expected NotInitialized, got {e}"),
        Ok(_) => panic!("pre-init get_stale: expected error, got Ok"),
    }
    match Arachne::handle().await {
        Err(ArachneError::NotInitialized) => {}
        Err(e) => panic!("pre-init handle: expected NotInitialized, got {e}"),
        Ok(_) => panic!("pre-init handle: expected error, got Ok"),
    }

    // ---- 2. Initialize once — succeeds. `new` is synchronous (synchronous
    //        assembly: create dir, open WAL, spawn the actor thread). ----
    Arachne::new(1, &dir, WalConfig::default()).expect("first new");

    // ---- 3. A second init in the same process must fail with AlreadyInitialized. ----
    match Arachne::new(2, &dir, WalConfig::default()) {
        Err(ArachneError::AlreadyInitialized) => {}
        Err(e) => panic!("second new: expected AlreadyInitialized, got {e}"),
        Ok(_) => panic!("second new: expected AlreadyInitialized, got Ok"),
    }

    // ---- 4. Wait for the singleton to self-elect. Writes require leadership,
    //         so we confirm it via the linear-read fast-path before proposing. ----
    assert!(
        wait_for_leader().await,
        "singleton should self-elect within {MAX_ATTEMPTS} poll attempts"
    );

    // ---- 5. Now write (safe: node is a leader). ----
    Arachne::set(b"hello", b"world").await.expect("set after leader ready");

    // ---- 6. Linear-read back, polling up to MAX_ATTEMPTS until the value is
    //         observed. ----
    let mut got_value: Option<Vec<u8>> = None;
    let mut last_err: String = String::new();
    for _ in 0..MAX_ATTEMPTS {
        match Arachne::get(b"hello").await {
            Ok(Some(v)) => {
                got_value = Some(v);
                break;
            }
            _ => last_err = "not Some(b\"world\") yet".into(),
        }
        poll().await;
    }
    assert!(
        got_value.is_some(),
        "get after set should succeed within {MAX_ATTEMPTS} poll attempts; last: {last_err}"
    );
    assert_eq!(got_value.unwrap(), b"world".to_vec());

    // ---- 7. Stale-read should also see the value (on a singleton `get_stale`
    //         is quorum-free; poll in case it is still settling). ----
    let mut got_stale: Option<Vec<u8>> = None;
    for _ in 0..MAX_ATTEMPTS {
        match Arachne::get_stale(b"hello").await {
            Ok(Some(v)) => {
                got_stale = Some(v);
                break;
            }
            _ => {}
        }
        poll().await;
    }
    assert_eq!(
        got_stale.as_deref(),
        Some(b"world".as_slice()),
        "get_stale after set: should return the written value within the poll window"
    );

    // ---- 8. Delete → linear-read returns None (poll until the delete commits). ----
    Arachne::delete(b"hello").await.expect("delete after leader ready");
    let mut saw_none = false;
    let mut last_err: String = String::new();
    for _ in 0..MAX_ATTEMPTS {
        match Arachne::get(b"hello").await {
            Ok(None) => {
                saw_none = true;
                break;
            }
            Err(e) => last_err = format!("{e:?}"),
            Ok(Some(_)) => last_err = "still Some after delete".into(),
        }
        poll().await;
    }
    assert!(
        saw_none,
        "get after delete should return None within {MAX_ATTEMPTS} poll attempts; last: {last_err}"
    );

    // ---- 9. Shutdown. Synchronous: takes the state out of the static, releases
    //         the lock, then blocks on the actor-thread join. ----
    Arachne::shutdown().expect("shutdown");

    // ---- 10. Post-shutdown: every op fails with NotInitialized. ----
    match Arachne::get(b"hello").await {
        Err(ArachneError::NotInitialized) => {}
        Err(e) => panic!("post-shutdown get: expected NotInitialized, got {e}"),
        Ok(_) => panic!("post-shutdown get: expected error, got Ok"),
    }
    match Arachne::get_stale(b"hello").await {
        Err(ArachneError::NotInitialized) => {}
        Err(e) => panic!("post-shutdown get_stale: expected NotInitialized, got {e}"),
        Ok(_) => panic!("post-shutdown get_stale: expected error, got Ok"),
    }
    match Arachne::handle().await {
        Err(ArachneError::NotInitialized) => {}
        Err(e) => panic!("post-shutdown handle: expected NotInitialized, got {e}"),
        Ok(_) => panic!("post-shutdown handle: expected error, got Ok"),
    }

    // ---- 11. Shutdown is idempotent (second call also fails cleanly). ----
    match Arachne::shutdown() {
        Err(ArachneError::NotInitialized) => {}
        Err(e) => panic!("second shutdown: expected NotInitialized, got {e}"),
        Ok(_) => panic!("second shutdown: expected NotInitialized, got Ok"),
    }

    // Best-effort cleanup of the temp dir.
    let _ = fs::remove_dir_all(&dir);
}

/// The unified entry `Arachne::start(ClusterConfig::single_node)` must build the
/// same peerless node as the legacy `Arachne::new` and serve the full data
/// plane (set → get/get_stale round-trip → delete), then shut down cleanly.
///
/// This is the in-process analogue of the cluster test's single-node member:
/// a `ClusterConfig::single_node` config has no peers and no listeners, so it
/// takes the peerless path in `Arachne::start` (synchronous assembly).
#[tokio::test]
async fn facade_single_node_still_works_via_start() {
    // Serialize against [`facade_lifecycle_end_to_end`].
    let _guard = FACADE_LOCK.lock().unwrap();
    let dir = unique_tempdir();

    // Pre-start: no node yet.
    match Arachne::get(b"hello").await {
        Err(ArachneError::NotInitialized) => {}
        Err(e) => panic!("pre-start get: expected NotInitialized, got {e}"),
        Ok(_) => panic!("pre-start get: expected error, got Ok"),
    }

    // Start the peerless node via the unified entry. `start` is synchronous.
    Arachne::start(ClusterConfig::single_node(1, &dir, WalConfig::default()))
        .expect("start(ClusterConfig::single_node)");

    // A second start must fail with AlreadyInitialized.
    match Arachne::start(ClusterConfig::single_node(2, &dir, WalConfig::default())) {
        Err(ArachneError::AlreadyInitialized) => {}
        Err(e) => panic!("second start: expected AlreadyInitialized, got {e}"),
        Ok(_) => panic!("second start: expected AlreadyInitialized, got Ok"),
    }

    // Wait for self-election, then round-trip through the static façade.
    assert!(
        wait_for_leader().await,
        "singleton should self-elect within {MAX_ATTEMPTS} poll attempts"
    );

    Arachne::set(b"hello", b"world").await.expect("set via start-based node");

    // Linear-read back (poll until the value is observed).
    let mut got: Option<Vec<u8>> = None;
    let mut last_err = String::new();
    for _ in 0..MAX_ATTEMPTS {
        match Arachne::get(b"hello").await {
            Ok(Some(v)) => {
                got = Some(v);
                break;
            }
            _ => last_err = "not Some(b\"world\") yet".into(),
        }
        poll().await;
    }
    assert!(
        got.is_some(),
        "get after set should succeed within {MAX_ATTEMPTS} polls; last: {last_err}"
    );
    assert_eq!(got.unwrap(), b"world".to_vec());

    // Stale-read also sees it.
    let mut got_stale: Option<Vec<u8>> = None;
    for _ in 0..MAX_ATTEMPTS {
        if let Ok(Some(v)) = Arachne::get_stale(b"hello").await {
            got_stale = Some(v);
            break;
        }
        poll().await;
    }
    assert_eq!(
        got_stale.as_deref(),
        Some(b"world".as_slice()),
        "get_stale after set should return the written value"
    );

    // Delete → linear-read returns None.
    Arachne::delete(b"hello").await.expect("delete via start-based node");
    let mut saw_none = false;
    let mut last_err = String::new();
    for _ in 0..MAX_ATTEMPTS {
        match Arachne::get(b"hello").await {
            Ok(None) => {
                saw_none = true;
                break;
            }
            Err(e) => last_err = format!("{e:?}"),
            Ok(Some(_)) => last_err = "still Some after delete".into(),
        }
        poll().await;
    }
    assert!(
        saw_none,
        "get after delete should return None within {MAX_ATTEMPTS} polls; last: {last_err}"
    );

    // Handle access works on a start-based node.
    match Arachne::handle().await {
        Ok(_h) => {}
        Err(e) => panic!("handle after start: expected Ok, got {e}"),
    }

    // Shutdown → post-shutdown ops fail with NotInitialized.
    Arachne::shutdown().expect("shutdown start-based node");
    match Arachne::get(b"hello").await {
        Err(ArachneError::NotInitialized) => {}
        Err(e) => panic!("post-shutdown get: expected NotInitialized, got {e}"),
        Ok(_) => panic!("post-shutdown get: expected error, got Ok"),
    }

    // Cleanup.
    let _ = fs::remove_dir_all(&dir);
}
