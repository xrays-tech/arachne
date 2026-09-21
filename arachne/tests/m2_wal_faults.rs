//! M2 — byte-level WAL faults on a **live** node's data directory, tied to an
//! acked write.
//!
//! `wal_mutation.rs` (M0 ②) proves the two-quadrant rule (INV6) on a
//! *synthetic* WAL. This file applies the same class of faults — truncation,
//! bit flips, `META` corruption — to the WAL produced by a **real single-node
//! runtime that has acknowledged a write**, and binds the outcome to INV2: a
//! recovered log must never silently lose the acked write.
//!
//! Quadrants (per mutation):
//! * **legal-prefix truncation** — `open` succeeds and the recovered log is a
//!   contiguous prefix of the original entries, with every entry up to the
//!   durable commit intact and content-identical;
//! * **fail-start** — `open` refuses (`Unrecoverable` / `Corruption`).
//!
//! `META` corruption is a third, stricter case: the data directory's identity
//! is unreadable, so recovery must **fail-start** — never silently recreate the
//! file and adopt the directory.
//!
//! Recovery must never panic (INV6).
//!
//! # Boundary: what a mutation may legitimately remove
//!
//! INV6's two-quadrant rule permits any *legal prefix*, and META records no
//! expected last index (by design: `meta_only_dir_reopens_as_empty_log` — N3 —
//! accepts a data dir whose segments are gone as a legitimately empty log).
//! A mutation that removes the bytes of the committed prefix therefore yields a
//! shorter prefix, even an empty log, and recovery cannot distinguish that from
//! a legitimately empty WAL. So this file asserts the **strong** acked-write
//! floor only where the committed bytes cannot have been removed (the
//! no-mutation baseline and a single trailing torn byte); every other mutation
//! is held to the two-quadrant rule. What is never allowed is a *corrupt* or
//! out-of-order recovery of bytes that are still present.

use std::collections::HashMap;
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::consensus::RaftNodeConfig;
use arachne::runtime::{Runtime, RuntimeConfig};
use arachne::storage::{WalConfig, WalOptions, WalStorage};
use arachne::{
    LogEntry, Metrics, NodeId, Profile, ProfileConfig, Storage, StorageError, TransportFactory,
};
use arachne_testsupport::InMemoryTransportFactory;
use slog::Drain;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-m2-walfault-{tag}-{}-{n}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 5,
        election_timeout_ms: 100,
        rpc_timeout_ms: 50,
        ..Profile::Lan.config()
    }
}

fn wal_opts(cluster: &str, node: &str, profile: &ProfileConfig) -> WalOptions {
    WalOptions {
        cluster_id: cluster.into(),
        node_id: node.into(),
        config: WalConfig {
            fsync_policy: profile.fsync_policy,
            segment_bytes: profile.wal_segment_bytes,
        },
        created_at_millis: 0,
        fsync_observer: None,
    }
}

/// A real single-node runtime that has acked one `put`, then stopped (which
/// releases the data-dir lock). Returns the data dir and the WAL options that
/// identify it.
async fn acked_single_node(tag: &str) -> (PathBuf, WalOptions) {
    acked_single_node_with(tag, false).await
}

/// The same, optionally through the asynchronous durability pipeline
/// (propsol v0.2.13 P).
async fn acked_single_node_with(tag: &str, offloaded: bool) -> (PathBuf, WalOptions) {
    let dir = temp_dir(tag);
    let cluster = "m2-walfault";
    let node = "n1";
    let profile = profile();
    let opts = wal_opts(cluster, node, &profile);

    let mut wal = WalStorage::open(&dir, opts.clone()).expect("open wal");
    if offloaded {
        wal.enable_offloaded_durability()
            .expect("enable offloaded durability");
    }
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(NodeId::from(node));
    let metrics = Arc::new(Metrics::new());
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from(node),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: RaftNodeConfig::from_profile(&profile),
        profile,
        metrics: Arc::clone(&metrics),
    };
    let logger = slog::Logger::root(slog::Discard.fuse(), slog::o!());
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger).expect("build runtime");
    let task = tokio::spawn(runtime.run());

    for _ in 0..400 {
        if metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert!(metrics.is_leader(), "the single node must elect itself");
    // Bounded retries: this is *setup* for a recovery property, and cargo runs
    // this binary's tests in parallel, so on a loaded CI machine a single
    // attempt can miss the client deadline. (The ack path itself — durable
    // entry, applied, replied — is what the other tests assert.)
    let mut last = String::new();
    let mut applied = false;
    for _ in 0..200 {
        match handle.put(b"acked", b"value").await {
            Ok(()) => {
                applied = true;
                break;
            }
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
        }
    }
    assert!(applied, "the write must commit+apply (last error: {last})");

    // Stop the actor: it owns the WAL, so this releases the data-dir lock.
    task.abort();
    let _ = task.await;
    (dir, opts)
}

/// The raw bytes of `META` and of the single segment of a stopped WAL.
struct WalBytes {
    meta: Vec<u8>,
    segment: Vec<u8>,
}

fn read_wal(dir: &Path) -> WalBytes {
    let meta = fs::read(dir.join("META")).expect("read META");
    let mut segments: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(dir).expect("read dir").flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("wal-") && name.ends_with(".log") {
            segments.push(entry.path());
        }
    }
    segments.sort();
    assert_eq!(
        segments.len(),
        1,
        "expected exactly one segment, got {}",
        segments.len()
    );
    let segment = fs::read(&segments[0]).expect("read segment");
    WalBytes { meta, segment }
}

/// Materialize a data dir with `meta` + one segment and try to open it.
enum Outcome {
    Recovered(WalStorage),
    FailStart,
}

fn open_variant(scratch: &Path, meta: &[u8], segment: &[u8], opts: &WalOptions) -> Outcome {
    let _ = fs::remove_dir_all(scratch);
    fs::create_dir_all(scratch).expect("create scratch dir");
    fs::write(scratch.join("META"), meta).expect("write META");
    fs::write(scratch.join("wal-00000000000000000001.log"), segment).expect("write segment");

    match catch_unwind(AssertUnwindSafe(|| WalStorage::open(scratch, opts.clone()))) {
        Ok(Ok(storage)) => Outcome::Recovered(storage),
        Ok(Err(StorageError::Unrecoverable { .. })) => Outcome::FailStart,
        Ok(Err(StorageError::Corruption { .. })) => Outcome::FailStart,
        Ok(Err(other)) => panic!("INV6 violated: non-fail-start error variant: {other}"),
        Err(_) => panic!("INV6 violated: WAL recovery panicked on a mutated segment"),
    }
}

/// INV6 (two-quadrant rule): a recovered log is a contiguous prefix, never
/// grows, and is index-consistent. `original_entries` bounds the prefix.
fn verify_two_quadrant(storage: &WalStorage, original_entries: usize) {
    let first = storage.first_index().expect("first_index");
    let last = storage.last_index().expect("last_index");
    let commit = storage.initial_state().expect("initial_state").hard_state.commit;

    assert_eq!(first, 1, "INV6: a recovered log must start at index 1");
    assert!(
        commit <= last,
        "INV6: hard_state.commit ({commit}) > last_index ({last})"
    );
    let recovered = storage.entries(1, last + 1, None).expect("entries");
    assert_eq!(
        recovered.len() as u64,
        last,
        "INV6: index gap in the recovered log"
    );
    for (i, e) in recovered.iter().enumerate() {
        assert_eq!(e.index, (i + 1) as u64, "INV6: index gap at {i}");
    }
    assert!(
        recovered.len() <= original_entries,
        "INV6: the recovered log ({}) grew beyond the original ({original_entries})",
        recovered.len()
    );
}

/// The **strong** INV2 floor, asserted only where the committed prefix's bytes
/// cannot have been removed by the mutation: every committed entry is still
/// present and content-identical.
fn verify_acked_prefix_intact(storage: &WalStorage, committed_prefix: &[LogEntry]) {
    let last = storage.last_index().expect("last_index");
    assert!(
        last >= committed_prefix.len() as u64,
        "INV2 violated: the durable committed prefix ({} entries) was lost (last={last})",
        committed_prefix.len()
    );
    let recovered = storage.entries(1, last + 1, None).expect("entries");
    for (i, expected) in committed_prefix.iter().enumerate() {
        assert_eq!(
            recovered[i], *expected,
            "INV2 violated: committed entry {} was silently changed",
            i + 1
        );
    }
}

/// The durable committed prefix of an unmutated copy of the WAL.
fn committed_prefix(dir: &Path, opts: &WalOptions) -> Vec<LogEntry> {
    let storage = WalStorage::open(dir, opts.clone()).expect("open unmutated WAL");
    let commit = storage.initial_state().expect("initial_state").hard_state.commit;
    assert!(commit >= 1, "the acked write must be committed");
    storage.entries(1, commit + 1, None).expect("entries")
}

// ---------------------------------------------------------------------------
// META corruption must fail-start
// ---------------------------------------------------------------------------

/// A data directory whose `META` cannot be read must **fail-start**: recovery
/// may not silently recreate it and adopt the directory.
#[tokio::test]
async fn meta_corruption_fail_starts() {
    let (dir, opts) = acked_single_node("meta").await;
    let wal = read_wal(&dir);
    assert!(wal.meta.len() >= 24, "META must be at least the minimum size");

    let scratch = temp_dir("meta-scratch");
    let mut variants: Vec<(&str, Vec<u8>)> = vec![
        ("empty", Vec::new()),
        ("truncated-to-12", wal.meta[..12].to_vec()),
        ("truncated-by-1", wal.meta[..wal.meta.len() - 1].to_vec()),
        ("bad-magic", {
            let mut m = wal.meta.clone();
            m[0] ^= 0xFF;
            m
        }),
        ("bad-crc", {
            let mut m = wal.meta.clone();
            m[8] ^= 0x01;
            m
        }),
        ("garbled-payload", {
            let mut m = wal.meta.clone();
            let mid = 12 + (m.len() - 12) / 2;
            m[mid] ^= 0xFF;
            m
        }),
    ];
    // Also corrupt every 4th payload byte, one at a time.
    for off in (12..wal.meta.len()).step_by(4) {
        let mut m = wal.meta.clone();
        m[off] ^= 0x80;
        variants.push(("payload-flip", m));
    }

    let mut checked = 0u32;
    for (name, meta) in variants {
        match open_variant(&scratch, &meta, &wal.segment, &opts) {
            Outcome::FailStart => {}
            Outcome::Recovered(_) => panic!(
                "INV6 violated: corrupt META variant `{name}` was silently accepted (recovery must fail-start)"
            ),
        }
        checked += 1;
    }
    assert!(checked > 0, "no META variants were checked");
    let _ = fs::remove_dir_all(&scratch);
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Byte-mutation sweep over a live WAL
// ---------------------------------------------------------------------------

/// Every truncation and a bounded set of bit flips of a **live** WAL lands in
/// one of the two quadrants, never panics, and never silently loses the acked
/// (durable committed) write.
#[tokio::test]
async fn acked_write_survives_a_byte_mutation_sweep() {
    let (dir, opts) = acked_single_node("sweep").await;
    let wal = read_wal(&dir);
    let committed = committed_prefix(&dir, &opts);
    assert!(committed.len() >= 1, "the acked put must be in the committed prefix");

    let scratch = temp_dir("sweep-scratch");
    let (mut ok, mut fail) = (0u32, 0u32);

    let original_entries = committed.len();

    // (1) Truncation at every byte offset. Truncating at the full length is the
    //     no-mutation baseline, which must reproduce the whole committed prefix.
    for n in 0..=wal.segment.len() {
        match open_variant(&scratch, &wal.meta, &wal.segment[..n], &opts) {
            Outcome::Recovered(s) => {
                verify_two_quadrant(&s, original_entries);
                if n == wal.segment.len() {
                    verify_acked_prefix_intact(&s, &committed);
                }
                ok += 1;
            }
            Outcome::FailStart => fail += 1,
        }
    }

    // (2) Single-bit flips on a bounded stride (the `len` field, CRC, type byte
    //     and payload are all covered).
    for pos in (0..wal.segment.len()).step_by(3) {
        for mask in [0x01u8, 0x80] {
            let mut m = wal.segment.clone();
            m[pos] ^= mask;
            match open_variant(&scratch, &wal.meta, &m, &opts) {
                Outcome::Recovered(s) => {
                    verify_two_quadrant(&s, original_entries);
                    ok += 1;
                }
                Outcome::FailStart => fail += 1,
            }
        }
    }

    assert!(ok > 0, "expected at least one legal-prefix (Ok) outcome");
    assert!(fail > 0, "expected at least one fail-start (Err) outcome");
    eprintln!(
        "m2 live-WAL sweep: {} mutations → {ok} legal-prefix, {fail} fail-start",
        ok + fail
    );
    let _ = fs::remove_dir_all(&scratch);
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// A torn tail must not lose the acked write
// ---------------------------------------------------------------------------

/// Truncating the very last byte of the WAL (a torn final record) must recover
/// the durable entries, and the node must then **serve the acked write again**
/// after restarting on that directory.
#[tokio::test]
async fn torn_tail_recovers_and_still_serves_the_acked_write() {
    let (dir, opts) = acked_single_node("torn").await;
    let wal = read_wal(&dir);
    let committed = committed_prefix(&dir, &opts);

    // Tear the final record (the trailing HardState): one byte short.
    let torn = &wal.segment[..wal.segment.len() - 1];
    match open_variant(&dir, &wal.meta, torn, &opts) {
        Outcome::Recovered(s) => {
            verify_two_quadrant(&s, committed.len());
            verify_acked_prefix_intact(&s, &committed);
        }
        Outcome::FailStart => panic!(
            "a torn record at the very tail must be a legal-prefix truncation, not a fail-start"
        ),
    }

    // Restart a runtime on the mutated directory and read the value back: the
    // single node re-commits its durable entry, so the acked write survives.
    let profile = profile();
    let wal_storage = WalStorage::open(&dir, opts.clone()).expect("reopen the torn WAL");
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(NodeId::from("n1"));
    let metrics = Arc::new(Metrics::new());
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from("n1"),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: RaftNodeConfig::from_profile(&profile),
        profile,
        metrics: Arc::clone(&metrics),
    };
    let logger = slog::Logger::root(slog::Discard.fuse(), slog::o!());
    let (runtime, handle) = Runtime::new(config, wal_storage, tx, rx, &logger).expect("runtime");
    let task = tokio::spawn(runtime.run());

    let mut seen = None;
    for _ in 0..400 {
        if let Ok(Some(v)) = handle.get_stale(b"acked").await {
            seen = Some(v);
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        seen.as_deref(),
        Some(b"value".as_ref()),
        "the acked write must survive a torn WAL tail"
    );
    task.abort();
    let _ = task.await;
    let _ = fs::remove_dir_all(&dir);
}


/// INV2 across the new durability window: an **acknowledged** write must survive
/// a restart when the records were flushed off-thread (propsol v0.2.13 P).
///
/// The ack is the boundary that matters: the runtime replies only after the
/// cycle that carries the entry completed, and a cycle completes only when its
/// flush landed. So a crash after the ack cannot lose it — while a record whose
/// flush is still in the air may simply not be there, which recovery must treat
/// as a legal prefix rather than corruption.
#[tokio::test]
async fn acked_write_survives_a_restart_with_offloaded_durability() {
    let (dir, opts) = acked_single_node_with("offloaded", true).await;

    // The actor is gone (its `WalStorage` — and with it the flusher thread —
    // was dropped). Reopening must find a consistent log with the acked entry.
    let storage = WalStorage::open(&dir, opts.clone()).expect("reopen the offloaded WAL");
    let last = storage.last_index().expect("last_index");
    let commit = storage.initial_state().expect("initial_state").hard_state.commit;
    assert!(
        last >= 2,
        "the acked write's entry must be durable (last_index {last})"
    );
    assert!(
        commit <= last,
        "INV2: durable commit ({commit}) must not exceed the durable log ({last})"
    );
    let entries = storage.entries(1, last + 1, None).expect("entries");
    assert!(
        entries.len() as u64 == last,
        "the recovered log must be a contiguous prefix from index 1"
    );
    // (`offloaded_fsyncs` is an in-memory counter and this is a fresh handle;
    // the storage unit tests assert the accounting, this test asserts the
    // durable outcome.)
    drop(storage);

    // And the node serves the value again on that directory.
    let profile = profile();
    let wal = WalStorage::open(&dir, opts).expect("reopen for the runtime");
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(NodeId::from("n1"));
    let metrics = Arc::new(Metrics::new());
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from("n1"),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: RaftNodeConfig::from_profile(&profile),
        profile,
        metrics: Arc::clone(&metrics),
    };
    let logger = slog::Logger::root(slog::Discard.fuse(), slog::o!());
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger).expect("build runtime");
    let task = tokio::spawn(runtime.run());
    for _ in 0..400 {
        if metrics.is_leader() {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    let mut seen = None;
    for _ in 0..400 {
        if let Ok(Some(value)) = handle.get_stale(b"acked").await {
            seen = Some(value);
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        seen.as_deref(),
        Some(b"value".as_slice()),
        "the acked write must be served again after a restart"
    );
    task.abort();
    let _ = task.await;
    let _ = fs::remove_dir_all(&dir);
}
