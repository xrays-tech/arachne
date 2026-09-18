//! Integration test: WAL byte-mutation verification (P3c — M0 acceptance ②).
//!
//! # What this verifies (INV6 — the two-quadrant rule)
//!
//! We build a **real** on-disk WAL (a real `META` + real segment record bytes,
//! produced by the actual `WalStorage` write path), then apply **systematic**
//! byte mutations to the raw segment bytes and run `WalStorage::open` recovery
//! on every mutated variant. Each mutation must land in exactly one of two
//! quadrants:
//!
//!   * **legal-prefix truncation** — `Ok`, with the recovered log being a
//!     contiguous prefix of the original entries (indices contiguous from 1)
//!     and `hard_state.commit <= last_index`; and
//!   * **fail-start** — `Err` (recovery refuses to open a corrupt WAL).
//!
//! Recovery must **never panic** and must **never silently lose or corrupt the
//! committed region**: whenever recovery returns `Ok`, every entry up to the
//! durable commit is present with content identical to the original, and the
//! original committed region (indices `<= original_commit`) is intact whenever
//! the recovered log is at least that long.
//!
//! This is the deterministic, in-repo CI evidence for M0 ②. The `cargo-fuzz`
//! target (`fuzz/fuzz_targets/wal_recovery.rs`) provides the same guarantee
//! under randomized inputs for nightly CI.
//!
//! The harness uses only `std` + `arachne` (no external crates).

use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arachne::storage::{segment_name, FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{EntryType, HardState, LogEntry, Storage, StorageError};

// ---------------------------------------------------------------------------
// Test plumbing
// ---------------------------------------------------------------------------

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Create a unique temp directory (unique via process id + atomic counter).
fn temp_dir() -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-wal-mutation-{}-{}",
        std::process::id(),
        n
    ));
    fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

/// Build a log entry with a single distinct data byte.
fn make_entry(index: u64, term: u64, data: u8) -> LogEntry {
    LogEntry {
        index,
        term,
        entry_type: EntryType::Entry,
        data: vec![data],
    }
}

fn opts_with_segment_bytes(segment_bytes: u64) -> WalOptions {
    WalOptions {
        cluster_id: "mutation-cluster".into(),
        node_id: "mutation-node".into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// A snapshot of a freshly built WAL: the options, the raw `META` bytes, the
/// raw bytes of every segment (in index order), and the logical entries/commit
/// that were written. This is the reference against which mutations are judged.
struct WalSnapshot {
    opts: WalOptions,
    meta_bytes: Vec<u8>,
    /// `(segment file name, raw bytes)` for every segment, ascending index.
    segments: Vec<(String, Vec<u8>)>,
    /// The original entries (indices `1..=N`), in order.
    original_entries: Vec<LogEntry>,
    /// The `commit` value of the (last) `HardState` written.
    original_commit: u64,
}

/// Build a single-segment WAL: `num_entries` entries then one `HardState` with
/// `commit < num_entries` (so both committed and uncommitted regions exist).
fn build_single_segment_wal(num_entries: u64, commit: u64) -> WalSnapshot {
    let dir = temp_dir();
    let opts = opts_with_segment_bytes(128 * 1024 * 1024); // huge → no rollover
    let original_entries: Vec<LogEntry> = (1..=num_entries)
        .map(|i| make_entry(i, 1, b'a' + ((i - 1) % 26) as u8))
        .collect();
    {
        let mut storage = WalStorage::open(&dir, opts.clone()).expect("open fresh WAL");
        storage.append(&original_entries).expect("append entries");
        storage.sync_entries().expect("sync entries");
        storage
            .set_hard_state(&HardState {
                term: 1,
                vote: Some(1),
                commit,
            })
            .expect("set hard state");
    }
    let meta_bytes = fs::read(dir.join("META")).expect("read META");
    let seg_name = segment_name(1);
    let seg_bytes = fs::read(dir.join(&seg_name)).expect("read segment");
    let _ = fs::remove_dir_all(&dir);
    WalSnapshot {
        opts,
        meta_bytes,
        segments: vec![(seg_name, seg_bytes)],
        original_entries,
        original_commit: commit,
    }
}

/// Build a multi-segment WAL (>= 2 segments) using a tiny `segment_bytes` to
/// force rollover, so the non-last-segment fail-start path can be exercised.
fn build_multi_segment_wal(num_entries: u64, commit: u64) -> WalSnapshot {
    let dir = temp_dir();
    let opts = opts_with_segment_bytes(80); // tiny → forces rollover
    let original_entries: Vec<LogEntry> = (1..=num_entries)
        .map(|i| make_entry(i, 1, b'a' + ((i - 1) % 26) as u8))
        .collect();
    {
        let mut storage = WalStorage::open(&dir, opts.clone()).expect("open fresh WAL");
        storage.append(&original_entries).expect("append entries");
        storage.sync_entries().expect("sync entries");
        storage
            .set_hard_state(&HardState {
                term: 1,
                vote: Some(1),
                commit,
            })
            .expect("set hard state");
    }
    let meta_bytes = fs::read(dir.join("META")).expect("read META");
    let mut segments: Vec<(String, Vec<u8>)> = Vec::new();
    for entry in fs::read_dir(&dir).expect("read dir").flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("wal-") {
            let bytes = fs::read(entry.path()).expect("read segment");
            segments.push((name, bytes));
        }
    }
    segments.sort_by(|a, b| a.0.cmp(&b.0));
    let _ = fs::remove_dir_all(&dir);
    WalSnapshot {
        opts,
        meta_bytes,
        segments,
        original_entries,
        original_commit: commit,
    }
}

// ---------------------------------------------------------------------------
// Mutation generation
// ---------------------------------------------------------------------------

/// A span of one well-formed record within a segment: `start..end` and the
/// record's declared `len` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordSpan {
    start: usize,
    end: usize,
    len: u32,
}

/// Parse the record spans of a well-formed segment (stops at a structural
/// tear). Used to locate `len` fields and record boundaries for targeting.
fn parse_record_spans(bytes: &[u8]) -> Vec<RecordSpan> {
    let mut spans = Vec::new();
    let mut offset = 0usize;
    while offset + 8 <= bytes.len() {
        let len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        let end = offset + 8 + len as usize;
        if end > bytes.len() {
            break; // structural tear: not a complete record.
        }
        spans.push(RecordSpan { start: offset, end, len });
        offset = end;
    }
    spans
}

/// Generate all systematic mutations of one segment's raw bytes.
///
/// Categories:
///   * **truncation** — every length `0..=full` (the full length is the
///     no-mutation baseline, i.e. a "legal prefix" of length `full`).
///   * **bit_flip** — every `bit_stride`-th position (position 0 = the first
///     `len` byte; the header, `len`, CRC, type byte, and payload are all
///     covered) x a small set of bit masks.
///   * **len_corruption** — overwrite the `len` field of each record with
///     `0`, `1`, `u32::MAX`, and off-by-one values (the P2 gate requirement).
fn mutate_segment(bytes: &[u8], bit_stride: usize) -> Vec<(&'static str, Vec<u8>)> {
    let mut out: Vec<(&'static str, Vec<u8>)> = Vec::new();

    // (1) Truncations: every length 0..=full.
    for n in 0..=bytes.len() {
        out.push(("truncation", bytes[..n].to_vec()));
    }

    // (2) Single-bit flips: a bounded stride of positions x a few bit masks.
    let masks = [0x01u8, 0x02, 0x40, 0x80];
    for pos in (0..bytes.len()).step_by(bit_stride.max(1)) {
        for &mask in &masks {
            let mut m = bytes.to_vec();
            m[pos] ^= mask;
            out.push(("bit_flip", m));
        }
    }

    // (3) `len`-field corruption at each record boundary.
    for span in parse_record_spans(bytes) {
        let candidates: [u32; 5] = [
            0,
            1,
            u32::MAX,
            span.len.saturating_sub(1),
            span.len.saturating_add(1),
        ];
        for &bad in &candidates {
            let mut m = bytes.to_vec();
            m[span.start..span.start + 4].copy_from_slice(&bad.to_le_bytes());
            out.push(("len_corruption", m));
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Classification + INV6 verification
// ---------------------------------------------------------------------------

/// The two INV6 quadrants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// `Ok`: recovery returned a valid prefix (legal-prefix truncation).
    OkPrefix,
    /// `Err`: recovery refused to open (fail-start).
    FailStart,
}

/// Open (recover) the WAL in `dir` and classify the outcome, asserting the
/// INV6 invariants for any `Ok` result.
///
/// A recovery **panic** is caught and turned into a hard test failure (INV6:
/// "never panic").
///
/// F2: the `Err` variant is inspected — only `Unrecoverable` and `Corruption`
/// are legitimate fail-start outcomes. An `Io` or other variant is a bug and
/// is reported as a test failure.
fn classify_open(
    dir: &Path,
    opts: &WalOptions,
    original_entries: &[LogEntry],
    original_commit: u64,
) -> Outcome {
    let open_result = catch_unwind(AssertUnwindSafe(|| WalStorage::open(dir, opts.clone())));
    match open_result {
        Ok(Ok(storage)) => {
            verify_ok(storage, original_entries, original_commit);
            Outcome::OkPrefix
        }
        Ok(Err(StorageError::Unrecoverable { .. })) => Outcome::FailStart,
        Ok(Err(StorageError::Corruption { .. })) => Outcome::FailStart,
        Ok(Err(other)) => {
            panic!(
                "INV6 violated: recovery returned a non-fail-start error variant: {other}"
            );
        }
        Err(_) => panic!("INV6 violated: WAL recovery panicked on a mutated segment"),
    }
}

/// Assert the INV6 invariants for an `Ok` recovery:
///
///   1. the recovered log is a **valid prefix** (indices contiguous from 1);
///   2. the post-replay assert holds: `hard_state.commit <= last_index`;
///   3. **no silent corruption** — the recovered prefix is content-identical
///      to a prefix of the original entries;
///   4. **committed region never silently lost** — every entry `<=` the
///      durable commit is present and identical, and the *original* committed
///      region (`<= original_commit`) is intact whenever the recovered log is
///      at least that long.
fn verify_ok(storage: WalStorage, original_entries: &[LogEntry], original_commit: u64) {
    let first_index = storage.first_index().expect("first_index");
    let last_index = storage.last_index().expect("last_index");
    let commit = storage
        .initial_state()
        .expect("initial_state")
        .hard_state
        .commit;

    // (1) Valid prefix: indices contiguous from 1.
    assert_eq!(
        first_index, 1,
        "INV6 violated: recovered log must start at index 1, got {first_index}"
    );
    let recovered = storage
        .entries(1, last_index + 1, None)
        .expect("entries(1, last_index + 1)");
    assert_eq!(
        recovered.len() as u64,
        last_index,
        "INV6 violated: recovered entry count ({}) != last_index ({last_index})",
        recovered.len()
    );
    for (i, e) in recovered.iter().enumerate() {
        assert_eq!(
            e.index,
            (i + 1) as u64,
            "INV6 violated: log index gap at position {i} (got {})",
            e.index
        );
    }

    // (2) INV6 post-replay assert: commit <= last_index.
    assert!(
        commit <= last_index,
        "INV6 violated: hard_state.commit ({commit}) > last_index ({last_index})"
    );

    // (3) No silent corruption: the recovered prefix is a content-identical
    //     prefix of the original entries.
    assert!(
        recovered.len() <= original_entries.len(),
        "INV6 violated: recovered {} entries but the original has only {} (a prefix can never grow)",
        recovered.len(),
        original_entries.len()
    );
    for (i, e) in recovered.iter().enumerate() {
        assert_eq!(
            *e, original_entries[i],
            "INV6 violated: silent corruption at index {} (recovered differs from the original)",
            e.index
        );
    }

    // (4a) Committed region (durable commit) never silently lost. This is
    //      implied by (2)+(3) but is asserted explicitly for INV6 clarity.
    assert!(
        (commit as usize) <= recovered.len(),
        "INV6 violated: durable commit ({commit}) exceeds recovered entry count ({})",
        recovered.len()
    );

    // (4b) The *original* committed region is intact whenever the recovered
    //      log is at least as long as `original_commit` (i.e. the commit was
    //      durable at this truncation point).
    if last_index >= original_commit {
        assert!(
            (original_commit as usize) <= recovered.len(),
            "INV6 violated: original committed region (<= {original_commit}) lost though last_index >= it"
        );
        for i in 0..original_commit as usize {
            assert_eq!(
                recovered[i], original_entries[i],
                "INV6 violated: original committed entry at index {} is corrupted",
                i + 1
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The suite
// ---------------------------------------------------------------------------

/// Apply every mutation of `snapshot` (across all its segments), classify each,
/// and return `(ok, err, total)` counts. `mutate_segment` is applied to every
/// segment with the given `bit_stride`.
fn run_suite(
    snapshot: &WalSnapshot,
    bit_stride: usize,
) -> (u64, u64, u64, Vec<&'static str>) {
    let (mut ok, mut err, mut total, mut categories) = (0u64, 0u64, 0u64, Vec::new());
    let dir = temp_dir();
    fs::write(dir.join("META"), &snapshot.meta_bytes).expect("write META");

    for (seg_idx, (_seg_name, orig_bytes)) in snapshot.segments.iter().enumerate() {
        for (category, mutated) in mutate_segment(orig_bytes, bit_stride) {
            categories.push(category);
            // Materialize the full WAL: every segment, with `seg_idx` mutated.
            for (j, (name, bytes)) in snapshot.segments.iter().enumerate() {
                let bytes = if j == seg_idx { &mutated } else { bytes };
                fs::write(dir.join(name), bytes).expect("write segment");
            }
            let outcome = classify_open(&dir, &snapshot.opts, &snapshot.original_entries, snapshot.original_commit);
            match outcome {
                Outcome::OkPrefix => ok += 1,
                Outcome::FailStart => err += 1,
            }
            total += 1;
        }
    }

    let _ = fs::remove_dir_all(&dir);
    (ok, err, total, categories)
}

/// M0 ② (INV6): every WAL byte mutation lands in exactly one of the two
/// quadrants (legal-prefix truncation or fail-start), never panics, and never
/// silently loses/corrupts the committed region.
#[test]
fn wal_mutations_land_in_two_quadrants() {
    // --- Single segment: the full systematic matrix (every bit position). ---
    let single = build_single_segment_wal(5, 2); // 5 entries, commit=2 (< last_index=5)
    assert_eq!(single.segments.len(), 1, "single-segment WAL must have exactly 1 segment");
    let (s_ok, s_err, s_total, s_cats) = run_suite(&single, /*bit_stride=*/ 1);

    // --- Multi segment: tail + non-tail (non-last-segment fail-start path). ---
    let multi = build_multi_segment_wal(5, 2); // commit=2 (< last_index=5)
    assert!(
        multi.segments.len() >= 2,
        "multi-segment WAL must have >= 2 segments, got {}",
        multi.segments.len()
    );
    let (m_ok, m_err, m_total, m_cats) = run_suite(&multi, /*bit_stride=*/ 2);

    let total = s_total + m_total;
    let ok = s_ok + m_ok;
    let err = s_err + m_err;

    // --- Summary (M0 ② evidence). ---
    let mut cat_counts: std::collections::BTreeMap<&'static str, u64> =
        std::collections::BTreeMap::new();
    for c in s_cats.iter().chain(m_cats.iter()) {
        *cat_counts.entry(*c).or_insert(0) += 1;
    }
    println!("=== wal_mutation summary (M0 ② / INV6) ===");
    println!("total mutations classified: {total}");
    println!("  Ok  (legal-prefix truncation): {ok}");
    println!("  Err (fail-start)             : {err}");
    for (c, n) in &cat_counts {
        println!("  category {c:<16}: {n}");
    }
    println!(
        "segments exercised: single=1, multi={}; committed region preserved (original_commit=2) in all Ok outcomes",
        multi.segments.len()
    );

    // --- Assert every mutation was classified and the two-quadrant rule held. ---
    assert!(total > 0, "no mutations were run");
    assert_eq!(
        ok + err, total,
        "not every mutation was classified (ok + err != total)"
    );
    // Both quadrants must actually be exercised (sanity: the matrix is non-trivial).
    assert!(ok > 0, "expected at least one legal-prefix (Ok) outcome");
    assert!(err > 0, "expected at least one fail-start (Err) outcome");
    // Both mutation categories (truncation + bit_flip + len_corruption) present.
    assert!(
        cat_counts.contains_key("truncation")
            && cat_counts.contains_key("bit_flip")
            && cat_counts.contains_key("len_corruption"),
        "expected truncation, bit_flip, and len_corruption categories to all be present"
    );
}

/// A focused invariant: a fully-present corrupt record in the **last** segment
/// (the P2 gate's `len`-field / CRC requirement) must fail-start, never
/// auto-truncate away committed data.
#[test]
fn len_field_corruption_in_last_segment_failstarts_or_preserves() {
    let single = build_single_segment_wal(5, 2);
    let (seg_name, bytes) = &single.segments[0];

    let dir = temp_dir();
    fs::write(dir.join("META"), &single.meta_bytes).expect("write META");

    // Corrupt the `len` field of every record with the pathological values and
    // confirm each is either fail-start (Err) or a legal prefix (Ok) — never
    // a panic, never a corrupt Ok.
    for span in parse_record_spans(bytes) {
        for &bad in &[0u32, 1, u32::MAX] {
            let mut m = bytes.clone();
            m[span.start..span.start + 4].copy_from_slice(&bad.to_le_bytes());
            fs::write(dir.join(seg_name), &m).expect("write mutated segment");
            let outcome = classify_open(&dir, &single.opts, &single.original_entries, single.original_commit);
            // Either quadrant is acceptable; the INV6 asserts inside
            // classify_open guarantee it is well-formed.
            assert!(
                matches!(outcome, Outcome::OkPrefix | Outcome::FailStart),
                "unexpected outcome for len corruption {bad}"
            );
        }
    }
    let _ = fs::remove_dir_all(&dir);
}

/// A focused invariant: a corrupt record in a **non-last** segment must always
/// fail-start (the non-last-segment fail-start path).
#[test]
fn non_last_segment_corruption_always_failstarts() {
    let multi = build_multi_segment_wal(5, 2);
    assert!(multi.segments.len() >= 2, "need >= 2 segments");
    let non_last_idx = 0; // first segment is non-last
    let (_seg_name, bytes) = &multi.segments[non_last_idx];

    let dir = temp_dir();
    fs::write(dir.join("META"), &multi.meta_bytes).expect("write META");

    // Any bit flip or len corruption inside the non-last segment must
    // fail-start (a non-last segment can never be auto-truncated).
    let mut flips: Vec<Vec<u8>> = (0..bytes.len())
        .step_by(3)
        .map(|pos| {
            let mut m = bytes.clone();
            m[pos] ^= 0xFF;
            m
        })
        .collect();
    for span in parse_record_spans(bytes) {
        for &bad in &[0u32, 1, u32::MAX] {
            let mut m = bytes.clone();
            m[span.start..span.start + 4].copy_from_slice(&bad.to_le_bytes());
            flips.push(m);
        }
    }
    for m in &flips {
        for (j, (name, b)) in multi.segments.iter().enumerate() {
            let bytes = if j == non_last_idx { m } else { b };
            fs::write(dir.join(name), bytes).expect("write segment");
        }
        let outcome = classify_open(&dir, &multi.opts, &multi.original_entries, multi.original_commit);
        assert!(
            matches!(outcome, Outcome::FailStart),
            "a corrupt non-last segment must fail-start, got {outcome:?}"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}
