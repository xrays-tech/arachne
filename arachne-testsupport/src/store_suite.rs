//! The T6 storage conformance suite: a reusable battery any `Storage` impl
//! must pass (test-plan §4 T6).
//!
//! This suite is **generic** over any `S: arachne_seam::Storage` and tests
//! only the logical contract of the seam — not WAL-specific behavior. Any
//! future engine (e.g., redb if D1 is revisited) must pass this suite.
//!
//! # Contract checks
//!
//! * **fresh_defaults** — a fresh store has `hard_state == default`,
//!   `first_index() == 1`, `last_index() == 0`, `snapshot() == Ok(None)`.
//! * **append_then_indices_consistent** — after appending entries with
//!   sequential indices, `first_index`, `last_index`, and `term` are correct.
//! * **entries_semantics** — `entries(low, high, max_size)`:
//!   * half-open: `[low, high)` returns `high - low` entries.
//!   * degenerate: `low == high` returns `Ok(vec![])`.
//!   * out-of-window: `low < first_index` or `high > last_index + 1` →
//!     `Compacted`.
//!   * `max_size = Some(0)` → empty prefix.
//! * **append_sequential_contract** — `append` requires strict sequential
//!   indices (`last_index + 1`). The suite verifies that sequential appends
//!   produce a gap-free log. (Out-of-order appends are a contract violation;
//!   `WalStorage` catches them via `debug_assert!` in debug builds.)
//! * **set_hard_state_roundtrip** — after `set_hard_state`, `initial_state`
//!   returns the same value.
//! * **sync_entries_idempotent** — calling `sync_entries` multiple times is
//!   safe and does not change observable state.
//! * **index_continuity** — after N sequential appends, entries 1..=N are
//!   all present with no gaps.
//!
//! # Non-vacuity
//!
//! The suite includes a negative test: a deliberately-broken `Storage` impl
//! that the suite **must** reject (see `tests::broken_impl_is_rejected`).

use arachne_seam::storage::{
    EntryType, HardState, LogEntry, Storage, StorageError,
};
use arachne_seam::types::{LogIndex, Term};

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

/// The result of a single named check.
#[derive(Clone, Debug)]
pub struct CheckResult {
    /// The name of the check.
    pub name: &'static str,
    /// Whether the check passed.
    pub passed: bool,
    /// A human-readable detail (only present on failure).
    pub detail: Option<String>,
}

impl CheckResult {
    fn pass(name: &'static str) -> Self {
        Self {
            name,
            passed: true,
            detail: None,
        }
    }

    fn fail(name: &'static str, detail: String) -> Self {
        Self {
            name,
            passed: false,
            detail: Some(detail),
        }
    }
}

/// The full report from a suite run.
#[derive(Clone, Debug)]
pub struct SuiteReport {
    /// The label given to the suite run.
    pub label: String,
    /// Individual check results.
    pub checks: Vec<CheckResult>,
}

impl SuiteReport {
    /// Return `true` iff all checks passed.
    pub fn all_passed(&self) -> bool {
        self.checks.iter().all(|c| c.passed)
    }

    /// Return the names of all failed checks.
    pub fn failures(&self) -> Vec<&'static str> {
        self.checks
            .iter()
            .filter(|c| !c.passed)
            .map(|c| c.name)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

/// Run the T6 storage conformance suite against a fresh `Storage` instance.
///
/// `fresh` is called once per check to obtain an independent storage instance.
/// The suite never panics: all failures are recorded in the returned
/// [`SuiteReport`].
pub fn run_storage_suite<S, F>(label: &str, mut fresh: F) -> SuiteReport
where
    S: Storage,
    F: FnMut() -> S,
{
    let mut checks: Vec<CheckResult> = Vec::new();

    // 1. fresh_defaults
    checks.push(check_fresh_defaults(label, &mut fresh));

    // 2. append_then_indices_consistent
    checks.push(check_append_indices(label, &mut fresh));

    // 3. entries_semantics
    checks.push(check_entries_semantics(label, &mut fresh));

    // 4. append_sequential_contract
    checks.push(check_append_sequential(label, &mut fresh));

    // 5. set_hard_state_roundtrip
    checks.push(check_hard_state_roundtrip(label, &mut fresh));

    // 6. sync_entries_idempotent
    checks.push(check_sync_idempotent(label, &mut fresh));

    // 7. index_continuity
    checks.push(check_index_continuity(label, &mut fresh));

    SuiteReport {
        label: label.to_string(),
        checks,
    }
}

// ---------------------------------------------------------------------------
// Individual checks
// ---------------------------------------------------------------------------

fn check_fresh_defaults<S, F>(label: &str, fresh: &mut F) -> CheckResult
where
    S: Storage,
    F: FnMut() -> S,
{
    let name = "fresh_defaults";
    let store = fresh();

    let state = match store.initial_state() {
        Ok(s) => s,
        Err(e) => return CheckResult::fail(name, format!("{label}: initial_state: {e}")),
    };
    if state.hard_state != HardState::default() {
        return CheckResult::fail(
            name,
            format!("{label}: expected default hard_state, got {:?}", state.hard_state),
        );
    }

    match store.first_index() {
        Ok(1) => {}
        Ok(idx) => {
            return CheckResult::fail(name, format!("{label}: first_index()={idx}, expected 1"))
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: first_index: {e}")),
    }

    match store.last_index() {
        Ok(0) => {}
        Ok(idx) => {
            return CheckResult::fail(name, format!("{label}: last_index()={idx}, expected 0"))
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: last_index: {e}")),
    }

    match store.snapshot() {
        Ok(None) => {}
        Ok(Some(_)) => {
            return CheckResult::fail(name, format!("{label}: snapshot() should be None on fresh store"))
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: snapshot: {e}")),
    }

    CheckResult::pass(name)
}

fn check_append_indices<S, F>(label: &str, fresh: &mut F) -> CheckResult
where
    S: Storage,
    F: FnMut() -> S,
{
    let name = "append_then_indices_consistent";
    let mut store = fresh();

    let entries = vec![
        make_entry(1, 1, b"a"),
        make_entry(2, 2, b"b"),
        make_entry(3, 3, b"c"),
    ];
    if let Err(e) = store.append(&entries) {
        return CheckResult::fail(name, format!("{label}: append: {e}"));
    }

    match store.first_index() {
        Ok(1) => {}
        Ok(idx) => {
            return CheckResult::fail(name, format!("{label}: first_index()={idx}, expected 1"))
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: first_index: {e}")),
    }

    match store.last_index() {
        Ok(3) => {}
        Ok(idx) => {
            return CheckResult::fail(name, format!("{label}: last_index()={idx}, expected 3"))
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: last_index: {e}")),
    }

    // term must match the entry's term.
    for (idx, term) in [(1, 1u64), (2, 2), (3, 3)] {
        match store.term(idx) {
            Ok(t) if t == term => {}
            Ok(t) => {
                return CheckResult::fail(
                    name,
                    format!("{label}: term({idx})={t}, expected {term}"),
                )
            }
            Err(e) => return CheckResult::fail(name, format!("{label}: term({idx}): {e}")),
        }
    }

    CheckResult::pass(name)
}

fn check_entries_semantics<S, F>(label: &str, fresh: &mut F) -> CheckResult
where
    S: Storage,
    F: FnMut() -> S,
{
    let name = "entries_semantics";
    let mut store = fresh();

    let entries = vec![
        make_entry(1, 1, b"aa"),
        make_entry(2, 1, b"bb"),
        make_entry(3, 1, b"cc"),
    ];
    if let Err(e) = store.append(&entries) {
        return CheckResult::fail(name, format!("{label}: append: {e}"));
    }

    // Half-open: [1, 4) → 3 entries.
    match store.entries(1, 4, None) {
        Ok(v) if v.len() == 3 => {}
        Ok(v) => {
            return CheckResult::fail(
                name,
                format!("{label}: entries(1,4) returned {} entries, expected 3", v.len()),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries(1,4): {e}")),
    }

    // Half-open: [2, 4) → 2 entries.
    match store.entries(2, 4, None) {
        Ok(v) if v.len() == 2 => {}
        Ok(v) => {
            return CheckResult::fail(
                name,
                format!("{label}: entries(2,4) returned {} entries, expected 2", v.len()),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries(2,4): {e}")),
    }

    // Degenerate: low == high → empty.
    match store.entries(2, 2, None) {
        Ok(v) if v.is_empty() => {}
        Ok(v) => {
            return CheckResult::fail(
                name,
                format!(
                    "{label}: entries(2,2) returned {} entries, expected 0",
                    v.len()
                ),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries(2,2): {e}")),
    }

    // Out-of-window below: low < first_index → Compacted.
    match store.entries(0, 2, None) {
        Err(StorageError::Compacted) => {}
        Ok(v) => {
            return CheckResult::fail(
                name,
                format!(
                    "{label}: entries(0,2) should be Compacted, got {} entries",
                    v.len()
                ),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries(0,2): {e}")),
    }

    // Out-of-window above: high > last_index + 1 → Compacted.
    match store.entries(1, 5, None) {
        Err(StorageError::Compacted) => {}
        Ok(v) => {
            return CheckResult::fail(
                name,
                format!(
                    "{label}: entries(1,5) should be Compacted, got {} entries",
                    v.len()
                ),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries(1,5): {e}")),
    }

    // max_size = Some(0) → empty prefix.
    match store.entries(1, 4, Some(0)) {
        Ok(v) if v.is_empty() => {}
        Ok(v) => {
            return CheckResult::fail(
                name,
                format!(
                    "{label}: entries(1,4,max_size=0) returned {} entries, expected 0",
                    v.len()
                ),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries max_size=0: {e}")),
    }

    // max_size bounds the prefix: entries have data len 2 each.
    // max_size=4 → at most 2 entries.
    match store.entries(1, 4, Some(4)) {
        Ok(v) if v.len() <= 2 => {}
        Ok(v) => {
            return CheckResult::fail(
                name,
                format!(
                    "{label}: entries(1,4,max_size=4) returned {} entries, expected <= 2",
                    v.len()
                ),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries max_size=4: {e}")),
    }

    CheckResult::pass(name)
}

/// The append contract: entries must be appended with strict sequential
/// indices (`last_index + 1`). This check verifies that N sequential appends
/// produce a gap-free log from 1 to N.
fn check_append_sequential<S, F>(label: &str, fresh: &mut F) -> CheckResult
where
    S: Storage,
    F: FnMut() -> S,
{
    let name = "append_sequential_contract";
    let mut store = fresh();

    // Append one entry at a time (each in its own call).
    for i in 1..=5u64 {
        let entry = make_entry(i, 1, &[i as u8]);
        if let Err(e) = store.append(&[entry]) {
            return CheckResult::fail(
                name,
                format!("{label}: append(entry {i}): {e}"),
            );
        }
    }

    // Verify last_index.
    match store.last_index() {
        Ok(5) => {}
        Ok(idx) => {
            return CheckResult::fail(
                name,
                format!("{label}: last_index()={idx}, expected 5"),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: last_index: {e}")),
    }

    // Verify all 5 entries are retrievable.
    match store.entries(1, 6, None) {
        Ok(v) if v.len() == 5 => {}
        Ok(v) => {
            return CheckResult::fail(
                name,
                format!(
                    "{label}: entries(1,6) returned {} entries, expected 5",
                    v.len()
                ),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries(1,6): {e}")),
    }

    CheckResult::pass(name)
}

fn check_hard_state_roundtrip<S, F>(label: &str, fresh: &mut F) -> CheckResult
where
    S: Storage,
    F: FnMut() -> S,
{
    let name = "set_hard_state_roundtrip";
    let mut store = fresh();

    let hs = HardState {
        term: 5,
        vote: Some(3),
        commit: 0,
    };
    if let Err(e) = store.set_hard_state(&hs) {
        return CheckResult::fail(name, format!("{label}: set_hard_state: {e}"));
    }

    match store.initial_state() {
        Ok(state) => {
            if state.hard_state != hs {
                return CheckResult::fail(
                    name,
                    format!(
                        "{label}: initial_state hard_state={:?}, expected {:?}",
                        state.hard_state, hs
                    ),
                );
            }
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: initial_state: {e}")),
    }

    CheckResult::pass(name)
}

fn check_sync_idempotent<S, F>(label: &str, fresh: &mut F) -> CheckResult
where
    S: Storage,
    F: FnMut() -> S,
{
    let name = "sync_entries_idempotent";
    let mut store = fresh();

    let entries = vec![make_entry(1, 1, b"x"), make_entry(2, 1, b"y")];
    if let Err(e) = store.append(&entries) {
        return CheckResult::fail(name, format!("{label}: append: {e}"));
    }

    // First sync.
    if let Err(e) = store.sync_entries() {
        return CheckResult::fail(name, format!("{label}: first sync_entries: {e}"));
    }

    // Second sync (idempotent).
    if let Err(e) = store.sync_entries() {
        return CheckResult::fail(name, format!("{label}: second sync_entries: {e}"));
    }

    // State is unchanged after multiple syncs.
    match store.last_index() {
        Ok(2) => {}
        Ok(idx) => {
            return CheckResult::fail(
                name,
                format!("{label}: last_index()={idx} after double sync, expected 2"),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: last_index: {e}")),
    }

    match store.entries(1, 3, None) {
        Ok(v) if v.len() == 2 => {}
        Ok(v) => {
            return CheckResult::fail(
                name,
                format!(
                    "{label}: entries after double sync returned {} entries, expected 2",
                    v.len()
                ),
            )
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries: {e}")),
    }

    CheckResult::pass(name)
}

fn check_index_continuity<S, F>(label: &str, fresh: &mut F) -> CheckResult
where
    S: Storage,
    F: FnMut() -> S,
{
    let name = "index_continuity";
    let mut store = fresh();

    let n: u64 = 10;
    let entries: Vec<LogEntry> = (1..=n).map(|i| make_entry(i, 1, &[i as u8])).collect();
    if let Err(e) = store.append(&entries) {
        return CheckResult::fail(name, format!("{label}: append: {e}"));
    }

    // All indices 1..=n must be present and contiguous.
    match store.entries(1, n + 1, None) {
        Ok(v) => {
            if v.len() != n as usize {
                return CheckResult::fail(
                    name,
                    format!(
                        "{label}: expected {n} entries, got {}",
                        v.len()
                    ),
                );
            }
            for (i, entry) in v.iter().enumerate() {
                let expected_idx = (i + 1) as LogIndex;
                if entry.index != expected_idx {
                    return CheckResult::fail(
                        name,
                        format!(
                            "{label}: gap at position {i}: got index {}, expected {expected_idx}",
                            entry.index
                        ),
                    );
                }
            }
        }
        Err(e) => return CheckResult::fail(name, format!("{label}: entries: {e}")),
    }

    CheckResult::pass(name)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_entry(index: LogIndex, term: Term, data: &[u8]) -> LogEntry {
    LogEntry {
        index,
        term,
        entry_type: EntryType::Entry,
        data: data.to_vec(),
    }
}

// ---------------------------------------------------------------------------
// Non-vacuity tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arachne_seam::storage::{ConfState, RaftState, Snapshot};

    /// A minimal in-memory `Storage` double that correctly implements the
    /// contract. Used to prove the suite passes on a correct impl.
    struct GoodMemStorage {
        entries: Vec<LogEntry>,
        hard_state: HardState,
    }

    impl GoodMemStorage {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                hard_state: HardState::default(),
            }
        }
    }

    impl Storage for GoodMemStorage {
        fn initial_state(&self) -> Result<RaftState, StorageError> {
            Ok(RaftState {
                hard_state: self.hard_state.clone(),
                conf_state: ConfState::default(),
            })
        }
        fn entries(
            &self,
            low: LogIndex,
            high: LogIndex,
            max_size: Option<u64>,
        ) -> Result<Vec<LogEntry>, StorageError> {
            if low >= high {
                return Ok(Vec::new());
            }
            let first = self.entries.first().map_or(1, |e| e.index);
            let last = self.entries.last().map_or(0, |e| e.index);
            if low < first || high > last + 1 {
                return Err(StorageError::Compacted);
            }
            let mut out: Vec<LogEntry> =
                self.entries[(low - first) as usize..(high - first) as usize].to_vec();
            if let Some(limit) = max_size {
                let mut size: u64 = 0;
                let mut keep = 0;
                while keep < out.len() {
                    let add = out[keep].data.len() as u64;
                    if size + add > limit {
                        break;
                    }
                    size += add;
                    keep += 1;
                }
                out.truncate(keep);
            }
            Ok(out)
        }
        fn term(&self, index: LogIndex) -> Result<Term, StorageError> {
            let first = self.entries.first().map_or(1, |e| e.index);
            let last = self.entries.last().map_or(0, |e| e.index);
            if index < first || index > last {
                return Err(StorageError::Compacted);
            }
            Ok(self.entries[(index - first) as usize].term)
        }
        fn first_index(&self) -> Result<LogIndex, StorageError> {
            Ok(self.entries.first().map_or(1, |e| e.index))
        }
        fn last_index(&self) -> Result<LogIndex, StorageError> {
            Ok(self.entries.last().map_or(0, |e| e.index))
        }
        fn snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
            Ok(None)
        }
        fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
            self.entries.extend_from_slice(entries);
            Ok(())
        }
        fn set_hard_state(&mut self, hs: &HardState) -> Result<(), StorageError> {
            self.hard_state = hs.clone();
            Ok(())
        }
        fn sync_entries(&mut self) -> Result<(), StorageError> {
            Ok(())
        }
        fn compact(&mut self, _compact_to: LogIndex) -> Result<(), StorageError> {
            Ok(())
        }
    }

    /// A deliberately-broken `Storage` impl that violates the contract:
    /// `last_index()` always returns 0 (even after appends), and
    /// `entries()` always returns empty.
    struct BrokenStorage {
        entries: Vec<LogEntry>,
        hard_state: HardState,
    }

    impl BrokenStorage {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                hard_state: HardState::default(),
            }
        }
    }

    impl Storage for BrokenStorage {
        fn initial_state(&self) -> Result<RaftState, StorageError> {
            Ok(RaftState {
                hard_state: self.hard_state.clone(),
                conf_state: ConfState::default(),
            })
        }
        fn entries(
            &self,
            _low: LogIndex,
            _high: LogIndex,
            _max_size: Option<u64>,
        ) -> Result<Vec<LogEntry>, StorageError> {
            // Always returns empty — violates the contract.
            Ok(Vec::new())
        }
        fn term(&self, _index: LogIndex) -> Result<Term, StorageError> {
            // Always returns Compacted — violates the contract.
            Err(StorageError::Compacted)
        }
        fn first_index(&self) -> Result<LogIndex, StorageError> {
            Ok(1)
        }
        fn last_index(&self) -> Result<LogIndex, StorageError> {
            // Always returns 0 — violates the contract.
            Ok(0)
        }
        fn snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
            Ok(None)
        }
        fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
            self.entries.extend_from_slice(entries);
            Ok(())
        }
        fn set_hard_state(&mut self, hs: &HardState) -> Result<(), StorageError> {
            self.hard_state = hs.clone();
            Ok(())
        }
        fn sync_entries(&mut self) -> Result<(), StorageError> {
            Ok(())
        }
        fn compact(&mut self, _compact_to: LogIndex) -> Result<(), StorageError> {
            Ok(())
        }
    }

    #[test]
    fn correct_impl_passes_suite() {
        let report = run_storage_suite("good", || GoodMemStorage::new());
        assert!(
            report.all_passed(),
            "expected all checks to pass, failures: {:?}",
            report.failures()
        );
    }

    #[test]
    fn broken_impl_is_rejected() {
        let report = run_storage_suite("broken", || BrokenStorage::new());
        assert!(
            !report.all_passed(),
            "expected the suite to reject the broken impl"
        );
        // Specifically, the index/continuity checks must fail.
        let failures = report.failures();
        assert!(
            failures.contains(&"append_then_indices_consistent"),
            "expected append_then_indices_consistent to fail, got: {failures:?}"
        );
        assert!(
            failures.contains(&"entries_semantics"),
            "expected entries_semantics to fail, got: {failures:?}"
        );
        assert!(
            failures.contains(&"index_continuity"),
            "expected index_continuity to fail, got: {failures:?}"
        );
    }

    #[test]
    fn report_tracks_all_checks() {
        let report = run_storage_suite("count", || GoodMemStorage::new());
        assert_eq!(report.checks.len(), 7);
        assert_eq!(report.label, "count");
    }
}
