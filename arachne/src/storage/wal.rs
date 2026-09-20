//! The WAL-backed [`Storage`] implementation: crash-safe durable log with
//! startup recovery, fsync policy, and fsync accounting.
//!
//! # Crash-safety invariants
//!
//! * **I1** — [`Storage::set_hard_state`] is ALWAYS fsynced before returning,
//!   regardless of [`FsyncPolicy`]. Batching HardState can cause double-voting
//!   after a crash.
//! * **I2/I4** — entries appended via [`Storage::append`] are made durable by
//!   [`Storage::sync_entries`] per the configured [`FsyncPolicy`].
//!   `sync_entries` is the **synchronous durability barrier**: it returns only
//!   after pending entries are fsynced. A future timer may only pre-flush,
//!   never weaken this guarantee.
//!
//! # Startup recovery decision tree
//!
//! On [`WalStorage::open`], the WAL is replayed from disk:
//! 1. Acquire the data-dir lock (`File::try_lock` on `LOCK`).
//! 2. Read and validate the META file.
//! 3. (P2: no snapshot; M2 adds snapshot loading.)
//! 4. Replay all segments in ascending index order:
//!    * A **structural tear** (incomplete record at end of file) may occur
//!      only in the **last** segment. For a tear, apply the commit-window
//!      check: if `estimated_index <= hard_state.commit + 1` → **fail-start**;
//!      else **auto-truncate** at the last valid record boundary.
//!    * **Any fully-present record that fails CRC/decode for ANY reason**
//!      (`ChecksumMismatch`, `UnknownRecordType`, `UnsupportedRecordType`,
//!      payload too short, wrong-but-plausible len) → **fail-start
//!      `Unrecoverable` unconditionally**. No type-byte sniffing.
//!    * A malformed record in a **non-last** segment → **fail-start**.
//! 5. Rebuild the in-memory log index; verify continuity (no gaps).
//! 6. Assert `hard_state.commit <= last_entry_index` (else `Unrecoverable`).
//!
//! # Fsync policy
//!
//! * [`FsyncPolicy::Always`] — every `sync_entries` performs a real `fsync`.
//! * [`FsyncPolicy::BatchMs(ms)`] — `sync_entries` performs a real `fsync`
//!   only if there are new (unfsynced) entries since the last real fsync.
//!   The `ms` parameter is **advisory** until P4 (timer-driven pre-flush);
//!   `sync_entries` remains the synchronous durability barrier.
//!
//! HardState is **always** fsynced per-call (I1), independent of policy.
//!
//! # Segment naming
//!
//! A segment file is named `wal-{first_log_index:020}.log` where
//! `first_log_index` is the log index of the first **Entry** the segment
//! will contain. A segment that begins with a HardState record (before any
//! Entry) is still named by the first Entry it will receive (the next
//! `last_index + 1` at the time of creation).

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arachne_seam::storage::{
    FsyncObserver, HardState, LogEntry, RaftState, Snapshot, Storage, StorageError,
};
use arachne_seam::types::{LogIndex, Term};

use crate::storage::format::{
    decode_entry, decode_hard_state, decode_record, encode_entry, encode_hard_state,
    RecordType, MAX_RECORD_BYTES,
};
use crate::storage::meta::{read_meta, write_meta, fsync_dir, Meta, FORMAT_VERSION};
use crate::storage::snapshot::{
    decode_snapshot, encode_snapshot, parse_snapshot_file_name, snapshot_file_name,
};
use crate::storage::segment::{
    parse_segment_name, segment_path, Segment, DEFAULT_SEGMENT_BYTES,
};

/// How many snapshot files to retain on disk.
///
/// The newest is the live snapshot; the runner-up exists so that `open` can
/// fall back to it when the newest file fails its CRC check. Older files are
/// superseded by the compaction watermark and only waste space.
const SNAPSHOT_RETENTION: usize = 2;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The fsync policy for entry (log) records.
///
/// HardState is **always** fsynced on every `set_hard_state` call regardless
/// of this policy (invariant I1).
///
/// The `BatchMs(ms)` parameter is **advisory** until P4: it indicates the
/// intended batch window for a future timer-driven pre-flush. `sync_entries`
/// remains the synchronous durability barrier in all cases — it returns only
/// after pending entries are fsynced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Every `sync_entries` call performs a real `fsync`.
    Always,
    /// `sync_entries` performs a real `fsync` only if there are new (unfsynced)
    /// entries since the last real fsync. The `ms` value is advisory until P4.
    BatchMs(u64),
}

impl Default for FsyncPolicy {
    fn default() -> Self {
        FsyncPolicy::Always
    }
}

/// Configuration for a [`WalStorage`].
#[derive(Clone, Debug)]
pub struct WalConfig {
    /// The fsync policy for entry records.
    pub fsync_policy: FsyncPolicy,
    /// The maximum size of a single segment file before rollover.
    pub segment_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: DEFAULT_SEGMENT_BYTES,
        }
    }
}

/// Options for opening a [`WalStorage`].
pub struct WalOptions {
    /// The cluster identifier.
    pub cluster_id: String,
    /// The node identifier.
    pub node_id: String,
    /// The storage configuration.
    pub config: WalConfig,
    /// The creation timestamp in milliseconds since the Unix epoch, supplied
    /// by the caller (the storage core does not read the system clock).
    pub created_at_millis: u64,
    /// An optional observer that is notified after every real fsync of a
    /// segment. `None` (zero-overhead) by default.
    pub fsync_observer: Option<Arc<dyn FsyncObserver>>,
}

impl Clone for WalOptions {
    fn clone(&self) -> Self {
        Self {
            cluster_id: self.cluster_id.clone(),
            node_id: self.node_id.clone(),
            config: self.config.clone(),
            created_at_millis: self.created_at_millis,
            fsync_observer: self.fsync_observer.clone(),
        }
    }
}

impl std::fmt::Debug for WalOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalOptions")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("config", &self.config)
            .field("created_at_millis", &self.created_at_millis)
            .field("fsync_observer", &self.fsync_observer.as_ref().map(|_| "Some(observer)"))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Fsync accounting
// ---------------------------------------------------------------------------

/// Fsync and recovery counters for a [`WalStorage`].
///
/// Used for M0 acceptance criteria ③④ (fsync cost measurement).
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageStats {
    /// Number of real `fsync` calls made for HardState records (I1).
    pub hard_state_fsyncs: u64,
    /// Number of real `fsync` calls made for entry records.
    pub entry_fsyncs: u64,
    /// Number of `sync_entries` calls that resulted in a batched fsync
    /// (under `BatchMs` policy; equals `entry_fsyncs` under `Always`).
    pub entry_sync_batches: u64,
    /// Number of directory `fsync` calls (META write, segment creation).
    pub dir_fsyncs: u64,
    /// Number of records removed by auto-truncation during recovery.
    pub wal_truncated_records: u64,
}

/// The outcome of a [`WalStorage::force_recovery`] rewrite (propsol §6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForceRecoveryReport {
    /// The `cluster_id` found in META before the rewrite.
    pub previous_cluster_id: String,
    /// The `cluster_id` now recorded in META.
    pub cluster_id: String,
    /// The new term (previous term + 1).
    pub term: Term,
    /// The retained commit (== applied) index.
    pub commit: LogIndex,
    /// Log entries discarded from the uncommitted tail.
    pub discarded_entries: u64,
}

// ---------------------------------------------------------------------------
// WalStorage
// ---------------------------------------------------------------------------

/// A WAL-backed [`Storage`] implementation.
///
/// Owns the active segment file, the in-memory log index, and the data-dir
/// lock. The lock is held for the lifetime of the `WalStorage` and released
/// on drop.
pub struct WalStorage {
    /// The data directory.
    data_dir: PathBuf,
    /// The active (last) segment.
    segment: Segment,
    /// The first log index of the active segment (from its filename).
    segment_first_index: LogIndex,
    /// The highest Entry index physically present in the active segment.
    /// 0 if the segment holds no Entry yet.
    max_entry_in_segment: LogIndex,
    /// The in-memory log entries: contiguous, starting at
    /// `compacted_to + 1` (or later, then trimmed at open).
    entries: Vec<LogEntry>,
    /// The index the log has been compacted through (0 = nothing compacted).
    /// `first_index() == compacted_to + 1`.
    compacted_to: LogIndex,
    /// The persisted snapshot, if any (loaded at open, written by
    /// [`WalStorage::save_snapshot`]).
    snapshot: Option<Snapshot>,
    /// The META record as loaded, kept so `save_snapshot` can update the
    /// snapshot pointer without re-reading the file.
    meta: Meta,
    /// The current hard state.
    hard_state: HardState,
    /// Whether there are unfsynced entries since the last real entry fsync.
    pending_entry_fsync: bool,
    /// The fsync policy.
    fsync_policy: FsyncPolicy,
    /// The segment rollover threshold.
    segment_bytes: u64,
    /// Fsync/recovery counters.
    stats: StorageStats,
    /// Optional fsync observer (zero-overhead when `None`).
    fsync_observer: Option<Arc<dyn FsyncObserver>>,
    /// The data-dir lock file (held for the lifetime of this struct).
    /// Dropping it releases the lock.
    _lock: File,
}

impl WalStorage {
    /// Open (or create) a WAL storage at the given directory.
    ///
    /// On an existing directory, performs startup recovery. On a fresh
    /// directory, creates the META file and an empty log.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Unrecoverable`] for:
    /// * Lock contention (another process holds the data-dir lock).
    /// * META mismatch (cluster_id, node_id, or format_version differ).
    /// * A fully-present record that fails CRC/decode (any reason).
    /// * A structural tear within the committed window.
    /// * Index gap (non-contiguous log indices).
    /// * `hard_state.commit > last_entry_index` after replay.
    ///
    /// Returns [`StorageError::Io`] for low-level I/O failures.
    pub fn open(dir: &Path, opts: WalOptions) -> Result<Self, StorageError> {
        // Step 0: ensure the data directory exists.
        fs::create_dir_all(dir).map_err(StorageError::Io)?;

        // Step 1: acquire the data-dir lock.
        let lock_path = dir.join("LOCK");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)
            .map_err(StorageError::Io)?;
        match lock_file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(StorageError::Unrecoverable {
                    detail: format!(
                        "data directory {:?} is locked by another process",
                        dir
                    ),
                });
            }
            Err(e) => {
                return Err(StorageError::Unrecoverable {
                    detail: format!("failed to acquire data-dir lock: {e}"),
                });
            }
        }

        // Step 2: read and validate META.
        let mut stats = StorageStats::default();
        let meta = Self::recover_meta(dir, &opts, &mut stats)?;

        // Step 3: load the newest valid snapshot META names (propsol §5.5.3 step
        // 3). A corrupt newest snapshot falls back to an earlier one; if META
        // names a snapshot but none can be loaded, the WAL must still contain
        // the whole log from index 1, else recovery cannot be complete.
        let mut snapshot = Self::load_snapshot(dir, meta.snapshot_index)?;
        let mut compacted_to = snapshot.as_ref().map(|s| s.meta.index).unwrap_or(0);
        let (mut entries, hard_state, truncated_records, dir_fsyncs_from_recovery) =
            Self::recover_wal(dir, compacted_to)?;
        stats.wal_truncated_records = truncated_records;
        stats.dir_fsyncs += dir_fsyncs_from_recovery;

        if meta.snapshot_index != 0 && snapshot.is_none() {
            let first = entries.first().map(|e| e.index).unwrap_or(1);
            if first != 1 {
                return Err(StorageError::Unrecoverable {
                    detail: format!(
                        "META names a snapshot at index {} but no valid snapshot file \
                         could be loaded and the WAL starts at index {first}",
                        meta.snapshot_index
                    ),
                });
            }
            // No snapshot after all: recover as a plain (non-compacted) log.
            compacted_to = 0;
        }

        // Step 5: verify index continuity within the retained set and that the
        // retained entries meet the snapshot with no gap.
        Self::verify_continuity(&entries)?;
        if let Some(first) = entries.first().map(|e| e.index)
            && first > compacted_to + 1
        {
            return Err(StorageError::Unrecoverable {
                detail: format!(
                    "gap between the snapshot (index {compacted_to}) and the first \
                     retained entry (index {first})"
                ),
            });
        }
        // Entries at or below the snapshot are covered by it; drop them from the
        // in-memory cache (propsol §5.5.3 step 5: rebuild from `S.index + 1`).
        entries.retain(|e| e.index > compacted_to);
        if snapshot.is_none() {
            snapshot = None;
        }

        // Step 6: assert commit <= last_entry_index (unconditional).
        let last_entry_index = entries
            .last()
            .map(|e| e.index)
            .unwrap_or(compacted_to);
        if hard_state.commit > last_entry_index {
            return Err(StorageError::Unrecoverable {
                detail: format!(
                    "hard_state.commit ({}) > last_entry_index ({}) after replay",
                    hard_state.commit, last_entry_index
                ),
            });
        }

        // Open the active segment: the highest-numbered existing segment,
        // or create wal-1.log if no segments exist.
        let segment_indices = list_segment_indices(dir).map_err(StorageError::Io)?;
        let is_new_segment = segment_indices.is_empty();
        let active_first_index = match segment_indices.last().copied() {
            Some(idx) => idx,
            None => 1,
        };
        let segment = open_segment_at(dir, active_first_index, opts.config.segment_bytes)?;
        // N3: fsync the directory after creating a new segment file (not
        // just reopening an existing one). This ensures the directory entry
        // for the new file is durable.
        if is_new_segment {
            fsync_dir(dir).map_err(StorageError::Io)?;
            stats.dir_fsyncs += 1;
        }

        // Compute the max entry index in the active segment. Since entries
        // are sequential and the segment is named by its first entry index,
        // the max entry in the segment is min(global_last, ...) — but since
        // the active segment is the LAST segment, it holds entries from
        // `active_first_index` through the global last.
        let max_entry_in_segment = entries.last().map(|e| e.index).unwrap_or(0);

        Ok(Self {
            data_dir: dir.to_path_buf(),
            segment,
            segment_first_index: active_first_index,
            max_entry_in_segment,
            entries,
            compacted_to,
            snapshot,
            meta,
            hard_state,
            pending_entry_fsync: false,
            fsync_policy: opts.config.fsync_policy,
            segment_bytes: opts.config.segment_bytes,
            stats,
            fsync_observer: opts.fsync_observer,
            _lock: lock_file,
        })
    }

    /// Return the current fsync/recovery counters.
    pub fn stats(&self) -> StorageStats {
        self.stats
    }

    /// Return the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Notify the fsync observer (no-op if `None`).
    fn notify_fsynced(&self) {
        if let Some(observer) = &self.fsync_observer {
            observer.on_segment_fsynced(
                self.segment_first_index,
                self.max_entry_in_segment,
            );
        }
    }

    // ---- Recovery helpers ----

    fn recover_meta(
        dir: &Path,
        opts: &WalOptions,
        stats: &mut StorageStats,
    ) -> Result<Meta, StorageError> {
        let meta_path = dir.join("META");
        if meta_path.exists() {
            let meta = read_meta(dir).map_err(|e| StorageError::Unrecoverable {
                detail: format!("failed to read META: {e}"),
            })?;
            if meta.cluster_id != opts.cluster_id {
                return Err(StorageError::Unrecoverable {
                    detail: format!(
                        "META cluster_id mismatch: expected {:?}, got {:?}",
                        opts.cluster_id, meta.cluster_id
                    ),
                });
            }
            if meta.node_id != opts.node_id {
                return Err(StorageError::Unrecoverable {
                    detail: format!(
                        "META node_id mismatch: expected {:?}, got {:?}",
                        opts.node_id, meta.node_id
                    ),
                });
            }
            if meta.format_version != FORMAT_VERSION {
                return Err(StorageError::Unrecoverable {
                    detail: format!(
                        "META format_version mismatch: expected {FORMAT_VERSION}, got {}",
                        meta.format_version
                    ),
                });
            }
            Ok(meta)
        } else {
            // Fresh directory: create META atomically.
            let meta = Meta {
                cluster_id: opts.cluster_id.clone(),
                node_id: opts.node_id.clone(),
                format_version: FORMAT_VERSION,
                created_at: opts.created_at_millis,
                snapshot_index: 0,
                snapshot_term: 0,
            };
            write_meta(dir, &meta).map_err(|e| StorageError::Unrecoverable {
                detail: format!("failed to write META: {e}"),
            })?;
            stats.dir_fsyncs += 1;
            Ok(meta)
        }
    }

    fn recover_wal(
        dir: &Path,
        compacted_to: LogIndex,
    ) -> Result<(Vec<LogEntry>, HardState, u64, u64), StorageError> {
        let segment_indices = list_segment_indices(dir).map_err(StorageError::Io)?;
        let last_segment_idx = segment_indices.len().saturating_sub(1);

        let mut entries: Vec<LogEntry> = Vec::new();
        let mut hard_state = HardState::default();
        let mut truncated_records: u64 = 0;
        let dir_fsyncs: u64 = 0; // No dir fsyncs during replay (only truncation).

        for (seg_pos, &idx) in segment_indices.iter().enumerate() {
            let path = segment_path(dir, idx);
            let raw = fs::read(&path).map_err(StorageError::Io)?;
            let is_last_segment = seg_pos == last_segment_idx;

            let mut offset: u64 = 0;
            let file_len = raw.len() as u64;

            while offset < file_len {
                // Determine if this position holds a structural tear.
                // A tear is: (a) fewer than 8 bytes remain, OR
                // (b) the declared len makes record_end exceed file_len.
                let is_tear = if file_len - offset < 8 {
                    true
                } else {
                    let len_u32 = u32::from_le_bytes([
                        raw[offset as usize],
                        raw[offset as usize + 1],
                        raw[offset as usize + 2],
                        raw[offset as usize + 3],
                    ]);
                    let len = len_u32 as u64;
                    let record_end = offset + 8 + len;
                    record_end > file_len
                };

                if is_tear {
                    // Unified tear handling: all three tear forms share this
                    // single path.
                    if !is_last_segment {
                        return Err(StorageError::Unrecoverable {
                            detail: format!(
                                "structural tear in non-last segment {:?} at byte offset {offset}",
                                path
                            ),
                        });
                    }
                    // Tear in the last segment: apply the commit-window check.
                    // The estimated index of the torn record: one past the
                    // last entry actually replayed (or one past the snapshot
                    // when nothing was replayed yet).
                    let estimated_index = entries
                        .last()
                        .map(|e| e.index + 1)
                        .unwrap_or(compacted_to + 1);
                    if estimated_index <= hard_state.commit + 1 {
                        return Err(StorageError::Unrecoverable {
                            detail: format!(
                                "structural tear at estimated index {estimated_index} \
                                 (within committed window commit={}): cannot auto-truncate \
                                 committed data",
                                hard_state.commit
                            ),
                        });
                    }
                    // Safe to auto-truncate.
                    truncate_file_at(&path, offset)?;
                    truncated_records += 1;
                    break;
                }

                // The record is fully present on disk. Decode it.
                let len_u32 = u32::from_le_bytes([
                    raw[offset as usize],
                    raw[offset as usize + 1],
                    raw[offset as usize + 2],
                    raw[offset as usize + 3],
                ]);
                let record_end = offset + 8 + len_u32 as u64;
                let record_bytes = &raw[offset as usize..record_end as usize];

                match decode_record(record_bytes) {
                    Ok((rt, payload)) => match rt {
                        RecordType::HardState => {
                            let (term, vote, commit) = decode_hard_state(&payload)
                                .map_err(|e| StorageError::Unrecoverable {
                                    detail: format!("corrupt HardState record: {e}"),
                                })?;
                            hard_state = HardState {
                                term,
                                vote,
                                commit,
                            };
                        }
                        RecordType::Entry => {
                            let (index, term, entry_type, data) =
                                decode_entry(&payload).map_err(|e| {
                                    StorageError::Unrecoverable {
                                        detail: format!("corrupt Entry record: {e}"),
                                    }
                                })?;
                            entries.push(LogEntry {
                                index,
                                term,
                                entry_type: entry_type_to_seam(entry_type),
                                data,
                            });
                        }
                        RecordType::Meta => {
                            return Err(StorageError::Unrecoverable {
                                detail: "unexpected Meta record in WAL".into(),
                            });
                        }
                    },
                    Err(decode_err) => {
                        // A fully-present record that fails CRC/decode for
                        // ANY reason → fail-start unconditionally.
                        return Err(StorageError::Unrecoverable {
                            detail: format!(
                                "corrupt record in segment {:?} at byte offset {}: {decode_err}",
                                path, offset
                            ),
                        });
                    }
                }
                offset = record_end;
            }
        }

        Ok((entries, hard_state, truncated_records, dir_fsyncs))
    }

    /// Verify the retained entries are contiguous, starting from the first one.
    ///
    /// After compaction the first retained index is no longer 1 (and trailing
    /// retention may even keep entries at or below the snapshot), so contiguity
    /// is checked relative to `entries[0]`; the caller additionally verifies
    /// that the retained set meets the snapshot without a gap.
    fn verify_continuity(entries: &[LogEntry]) -> Result<(), StorageError> {
        let Some(first) = entries.first().map(|e| e.index) else {
            return Ok(());
        };
        for (i, entry) in entries.iter().enumerate() {
            let expected = first + i as LogIndex;
            if entry.index != expected {
                return Err(StorageError::Unrecoverable {
                    detail: format!(
                        "log index gap: expected {expected}, got {} at position {i}",
                        entry.index
                    ),
                });
            }
        }
        Ok(())
    }

    /// Load the newest valid snapshot META names, falling back to an earlier one
    /// when the newest is corrupt (propsol §5.5.3 step 3).
    ///
    /// A META pointer of 0 means "no snapshot": snapshot files are ignored, which
    /// is what makes a crash between renaming the snapshot file and updating META
    /// safe (the WAL is still complete, because compaction happens after the META
    /// update).
    fn load_snapshot(dir: &Path, expected_index: LogIndex) -> Result<Option<Snapshot>, StorageError> {
        if expected_index == 0 {
            return Ok(None);
        }
        let mut candidates: Vec<(LogIndex, PathBuf)> = Vec::new();
        for entry in fs::read_dir(dir).map_err(StorageError::Io)? {
            let entry = entry.map_err(StorageError::Io)?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some((index, _term)) = parse_snapshot_file_name(&name)
                && index <= expected_index
            {
                candidates.push((index, entry.path()));
            }
        }
        // Newest first, so the fallback walks backwards through history.
        candidates.sort_by(|a, b| b.0.cmp(&a.0));
        for (index, path) in candidates {
            let bytes = fs::read(&path).map_err(StorageError::Io)?;
            if let Ok(snapshot) = decode_snapshot(&bytes)
                && snapshot.meta.index == index
            {
                return Ok(Some(snapshot));
            }
            // A corrupt or mislabelled file: fall back to the next candidate.
        }
        Ok(None)
    }

    // ---- Rollover helper ----

    /// Roll over to a new segment if the current one is full.
    ///
    /// If the outgoing segment has unfsynced entries, it is fsynced **before**
    /// the switch (guaranteeing cross-segment durability: `sync_entries`
    /// covers every appended-but-unsynced entry across all segments).
    fn maybe_rollover(&mut self, next_entry_index: LogIndex) -> Result<(), StorageError> {
        let current_size = self.segment.size().map_err(StorageError::Io)?;
        if current_size >= self.segment_bytes {
            // N2: fsync the outgoing segment if it has pending entries.
            // This ensures that a crash after rollover does not lose
            // acknowledged entries in the outgoing segment.
            if self.pending_entry_fsync {
                self.segment.sync().map_err(StorageError::Io)?;
                self.stats.entry_fsyncs += 1;
                self.pending_entry_fsync = false;
                // Notify the observer for the outgoing segment (before we
                // reset its identity for the new segment).
                self.notify_fsynced();
            }
            // Create a new segment named by the next entry's index.
            let new_segment = open_segment_at(
                &self.data_dir,
                next_entry_index,
                self.segment_bytes,
            )?;
            // Fsync the directory for the new segment file's creation.
            fsync_dir(&self.data_dir).map_err(StorageError::Io)?;
            self.stats.dir_fsyncs += 1;
            self.segment = new_segment;
            // Reset per-segment tracking for the new segment.
            self.segment_first_index = next_entry_index;
            self.max_entry_in_segment = 0;
        }
        Ok(())
    }

    // ---- force-recovery (propsol §6.1) ----

    /// Truncate the durable log to `1..=keep`, discarding every entry after
    /// `keep` (and any records following them in the active segment).
    ///
    /// Called by [`WalStorage::append`] to overwrite a **conflicting suffix**
    /// (raft re-sends entries after a leader change; the storage must overwrite
    /// from the first differing index) and by [`WalStorage::force_recovery`] to
    /// drop the uncommitted tail. It is a no-op (returning `0`) when the log
    /// already ends at `keep`. The retained segment is truncated in place and
    /// every later segment is removed; the in-memory log/segment state is then
    /// updated to match.
    fn truncate_log_to(&mut self, keep: LogIndex) -> Result<u64, StorageError> {
        let last = self.entries.last().map(|e| e.index).unwrap_or(0);
        if last <= keep {
            return Ok(0);
        }
        let discarded = last - keep;
        let data_dir = self.data_dir.clone();
        let segment_bytes = self.segment_bytes;
        let segment_indices = list_segment_indices(&data_dir).map_err(StorageError::Io)?;

        // Locate the byte offset just past the record for entry `keep`, and the
        // segment that holds it. (Entries are contiguous and 1-based, so entry
        // `keep` exists whenever `keep >= 1`.)
        let mut holding: Option<(u64, u64)> = None; // (segment first index, offset)
        'segments: for &idx in &segment_indices {
            let path = segment_path(&data_dir, idx);
            let raw = fs::read(&path).map_err(StorageError::Io)?;
            let file_len = raw.len() as u64;
            let mut offset: u64 = 0;
            while offset < file_len {
                if file_len - offset < 8 {
                    break; // torn tail; `open` already handled it
                }
                let len = u32::from_le_bytes([
                    raw[offset as usize],
                    raw[offset as usize + 1],
                    raw[offset as usize + 2],
                    raw[offset as usize + 3],
                ]) as u64;
                let record_end = offset + 8 + len;
                if record_end > file_len {
                    break;
                }
                let record_bytes = &raw[offset as usize..record_end as usize];
                if let Ok((RecordType::Entry, payload)) = decode_record(record_bytes)
                    && let Ok((index, _, _, _)) = decode_entry(&payload)
                    && index == keep
                {
                    holding = Some((idx, record_end));
                    break 'segments;
                }
                offset = record_end;
            }
        }

        match holding {
            Some((first, offset)) => {
                let seg = open_segment_at(&data_dir, first, segment_bytes)?;
                seg.truncate_at(offset).map_err(StorageError::Io)?;
                seg.sync().map_err(StorageError::Io)?;
                for &idx in &segment_indices {
                    if idx > first {
                        fs::remove_file(segment_path(&data_dir, idx)).map_err(StorageError::Io)?;
                    }
                }
                fsync_dir(&data_dir).map_err(StorageError::Io)?;
                self.segment = seg;
                self.segment_first_index = first;
                self.max_entry_in_segment = keep;
            }
            None => {
                // Nothing is retained (`keep == 0`): drop every segment and
                // start a fresh, empty one at index 1.
                for &idx in &segment_indices {
                    fs::remove_file(segment_path(&data_dir, idx)).map_err(StorageError::Io)?;
                }
                fsync_dir(&data_dir).map_err(StorageError::Io)?;
                self.segment = open_segment_at(&data_dir, 1, segment_bytes)?;
                self.segment_first_index = 1;
                self.max_entry_in_segment = 0;
                fsync_dir(&data_dir).map_err(StorageError::Io)?;
            }
        }

        self.entries.truncate(keep as usize);
        self.pending_entry_fsync = false;
        Ok(discarded)
    }

    /// Force-recovery rewrite of a data dir (propsol §6.1). **Destructive.**
    ///
    /// The operator-gated procedure (the CLI requires `--i-know-data-loss`)
    /// that resets a node to a single-voter cluster at its committed point:
    ///
    /// 1. reads META (the data dir must exist and its `node_id` must match),
    /// 2. opens the WAL with the **old** `cluster_id`, which acquires the
    ///    data-dir lock and refuses if another process holds it,
    /// 3. **discards the uncommitted tail** (entries above `commit`), so
    ///    resurrected-but-never-committed entries cannot be promoted by the
    ///    reset single-node leader,
    /// 4. writes a new HardState: `term + 1`, `vote = self` (raft id 1 — this
    ///    node is the only voter of the reset cluster), `commit` kept,
    /// 5. rewrites META with `new_cluster_id` (or keeps the old one).
    ///
    /// # Membership (M1 boundary)
    ///
    /// Membership is **bootstrap-only** until M3 persists a `ConfState`
    /// (`raft_storage.rs` notes it is static). This method therefore does not
    /// (and cannot) persist `ConfState = [self]`; the caller must start the
    /// recovered node with `initial_cluster = [self]` — a single-voter cluster.
    /// The node's `force-recovery` command enforces that by emitting such a
    /// config. Persisting the reset membership is M3 work.
    ///
    /// # Errors
    ///
    /// [`StorageError::Unrecoverable`] if META is missing/mismatched, the lock
    /// is held, or the rewrite fails; [`StorageError::Io`] for I/O failures.
    pub fn force_recovery(
        dir: &Path,
        node_id: &str,
        new_cluster_id: Option<String>,
        config: WalConfig,
        created_at_millis: u64,
    ) -> Result<ForceRecoveryReport, StorageError> {
        let meta = read_meta(dir).map_err(|e| StorageError::Unrecoverable {
            detail: format!("force-recovery: cannot read META: {e}"),
        })?;
        if meta.node_id != node_id {
            return Err(StorageError::Unrecoverable {
                detail: format!(
                    "force-recovery: data dir node_id {:?} does not match {:?}",
                    meta.node_id, node_id
                ),
            });
        }
        let previous_cluster_id = meta.cluster_id.clone();
        let target_cluster_id = new_cluster_id.unwrap_or_else(|| previous_cluster_id.clone());
        if target_cluster_id.is_empty() {
            return Err(StorageError::Unrecoverable {
                detail: "force-recovery: cluster_id must be non-empty".into(),
            });
        }

        // Open with the OLD cluster_id: META validation passes, and this
        // acquires the data-dir lock (refusing when another process holds it).
        let mut wal = Self::open(
            dir,
            WalOptions {
                cluster_id: previous_cluster_id.clone(),
                node_id: node_id.to_string(),
                config,
                created_at_millis,
                fsync_observer: None,
            },
        )?;

        let commit = wal.hard_state.commit;
        let previous_term = wal.hard_state.term;
        let discarded = wal.truncate_log_to(commit)?;
        // New hard state: bump the term and vote for self. `vote = 1` because
        // this node is voter 1 of the reset single-voter cluster.
        wal.set_hard_state(&HardState {
            term: previous_term + 1,
            vote: Some(1),
            commit,
        })?;

        // Release the data-dir lock before rewriting META.
        drop(wal);

        let mut new_meta = meta;
        new_meta.cluster_id = target_cluster_id.clone();
        write_meta(dir, &new_meta).map_err(|e| StorageError::Unrecoverable {
            detail: format!("force-recovery: cannot write META: {e}"),
        })?;
        fsync_dir(dir).map_err(StorageError::Io)?;

        Ok(ForceRecoveryReport {
            previous_cluster_id,
            cluster_id: target_cluster_id,
            term: previous_term + 1,
            commit,
            discarded_entries: discarded,
        })
    }
    /// Bytes the durable log currently occupies on disk.
    ///
    /// Sums the segment files (not the in-memory index), which is what the
    /// snapshot trigger budgets against and what the `wal_bytes` metric
    /// reports. Segment files are few, so this is a handful of `stat` calls.
    pub fn log_bytes(&self) -> Result<u64, StorageError> {
        let mut total = 0u64;
        for index in list_segment_indices(&self.data_dir).map_err(StorageError::Io)? {
            let path = segment_path(&self.data_dir, index);
            match fs::metadata(&path) {
                Ok(meta) => total += meta.len(),
                // A segment listed and then unlinked by another handle: count
                // nothing rather than fail the trigger.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(StorageError::Io(e)),
            }
        }
        Ok(total)
    }

    /// Replace the physical log with a single empty segment starting at
    /// `snapshot_index + 1`.
    ///
    /// An installed snapshot supersedes the entire log, so every segment —
    /// including ones whose first index lies *above* the snapshot — holds
    /// entries from a diverged branch and must go. Leaving them behind would
    /// also make recovery see a hole (stale entries, then entries re-sent by
    /// the leader after the snapshot), so this is a fresh-start rotation
    /// rather than a prefix trim.
    fn replace_log_with_snapshot(&mut self, snapshot_index: LogIndex) -> Result<(), StorageError> {
        let next = snapshot_index + 1;
        let old = list_segment_indices(&self.data_dir).map_err(StorageError::Io)?;
        // Unlink first, then rotate: the successor segment is named by `next`,
        // which an old segment may already occupy, and POSIX keeps the old
        // active segment's handle valid until it is replaced below.
        for index in &old {
            let path = segment_path(&self.data_dir, *index);
            match fs::remove_file(&path) {
                Ok(()) => {}
                // A missing file is not a failure: the active segment is
                // always present, but the listing races nothing else.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(StorageError::Io(e)),
            }
        }
        self.segment = open_segment_at(&self.data_dir, next, self.segment_bytes)?;
        self.segment_first_index = next;
        self.max_entry_in_segment = 0;
        self.pending_entry_fsync = false;
        if !old.is_empty() {
            fsync_dir(&self.data_dir).map_err(StorageError::Io)?;
        }
        Ok(())
    }

    /// Delete every segment **wholly** below `watermark`.
    ///
    /// A segment is wholly below iff its successor starts at or before
    /// `watermark + 1`; the loop only considers segments that have a successor,
    /// so the active segment is never removed.
    fn delete_segments_below(&self, watermark: LogIndex) -> Result<(), StorageError> {
        let keep_from = watermark + 1;
        let indices = list_segment_indices(&self.data_dir).map_err(StorageError::Io)?;
        let mut removed = 0u32;
        for window in indices.windows(2) {
            let (this, next) = (window[0], window[1]);
            if next <= keep_from && this != self.segment_first_index {
                fs::remove_file(segment_path(&self.data_dir, this)).map_err(StorageError::Io)?;
                removed += 1;
            }
        }
        if removed > 0 {
            fsync_dir(&self.data_dir).map_err(StorageError::Io)?;
        }
        Ok(())
    }

    /// Delete all but the newest [`SNAPSHOT_RETENTION`] snapshot files.
    ///
    /// One older snapshot is kept so that open can fall back when the newest
    /// file is found corrupt. A fallback is only usable if the WAL still
    /// covers the gap; otherwise recovery fails loudly rather than silently
    /// losing entries.
    fn prune_snapshots(&self) -> Result<(), StorageError> {
        let mut candidates: Vec<(LogIndex, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&self.data_dir).map_err(StorageError::Io)? {
            let entry = entry.map_err(StorageError::Io)?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some((index, _term)) = parse_snapshot_file_name(&name) {
                candidates.push((index, entry.path()));
            }
        }
        candidates.sort_by(|a, b| b.0.cmp(&a.0));
        let mut removed = false;
        for (_index, path) in candidates.into_iter().skip(SNAPSHOT_RETENTION) {
            fs::remove_file(&path).map_err(StorageError::Io)?;
            removed = true;
        }
        if removed {
            fsync_dir(&self.data_dir).map_err(StorageError::Io)?;
        }
        Ok(())
    }

}

// ---------------------------------------------------------------------------
// Storage trait impl
// ---------------------------------------------------------------------------

impl Storage for WalStorage {
    fn initial_state(&self) -> Result<RaftState, StorageError> {
        // A snapshot carries the membership it was taken under, so a node that
        // installed one (or restarted after doing so) recovers membership from
        // it. Without a snapshot the durable membership log is still M3 work,
        // and the caller falls back to the bootstrap voter set.
        let conf_state = self
            .snapshot
            .as_ref()
            .map(|s| s.meta.conf_state.clone())
            .unwrap_or_default();
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state,
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
        // Entries at or below the compaction watermark live in the snapshot.
        let first = self.compacted_to + 1;
        let last = self.last_index()?;
        if low < first {
            return Err(StorageError::Compacted);
        }
        if high > last + 1 {
            return Err(StorageError::Compacted);
        }
        let start = (low - first) as usize;
        let end = (high - first) as usize;
        let mut out: Vec<LogEntry> = self.entries[start..end].to_vec();
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
        let first = self.compacted_to + 1;
        if index < first {
            // raft asks for the term at the snapshot index when it has one;
            // anything older is genuinely compacted.
            if let Some(snapshot) = &self.snapshot
                && index == snapshot.meta.index
            {
                return Ok(snapshot.meta.term);
            }
            return Err(StorageError::Compacted);
        }
        let last = self.last_index()?;
        if index > last {
            return Err(StorageError::Compacted);
        }
        Ok(self.entries[(index - first) as usize].term)
    }

    fn first_index(&self) -> Result<LogIndex, StorageError> {
        Ok(self.compacted_to + 1)
    }

    fn last_index(&self) -> Result<LogIndex, StorageError> {
        Ok(self
            .entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.compacted_to))
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        Ok(self.snapshot.clone())
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        // Overwrite semantics. raft can hand the storage a **conflicting
        // suffix** after an election: a follower that persisted a short-lived
        // leader's entry must replace it with the new leader's entry. The seam
        // has no separate truncate call (compaction is M2), so `append` must
        // truncate to the first incoming index before appending — exactly what
        // `MemStorage` does, and what keeps the log contiguous.
        if let Some(first) = entries.first() {
            // `last_index` — not `entries.last()` — because the log's tail is
            // `compacted_to` once every retained entry has been dropped by a
            // snapshot install or a compaction through the last index.
            let last = self.last_index()?;
            if first.index <= last {
                let keep = first.index - 1;
                // An entry at or below the watermark has been compacted away:
                // raft must not re-send it, and re-appending it would punch a
                // hole in the log.
                if keep < self.compacted_to {
                    return Err(StorageError::Unrecoverable {
                        detail: format!(
                            "append at index {} is at or below the compaction \
                             watermark {}",
                            first.index, self.compacted_to
                        ),
                    });
                }
                // A committed entry is never overwritten by a correct leader;
                // truncating one would be a safety violation, so fail-stop
                // rather than silently discard committed data.
                if keep < self.hard_state.commit {
                    return Err(StorageError::Unrecoverable {
                        detail: format!(
                            "append would overwrite committed entries: first={}, commit={}",
                            first.index, self.hard_state.commit
                        ),
                    });
                }
                self.truncate_log_to(keep)?;
            }
        }
        for entry in entries {
            // Continuity hardening: the next entry must be `last_index + 1`.
            // A gap would silently turn the durable log into a log with a
            // hole, so this is a hard error, not a debug assertion.
            let last = self.last_index()?;
            if entry.index != last + 1 {
                return Err(StorageError::Unrecoverable {
                    detail: format!(
                        "append continuity violation: expected {}, got {}",
                        last + 1,
                        entry.index
                    ),
                });
            }

            // Reject records larger than MAX_RECORD_BYTES.
            let payload = encode_entry(
                entry.index,
                entry.term,
                seam_entry_type_to_u8(entry.entry_type),
                &entry.data,
            );
            let framed_size = 8 + 1 + payload.len(); // header + type + payload
            if framed_size as u64 > MAX_RECORD_BYTES as u64 {
                return Err(StorageError::Unrecoverable {
                    detail: format!(
                        "entry at index {} produces a record of {framed_size} bytes, \
                         exceeding MAX_RECORD_BYTES ({MAX_RECORD_BYTES})",
                        entry.index
                    ),
                });
            }

            // Roll over if the current segment is full.
            self.maybe_rollover(entry.index)?;

            // Write the entry record to the active segment.
            self.segment
                .append_record(RecordType::Entry, &payload)
                .map_err(segment_err_to_storage_err)?;

            // Update the in-memory index and per-segment tracking.
            self.entries.push(entry.clone());
            self.max_entry_in_segment = entry.index;

            // Mark unsynced *per record* so that a mid-batch rollover
            // (checked at the top of the next iteration) fsyncs the outgoing
            // segment (N2). An empty batch leaves the flag unchanged.
            self.pending_entry_fsync = true;
        }
        Ok(())
    }

    fn set_hard_state(&mut self, hs: &HardState) -> Result<(), StorageError> {
        // Write the HardState record to the active segment.
        let payload = encode_hard_state(hs.term, hs.vote, hs.commit);
        self.segment
            .append_record(RecordType::HardState, &payload)
            .map_err(segment_err_to_storage_err)?;

        // I1: ALWAYS fsync HardState before returning.
        self.segment
            .sync()
            .map_err(StorageError::Io)?;
        self.stats.hard_state_fsyncs += 1;
        self.notify_fsynced();

        self.hard_state = hs.clone();
        Ok(())
    }

    fn sync_entries(&mut self) -> Result<(), StorageError> {
        match self.fsync_policy {
            FsyncPolicy::Always => {
                self.segment
                    .sync()
                    .map_err(StorageError::Io)?;
                self.stats.entry_fsyncs += 1;
                self.stats.entry_sync_batches += 1;
                self.pending_entry_fsync = false;
                self.notify_fsynced();
            }
            FsyncPolicy::BatchMs(_) => {
                // sync_entries is the synchronous durability barrier: it
                // returns only after pending entries are fsynced. The `ms`
                // parameter is advisory (P4 timer-driven pre-flush).
                if self.pending_entry_fsync {
                    self.segment
                        .sync()
                        .map_err(StorageError::Io)?;
                    self.stats.entry_fsyncs += 1;
                    self.stats.entry_sync_batches += 1;
                    self.pending_entry_fsync = false;
                    self.notify_fsynced();
                }
            }
        }
        Ok(())
    }

    /// Persist `snapshot` and point META at it.
    ///
    /// File protocol: write `*.snap.tmp` → fsync → rename to `*.snap` → fsync
    /// the data dir; then rewrite META (its own atomic protocol) with the new
    /// snapshot pointer. The directory scan at open is the authority, so a
    /// crash between the two steps leaves a usable, if unadvertised, snapshot.
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        if let Some(existing) = &self.snapshot
            && existing.meta.index >= snapshot.meta.index
        {
            // Idempotent, and never move the snapshot backwards.
            return Ok(());
        }

        let name = snapshot_file_name(snapshot.meta.index, snapshot.meta.term);
        let final_path = self.data_dir.join(&name);
        let tmp_path = self.data_dir.join(format!("{name}.tmp"));
        let blob = encode_snapshot(snapshot);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(StorageError::Io)?;
        file.write_all(&blob).map_err(StorageError::Io)?;
        // I3: the snapshot bytes must be durable before the rename makes the
        // file visible, and before META ever points at it.
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        fs::rename(&tmp_path, &final_path).map_err(StorageError::Io)?;
        fsync_dir(&self.data_dir).map_err(StorageError::Io)?;

        let mut meta = self.meta.clone();
        meta.snapshot_index = snapshot.meta.index;
        meta.snapshot_term = snapshot.meta.term;
        write_meta(&self.data_dir, &meta).map_err(|e| StorageError::Unrecoverable {
            detail: format!("failed to persist the snapshot pointer in META: {e}"),
        })?;
        self.meta = meta;
        self.snapshot = Some(snapshot.clone());

        self.prune_snapshots()?;
        Ok(())
    }

    /// Adopt an installed snapshot and reset the log to it.
    ///
    /// See [`Storage::install_snapshot`]: the snapshot is authoritative, so
    /// every locally retained entry is dropped. Segments below the new
    /// watermark are reclaimed as well.
    fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        if snapshot.meta.index < self.compacted_to {
            return Err(StorageError::Unrecoverable {
                detail: format!(
                    "installed snapshot at {} is older than the local watermark {}",
                    snapshot.meta.index, self.compacted_to
                ),
            });
        }
        // Already exactly here (raft can hand the same snapshot twice): the
        // log is empty and the watermark matches, so there is nothing to do.
        if snapshot.meta.index == self.compacted_to && self.entries.is_empty() {
            return Ok(());
        }

        // Persist (fsync, rename, META pointer) before touching the log: a
        // crash in between must leave the store readable, and a repeated
        // install of the same snapshot is a no-op.
        self.save_snapshot(snapshot)?;

        self.entries.clear();
        self.compacted_to = snapshot.meta.index;
        self.replace_log_with_snapshot(snapshot.meta.index)?;
        Ok(())
    }

    fn compact(&mut self, compact_to: LogIndex) -> Result<(), StorageError> {
        if compact_to <= self.compacted_to {
            return Ok(()); // already compacted at least this far
        }
        let last = self.last_index()?;
        if compact_to > last {
            return Err(StorageError::Unrecoverable {
                detail: format!("compact_to ({compact_to}) > last_index ({last})"),
            });
        }
        // Never drop entries nothing covers: a snapshot at or beyond the
        // watermark must already be durable (propsol §5.5.4: snapshot first,
        // then compact).
        match &self.snapshot {
            Some(snapshot) if snapshot.meta.index >= compact_to => {}
            Some(snapshot) => {
                return Err(StorageError::Unrecoverable {
                    detail: format!(
                        "compact_to ({compact_to}) is beyond the snapshot index ({})",
                        snapshot.meta.index
                    ),
                });
            }
            None => {
                return Err(StorageError::Unrecoverable {
                    detail: "compact called with no durable snapshot".into(),
                });
            }
        }

        self.delete_segments_below(compact_to)?;

        // The snapshot covers everything at or below the watermark.
        self.entries.retain(|e| e.index > compact_to);
        self.compacted_to = compact_to;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// List all segment file indices in a data directory, sorted ascending.
fn list_segment_indices(dir: &Path) -> std::io::Result<Vec<u64>> {
    let mut indices: Vec<u64> = Vec::new();
    let read_dir = fs::read_dir(dir)?;
    for entry in read_dir.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Ok(idx) = parse_segment_name(&name_str) {
            indices.push(idx);
        }
    }
    indices.sort();
    Ok(indices)
}

/// Open or create a segment file at the given first index.
fn open_segment_at(
    dir: &Path,
    first_index: u64,
    max_bytes: u64,
) -> Result<Segment, StorageError> {
    Segment::open_with_max_bytes(dir, first_index, max_bytes)
        .map_err(segment_err_to_storage_err)
}

/// Convert a [`SegmentError`](crate::storage::segment::SegmentError) to a
/// [`StorageError`].
fn segment_err_to_storage_err(e: crate::storage::segment::SegmentError) -> StorageError {
    match e {
        crate::storage::segment::SegmentError::Io(io_err) => StorageError::Io(io_err),
        other => StorageError::Unrecoverable {
            detail: format!("segment error: {other}"),
        },
    }
}

/// Truncate a file to the given byte offset.
fn truncate_file_at(path: &Path, offset: u64) -> Result<(), StorageError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(StorageError::Io)?;
    file.set_len(offset).map_err(StorageError::Io)?;
    Ok(())
}

/// Convert a `u8` entry type (from the wire format) to the seam `EntryType`.
fn entry_type_to_seam(ety: u8) -> arachne_seam::storage::EntryType {
    match ety {
        0 => arachne_seam::storage::EntryType::Entry,
        1 => arachne_seam::storage::EntryType::ConfChange,
        2 => arachne_seam::storage::EntryType::ConfChangeV2,
        _ => arachne_seam::storage::EntryType::Entry,
    }
}

/// Convert a seam `EntryType` to its `u8` wire representation.
fn seam_entry_type_to_u8(ety: arachne_seam::storage::EntryType) -> u8 {
    match ety {
        arachne_seam::storage::EntryType::Entry => 0,
        arachne_seam::storage::EntryType::ConfChange => 1,
        arachne_seam::storage::EntryType::ConfChangeV2 => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::segment_name;
    use arachne_seam::storage::{ConfState, SnapshotMeta};
    use std::fs;

    /// Create a unique temp directory for a test.
    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "arachne-wal-test-{}-{}",
            std::process::id(),
            n
        ));
        fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    fn test_opts() -> WalOptions {
        WalOptions {
            cluster_id: "test-cluster".into(),
            node_id: "node-1".into(),
            config: WalConfig::default(),
            created_at_millis: 1_700_000_000_000,
            fsync_observer: None,
        }
    }

    fn test_opts_with_policy(policy: FsyncPolicy) -> WalOptions {
        WalOptions {
            cluster_id: "test-cluster".into(),
            node_id: "node-1".into(),
            config: WalConfig {
                fsync_policy: policy,
                ..WalConfig::default()
            },
            created_at_millis: 1_700_000_000_000,
            fsync_observer: None,
        }
    }

    fn test_opts_with_segment_bytes(bytes: u64) -> WalOptions {
        WalOptions {
            cluster_id: "test-cluster".into(),
            node_id: "node-1".into(),
            config: WalConfig {
                segment_bytes: bytes,
                ..WalConfig::default()
            },
            created_at_millis: 1_700_000_000_000,
            fsync_observer: None,
        }
    }

    fn make_entry(index: u64, term: u64, data: &[u8]) -> LogEntry {
        LogEntry {
            index,
            term,
            entry_type: arachne_seam::storage::EntryType::Entry,
            data: data.to_vec(),
        }
    }

    // ---- Basic open / recovery ----

    #[test]
    fn open_empty_dir_creates_meta() {
        let dir = temp_dir();
        let opts = test_opts();
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert!(dir.join("META").exists());
        assert!(dir.join("LOCK").exists());
        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_recovers_empty_log() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let _storage = WalStorage::open(&dir, opts.clone()).unwrap();
        }
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_sync_reopen_entries_survive() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage
                .append(&[
                    make_entry(1, 1, b"hello"),
                    make_entry(2, 1, b"world"),
                ])
                .unwrap();
            storage.sync_entries().unwrap();
            storage
                .set_hard_state(&HardState {
                    term: 1,
                    vote: Some(1),
                    commit: 2,
                })
                .unwrap();
        }
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 2);
        let entries = storage.entries(1, 3, None).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].data, b"hello");
        assert_eq!(entries[1].data, b"world");
        let state = storage.initial_state().unwrap();
        assert_eq!(state.hard_state.term, 1);
        assert_eq!(state.hard_state.vote, Some(1));
        assert_eq!(state.hard_state.commit, 2);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- I1: HardState fsync counting ----

    #[test]
    fn i1_hard_state_fsync_always_policy() {
        let dir = temp_dir();
        let opts = test_opts_with_policy(FsyncPolicy::Always);
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.stats().hard_state_fsyncs, 0);
        storage
            .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 0 })
            .unwrap();
        assert_eq!(storage.stats().hard_state_fsyncs, 1);
        storage
            .set_hard_state(&HardState { term: 2, vote: Some(2), commit: 1 })
            .unwrap();
        assert_eq!(storage.stats().hard_state_fsyncs, 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn i1_hard_state_fsync_batch_policy() {
        let dir = temp_dir();
        let opts = test_opts_with_policy(FsyncPolicy::BatchMs(100));
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage
            .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 0 })
            .unwrap();
        assert_eq!(storage.stats().hard_state_fsyncs, 1);
        storage
            .set_hard_state(&HardState { term: 2, vote: Some(2), commit: 1 })
            .unwrap();
        assert_eq!(storage.stats().hard_state_fsyncs, 2);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- FsyncPolicy: Always ----

    #[test]
    fn always_policy_each_sync_fsyncs() {
        let dir = temp_dir();
        let opts = test_opts_with_policy(FsyncPolicy::Always);
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage.append(&[make_entry(1, 1, b"a")]).unwrap();
        storage.sync_entries().unwrap();
        assert_eq!(storage.stats().entry_fsyncs, 1);
        assert_eq!(storage.stats().entry_sync_batches, 1);
        storage.append(&[make_entry(2, 1, b"b")]).unwrap();
        storage.sync_entries().unwrap();
        assert_eq!(storage.stats().entry_fsyncs, 2);
        assert_eq!(storage.stats().entry_sync_batches, 2);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- FsyncPolicy: BatchMs ----

    #[test]
    fn batch_ms_coalesces_fsyncs() {
        let dir = temp_dir();
        let opts = test_opts_with_policy(FsyncPolicy::BatchMs(100));
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage.append(&[make_entry(1, 1, b"a")]).unwrap();
        storage.append(&[make_entry(2, 1, b"b")]).unwrap();
        storage.append(&[make_entry(3, 1, b"c")]).unwrap();
        storage.sync_entries().unwrap();
        assert_eq!(storage.stats().entry_fsyncs, 1);
        assert_eq!(storage.stats().entry_sync_batches, 1);
        storage.sync_entries().unwrap();
        assert_eq!(storage.stats().entry_fsyncs, 1);
        assert_eq!(storage.stats().entry_sync_batches, 1);
        storage.append(&[make_entry(4, 1, b"d")]).unwrap();
        storage.sync_entries().unwrap();
        assert_eq!(storage.stats().entry_fsyncs, 2);
        assert_eq!(storage.stats().entry_sync_batches, 2);
        storage
            .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 4 })
            .unwrap();
        assert_eq!(storage.stats().hard_state_fsyncs, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Recovery: structural tear auto-truncation ----

    #[test]
    fn recovery_truncates_structural_tear_beyond_commit() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage
                .append(&[
                    make_entry(1, 1, b"committed"),
                    make_entry(2, 1, b"uncommitted"),
                    make_entry(3, 1, b"also-uncommitted"),
                ])
                .unwrap();
            storage.sync_entries().unwrap();
            storage
                .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 1 })
                .unwrap();
        }

        // Construct a structural tear: truncate the file mid-record at the
        // end (beyond commit+1=2, so auto-truncate is allowed).
        let seg_path = dir.join(segment_name(1));
        let buf = fs::read(&seg_path).unwrap();
        // Truncate to leave a partial record at the end (e.g., cut off the
        // last few bytes of the final record).
        let truncated_len = buf.len() - 3;
        let mut truncated = buf;
        truncated.truncate(truncated_len);
        fs::write(&seg_path, &truncated).unwrap();

        let storage = WalStorage::open(&dir, opts).unwrap();
        // Entries 1, 2, 3 should be intact (they were fully present).
        assert_eq!(storage.last_index().unwrap(), 3);
        assert_eq!(storage.stats().wal_truncated_records, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Recovery: fail-start on fully-present corrupt record ----

    #[test]
    fn recovery_failstart_corrupt_hardstate() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage.append(&[make_entry(1, 1, b"x")]).unwrap();
            storage.sync_entries().unwrap();
            storage
                .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 1 })
                .unwrap();
        }

        // Corrupt a byte in the HardState record payload (fully present on
        // disk, CRC will fail) → unconditional fail-start.
        let seg_path = dir.join(segment_name(1));
        let mut buf = fs::read(&seg_path).unwrap();
        let idx = buf.len() - 5;
        buf[idx] ^= 0xFF;
        fs::write(&seg_path, &buf).unwrap();

        match WalStorage::open(&dir, opts) {
            Err(StorageError::Unrecoverable { .. }) => {}
            Err(e) => panic!("expected Unrecoverable, got: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Recovery: fail-start on flipped type byte ----

    #[test]
    fn recovery_failstart_flipped_type_byte() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage.append(&[make_entry(1, 1, b"x")]).unwrap();
            storage.sync_entries().unwrap();
            storage
                .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 1 })
                .unwrap();
        }

        // Flip the type byte of the HardState record (0x02 → 0x01). The
        // record is fully present on disk; the CRC will still fail (since
        // the body changed). This must be fail-start, not auto-truncate.
        let seg_path = dir.join(segment_name(1));
        let mut buf = fs::read(&seg_path).unwrap();
        // The HardState record is the last record. Its type byte is at
        // position (record_start + 8). Find it by scanning from the end.
        // The HardState payload is 25 bytes, so the record is 8+1+25=34 bytes.
        let hs_record_start = buf.len() - 34;
        let type_byte_pos = hs_record_start + 8;
        buf[type_byte_pos] = 0x01; // flip 0x02 → 0x01
        fs::write(&seg_path, &buf).unwrap();

        match WalStorage::open(&dir, opts) {
            Err(StorageError::Unrecoverable { .. }) => {}
            Err(e) => panic!("expected Unrecoverable, got: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Recovery: fail-start on corrupt len in committed window ----

    #[test]
    fn recovery_failstart_corrupt_len_committed_tail() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage
                .append(&[
                    make_entry(1, 1, b"a"),
                    make_entry(2, 1, b"b"),
                ])
                .unwrap();
            storage.sync_entries().unwrap();
            storage
                .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 2 })
                .unwrap();
        }

        // Corrupt the `len` field of the last Entry record (index 2, which
        // is at commit). Make the len field absurdly large so the record
        // appears to extend past EOF → but since it's a fully-present
        // record with wrong len, decode_record will fail with BadLength.
        // Actually, let's corrupt the len to a value that makes record_end
        // <= file_len but wrong (e.g., len+1). This makes it a fully-present
        // record with wrong CRC → fail-start.
        let seg_path = dir.join(segment_name(1));
        let mut buf = fs::read(&seg_path).unwrap();
        // The Entry records are at the beginning. The second entry record
        // starts after the first. Let's find it:
        // Entry record: [u32 len][u32 crc][u8 type][u64 index][u64 term][u8 etype][data]
        // For "a": len = 1 + 17 + 1 = 19, total = 8 + 19 = 27
        // For "b": starts at offset 27
        let second_entry_offset = 27;
        // Corrupt the len field (first 4 bytes of the second record).
        let original_len = u32::from_le_bytes([
            buf[second_entry_offset],
            buf[second_entry_offset + 1],
            buf[second_entry_offset + 2],
            buf[second_entry_offset + 3],
        ]);
        // Set len to original_len + 1 (wrong but plausible, record still
        // fits in file if there's enough room, or it'll be a tear).
        // To ensure it's fully present, make it original_len - 1 (too short
        // for the body) → decode_record will fail with ChecksumMismatch
        // or the body won't match.
        let bad_len = original_len.saturating_sub(1);
        buf[second_entry_offset..second_entry_offset + 4]
            .copy_from_slice(&bad_len.to_le_bytes());
        fs::write(&seg_path, &buf).unwrap();

        // This is a fully-present record with a wrong len → decode fails →
        // fail-start (commit=2, entry 2 is at commit).
        match WalStorage::open(&dir, opts) {
            Err(StorageError::Unrecoverable { .. }) => {}
            Err(e) => panic!("expected Unrecoverable, got: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Recovery: fail-start on index gap ----

    #[test]
    fn recovery_failstart_index_gap() {
        let dir = temp_dir();
        let opts = test_opts();
        // Create a valid storage, then overwrite the segment file with
        // entries that have a gap (index 1, then index 3 — skipping 2).
        // This simulates a state that could exist from an older version
        // or corruption.
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage.append(&[make_entry(1, 1, b"a")]).unwrap();
            storage.sync_entries().unwrap();
        }
        // Now rewrite the segment file with entry 1 + entry 3 (gap at 2).
        let seg_path = dir.join(segment_name(1));
        let mut buf = Vec::new();
        // Entry 1: index=1, term=1, type=0, data="a"
        let payload1 = crate::storage::format::encode_entry(1, 1, 0, b"a");
        buf.extend_from_slice(&crate::storage::format::encode_record(
            crate::storage::format::RecordType::Entry,
            &payload1,
        ));
        // Entry 3 (gap!): index=3, term=1, type=0, data="c"
        let payload3 = crate::storage::format::encode_entry(3, 1, 0, b"c");
        buf.extend_from_slice(&crate::storage::format::encode_record(
            crate::storage::format::RecordType::Entry,
            &payload3,
        ));
        fs::write(&seg_path, &buf).unwrap();

        match WalStorage::open(&dir, opts) {
            Err(StorageError::Unrecoverable { .. }) => {}
            Err(e) => panic!("expected Unrecoverable for index gap, got: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Data-dir lock ----

    #[test]
    fn second_open_on_locked_dir_fails() {
        let dir = temp_dir();
        let opts = test_opts();
        let _first = WalStorage::open(&dir, opts.clone()).unwrap();
        match WalStorage::open(&dir, opts) {
            Err(StorageError::Unrecoverable { detail }) => {
                assert!(detail.contains("locked"));
            }
            Err(e) => panic!("expected Unrecoverable, got: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- META validation ----

    #[test]
    fn meta_mismatch_fails() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let _storage = WalStorage::open(&dir, opts).unwrap();
        }
        let bad_opts = WalOptions {
            cluster_id: "wrong-cluster".into(),
            node_id: "node-1".into(),
            config: WalConfig::default(),
            created_at_millis: 1_700_000_000_000,
            fsync_observer: None,
        };
        let result = WalStorage::open(&dir, bad_opts);
        assert!(result.is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Multi-segment rollover ----

    #[test]
    fn multi_segment_rollover_and_recovery() {
        let dir = temp_dir();
        // Use a very small segment size to force rollover.
        // Each entry record is ~36 bytes (8 header + 1 type + 17 entry + data).
        // Set segment_bytes to ~80 bytes so 2 entries fit per segment.
        let opts = test_opts_with_segment_bytes(80);
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            // Append 6 entries with small data.
            for i in 1..=6u64 {
                storage
                    .append(&[make_entry(i, 1, &[b'x'; 4])])
                    .unwrap();
            }
            storage.sync_entries().unwrap();
            storage
                .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 6 })
                .unwrap();
        }
        // Verify multiple segments were created.
        let segs: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("wal-"))
            .collect();
        assert!(segs.len() >= 2, "expected >= 2 segments, got {}", segs.len());

        // Reopen: all entries should recover.
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.last_index().unwrap(), 6);
        let entries = storage.entries(1, 7, None).unwrap();
        assert_eq!(entries.len(), 6);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[5].index, 6);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Reopen continues existing segment ----

    #[test]
    fn reopen_continues_existing_segment() {
        let dir = temp_dir();
        let opts = test_opts();
        let num_segments_before;
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage.append(&[make_entry(1, 1, b"hello")]).unwrap();
            storage.sync_entries().unwrap();
            num_segments_before = count_segments(&dir);
        }
        // Reopen and append: should continue the existing segment, not
        // create a new one.
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage.append(&[make_entry(2, 1, b"world")]).unwrap();
            storage.sync_entries().unwrap();
        }
        let num_segments_after = count_segments(&dir);
        assert_eq!(
            num_segments_before, num_segments_after,
            "reopen should not create a new empty segment"
        );
        // Verify the entry is in the same segment.
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.last_index().unwrap(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    fn count_segments(dir: &Path) -> u64 {
        fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("wal-"))
            .count() as u64
    }

    // ---- compact / snapshot ----

    #[test]
    fn compact_returns_unrecoverable() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        assert!(matches!(storage.compact(1), Err(StorageError::Unrecoverable { .. })));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_returns_none() {
        let dir = temp_dir();
        let opts = test_opts();
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.snapshot().unwrap(), None);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- snapshots and compaction (propsol v0.2.10 §M) ----

    fn make_snapshot(index: LogIndex, term: Term, data: &[u8]) -> Snapshot {
        Snapshot {
            meta: SnapshotMeta {
                index,
                term,
                conf_state: ConfState {
                    voters: vec![1, 2, 3],
                    learners: vec![],
                },
            },
            data: data.to_vec(),
        }
    }

    /// Snapshot files present in `dir`, sorted by index.
    fn snapshot_files(dir: &Path) -> Vec<LogIndex> {
        let mut found: Vec<LogIndex> = fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter_map(|e| parse_snapshot_file_name(&e.file_name().to_string_lossy()))
            .map(|(index, _term)| index)
            .collect();
        found.sort();
        found
    }

    #[test]
    fn save_snapshot_survives_reopen_and_raises_the_watermark() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage
                .append(&[
                    make_entry(1, 1, b"a"),
                    make_entry(2, 1, b"b"),
                    make_entry(3, 1, b"c"),
                    make_entry(4, 1, b"d"),
                    make_entry(5, 1, b"e"),
                ])
                .unwrap();
            storage.sync_entries().unwrap();
            storage
                .save_snapshot(&make_snapshot(3, 1, b"state@3"))
                .unwrap();

            assert_eq!(snapshot_files(&dir), vec![3]);
            assert_eq!(storage.snapshot().unwrap().unwrap().data, b"state@3");
        }

        // Reopen: the snapshot is loaded and is authoritative for everything
        // at or below its index, so the log's window starts above it.
        let storage = WalStorage::open(&dir, opts).unwrap();
        let snapshot = storage.snapshot().unwrap().expect("snapshot survives reopen");
        assert_eq!(snapshot.meta.index, 3);
        assert_eq!(snapshot.data, b"state@3");
        assert_eq!(storage.first_index().unwrap(), 4);
        assert_eq!(storage.last_index().unwrap(), 5);
        assert_eq!(storage.term(3).unwrap(), 1);
        assert!(matches!(storage.term(2), Err(StorageError::Compacted)));
        assert!(matches!(
            storage.entries(1, 4, None),
            Err(StorageError::Compacted)
        ));
        let replayed = storage.entries(4, 6, None).unwrap();
        assert_eq!(
            replayed.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![4, 5]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_drops_entries_and_answers_term_at_the_snapshot_index() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage
            .append(&[
                make_entry(1, 1, b"a"),
                make_entry(2, 1, b"b"),
                make_entry(3, 1, b"c"),
                make_entry(4, 1, b"d"),
            ])
            .unwrap();
        storage.sync_entries().unwrap();
        storage
            .save_snapshot(&make_snapshot(3, 1, b"state@3"))
            .unwrap();
        storage.compact(3).unwrap();

        assert_eq!(storage.first_index().unwrap(), 4);
        assert_eq!(storage.last_index().unwrap(), 4);
        // raft must be able to ask for the term at the snapshot index.
        assert_eq!(storage.term(3).unwrap(), 1);
        assert!(matches!(
            storage.term(2),
            Err(StorageError::Compacted)
        ));
        assert!(matches!(
            storage.entries(3, 5, None),
            Err(StorageError::Compacted)
        ));
        let kept = storage.entries(4, 5, None).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].index, 4);

        // Idempotent: compacting to or below the watermark is a no-op.
        storage.compact(3).unwrap();
        storage.compact(1).unwrap();
        assert_eq!(storage.first_index().unwrap(), 4);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_after_restart_uses_the_reloaded_snapshot() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage
                .append(&[
                    make_entry(1, 1, b"a"),
                    make_entry(2, 1, b"b"),
                    make_entry(3, 1, b"c"),
                ])
                .unwrap();
            storage.sync_entries().unwrap();
            storage
                .save_snapshot(&make_snapshot(2, 1, b"state@2"))
                .unwrap();
        }
        let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
        storage.compact(2).unwrap();
        assert_eq!(storage.first_index().unwrap(), 3);
        assert_eq!(storage.term(2).unwrap(), 1);
        drop(storage);

        // The watermark is durable: a second reopen sees the compacted view.
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.first_index().unwrap(), 3);
        assert_eq!(storage.last_index().unwrap(), 3);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_requires_a_covering_snapshot() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage
            .append(&[make_entry(1, 1, b"a"), make_entry(2, 1, b"b")])
            .unwrap();
        storage.sync_entries().unwrap();

        // No snapshot at all.
        assert!(matches!(
            storage.compact(1),
            Err(StorageError::Unrecoverable { .. })
        ));
        // A snapshot that does not cover the watermark.
        storage
            .save_snapshot(&make_snapshot(1, 1, b"state@1"))
            .unwrap();
        assert!(matches!(
            storage.compact(2),
            Err(StorageError::Unrecoverable { .. })
        ));
        // Beyond the end of the log.
        assert!(matches!(
            storage.compact(9),
            Err(StorageError::Unrecoverable { .. })
        ));
        assert_eq!(storage.first_index().unwrap(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_snapshot_never_moves_backwards() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage
            .append(&[
                make_entry(1, 1, b"a"),
                make_entry(2, 1, b"b"),
                make_entry(3, 1, b"c"),
            ])
            .unwrap();
        storage.sync_entries().unwrap();
        storage
            .save_snapshot(&make_snapshot(2, 1, b"state@2"))
            .unwrap();
        storage
            .save_snapshot(&make_snapshot(1, 1, b"state@1"))
            .unwrap();

        let snapshot = storage.snapshot().unwrap().unwrap();
        assert_eq!(snapshot.meta.index, 2);
        assert_eq!(snapshot.data, b"state@2");
        assert_eq!(snapshot_files(&dir), vec![2]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_deletes_only_segments_wholly_below_the_watermark() {
        let dir = temp_dir();
        // A tiny segment budget forces one segment per entry.
        let opts = test_opts_with_segment_bytes(1);
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        for index in 1..=6 {
            storage.append(&[make_entry(index, 1, b"x")]).unwrap();
            storage.sync_entries().unwrap();
        }
        let before = list_segment_indices(&dir).unwrap();
        assert!(before.len() > 2, "expected several segments, got {before:?}");

        storage
            .save_snapshot(&make_snapshot(4, 1, b"state@4"))
            .unwrap();
        storage.compact(4).unwrap();

        // Only segments that begin at or before 4 are covered by the snapshot;
        // later segments must survive.
        let after = list_segment_indices(&dir).unwrap();
        assert_eq!(after, vec![5, 6]);
        assert!(before.len() > after.len());
        assert_eq!(storage.last_index().unwrap(), 6);
        let kept = storage.entries(5, 7, None).unwrap();
        assert_eq!(kept.len(), 2);

        // And the trimmed segments are not resurrected by a reopen.
        drop(storage);
        let storage = WalStorage::open(&dir, test_opts_with_segment_bytes(1)).unwrap();
        assert_eq!(storage.first_index().unwrap(), 5);
        assert_eq!(storage.last_index().unwrap(), 6);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_snapshot_prunes_all_but_the_newest_two() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage
            .append(&[
                make_entry(1, 1, b"a"),
                make_entry(2, 1, b"b"),
                make_entry(3, 1, b"c"),
                make_entry(4, 1, b"d"),
                make_entry(5, 1, b"e"),
            ])
            .unwrap();
        storage.sync_entries().unwrap();
        for index in [2, 3, 4, 5] {
            storage
                .save_snapshot(&make_snapshot(index, 1, b"state"))
                .unwrap();
        }
        // Newest two survive; superseded files are gone.
        assert_eq!(snapshot_files(&dir), vec![4, 5]);
        assert_eq!(storage.snapshot().unwrap().unwrap().meta.index, 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn install_snapshot_replaces_the_local_log() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
        storage
            .append(&[
                make_entry(1, 1, b"a"),
                make_entry(2, 1, b"b"),
                make_entry(3, 1, b"c"),
            ])
            .unwrap();
        storage.sync_entries().unwrap();

        // The leader is far ahead and sends a snapshot past everything local.
        let installed = make_snapshot(8, 3, b"state@8");
        storage.install_snapshot(&installed).unwrap();

        assert_eq!(storage.last_index().unwrap(), 8);
        assert_eq!(storage.first_index().unwrap(), 9);
        assert_eq!(storage.snapshot().unwrap().unwrap().meta.index, 8);
        assert!(matches!(
            storage.entries(1, 9, None),
            Err(StorageError::Compacted)
        ));

        // Membership travels with the snapshot, so a node that installs one
        // recovers the configuration from it rather than from the bootstrap
        // voter set.
        let state = storage.initial_state().unwrap();
        assert_eq!(state.conf_state.voters, vec![1, 2, 3]);

        // The leader resumes replication directly after the snapshot.
        storage
            .append(&[make_entry(9, 3, b"d")])
            .unwrap();
        storage.sync_entries().unwrap();
        assert_eq!(storage.last_index().unwrap(), 9);
        drop(storage);

        // Restart: the installed snapshot and the resumed tail both survive.
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.first_index().unwrap(), 9);
        assert_eq!(storage.last_index().unwrap(), 9);
        assert_eq!(storage.term(8).unwrap(), 3);
        let replayed = storage.entries(9, 10, None).unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].data, b"d");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn install_snapshot_discards_a_diverged_tail() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        // A local branch that was never committed anywhere.
        storage
            .append(&[
                make_entry(1, 1, b"a"),
                make_entry(2, 1, b"b"),
                make_entry(3, 2, b"local-1"),
                make_entry(4, 2, b"local-2"),
                make_entry(5, 2, b"local-3"),
            ])
            .unwrap();
        storage.sync_entries().unwrap();

        storage
            .install_snapshot(&make_snapshot(4, 3, b"state@4"))
            .unwrap();

        // Nothing of the local branch survives, not even above the snapshot:
        // raft re-sends what it still needs.
        assert_eq!(storage.last_index().unwrap(), 4);
        assert_eq!(storage.first_index().unwrap(), 5);
        assert!(storage.entries(3, 6, None).is_err());
        // and the store is immediately usable for the leader's next append
        storage.append(&[make_entry(5, 3, b"remote")]).unwrap();
        storage.sync_entries().unwrap();
        assert_eq!(storage.term(5).unwrap(), 3);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn install_snapshot_rejects_a_stale_snapshot() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage
            .install_snapshot(&make_snapshot(6, 2, b"state@6"))
            .unwrap();
        assert!(matches!(
            storage.install_snapshot(&make_snapshot(5, 2, b"state@5")),
            Err(StorageError::Unrecoverable { .. })
        ));
        assert_eq!(storage.last_index().unwrap(), 6);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_newest_snapshot_falls_back_to_the_previous_one() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            storage
                .append(&[
                    make_entry(1, 1, b"a"),
                    make_entry(2, 1, b"b"),
                    make_entry(3, 1, b"c"),
                ])
                .unwrap();
            storage.sync_entries().unwrap();
            storage
                .save_snapshot(&make_snapshot(1, 1, b"state@1"))
                .unwrap();
            storage
                .save_snapshot(&make_snapshot(3, 1, b"state@3"))
                .unwrap();
        }
        let newest = dir.join(snapshot_file_name(3, 1));
        let mut bytes = fs::read(&newest).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF; // corrupt the trailing CRC
        fs::write(&newest, &bytes).unwrap();

        let mut storage = WalStorage::open(&dir, opts).unwrap();
        let snapshot = storage.snapshot().unwrap().expect("falls back");
        assert_eq!(snapshot.meta.index, 1);
        assert_eq!(snapshot.data, b"state@1");
        // META still points at the corrupt file, so the watermark must come
        // from the snapshot actually loaded.
        storage.compact(1).unwrap();
        assert_eq!(storage.first_index().unwrap(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- entries/term bounds ----

    #[test]
    fn entries_bounds_semantics() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage
            .append(&[
                make_entry(1, 1, b"a"),
                make_entry(2, 1, b"b"),
                make_entry(3, 1, b"c"),
            ])
            .unwrap();
        storage.sync_entries().unwrap();
        assert_eq!(storage.entries(1, 4, None).unwrap().len(), 3);
        assert_eq!(storage.entries(2, 4, None).unwrap().len(), 2);
        assert!(storage.entries(2, 2, None).unwrap().is_empty());
        assert!(matches!(storage.entries(0, 2, None), Err(StorageError::Compacted)));
        assert!(matches!(storage.entries(1, 5, None), Err(StorageError::Compacted)));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn term_bounds_semantics() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        storage
            .append(&[make_entry(1, 7, b"a"), make_entry(2, 9, b"b")])
            .unwrap();
        storage.sync_entries().unwrap();
        assert_eq!(storage.term(1).unwrap(), 7);
        assert_eq!(storage.term(2).unwrap(), 9);
        assert!(matches!(storage.term(0), Err(StorageError::Compacted)));
        assert!(matches!(storage.term(3), Err(StorageError::Compacted)));
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- dir_fsyncs real counter ----

    #[test]
    fn dir_fsyncs_counts_segment_creation() {
        let dir = temp_dir();
        // Small segment to force rollover → dir fsync on new segment.
        let opts = test_opts_with_segment_bytes(80);
        let mut storage = WalStorage::open(&dir, opts).unwrap();
        // Fresh dir: META write = 1 dir fsync + initial segment creation = 1
        // dir fsync → total 2.
        assert_eq!(
            storage.stats().dir_fsyncs, 2,
            "fresh dir: META + initial segment = 2 dir fsyncs, got {}",
            storage.stats().dir_fsyncs
        );
        let before_rollover = storage.stats().dir_fsyncs;
        // Append enough to force rollover.
        for i in 1..=6u64 {
            storage.append(&[make_entry(i, 1, &[b'x'; 4])]).unwrap();
        }
        storage.sync_entries().unwrap();
        // After rollover, dir_fsyncs should have increased.
        assert!(
            storage.stats().dir_fsyncs > before_rollover,
            "expected dir_fsyncs > {before_rollover} after rollover, got {}",
            storage.stats().dir_fsyncs
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- N2: straddling rollover durability ----

    #[test]
    fn straddling_rollover_durability() {
        let dir = temp_dir();
        // Tiny segment_bytes so that a single append batch straddles a
        // rollover boundary.
        let opts = test_opts_with_segment_bytes(60);
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            // Append a batch of 4 entries in one call. With segment_bytes=60,
            // the first entry (~36 bytes) fits, but the second entry pushes
            // the segment past 60 → rollover mid-batch.
            storage
                .append(&[
                    make_entry(1, 1, b"aaaa"),
                    make_entry(2, 1, b"bbbb"),
                    make_entry(3, 1, b"cccc"),
                    make_entry(4, 1, b"dddd"),
                ])
                .unwrap();
            // sync_entries is the durability barrier for ALL entries in the
            // batch, including those in the outgoing (pre-rollover) segment.
            storage.sync_entries().unwrap();
            // N2: the outgoing segment must have been fsynced at the mid-batch
            // rollover (1), plus the barrier fsync of the active segment (1).
            assert_eq!(
                storage.stats().entry_fsyncs,
                2,
                "expected a rollover fsync AND a barrier fsync"
            );
        }
        // Reopen: all 4 entries must be recovered, no index gap, no error.
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 4);
        let entries = storage.entries(1, 5, None).unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].data, b"aaaa");
        assert_eq!(entries[1].data, b"bbbb");
        assert_eq!(entries[2].data, b"cccc");
        assert_eq!(entries[3].data, b"dddd");
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- N3: META-only directory reopens as empty log ----

    #[test]
    fn meta_only_dir_reopens_as_empty_log() {
        let dir = temp_dir();
        let opts = test_opts();
        // Open once (creates META + wal-1.log + LOCK), then drop.
        {
            let _storage = WalStorage::open(&dir, opts.clone()).unwrap();
        }
        // Delete all segment files, leaving only META and LOCK.
        for entry in fs::read_dir(&dir).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("wal-") {
                fs::remove_file(entry.path()).unwrap();
            }
        }
        // Reopen: META exists, no segments → legitimately empty log.
        let storage = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 0);
        // An empty log has no entries to return; querying beyond last_index
        // is Compacted (out-of-window), not an error.
        assert!(matches!(
            storage.entries(1, 2, None),
            Err(StorageError::Compacted)
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- N5(a): structural tear within committed window → fail-start ----

    #[test]
    fn recovery_failstart_tear_within_commit_window() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            // Append 2 entries, sync, then commit them.
            storage
                .append(&[make_entry(1, 1, b"a"), make_entry(2, 1, b"b")])
                .unwrap();
            storage.sync_entries().unwrap();
            storage
                .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 2 })
                .unwrap();
            // Now append a 3rd entry (beyond commit). This is written AFTER
            // the HardState record in the file.
            storage
                .append(&[make_entry(3, 1, b"c")])
                .unwrap();
            storage.sync_entries().unwrap();
        }

        // The file now contains: entry1 + entry2 + HardState(commit=2) + entry3.
        // Truncate the file to create a structural tear in entry3 (the last
        // record). On recovery: entries=[1,2], commit=2, estimated_index=3.
        // estimated_index (3) <= commit + 1 (3) → FAIL-START.
        let seg_path = dir.join(segment_name(1));
        let buf = fs::read(&seg_path).unwrap();
        // entry1 (data "a"=1 byte): 8 + 1 + 18 = 27 bytes
        // entry2 (data "b"=1 byte): 27 bytes
        // HardState: 8 + 1 + 25 = 34 bytes
        // entry3 (data "c"=1 byte): 27 bytes
        // Total = 27 + 27 + 34 + 27 = 115 bytes
        // Truncate at 27+27+34+5 = 93 (mid entry3, 5 bytes of its 27)
        let truncated_len = 27 + 27 + 34 + 5;
        let mut truncated = buf;
        truncated.truncate(truncated_len);
        fs::write(&seg_path, &truncated).unwrap();

        match WalStorage::open(&dir, opts) {
            Err(StorageError::Unrecoverable { .. }) => {}
            Err(e) => panic!("expected Unrecoverable, got: {e}"),
            Ok(_) => panic!("expected fail-start, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- N5(b): malformed record in non-last segment → fail-start ----

    #[test]
    fn recovery_failstart_malformed_non_last_segment() {
        let dir = temp_dir();
        // Use a tiny segment size to force multiple segments.
        let opts = test_opts_with_segment_bytes(80);
        {
            let mut storage = WalStorage::open(&dir, opts.clone()).unwrap();
            // Append enough entries to span at least 2 segments.
            for i in 1..=6u64 {
                storage
                    .append(&[make_entry(i, 1, &[b'x'; 4])])
                    .unwrap();
            }
            storage.sync_entries().unwrap();
            storage
                .set_hard_state(&HardState { term: 1, vote: Some(1), commit: 6 })
                .unwrap();
        }

        // Verify we have at least 2 segments.
        let segs: Vec<u64> = list_segment_indices(&dir).unwrap();
        assert!(
            segs.len() >= 2,
            "expected >= 2 segments for this test, got {}",
            segs.len()
        );

        // Corrupt a record in the FIRST (non-last) segment: flip a byte in
        // the middle of the first entry record → CRC fails → fail-start.
        let first_seg_path = dir.join(segment_name(segs[0]));
        let mut buf = fs::read(&first_seg_path).unwrap();
        // Flip a byte in the payload of the first record (offset 10 is within
        // the first entry's payload).
        buf[10] ^= 0xFF;
        fs::write(&first_seg_path, &buf).unwrap();

        match WalStorage::open(&dir, opts) {
            Err(StorageError::Unrecoverable { .. }) => {}
            Err(e) => panic!("expected Unrecoverable for non-last segment corruption, got: {e}"),
            Ok(_) => panic!("expected fail-start, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- force-recovery (propsol §6.1) ----

    /// Force-recovery discards the uncommitted tail, bumps the term, votes for
    /// self, keeps `commit`, and rewrites META with the new cluster id.
    #[test]
    fn force_recovery_discards_uncommitted_tail_and_rotates_cluster_id() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut wal = WalStorage::open(&dir, opts.clone()).unwrap();
            wal.append(&[
                make_entry(1, 1, b"a"),
                make_entry(2, 1, b"b"),
                make_entry(3, 1, b"c"),
            ])
            .unwrap();
            // Only 1..=2 are committed; entry 3 is an uncommitted tail.
            wal.set_hard_state(&HardState {
                term: 0,
                vote: None,
                commit: 2,
            })
            .unwrap();
            wal.sync_entries().unwrap();
        }

        let report = WalStorage::force_recovery(
            &dir,
            "node-1",
            Some("recovered-cluster".into()),
            WalConfig::default(),
            1_700_000_000_000,
        )
        .expect("force-recovery must succeed");

        assert_eq!(report.previous_cluster_id, "test-cluster");
        assert_eq!(report.cluster_id, "recovered-cluster");
        assert_eq!(report.term, 1, "term must be bumped by one");
        assert_eq!(report.commit, 2, "commit must be kept (== applied)");
        assert_eq!(report.discarded_entries, 1, "entry 3 must be discarded");

        // Reopening with the NEW cluster id sees the truncated log and the new
        // hard state.
        let mut new_opts = opts.clone();
        new_opts.cluster_id = "recovered-cluster".into();
        let wal = WalStorage::open(&dir, new_opts).expect("reopen with new cluster id");
        assert_eq!(wal.last_index().unwrap(), 2, "uncommitted tail must be gone");
        let hs = wal.initial_state().unwrap().hard_state;
        assert_eq!(hs.term, 1);
        assert_eq!(hs.vote, Some(1), "force-recovery votes for self");
        assert_eq!(hs.commit, 2);

        // The old cluster id no longer matches META.
        match WalStorage::open(&dir, opts) {
            Err(StorageError::Unrecoverable { .. }) => {}
            Err(e) => panic!("expected a META mismatch with the old cluster id, got error: {e}"),
            Ok(_) => panic!("expected a META mismatch with the old cluster id, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// With `new_cluster_id = None` the cluster id is preserved.
    #[test]
    fn force_recovery_keeps_the_cluster_id_when_not_rotating() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut wal = WalStorage::open(&dir, opts.clone()).unwrap();
            wal.append(&[make_entry(1, 1, b"a")]).unwrap();
            wal.set_hard_state(&HardState {
                term: 3,
                vote: Some(2),
                commit: 1,
            })
            .unwrap();
            wal.sync_entries().unwrap();
        }

        let report = WalStorage::force_recovery(
            &dir,
            "node-1",
            None,
            WalConfig::default(),
            1_700_000_000_000,
        )
        .expect("force-recovery must succeed");
        assert_eq!(report.cluster_id, "test-cluster");
        assert_eq!(report.discarded_entries, 0, "a fully committed log is untouched");
        assert_eq!(report.term, 4);

        let wal = WalStorage::open(&dir, opts).expect("reopen");
        assert_eq!(wal.last_index().unwrap(), 1);
        let hs = wal.initial_state().unwrap().hard_state;
        assert_eq!(hs.term, 4);
        assert_eq!(hs.vote, Some(1), "the vote is reset to self");
        assert_eq!(hs.commit, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    /// With nothing committed, the whole log is discarded and a fresh segment
    /// is started.
    #[test]
    fn force_recovery_with_zero_commit_discards_every_entry() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut wal = WalStorage::open(&dir, opts.clone()).unwrap();
            wal.append(&[make_entry(1, 5, b"x"), make_entry(2, 5, b"y")])
                .unwrap();
            // HardState is written at open with commit 0; append does not
            // advance commit, so both entries are uncommitted.
            wal.sync_entries().unwrap();
        }

        let report = WalStorage::force_recovery(
            &dir,
            "node-1",
            None,
            WalConfig::default(),
            1_700_000_000_000,
        )
        .expect("force-recovery must succeed");
        assert_eq!(report.commit, 0);
        assert_eq!(report.discarded_entries, 2);

        let wal = WalStorage::open(&dir, opts).expect("reopen");
        assert_eq!(wal.last_index().unwrap(), 0, "the log must be empty");
        let hs = wal.initial_state().unwrap().hard_state;
        assert_eq!(hs.commit, 0);
        // The log is still appendable after the rewrite.
        drop(wal);
        let mut wal = WalStorage::open(&dir, test_opts()).expect("reopen for append");
        wal.append(&[make_entry(1, 6, b"z")]).unwrap();
        wal.sync_entries().unwrap();
        assert_eq!(wal.last_index().unwrap(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A held data-dir lock refuses force-recovery (the old process is running).
    #[test]
    fn force_recovery_refuses_a_locked_data_dir() {
        let dir = temp_dir();
        let opts = test_opts();
        let _held = WalStorage::open(&dir, opts).unwrap();

        match WalStorage::force_recovery(
            &dir,
            "node-1",
            None,
            WalConfig::default(),
            1_700_000_000_000,
        ) {
            Err(StorageError::Unrecoverable { detail }) => {
                assert!(detail.contains("locked"), "unexpected detail: {detail}");
            }
            Err(e) => panic!("expected a lock refusal, got error: {e}"),
            Ok(_) => panic!("expected a lock refusal, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// A `node_id` that does not match META is refused.
    #[test]
    fn force_recovery_refuses_a_node_id_mismatch() {
        let dir = temp_dir();
        let opts = test_opts();
        drop(WalStorage::open(&dir, opts).unwrap());

        match WalStorage::force_recovery(
            &dir,
            "someone-else",
            None,
            WalConfig::default(),
            1_700_000_000_000,
        ) {
            Err(StorageError::Unrecoverable { detail }) => {
                assert!(detail.contains("does not match"), "unexpected detail: {detail}");
            }
            Err(e) => panic!("expected a node_id mismatch, got error: {e}"),
            Ok(_) => panic!("expected a node_id mismatch, got Ok"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Append overwrite (conflicting suffix after an election) ----

    /// raft re-sends a conflicting suffix after an election; `append` must
    /// overwrite from the first incoming index rather than duplicate it.
    #[test]
    fn append_overwrites_a_conflicting_suffix() {
        let dir = temp_dir();
        let opts = test_opts();
        {
            let mut wal = WalStorage::open(&dir, opts.clone()).unwrap();
            wal.append(&[
                make_entry(1, 1, b"a"),
                make_entry(2, 1, b"b"),
                make_entry(3, 1, b"c"),
            ])
            .unwrap();
            wal.sync_entries().unwrap();
        }
        {
            let mut wal = WalStorage::open(&dir, opts.clone()).unwrap();
            // Entries 2 and 3 come back from a new term: overwrite them.
            wal.append(&[make_entry(2, 2, b"B"), make_entry(3, 2, b"C")])
                .unwrap();
            wal.sync_entries().unwrap();
            let got = wal.entries(1, 4, None).unwrap();
            let idx_term: Vec<(u64, u64)> = got.iter().map(|e| (e.index, e.term)).collect();
            assert_eq!(idx_term, vec![(1, 1), (2, 2), (3, 2)]);
            let data: Vec<&[u8]> = got.iter().map(|e| e.data.as_slice()).collect();
            assert_eq!(data, vec![b"a".as_slice(), b"B", b"C"]);
        }
        // The overwrite is durable across a reopen.
        let wal = WalStorage::open(&dir, opts).unwrap();
        let got = wal.entries(1, 4, None).unwrap();
        let idx_term: Vec<(u64, u64)> = got.iter().map(|e| (e.index, e.term)).collect();
        assert_eq!(idx_term, vec![(1, 1), (2, 2), (3, 2)]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Overwriting from index 1 replaces the whole log.
    #[test]
    fn append_overwrites_from_index_one() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut wal = WalStorage::open(&dir, opts.clone()).unwrap();
        wal.append(&[make_entry(1, 1, b"a"), make_entry(2, 1, b"b")])
            .unwrap();
        wal.append(&[make_entry(1, 2, b"A")]).unwrap();
        wal.sync_entries().unwrap();
        let got = wal.entries(1, 2, None).unwrap();
        assert_eq!(got.len(), 1, "the conflicting suffix must be discarded");
        assert_eq!(
            (got[0].index, got[0].term, got[0].data.as_slice()),
            (1, 2, b"A".as_slice())
        );
        drop(wal);
        let wal = WalStorage::open(&dir, opts).unwrap();
        assert_eq!(wal.last_index().unwrap(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Overwriting a committed entry is a raft safety violation: fail-stop
    /// rather than silently discard committed data.
    #[test]
    fn append_refuses_to_overwrite_committed_entries() {
        let dir = temp_dir();
        let opts = test_opts();
        let mut wal = WalStorage::open(&dir, opts).unwrap();
        wal.append(&[make_entry(1, 1, b"a"), make_entry(2, 1, b"b")])
            .unwrap();
        wal.set_hard_state(&HardState {
            term: 1,
            vote: None,
            commit: 2,
        })
        .unwrap();
        match wal.append(&[make_entry(2, 2, b"B")]) {
            Err(StorageError::Unrecoverable { detail }) => {
                assert!(detail.contains("committed"), "unexpected detail: {detail}");
            }
            other => {
                panic!("expected a fail-stop on overwriting committed entries, got {other:?}")
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
