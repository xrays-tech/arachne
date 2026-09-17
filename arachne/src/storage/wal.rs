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
use std::path::{Path, PathBuf};

use arachne_seam::storage::{
    ConfState, HardState, LogEntry, RaftState, Snapshot, Storage, StorageError,
};
use arachne_seam::types::{LogIndex, Term};

use crate::storage::format::{
    decode_entry, decode_hard_state, decode_record, encode_entry, encode_hard_state,
    RecordType, MAX_RECORD_BYTES,
};
use crate::storage::meta::{read_meta, write_meta, fsync_dir, Meta, FORMAT_VERSION};
use crate::storage::segment::{
    parse_segment_name, segment_path, Segment, DEFAULT_SEGMENT_BYTES,
};

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
#[derive(Clone, Debug)]
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
    /// The in-memory log entries (contiguous, 1-based indices).
    entries: Vec<LogEntry>,
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
        let _meta = Self::recover_meta(dir, &opts, &mut stats)?;

        // Step 3: (P2: no snapshot loading; M2 adds it.)

        // Step 4: replay WAL segments.
        let (entries, hard_state, truncated_records, dir_fsyncs_from_recovery) =
            Self::recover_wal(dir)?;
        stats.wal_truncated_records = truncated_records;
        stats.dir_fsyncs += dir_fsyncs_from_recovery;

        // Step 5: verify index continuity.
        Self::verify_continuity(&entries)?;

        // Step 6: assert commit <= last_entry_index (unconditional).
        let last_entry_index = entries.last().map(|e| e.index).unwrap_or(0);
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

        Ok(Self {
            data_dir: dir.to_path_buf(),
            segment,
            entries,
            hard_state,
            pending_entry_fsync: false,
            fsync_policy: opts.config.fsync_policy,
            segment_bytes: opts.config.segment_bytes,
            stats,
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
                    // M2: at M2, estimated_index must derive from the first
                    // retained index (after snapshot/compaction), not from
                    // entries.len() + 1.
                    let estimated_index = entries.len() as LogIndex + 1;
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

    // M2: at M2, verify_continuity must derive `expected` from the first
    // retained index (after snapshot/compaction), not from position + 1.
    fn verify_continuity(entries: &[LogEntry]) -> Result<(), StorageError> {
        for (i, entry) in entries.iter().enumerate() {
            let expected = (i + 1) as LogIndex;
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
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Storage trait impl
// ---------------------------------------------------------------------------

impl Storage for WalStorage {
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
        // M2: at M2, `first` must derive from the first retained index.
        let first = self.entries.first().map(|e| e.index).unwrap_or(1);
        let last = self.entries.last().map(|e| e.index).unwrap_or(0);
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
        let first = self.entries.first().map(|e| e.index).unwrap_or(1);
        let last = self.entries.last().map(|e| e.index).unwrap_or(0);
        if index < first || index > last {
            return Err(StorageError::Compacted);
        }
        Ok(self.entries[(index - first) as usize].term)
    }

    fn first_index(&self) -> Result<LogIndex, StorageError> {
        // M2: at M2, this must derive from the first retained index.
        Ok(self.entries.first().map(|e| e.index).unwrap_or(1))
    }

    fn last_index(&self) -> Result<LogIndex, StorageError> {
        Ok(self.entries.last().map(|e| e.index).unwrap_or(0))
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        // P2: no snapshots.
        Ok(None)
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        for entry in entries {
            // Continuity hardening: the next entry must be last_index + 1.
            let last = self.entries.last().map(|e| e.index).unwrap_or(0);
            debug_assert!(
                entry.index == last + 1,
                "append continuity violation: expected {}, got {}",
                last + 1,
                entry.index
            );

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

            // Update the in-memory index.
            self.entries.push(entry.clone());

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
                }
            }
        }
        Ok(())
    }

    fn compact(&mut self, _compact_to: LogIndex) -> Result<(), StorageError> {
        // P2: compaction is not yet implemented.
        Err(StorageError::Unrecoverable {
            detail: "compaction is not implemented in P2 (lands in M2)".into(),
        })
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
}
