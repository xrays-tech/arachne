//! WAL segment file management: naming, append, and read of records.
//!
//! # Segment naming
//!
//! A segment file is named `wal-%020d.log` where the number is the **first
//! log index** in that segment. For example, a segment starting at index 1 is
//! `wal-00000000000000000001.log`.
//!
//! # Rollover
//!
//! A segment rolls over (a new segment file is started) once its size reaches
//! [`DEFAULT_SEGMENT_BYTES`] (128 MiB by default).
//!
//! # I/O discipline
//!
//! Append operations never panic; all I/O errors are surfaced as
//! [`SegmentError`].

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::storage::format::{encode_record, DecodeError, RecordType};
#[cfg(test)]
use crate::storage::format::decode_record;

/// Linux-only WAL segment extent pre-allocation (B4 / U2-bis).
use crate::storage::prealloc::preallocate;
use crate::storage::sync::sync_durable;

/// The default maximum size of a single segment file before rollover.
pub const DEFAULT_SEGMENT_BYTES: u64 = 128 * 1024 * 1024; // 128 MiB

/// The fixed prefix of a segment file name.
const SEGMENT_PREFIX: &str = "wal-";
/// The fixed suffix of a segment file name.
const SEGMENT_SUFFIX: &str = ".log";
/// The number of zero-padded digits in the segment file name.
const SEGMENT_DIGITS: usize = 20;

/// Errors from segment I/O operations.
#[derive(Debug)]
pub enum SegmentError {
    /// A low-level I/O failure.
    Io(io::Error),
    /// A record in the segment could not be decoded.
    Decode(DecodeError),
    /// The file name is not a valid segment name.
    InvalidName(String),
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmentError::Io(err) => write!(f, "segment I/O error: {err}"),
            SegmentError::Decode(err) => write!(f, "segment decode error: {err}"),
            SegmentError::InvalidName(name) => write!(f, "invalid segment file name: {name}"),
        }
    }
}

impl core::error::Error for SegmentError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            SegmentError::Io(err) => Some(err),
            SegmentError::Decode(err) => Some(err),
            SegmentError::InvalidName(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Segment naming
// ---------------------------------------------------------------------------

/// Format a segment file name for the given first log index.
///
/// The name is `wal-{index:020}.log` (e.g. `wal-00000000000000000001.log`).
pub fn segment_name(first_index: u64) -> String {
    format!("{SEGMENT_PREFIX}{first_index:0width$}{SEGMENT_SUFFIX}", width = SEGMENT_DIGITS)
}

/// Parse a segment file name back into its first log index.
///
/// # Errors
///
/// Returns [`SegmentError::InvalidName`] if the file name does not match the
/// expected `wal-%020d.log` pattern.
pub fn parse_segment_name(name: &str) -> Result<u64, SegmentError> {
    let base = name.strip_suffix(SEGMENT_SUFFIX).ok_or_else(|| {
        SegmentError::InvalidName(format!("{name} does not end with {SEGMENT_SUFFIX}"))
    })?;
    let digits = base.strip_prefix(SEGMENT_PREFIX).ok_or_else(|| {
        SegmentError::InvalidName(format!("{name} does not start with {SEGMENT_PREFIX}"))
    })?;
    if digits.len() != SEGMENT_DIGITS {
        return Err(SegmentError::InvalidName(format!(
            "segment name has {len} digits, expected {SEGMENT_DIGITS}",
            len = digits.len()
        )));
    }
    let index: u64 = digits.parse().map_err(|_| {
        SegmentError::InvalidName(format!("segment name digits are not a valid u64: {digits}"))
    })?;
    Ok(index)
}

/// The full path to a segment file within a data directory.
pub fn segment_path(data_dir: &Path, first_index: u64) -> PathBuf {
    data_dir.join(segment_name(first_index))
}

// ---------------------------------------------------------------------------
// Segment append + read
// ---------------------------------------------------------------------------

/// A WAL segment: a file that accumulates records until it reaches the
/// rollover threshold.
///
/// The segment is opened in append mode. `append_record` writes a single
/// framed record to the end of the file. `size` returns the current file
/// size (in bytes) for rollover decisions.
pub struct Segment {
    path: PathBuf,
    file: File,
    max_bytes: u64,
}

impl Segment {
    /// Open (or create) a segment file for appending.
    ///
    /// The file is opened in read-write + append mode. If the file does not
    /// exist, it is created.
    ///
    /// On Linux the segment extent is pre-allocated up front via
    /// [`prealloc::preallocate`] (`fallocate(2)` with `FALLOC_FL_KEEP_SIZE`).
    /// This reserves the full `max_bytes` extents for appends without growing
    /// `i_size`, so recovery, tear detection, and rollover (all `i_size`-
    /// driven) are byte-identical to the non-preallocated case. A failed
    /// pre-allocation is skipped, not fatal (see `storage::prealloc`).
    pub fn open(data_dir: &Path, first_index: u64) -> Result<Self, SegmentError> {
        Self::open_with_max_bytes(data_dir, first_index, DEFAULT_SEGMENT_BYTES)
    }

    /// Open a segment with a custom maximum size (useful for tests).
    #[cfg(test)]
    pub fn open_with_max(data_dir: &Path, first_index: u64, max_bytes: u64) -> Result<Self, SegmentError> {
        Self::open_with_max_bytes(data_dir, first_index, max_bytes)
    }

    /// Open a segment with a custom maximum rollover size.
    ///
    /// On Linux, after opening the file, the extent is pre-allocated (see
    /// [`Segment::open`]).
    pub fn open_with_max_bytes(data_dir: &Path, first_index: u64, max_bytes: u64) -> Result<Self, SegmentError> {
        let path = segment_path(data_dir, first_index);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&path)
            .map_err(SegmentError::Io)?;
        // Reserve the full segment extent up front (no-op off Linux / on
        // fallocate failure; see `storage::prealloc`). Never fatal.
        let _ = preallocate(&file, max_bytes);
        Ok(Self {
            path,
            file,
            max_bytes,
        })
    }

    /// Append a single framed record to the segment.
    ///
    /// The record is encoded (framed with length + CRC) and written atomically
    /// (a single `write_all` call). This method never panics; I/O errors are
    /// surfaced as [`SegmentError::Io`].
    pub fn append_record(&mut self, record_type: RecordType, payload: &[u8]) -> Result<(), SegmentError> {
        let framed = encode_record(record_type, payload);
        self.file.write_all(&framed).map_err(SegmentError::Io)
    }

    /// The current size of the segment file in bytes.
    pub fn size(&self) -> io::Result<u64> {
        self.file.metadata().map(|m| m.len())
    }

    /// Whether the segment has reached the rollover threshold.
    pub fn should_rollover(&self) -> io::Result<bool> {
        Ok(self.size()? >= self.max_bytes)
    }

    /// The path to the segment file.
    /// Duplicate this segment's descriptor so another thread can `fsync` it.
    ///
    /// `fsync` flushes the *inode*, so a duplicate descriptor covers every byte
    /// written through the original — which is what lets the durability
    /// pipeline run its flush off the actor thread (propsol v0.2.13 P).
    pub fn try_clone_file(&self) -> io::Result<File> {
        self.file.try_clone()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Fsync the segment file (durability barrier for appended records).
    ///
    /// Dispatches through [`sync_durable`]: on Linux this is `fdatasync(2)`
    /// (which covers the `i_size` extension of an appending write); on other
    /// platforms it is the platform's full `fsync` (`F_FULLFSYNC` on macOS),
    /// preserving the historical behaviour.
    pub fn sync(&self) -> io::Result<()> {
        sync_durable(&self.file)
    }

    /// Truncate the segment file to the given byte offset.
    ///
    /// Used by recovery to remove corrupt/torn trailing records.
    pub fn truncate_at(&self, offset: u64) -> io::Result<()> {
        self.file.set_len(offset)
    }

    /// Read the raw bytes of the segment file.
    pub fn read_raw(&self) -> io::Result<Vec<u8>> {
        let mut file = File::open(&self.path).map_err(|e| e)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Read all complete records from the segment file.
    ///
    /// **Test-only**: recovery does not use this method. It reads the entire
    /// file and decodes records sequentially; a truncated record at the end
    /// is ignored. Returns the list of decoded `(RecordType, payload)` pairs.
    #[cfg(test)]
    pub fn read_all_records(&self) -> Result<Vec<(RecordType, Vec<u8>)>, SegmentError> {
        let mut file = File::open(&self.path).map_err(SegmentError::Io)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).map_err(SegmentError::Io)?;

        let mut records = Vec::new();
        let mut offset = 0usize;
        while offset < buf.len() {
            // Need at least 8 bytes for the header.
            if buf.len() - offset < 8 {
                // Trailing partial header: ignore (crash-truncation).
                break;
            }
            let len = u32::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]) as usize;
            if len < 1 {
                // Invalid: a record body must be at least 1 byte.
                break;
            }
            let record_end = offset + 8 + len;
            if record_end > buf.len() {
                // Truncated record: ignore (crash-truncation).
                break;
            }
            let record_bytes = &buf[offset..record_end];
            match decode_record(record_bytes) {
                Ok((rt, payload)) => records.push((rt, payload)),
                Err(_) => {
                    // Corrupted record: stop reading (fail-fast for the
                    // caller to handle). We do NOT skip past it.
                    return Err(SegmentError::Decode(DecodeError::TooShort));
                }
            }
            offset = record_end;
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    /// Create a unique temp directory for a test (no external crates).
    /// Returns the path; caller must clean up (or let the OS handle it).
    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "arachne-seg-test-{}-{}",
            std::process::id(),
            n
        ));
        fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    // ---- Naming round-trip ----

    #[test]
    fn segment_name_format() {
        assert_eq!(segment_name(1), "wal-00000000000000000001.log");
        assert_eq!(segment_name(0), "wal-00000000000000000000.log");
        assert_eq!(segment_name(42), "wal-00000000000000000042.log");
        assert_eq!(
            segment_name(1_000_000_000_000),
            "wal-00000001000000000000.log"
        );
    }

    #[test]
    fn segment_name_parse_roundtrip() {
        for idx in [0u64, 1, 42, 999_999, u64::MAX] {
            let name = segment_name(idx);
            assert_eq!(parse_segment_name(&name).unwrap(), idx);
        }
    }

    #[test]
    fn parse_invalid_names() {
        assert!(parse_segment_name("wal-1.log").is_err());
        assert!(parse_segment_name("wal-.log").is_err());
        assert!(parse_segment_name("foo-00000000000000000001.log").is_err());
        assert!(parse_segment_name("wal-00000000000000000001.txt").is_err());
        assert!(parse_segment_name("").is_err());
        // Non-numeric digits.
        assert!(parse_segment_name("wal-xxxxxxxxxxxxxxxxxxxx.log").is_err());
    }

    // ---- Rollover threshold ----

    #[test]
    fn rollover_threshold_selection() {
        let dir = temp_dir();
        let mut seg = Segment::open_with_max(&dir, 1, 100).unwrap();
        // Fresh segment: size 0, should not roll over.
        assert!(!seg.should_rollover().unwrap());

        // Append a record that pushes size >= 100.
        let payload = vec![0u8; 200];
        seg.append_record(RecordType::Entry, &payload).unwrap();
        assert!(seg.size().unwrap() >= 100);
        assert!(seg.should_rollover().unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn default_rollover_is_128mib() {
        assert_eq!(DEFAULT_SEGMENT_BYTES, 128 * 1024 * 1024);
    }

    // ---- Rollover uses the *logical* size even on a preallocated segment ----

    // KEEP_SIZE reserves extents but never moves `i_size`, so `size()` stays
    // the real (logical) byte count and `should_rollover` is driven by it. If
    // pre-allocation had inflated the logical size to the reserved extent (as a
    // naive `set_len(max_bytes)` would), `should_rollover` would fire immediately.
    #[test]
    fn should_rollover_uses_logical_size_when_preallocated() {
        let dir = temp_dir();
        let mut seg = Segment::open_with_max(&dir, 1, 80).unwrap();
        // Fresh segment: logical size 0, not full — even though the 80-byte
        // extent is (Linux) reserved at open.
        assert_eq!(seg.size().unwrap(), 0, "pre-allocated extent must not change size");
        assert!(!seg.should_rollover().unwrap(), "fresh segment must not roll over");

        // A small record stays below the logical threshold.
        let small = vec![0u8; 10];
        seg.append_record(RecordType::Entry, &small).unwrap();
        // framed size = 8 (header) + 1 (type) + payload = 9 + payload.len()
        assert_eq!(
            seg.size().unwrap(),
            (9 + small.len()) as u64,
            "size is the logical byte count"
        );
        assert!(!seg.should_rollover().unwrap(), "19 < 80, still below threshold");

        // A record that crosses the threshold flips rollover.
        let big = vec![0u8; 200];
        seg.append_record(RecordType::Entry, &big).unwrap();
        assert!(seg.size().unwrap() >= 80);
        assert!(seg.should_rollover().unwrap(), ">= 80 must roll over");

        let _ = fs::remove_dir_all(&dir);
    }

    // Pre-allocation + downward truncation + further append must coexist:
    // the reserved extent survives truncation (extents are discarded), and appends
    // after truncation land in the real logical tail. Reopen and read back the
    // exact record sequence with no holes.
    #[test]
    fn truncate_then_append_roundtrip_on_preallocated_segment() {
        let dir = temp_dir();
        let mut seg = Segment::open_with_max(&dir, 1, 80).unwrap();

        // Two complete records A and B.
        let payload_a = crate::storage::format::encode_entry(1, 1, 0, b"AAA");
        let payload_b = crate::storage::format::encode_entry(2, 1, 0, b"BBB");
        let rec_a = crate::storage::format::encode_record(RecordType::Entry, &payload_a);
        // B is appended too, then cut away below.
        let rec_b = crate::storage::format::encode_record(RecordType::Entry, &payload_b);
        seg.append_record(RecordType::Entry, &payload_a).unwrap();
        seg.append_record(RecordType::Entry, &payload_b).unwrap();
        assert_eq!(
            seg.size().unwrap(),
            rec_a.len() as u64 + rec_b.len() as u64
        );

        // Truncate to the end of record A: keeps A, drops B.
        let mid: u64 = rec_a.len() as u64;
        seg.truncate_at(mid).unwrap();
        assert_eq!(seg.size().unwrap(), mid, "truncate_at must set the logical length");

        // Append record C into the (pre-allocated) space after the cut.
        let payload_c = crate::storage::format::encode_entry(3, 1, 0, b"CCC");
        seg.append_record(RecordType::Entry, &payload_c).unwrap();

        // Reopen and read back: exactly [A, C], B gone, no holes.
        let reopened = Segment::open_with_max(&dir, 1, 80).unwrap();
        let records = reopened.read_all_records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, RecordType::Entry);
        assert_eq!(records[1].0, RecordType::Entry);
        let (ia, _, _, da) = crate::storage::format::decode_entry(&records[0].1).unwrap();
        let (ic, _, _, dc) = crate::storage::format::decode_entry(&records[1].1).unwrap();
        assert_eq!(ia, 1);
        assert_eq!(da, b"AAA".to_vec());
        assert_eq!(ic, 3);
        assert_eq!(dc, b"CCC".to_vec());

        let _ = fs::remove_dir_all(&dir);
    }

    // ---- Append + read ----

    #[test]
    fn append_and_read_roundtrip() {
        let dir = temp_dir();
        let mut seg = Segment::open(&dir, 1).unwrap();

        let entry_payload = crate::storage::format::encode_entry(1, 1, 0, b"hello");
        seg.append_record(RecordType::Entry, &entry_payload).unwrap();

        let hs_payload = crate::storage::format::encode_hard_state(1, Some(7), 0);
        seg.append_record(RecordType::HardState, &hs_payload).unwrap();

        let records = seg.read_all_records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, RecordType::Entry);
        assert_eq!(records[1].0, RecordType::HardState);

        let (_, payload) = &records[0];
        let (index, term, etype, data) = crate::storage::format::decode_entry(payload).unwrap();
        assert_eq!(index, 1);
        assert_eq!(term, 1);
        assert_eq!(etype, 0);
        assert_eq!(data, b"hello");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncated_record_at_end_is_ignored() {
        let dir = temp_dir();
        let mut seg = Segment::open(&dir, 1).unwrap();

        // Write a complete record.
        let payload = crate::storage::format::encode_entry(1, 1, 0, b"ok");
        seg.append_record(RecordType::Entry, &payload).unwrap();

        // Write a partial record (just the header, no body).
        let mut file = OpenOptions::new()
            .append(true)
            .open(seg.path())
            .unwrap();
        let mut partial = [0u8; 8];
        partial[0..4].copy_from_slice(&10u32.to_le_bytes());
        file.write_all(&partial).unwrap();
        drop(file);

        // Reading should return only the complete record.
        let records = seg.read_all_records().unwrap();
        assert_eq!(records.len(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_segment_reads_zero_records() {
        let dir = temp_dir();
        let seg = Segment::open(&dir, 1).unwrap();
        let records = seg.read_all_records().unwrap();
        assert!(records.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_path_is_in_data_dir() {
        let dir = Path::new("/tmp/test-arachne");
        let path = segment_path(dir, 1);
        assert_eq!(path, PathBuf::from("/tmp/test-arachne/wal-00000000000000000001.log"));
    }
}
