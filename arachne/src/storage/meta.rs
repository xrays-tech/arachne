//! The `META` file: cluster identity and format version, written atomically.
//!
//! # File format
//!
//! The META file is a single binary blob:
//!
//! ```text
//! [u32 magic LE][u32 format_version LE][u32 crc32c LE]
//! [u16 cluster_id_len LE][cluster_id bytes]
//! [u16 node_id_len LE][node_id bytes]
//! [u64 created_at_millis LE]
//! [u64 snapshot_index LE][u64 snapshot_term LE]   <- optional payload extension
//! ```
//!
//! The trailing snapshot pointer (propsol v0.2.10 M) is an *extension* within
//! the same `format_version`: a file written before it was added simply ends
//! after `created_at_millis` and decodes with a zero pointer, and a reader that
//! predates it ignores the extra bytes (the decoder here never requires the
//! payload to end at a particular offset).
//!
//! * `magic` — the constant [`META_MAGIC`], used to detect a valid META file.
//! * `format_version` — the WAL format major version ([`FORMAT_VERSION`]).
//! * `crc32c` — CRC32C over the **payload** (everything after the 12-byte
//!   header).
//! * Fields are length-prefixed (u16 LE) for strings; `created_at` is a
//!   fixed-width u64 LE.
//!
//! # Atomic write
//!
//! The write protocol is:
//! 1. Write the full blob to `META.tmp` in the same directory.
//! 2. `fsync` the file (durability of the file content).
//! 3. `rename` `META.tmp` → `META` (atomic on POSIX).
//! 4. `fsync` the directory (durability of the rename).
//!
//! This guarantees that a crash at any point leaves either the old META or
//! the new META, never a torn file.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use arachne_seam::types::{LogIndex, Term};

use crate::storage::crc32c::crc32c;

/// The current WAL format major version.
pub const FORMAT_VERSION: u32 = 1;

/// Magic number identifying a valid META file.
const META_MAGIC: u32 = 0x41_52_41_4D; // "ARAM" in ASCII

/// Metadata for a raft node's data directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Meta {
    /// The cluster identifier (human-readable string).
    pub cluster_id: String,
    /// The node identifier (human-readable string).
    pub node_id: String,
    /// The WAL format version this data directory was created with.
    pub format_version: u32,
    /// The creation timestamp in milliseconds since the Unix epoch.
    pub created_at: u64,
    /// The index of the snapshot this directory's log has been compacted to
    /// (0 = no snapshot). Written after the snapshot file is durable, so it is
    /// the single-point record of the currently effective snapshot; recovery
    /// still validates it against the snapshot file itself.
    pub snapshot_index: LogIndex,
    /// The term of that snapshot (0 = no snapshot).
    pub snapshot_term: Term,
}

/// Errors from META file operations.
#[derive(Debug)]
pub enum MetaError {
    /// A low-level I/O failure.
    Io(io::Error),
    /// The file does not start with the expected magic number.
    BadMagic,
    /// The CRC32C checksum does not match.
    ChecksumMismatch {
        /// The stored checksum.
        expected: u32,
        /// The computed checksum.
        actual: u32,
    },
    /// The file is too short to contain a valid META blob.
    TooShort,
    /// A string field's length prefix exceeds the remaining bytes.
    TruncatedField,
}

impl std::fmt::Display for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaError::Io(err) => write!(f, "META I/O error: {err}"),
            MetaError::BadMagic => write!(f, "META file has an invalid magic number"),
            MetaError::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "META checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
                )
            }
            MetaError::TooShort => write!(f, "META file is too short"),
            MetaError::TruncatedField => write!(f, "META file has a truncated field"),
        }
    }
}

impl core::error::Error for MetaError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            MetaError::Io(err) => Some(err),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Encode a [`Meta`] into its on-disk byte representation.
fn encode_meta(meta: &Meta) -> Vec<u8> {
    let cluster_bytes = meta.cluster_id.as_bytes();
    let node_bytes = meta.node_id.as_bytes();

    // Payload: [u16 cluster_len][cluster][u16 node_len][node][u64 created_at]
    //          [u64 snapshot_index][u64 snapshot_term]  (extension, v0.2.10 M)
    let payload_len = 2 + cluster_bytes.len() + 2 + node_bytes.len() + 8 + 16;
    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&(cluster_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(cluster_bytes);
    payload.extend_from_slice(&(node_bytes.len() as u16).to_le_bytes());
    payload.extend_from_slice(node_bytes);
    payload.extend_from_slice(&meta.created_at.to_le_bytes());
    payload.extend_from_slice(&meta.snapshot_index.to_le_bytes());
    payload.extend_from_slice(&meta.snapshot_term.to_le_bytes());

    let crc = crc32c(&payload);

    // Full blob: [u32 magic][u32 format_version][u32 crc][payload]
    let mut buf = Vec::with_capacity(12 + payload_len);
    buf.extend_from_slice(&META_MAGIC.to_le_bytes());
    buf.extend_from_slice(&meta.format_version.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&payload);
    buf
}

/// Decode a META blob from its on-disk byte representation.
fn decode_meta(buf: &[u8]) -> Result<Meta, MetaError> {
    // Minimum size: 12 (header) + 2 + 0 + 2 + 0 + 8 = 24 bytes.
    if buf.len() < 24 {
        return Err(MetaError::TooShort);
    }

    let magic = le32_at(buf, 0)?;
    if magic != META_MAGIC {
        return Err(MetaError::BadMagic);
    }
    let format_version = le32_at(buf, 4)?;
    let stored_crc = le32_at(buf, 8)?;

    let payload = &buf[12..];
    let actual_crc = crc32c(payload);
    if actual_crc != stored_crc {
        return Err(MetaError::ChecksumMismatch {
            expected: stored_crc,
            actual: actual_crc,
        });
    }

    // Parse payload: [u16 cluster_len][cluster][u16 node_len][node][u64 created_at]
    // Minimum payload: 2 + 0 + 2 + 0 + 8 = 12 bytes (both strings empty).
    if payload.len() < 12 {
        return Err(MetaError::TruncatedField);
    }
    let cluster_len = le16_at(payload, 0)? as usize;
    if payload.len() < 2 + cluster_len + 2 + 8 {
        return Err(MetaError::TruncatedField);
    }
    let cluster_id = String::from_utf8(payload[2..2 + cluster_len].to_vec())
        .map_err(|_| MetaError::TruncatedField)?;

    let node_offset = 2 + cluster_len;
    let node_len = le16_at(payload, node_offset)? as usize;
    if payload.len() < node_offset + 2 + node_len + 8 {
        return Err(MetaError::TruncatedField);
    }
    let node_id = String::from_utf8(payload[node_offset + 2..node_offset + 2 + node_len].to_vec())
        .map_err(|_| MetaError::TruncatedField)?;

    let created_at_offset = node_offset + 2 + node_len;
    let created_at = le64_at(payload, created_at_offset)?;

    // Optional trailing snapshot pointer: a META written before v0.2.10 ends
    // right after `created_at`, which decodes as "no snapshot".
    let pointer_offset = created_at_offset + 8;
    let (snapshot_index, snapshot_term) = if payload.len() >= pointer_offset + 16 {
        (
            le64_at(payload, pointer_offset)?,
            le64_at(payload, pointer_offset + 8)?,
        )
    } else {
        (0, 0)
    };

    Ok(Meta {
        cluster_id,
        node_id,
        format_version,
        created_at,
        snapshot_index,
        snapshot_term,
    })
}

/// Read a little-endian `u16` from `buf` at the given offset.
#[inline(always)]
fn le16_at(buf: &[u8], offset: usize) -> Result<u16, MetaError> {
    let end = offset.checked_add(2).ok_or(MetaError::TruncatedField)?;
    if buf.len() < end {
        return Err(MetaError::TruncatedField);
    }
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(&buf[offset..end]);
    Ok(u16::from_le_bytes(bytes))
}

/// Read a little-endian `u32` from `buf` at the given offset.
#[inline(always)]
fn le32_at(buf: &[u8], offset: usize) -> Result<u32, MetaError> {
    let end = offset.checked_add(4).ok_or(MetaError::TruncatedField)?;
    if buf.len() < end {
        return Err(MetaError::TruncatedField);
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&buf[offset..end]);
    Ok(u32::from_le_bytes(bytes))
}

/// Read a little-endian `u64` from `buf` at the given offset.
#[inline(always)]
fn le64_at(buf: &[u8], offset: usize) -> Result<u64, MetaError> {
    let end = offset.checked_add(8).ok_or(MetaError::TruncatedField)?;
    if buf.len() < end {
        return Err(MetaError::TruncatedField);
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[offset..end]);
    Ok(u64::from_le_bytes(bytes))
}

// ---------------------------------------------------------------------------
// Atomic write + read
// ---------------------------------------------------------------------------

/// The path to the META file within a data directory.
pub fn meta_path(data_dir: &Path) -> PathBuf {
    data_dir.join("META")
}

/// Write a [`Meta`] to the META file atomically.
///
/// Protocol: write `META.tmp` → fsync file → rename to `META` → fsync dir.
pub fn write_meta(data_dir: &Path, meta: &Meta) -> Result<(), MetaError> {
    let final_path = meta_path(data_dir);
    let tmp_path = data_dir.join("META.tmp");

    let blob = encode_meta(meta);

    // Step 1: write to temp file.
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)
        .map_err(MetaError::Io)?;
    file.write_all(&blob).map_err(MetaError::Io)?;

    // Step 2: fsync the file.
    file.sync_all().map_err(MetaError::Io)?;
    drop(file);

    // Step 3: rename (atomic on POSIX).
    fs::rename(&tmp_path, &final_path).map_err(MetaError::Io)?;

    // Step 4: fsync the directory.
    fsync_dir(data_dir).map_err(MetaError::Io)?;

    Ok(())
}

/// Read a [`Meta`] from the META file.
pub fn read_meta(data_dir: &Path) -> Result<Meta, MetaError> {
    let path = meta_path(data_dir);
    let buf = fs::read(&path).map_err(MetaError::Io)?;
    decode_meta(&buf)
}

/// Fsync a directory (for rename durability on POSIX).
pub(crate) fn fsync_dir(dir: &Path) -> io::Result<()> {
    let file = File::open(dir)?;
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_meta() -> Meta {
        Meta {
            cluster_id: "test-cluster".into(),
            node_id: "node-1".into(),
            format_version: FORMAT_VERSION,
            created_at: 1_700_000_000_000,
            snapshot_index: 0,
            snapshot_term: 0,
        }
    }

    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "arachne-meta-test-{}-{}",
            std::process::id(),
            n
        ));
        fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    #[test]
    fn encode_decode_roundtrip() {
        let meta = test_meta();
        let blob = encode_meta(&meta);
        let decoded = decode_meta(&blob).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn encode_decode_roundtrip_empty_strings() {
        let meta = Meta {
            cluster_id: String::new(),
            node_id: String::new(),
            format_version: 1,
            created_at: 0,
            snapshot_index: 0,
            snapshot_term: 0,
        };
        let blob = encode_meta(&meta);
        let decoded = decode_meta(&blob).unwrap();
        assert_eq!(decoded, meta);
    }

    /// The snapshot pointer is an optional payload extension (v0.2.10 M): a
    /// non-zero pointer round-trips, and a META written before the extension
    /// (payload ending at `created_at`) decodes as "no snapshot".
    #[test]
    fn snapshot_pointer_roundtrips_and_old_files_decode_as_none() {
        let mut meta = test_meta();
        meta.snapshot_index = 42;
        meta.snapshot_term = 7;
        let blob = encode_meta(&meta);
        assert_eq!(decode_meta(&blob).unwrap(), meta);

        // Truncate the payload back to the pre-extension layout and fix the CRC.
        let stale = Meta {
            snapshot_index: 0,
            snapshot_term: 0,
            ..meta.clone()
        };
        let mut body = encode_meta(&stale);
        // Drop the 16-byte pointer from the payload (the last 16 payload bytes,
        // which sit just before the 4-byte CRC).
        // Drop the trailing 16-byte pointer and re-stamp the header CRC (META's
        // CRC lives at offset 8, over the payload at 12.. — not trailing).
        let n = body.len();
        body.truncate(n - 16);
        let crc = crc32c(&body[12..]);
        body[8..12].copy_from_slice(&crc.to_le_bytes());
        let decoded = decode_meta(&body).expect("a pre-extension META must decode");
        assert_eq!(decoded.snapshot_index, 0);
        assert_eq!(decoded.snapshot_term, 0);
        assert_eq!(decoded.cluster_id, meta.cluster_id);
    }

    #[test]
    fn write_read_roundtrip() {
        let dir = temp_dir();
        let meta = test_meta();
        write_meta(&dir, &meta).unwrap();
        let read_back = read_meta(&dir).unwrap();
        assert_eq!(read_back, meta);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_overwrites_existing() {
        let dir = temp_dir();
        write_meta(&dir, &test_meta()).unwrap();
        let meta2 = Meta {
            cluster_id: "new-cluster".into(),
            node_id: "node-2".into(),
            format_version: FORMAT_VERSION,
            created_at: 2_000_000_000_000,
            snapshot_index: 0,
            snapshot_term: 0,
        };
        write_meta(&dir, &meta2).unwrap();
        let read_back = read_meta(&dir).unwrap();
        assert_eq!(read_back, meta2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_meta_returns_error() {
        let dir = temp_dir();
        write_meta(&dir, &test_meta()).unwrap();

        // Corrupt a byte in the middle of the file.
        let path = meta_path(&dir);
        let mut buf = fs::read(&path).unwrap();
        if buf.len() > 20 {
            buf[20] ^= 0xFF;
        }
        fs::write(&path, &buf).unwrap();

        let result = read_meta(&dir);
        assert!(result.is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn bad_magic_returns_error() {
        let dir = temp_dir();
        let meta = test_meta();
        let mut blob = encode_meta(&meta);
        // Corrupt the magic.
        blob[0] = 0x00;
        fs::write(meta_path(&dir), &blob).unwrap();
        assert!(matches!(read_meta(&dir), Err(MetaError::BadMagic)));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn too_short_returns_error() {
        let dir = temp_dir();
        fs::write(meta_path(&dir), b"short").unwrap();
        assert!(matches!(read_meta(&dir), Err(MetaError::TooShort)));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn format_version_is_one() {
        assert_eq!(FORMAT_VERSION, 1);
    }

    #[test]
    fn meta_path_is_in_data_dir() {
        let path = meta_path(Path::new("/tmp/arachne"));
        assert_eq!(path, PathBuf::from("/tmp/arachne/META"));
    }

    #[test]
    fn display_errors_are_non_empty() {
        assert!(!MetaError::BadMagic.to_string().is_empty());
        assert!(!MetaError::TooShort.to_string().is_empty());
        assert!(!MetaError::TruncatedField.to_string().is_empty());
        let e = MetaError::ChecksumMismatch {
            expected: 1,
            actual: 2,
        };
        assert!(e.to_string().contains("checksum"));
    }
}
