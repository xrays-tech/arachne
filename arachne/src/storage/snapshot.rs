//! The on-disk **snapshot file** format (propsol §5.5.4, v0.2.10 M).
//!
//! A snapshot is written as an independent file, `snapshot-<index>-<term>.snap`,
//! never inside the WAL (a snapshot exceeds the 64 MiB record cap, and a huge
//! record would slow replay) and never inside `META` (that file carries the
//! cluster identity only).
//!
//! # Layout
//!
//! ```text
//! [u32  magic LE]
//! [u32  format_version LE]
//! [u64  index LE]
//! [u64  term LE]
//! [u32  voters_len LE]   [u64 voters...]
//! [u32  learners_len LE] [u64 learners...]
//! [u64  data_len LE]     [data bytes...]
//! [u32  crc32c LE]        <- over every byte before this field
//! ```
//!
//! The CRC covers the whole file body (header, membership, and state-machine
//! bytes), so a torn or bit-flipped snapshot is rejected rather than restored.
//!
//! # Where it is used
//!
//! * creation — serialize the state machine, append the header, atomically
//!   replace the file (see `WalStorage::save_snapshot`), then compact the WAL;
//! * recovery — scan the data directory, take the valid snapshot with the
//!   largest `index` (falling back to an earlier one if the newest is
//!   corrupt) and replay the WAL tail from `index + 1` (propsol §5.5.3).

use arachne_seam::RaftId;
use arachne_seam::storage::{ConfState, Snapshot, SnapshotMeta};
use arachne_seam::types::{LogIndex, Term};

use crate::storage::crc32c::crc32c;
use crate::storage::meta::FORMAT_VERSION;

/// The fixed prefix of a snapshot file name.
pub const SNAPSHOT_PREFIX: &str = "snapshot-";
/// The fixed suffix of a snapshot file name.
pub const SNAPSHOT_SUFFIX: &str = ".snap";
/// Magic number identifying a valid snapshot file (`"ARAS"` in ASCII).
pub const SNAPSHOT_MAGIC: u32 = 0x41_52_41_53;

/// Errors from encoding/decoding a snapshot file.
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// The file is too short to contain even a header + CRC.
    TooShort,
    /// The file does not start with [`SNAPSHOT_MAGIC`].
    BadMagic,
    /// The file's `format_version` differs from [`FORMAT_VERSION`].
    BadFormatVersion {
        /// The version found in the file.
        found: u32,
    },
    /// The stored CRC32C does not match the computed one.
    ChecksumMismatch {
        /// The checksum stored in the file.
        expected: u32,
        /// The checksum computed over the body.
        actual: u32,
    },
    /// A length field points past the end of the file.
    Truncated,
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::TooShort => write!(f, "snapshot file is too short"),
            SnapshotError::BadMagic => write!(f, "snapshot file has an invalid magic number"),
            SnapshotError::BadFormatVersion { found } => write!(
                f,
                "snapshot format_version {found} != supported {FORMAT_VERSION}"
            ),
            SnapshotError::ChecksumMismatch { expected, actual } => write!(
                f,
                "snapshot checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            SnapshotError::Truncated => write!(f, "snapshot file has a truncated field"),
        }
    }
}

impl core::error::Error for SnapshotError {}

/// The file name for a snapshot at `(index, term)`.
pub fn snapshot_file_name(index: LogIndex, term: Term) -> String {
    format!("{SNAPSHOT_PREFIX}{index:020}-{term:020}{SNAPSHOT_SUFFIX}")
}

/// Parse `(index, term)` back out of a snapshot file name.
///
/// Returns `None` for any name that does not match the exact pattern, so a
/// stray file in the data directory is ignored rather than mis-read.
pub fn parse_snapshot_file_name(name: &str) -> Option<(LogIndex, Term)> {
    let body = name
        .strip_prefix(SNAPSHOT_PREFIX)?
        .strip_suffix(SNAPSHOT_SUFFIX)?;
    let (index_str, term_str) = body.split_once('-')?;
    if index_str.len() != 20 || term_str.len() != 20 {
        return None;
    }
    let index = index_str.parse::<LogIndex>().ok()?;
    let term = term_str.parse::<Term>().ok()?;
    Some((index, term))
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_ids(out: &mut Vec<u8>, ids: &[RaftId]) {
    put_u32(out, ids.len() as u32);
    for id in ids {
        put_u64(out, *id);
    }
}

/// Encode a snapshot into its on-disk byte representation (CRC appended).
pub fn encode_snapshot(snapshot: &Snapshot) -> Vec<u8> {
    let mut body = Vec::with_capacity(snapshot.data.len() + 64);
    put_u32(&mut body, SNAPSHOT_MAGIC);
    put_u32(&mut body, FORMAT_VERSION);
    put_u64(&mut body, snapshot.meta.index);
    put_u64(&mut body, snapshot.meta.term);
    put_ids(&mut body, &snapshot.meta.conf_state.voters);
    put_ids(&mut body, &snapshot.meta.conf_state.learners);
    put_u64(&mut body, snapshot.data.len() as u64);
    body.extend_from_slice(&snapshot.data);

    let crc = crc32c(&body);
    put_u32(&mut body, crc);
    body
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u32(&mut self) -> Result<u32, SnapshotError> {
        let end = self.offset + 4;
        let slice = self.bytes.get(self.offset..end).ok_or(SnapshotError::Truncated)?;
        self.offset = end;
        let mut buf = [0u8; 4];
        buf.copy_from_slice(slice);
        Ok(u32::from_le_bytes(buf))
    }

    fn u64(&mut self) -> Result<u64, SnapshotError> {
        let end = self.offset + 8;
        let slice = self.bytes.get(self.offset..end).ok_or(SnapshotError::Truncated)?;
        self.offset = end;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(slice);
        Ok(u64::from_le_bytes(buf))
    }

    fn ids(&mut self) -> Result<Vec<RaftId>, SnapshotError> {
        let count = self.u32()? as usize;
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(self.u64()?);
        }
        Ok(ids)
    }
}

/// Decode and validate a snapshot file.
///
/// Validates the magic, the format version, every length field, and the trailing
/// CRC (which covers the whole body), so a torn or corrupted snapshot is
/// rejected here rather than restored.
pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, SnapshotError> {
    if bytes.len() < 4 {
        return Err(SnapshotError::TooShort);
    }
    let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
    let mut crc_buf = [0u8; 4];
    crc_buf.copy_from_slice(crc_bytes);
    let expected = u32::from_le_bytes(crc_buf);
    let actual = crc32c(body);
    if expected != actual {
        return Err(SnapshotError::ChecksumMismatch { expected, actual });
    }

    let mut r = Reader::new(body);
    let magic = r.u32()?;
    if magic != SNAPSHOT_MAGIC {
        return Err(SnapshotError::BadMagic);
    }
    let format_version = r.u32()?;
    if format_version != FORMAT_VERSION {
        return Err(SnapshotError::BadFormatVersion {
            found: format_version,
        });
    }
    let index = r.u64()?;
    let term = r.u64()?;
    let voters = r.ids()?;
    let learners = r.ids()?;
    let data_len = r.u64()? as usize;
    let data = body
        .get(r.offset..r.offset + data_len)
        .ok_or(SnapshotError::Truncated)?
        .to_vec();
    // Trailing bytes after `data` would mean the layout is not what we think.
    if r.offset + data_len != body.len() {
        return Err(SnapshotError::Truncated);
    }

    Ok(Snapshot {
        meta: SnapshotMeta {
            index,
            term,
            conf_state: ConfState { voters, learners },
        },
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Snapshot {
        Snapshot {
            meta: SnapshotMeta {
                index: 7,
                term: 3,
                conf_state: ConfState {
                    voters: vec![1, 2, 3],
                    learners: vec![9],
                },
            },
            data: b"serialized state machine".to_vec(),
        }
    }

    #[test]
    fn round_trips() {
        let snap = sample();
        let bytes = encode_snapshot(&snap);
        assert_eq!(decode_snapshot(&bytes).expect("decode"), snap);
    }

    #[test]
    fn round_trips_empty_state_and_membership() {
        let snap = Snapshot {
            meta: SnapshotMeta {
                index: 0,
                term: 0,
                conf_state: ConfState::default(),
            },
            data: Vec::new(),
        };
        let bytes = encode_snapshot(&snap);
        assert_eq!(decode_snapshot(&bytes).expect("decode"), snap);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = encode_snapshot(&sample());
        bytes[0] ^= 0xFF;
        // The CRC now also mismatches; either rejection is a refusal to load.
        assert!(decode_snapshot(&bytes).is_err());
    }

    #[test]
    fn rejects_a_flipped_payload_bit() {
        let mut bytes = encode_snapshot(&sample());
        let last_data = bytes.len() - 5; // last byte before the CRC
        bytes[last_data] ^= 0x01;
        match decode_snapshot(&bytes) {
            Err(SnapshotError::ChecksumMismatch { .. }) => {}
            other => panic!("expected a checksum mismatch, got {other:?}"),
        }
    }

    #[test]
    fn rejects_truncation() {
        let bytes = encode_snapshot(&sample());
        // Every strict prefix is either too short or truncated.
        for cut in 0..bytes.len() {
            assert!(
                decode_snapshot(&bytes[..cut]).is_err(),
                "a {cut}-byte prefix must not decode"
            );
        }
    }

    #[test]
    fn rejects_a_wrong_format_version() {
        let snap = sample();
        let mut bytes = encode_snapshot(&snap);
        // Bump the version field (offset 4) and fix the CRC so the version check
        // is what rejects it.
        bytes[4..8].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        let (body, _) = bytes.split_at(bytes.len() - 4);
        let crc = crc32c(body);
        let n = bytes.len();
        bytes[n - 4..].copy_from_slice(&crc.to_le_bytes());
        match decode_snapshot(&bytes) {
            Err(SnapshotError::BadFormatVersion { found }) => {
                assert_eq!(found, FORMAT_VERSION + 1);
            }
            other => panic!("expected a version rejection, got {other:?}"),
        }
    }

    #[test]
    fn file_names_round_trip() {
        let name = snapshot_file_name(42, 7);
        assert_eq!(name, "snapshot-00000000000000000042-00000000000000000007.snap");
        assert_eq!(parse_snapshot_file_name(&name), Some((42, 7)));
    }

    #[test]
    fn foreign_file_names_are_ignored() {
        for name in [
            "snapshot-.snap",
            "snapshot-1-2.snap",
            "snapshot-00000000000000000001-2.snap",
            "snapshot-00000000000000000001-00000000000000000002.txt",
            "wal-00000000000000000001.log",
            "META",
            "",
        ] {
            assert_eq!(parse_snapshot_file_name(name), None, "name: {name}");
        }
    }
}
