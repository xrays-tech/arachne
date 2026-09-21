//! WAL record format: framing, record types, and typed encode/decode.
//!
//! # Wire format
//!
//! Each record on disk is:
//!
//! ```text
//! [u32 len LE][u32 crc32c LE][u8 type][payload]
//! ```
//!
//! * `len` — the number of bytes **after** the 8-byte header, i.e.
//!   `1 + payload.len()` (the type byte plus the payload).
//! * `crc32c` — CRC32C over the **body** `[type][payload]` (the `len` field
//!   is validated separately against the actual buffer size).
//! * `type` — one of [`RecordType`].
//! * `payload` — the type-specific bytes (see [`encode_entry`],
//!   [`decode_entry`], [`encode_hard_state`], [`decode_hard_state`]).
//!
//! # Record-type set
//!
//! The record-type set is **FROZEN** within the WAL format major version
//! (currently [`crate::storage::meta::FORMAT_VERSION`]). Any change (adding,
//! removing, or redefining a type) requires a major bump + `format_version`
//! increment. `Meta = 0x03` is allocated but not yet encoded/decoded; decoding
//! a `Meta` record returns [`DecodeError::UnsupportedRecordType`].

use crate::storage::crc32c::crc32c;

/// Maximum size (in bytes) of a single record's body (`type` + `payload`).
///
/// Records with a `len` field exceeding this value are rejected as
/// [`DecodeError::BadLength`] (corruption guard against absurd lengths).
pub const MAX_RECORD_BYTES: u32 = 64 * 1024 * 1024; // 64 MiB

/// The kind of a WAL record.
///
/// The `u8` discriminants are part of the on-disk format and are frozen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// A replicated log entry.
    Entry = 0x01,
    /// A hard-state (term/vote/commit) record.
    HardState = 0x02,
    /// A META record (allocated but not yet implemented; decoding returns
    /// [`DecodeError::UnsupportedRecordType`]).
    Meta = 0x03,
}

impl TryFrom<u8> for RecordType {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(RecordType::Entry),
            0x02 => Ok(RecordType::HardState),
            0x03 => Ok(RecordType::Meta),
            _ => Err(DecodeError::UnknownRecordType(value)),
        }
    }
}

impl From<RecordType> for u8 {
    fn from(value: RecordType) -> Self {
        value as u8
    }
}

/// Errors that can occur when decoding a WAL record.
///
/// All variants are **non-panicking**: malformed input always yields an
/// `Err`, never a panic.
#[derive(Debug)]
pub enum DecodeError {
    /// The buffer is too short to contain a complete record header or body.
    TooShort,
    /// The `len` field is implausible (exceeds [`MAX_RECORD_BYTES`] or does
    /// not match the actual buffer size).
    BadLength,
    /// The CRC32C checksum does not match the body.
    ChecksumMismatch {
        /// The checksum stored in the record header.
        expected: u32,
        /// The checksum actually computed over the body.
        actual: u32,
    },
    /// The `type` byte does not correspond to any known [`RecordType`].
    UnknownRecordType(u8),
    /// The record type is known but not yet supported for decoding.
    UnsupportedRecordType(RecordType),
    /// Bytes remain after the record that do not form a valid next record.
    TrailingBytes,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::TooShort => write!(f, "buffer too short for a complete record"),
            DecodeError::BadLength => write!(f, "record length field is implausible"),
            DecodeError::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
                )
            }
            DecodeError::UnknownRecordType(byte) => {
                write!(f, "unknown record type byte: 0x{byte:02x}")
            }
            DecodeError::UnsupportedRecordType(t) => {
                write!(f, "record type {:?} is not yet supported for decoding", t)
            }
            DecodeError::TrailingBytes => {
                write!(f, "trailing bytes after record do not form a valid record")
            }
        }
    }
}

impl core::error::Error for DecodeError {}

// ---------------------------------------------------------------------------
// Generic record encode/decode
// ---------------------------------------------------------------------------

/// Encode a record from its type and payload bytes.
///
/// Returns the full on-disk representation: `[u32 len LE][u32 crc32c LE]`
/// followed by `[type][payload]`.
pub fn encode_record(record_type: RecordType, payload: &[u8]) -> Vec<u8> {
    let body_len = (1 + payload.len()) as u32;
    let mut buf = Vec::with_capacity(8 + body_len as usize);
    buf.extend_from_slice(&body_len.to_le_bytes());
    // Body is [type_byte][payload].
    let type_byte = u8::from(record_type);
    let mut body = Vec::with_capacity(1 + payload.len());
    body.push(type_byte);
    body.extend_from_slice(payload);
    let crc = crc32c(&body);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&body);
    buf
}

/// Decode a single record from a buffer.
///
/// The buffer must contain exactly one record (no trailing bytes). Returns
/// the record type and the raw payload (without the type byte).
///
/// # Errors
///
/// Returns [`DecodeError`] for any malformed input; never panics.
pub fn decode_record(buf: &[u8]) -> Result<(RecordType, Vec<u8>), DecodeError> {
    // Header is 8 bytes: [u32 len LE][u32 crc32c LE].
    if buf.len() < 8 {
        return Err(DecodeError::TooShort);
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let stored_crc = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);

    // The body must be exactly `len` bytes, and the buffer must be exactly
    // 8 + len bytes (no trailing bytes).
    if buf.len() != 8 + len as usize {
        return Err(DecodeError::BadLength);
    }
    if len > MAX_RECORD_BYTES {
        return Err(DecodeError::BadLength);
    }
    // A record with len < 1 cannot contain a type byte.
    if len < 1 {
        return Err(DecodeError::TooShort);
    }

    let body = &buf[8..];
    let actual_crc = crc32c(body);
    if actual_crc != stored_crc {
        return Err(DecodeError::ChecksumMismatch {
            expected: stored_crc,
            actual: actual_crc,
        });
    }

    let type_byte = body[0];
    let record_type = RecordType::try_from(type_byte)?;

    // Meta is allocated but not yet supported for decoding.
    if matches!(record_type, RecordType::Meta) {
        return Err(DecodeError::UnsupportedRecordType(record_type));
    }

    let payload = body[1..].to_vec();
    Ok((record_type, payload))
}

// ---------------------------------------------------------------------------
// Typed Entry encode/decode
// ---------------------------------------------------------------------------

/// Encode a log entry payload: `[u64 index LE][u64 term LE][u8 entry_type][data...]`.
pub fn encode_entry(index: u64, term: u64, entry_type: u8, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(17 + data.len());
    buf.extend_from_slice(&index.to_le_bytes());
    buf.extend_from_slice(&term.to_le_bytes());
    buf.push(entry_type);
    buf.extend_from_slice(data);
    buf
}

/// Decode a log entry payload.
///
/// # Errors
///
/// Returns [`DecodeError::TooShort`] if the payload is less than 17 bytes.
pub fn decode_entry(payload: &[u8]) -> Result<(u64, u64, u8, Vec<u8>), DecodeError> {
    if payload.len() < 17 {
        return Err(DecodeError::TooShort);
    }
    let index = le64_at(payload, 0)?;
    let term = le64_at(payload, 8)?;
    let entry_type = payload[16];
    let data = payload[17..].to_vec();
    Ok((index, term, entry_type, data))
}

/// Read a little-endian `u32` from `buf` at the given offset.
///
/// Returns [`DecodeError::TooShort`] if the buffer is too short.
#[inline(always)]
fn le32_at(buf: &[u8], offset: usize) -> Result<u32, DecodeError> {
    let end = offset.checked_add(4).ok_or(DecodeError::TooShort)?;
    if buf.len() < end {
        return Err(DecodeError::TooShort);
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&buf[offset..end]);
    Ok(u32::from_le_bytes(bytes))
}

/// Read a little-endian `u64` from `buf` at the given offset.
///
/// Returns [`DecodeError::TooShort`] if the buffer is too short.
#[inline(always)]
fn le64_at(buf: &[u8], offset: usize) -> Result<u64, DecodeError> {
    let end = offset.checked_add(8).ok_or(DecodeError::TooShort)?;
    if buf.len() < end {
        return Err(DecodeError::TooShort);
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buf[offset..end]);
    Ok(u64::from_le_bytes(bytes))
}

// ---------------------------------------------------------------------------
// Typed HardState encode/decode
// ---------------------------------------------------------------------------

/// Encode a hard-state payload: `[u64 term LE][u8 has_vote][u64 vote LE][u64 commit LE]`.
///
/// `has_vote` is `1` if `vote` is `Some(_)` (the vote value is stored), or
/// `0` if `vote` is `None` (the vote field is zeroed).
pub fn encode_hard_state(term: u64, vote: Option<u64>, commit: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(25);
    buf.extend_from_slice(&term.to_le_bytes());
    match vote {
        Some(v) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        None => {
            buf.push(0);
            buf.extend_from_slice(&0u64.to_le_bytes());
        }
    }
    buf.extend_from_slice(&commit.to_le_bytes());
    buf
}

/// Decode a hard-state payload.
///
/// # Errors
///
/// Returns [`DecodeError::TooShort`] if the payload is less than 25 bytes.
pub fn decode_hard_state(payload: &[u8]) -> Result<(u64, Option<u64>, u64), DecodeError> {
    if payload.len() < 25 {
        return Err(DecodeError::TooShort);
    }
    let term = le64_at(payload, 0)?;
    let has_vote = payload[8];
    let vote = if has_vote == 1 {
        Some(le64_at(payload, 9)?)
    } else {
        None
    };
    let commit = le64_at(payload, 17)?;
    Ok((term, vote, commit))
}

/// Encode a membership payload: the durable `ConfState` plus the log index of
/// the ConfChange entry that produced it (propsol v0.2.16 rev S).
///
/// Used by the membership file, not by a WAL record type.
///
/// Layout: `[u64 conf_change_index][u32 voters_len][u64 voters…]
/// [u32 learners_len][u64 learners…]`. The member tables are byte-identical to
/// the snapshot's, so recovery can share the reader.
pub fn encode_conf_state(conf_change_index: u64, voters: &[u64], learners: &[u64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + 8 * (voters.len() + learners.len()));
    buf.extend_from_slice(&conf_change_index.to_le_bytes());
    buf.extend_from_slice(&(voters.len() as u32).to_le_bytes());
    for v in voters {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf.extend_from_slice(&(learners.len() as u32).to_le_bytes());
    for l in learners {
        buf.extend_from_slice(&l.to_le_bytes());
    }
    buf
}

/// Read a length-prefixed `u64` id list starting at `offset`.
///
/// Returns the ids and the offset just past the list.
///
/// # Errors
///
/// [`DecodeError::TooShort`] if the buffer ends before the list does. A `len`
/// that is implausible for the remaining bytes is reported the same way: the
/// caller treats any malformed payload as corruption, so a distinct variant
/// would not change its behaviour.
fn read_id_list(payload: &[u8], offset: usize) -> Result<(Vec<u64>, usize), DecodeError> {
    let len = le32_at(payload, offset)? as usize;
    let mut cursor = offset + 4;
    let mut ids = Vec::with_capacity(len.min(1024));
    for _ in 0..len {
        ids.push(le64_at(payload, cursor)?);
        cursor += 8;
    }
    Ok((ids, cursor))
}

/// Decode a membership payload (the inverse of [`encode_conf_state`]).
///
/// # Errors
///
/// [`DecodeError::TooShort`] if the payload is truncated or its declared list
/// lengths exceed the remaining bytes.
pub fn decode_conf_state(payload: &[u8]) -> Result<(u64, Vec<u64>, Vec<u64>), DecodeError> {
    let conf_change_index = le64_at(payload, 0)?;
    let (voters, cursor) = read_id_list(payload, 8)?;
    let (learners, _) = read_id_list(payload, cursor)?;
    Ok((conf_change_index, voters, learners))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- RecordType ----

    #[test]
    fn record_type_discriminants() {
        assert_eq!(u8::from(RecordType::Entry), 0x01);
        assert_eq!(u8::from(RecordType::HardState), 0x02);
        assert_eq!(u8::from(RecordType::Meta), 0x03);
    }

    #[test]
    fn record_type_try_from_valid() {
        assert_eq!(RecordType::try_from(0x01).unwrap(), RecordType::Entry);
        assert_eq!(RecordType::try_from(0x02).unwrap(), RecordType::HardState);
        assert_eq!(RecordType::try_from(0x03).unwrap(), RecordType::Meta);
    }

    #[test]
    fn record_type_try_from_invalid() {
        assert!(matches!(
            RecordType::try_from(0x00),
            Err(DecodeError::UnknownRecordType(0x00))
        ));
        assert!(matches!(
            RecordType::try_from(0x7F),
            Err(DecodeError::UnknownRecordType(0x7F))
        ));
        assert!(matches!(
            RecordType::try_from(0xFF),
            Err(DecodeError::UnknownRecordType(0xFF))
        ));
    }

    // ---- encode_record / decode_record round-trip ----

    #[test]
    fn roundtrip_entry_empty_data() {
        let payload = encode_entry(1, 1, 0, b"");
        let record = encode_record(RecordType::Entry, &payload);
        let (rt, decoded) = decode_record(&record).unwrap();
        assert_eq!(rt, RecordType::Entry);
        let (index, term, etype, data) = decode_entry(&decoded).unwrap();
        assert_eq!(index, 1);
        assert_eq!(term, 1);
        assert_eq!(etype, 0);
        assert_eq!(data, b"");
    }

    #[test]
    fn roundtrip_entry_with_data() {
        let data = b"hello world".to_vec();
        let payload = encode_entry(42, 7, 1, &data);
        let record = encode_record(RecordType::Entry, &payload);
        let (rt, decoded) = decode_record(&record).unwrap();
        assert_eq!(rt, RecordType::Entry);
        let (index, term, etype, d) = decode_entry(&decoded).unwrap();
        assert_eq!(index, 42);
        assert_eq!(term, 7);
        assert_eq!(etype, 1);
        assert_eq!(d, data);
    }

    #[test]
    fn roundtrip_entry_large_payload() {
        let data = vec![0xAB; 1024 * 1024]; // 1 MiB
        let payload = encode_entry(999, 5, 2, &data);
        let record = encode_record(RecordType::Entry, &payload);
        let (rt, decoded) = decode_record(&record).unwrap();
        assert_eq!(rt, RecordType::Entry);
        let (index, term, _etype, d) = decode_entry(&decoded).unwrap();
        assert_eq!(index, 999);
        assert_eq!(term, 5);
        assert_eq!(d.len(), 1024 * 1024);
    }

    #[test]
    fn roundtrip_hard_state_vote_none() {
        let payload = encode_hard_state(5, None, 3);
        let record = encode_record(RecordType::HardState, &payload);
        let (rt, decoded) = decode_record(&record).unwrap();
        assert_eq!(rt, RecordType::HardState);
        let (term, vote, commit) = decode_hard_state(&decoded).unwrap();
        assert_eq!(term, 5);
        assert_eq!(vote, None);
        assert_eq!(commit, 3);
    }

    #[test]
    fn roundtrip_hard_state_vote_some() {
        let payload = encode_hard_state(10, Some(42), 7);
        let record = encode_record(RecordType::HardState, &payload);
        let (rt, decoded) = decode_record(&record).unwrap();
        assert_eq!(rt, RecordType::HardState);
        let (term, vote, commit) = decode_hard_state(&decoded).unwrap();
        assert_eq!(term, 10);
        assert_eq!(vote, Some(42));
        assert_eq!(commit, 7);
    }

    #[test]
    fn roundtrip_boundary_indices() {
        // u64::MAX index and term.
        let payload = encode_entry(u64::MAX, u64::MAX, 0, b"x");
        let record = encode_record(RecordType::Entry, &payload);
        let (_, decoded) = decode_record(&record).unwrap();
        let (index, term, _, data) = decode_entry(&decoded).unwrap();
        assert_eq!(index, u64::MAX);
        assert_eq!(term, u64::MAX);
        assert_eq!(data, b"x");
    }

    // ---- Corruption: checksum mismatch ----

    #[test]
    fn corruption_flip_bit_in_payload() {
        let payload = encode_entry(1, 1, 0, b"hello");
        let mut record = encode_record(RecordType::Entry, &payload);
        // Flip a bit in the payload region (after the 9-byte header+type).
        record[15] ^= 0x01;
        let err = decode_record(&record).unwrap_err();
        assert!(
            matches!(err, DecodeError::ChecksumMismatch { .. }),
            "expected ChecksumMismatch, got {err:?}"
        );
    }

    // ---- Truncation: every length from 0..full must be Err ----

    #[test]
    fn truncation_never_panics() {
        let payload = encode_entry(5, 3, 0, b"test data");
        let record = encode_record(RecordType::Entry, &payload);
        let full_len = record.len();
        for len in 0..full_len {
            let result = decode_record(&record[..len]);
            assert!(result.is_err(), "expected Err at truncation length {len}");
        }
        // At full length it must succeed.
        assert!(decode_record(&record).is_ok());
    }

    // ---- Bogus length field ----

    #[test]
    fn bogus_len_too_small() {
        // Build a valid record, then corrupt the len field to be too small.
        let payload = encode_entry(1, 1, 0, b"hi");
        let mut record = encode_record(RecordType::Entry, &payload);
        // Set len to 1 (body would be just the type byte, but buffer is longer).
        record[0..4].copy_from_slice(&1u32.to_le_bytes());
        let result = decode_record(&record);
        assert!(result.is_err());
    }

    #[test]
    fn bogus_len_too_large() {
        let payload = encode_entry(1, 1, 0, b"hi");
        let mut record = encode_record(RecordType::Entry, &payload);
        // Set len to a value larger than the actual body.
        record[0..4].copy_from_slice(&9999u32.to_le_bytes());
        let result = decode_record(&record);
        assert!(result.is_err());
    }

    #[test]
    fn len_exceeds_max_record_bytes() {
        // Build a buffer with a len field > MAX_RECORD_BYTES.
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_RECORD_BYTES + 1).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&[0x01]); // type byte
        let result = decode_record(&buf);
        // Buffer length is 9, but len says 64MiB+1, so BadLength.
        assert!(matches!(result, Err(DecodeError::BadLength)));
    }

    // ---- Unknown / unsupported type bytes ----

    #[test]
    fn unknown_type_byte_zero() {
        // Craft a record with type byte 0x00.
        let body = vec![0x00u8, 0xAA, 0xBB];
        let crc = crc32c(&body);
        let mut buf = Vec::new();
        buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&body);
        let err = decode_record(&buf).unwrap_err();
        assert!(matches!(err, DecodeError::UnknownRecordType(0x00)));
    }

    #[test]
    fn unknown_type_byte_0x7f() {
        let body = vec![0x7Fu8, 0xAA];
        let crc = crc32c(&body);
        let mut buf = Vec::new();
        buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&body);
        let err = decode_record(&buf).unwrap_err();
        assert!(matches!(err, DecodeError::UnknownRecordType(0x7F)));
    }

    #[test]
    fn meta_type_is_unsupported() {
        // Craft a valid CRC'd record with type 0x03 (Meta).
        let payload = vec![0x01, 0x02];
        let mut body = vec![0x03u8];
        body.extend_from_slice(&payload);
        let crc = crc32c(&body);
        let mut buf = Vec::new();
        buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&body);
        let err = decode_record(&buf).unwrap_err();
        assert!(matches!(
            err,
            DecodeError::UnsupportedRecordType(RecordType::Meta)
        ));
    }

    // ---- DecodeError Display ----

    #[test]
    fn decode_error_display() {
        assert!(!DecodeError::TooShort.to_string().is_empty());
        assert!(!DecodeError::BadLength.to_string().is_empty());
        let e = DecodeError::ChecksumMismatch {
            expected: 0xDEAD,
            actual: 0xBEEF,
        };
        assert!(e.to_string().contains("checksum mismatch"));
        assert!(!DecodeError::UnknownRecordType(0x42).to_string().is_empty());
        assert!(!DecodeError::TrailingBytes.to_string().is_empty());
    }

    // ---- decode_entry / decode_hard_state too short ----

    #[test]
    fn decode_entry_too_short() {
        assert!(matches!(
            decode_entry(&[0u8; 16]),
            Err(DecodeError::TooShort)
        ));
    }

    #[test]
    fn decode_hard_state_too_short() {
        assert!(matches!(
            decode_hard_state(&[0u8; 24]),
            Err(DecodeError::TooShort)
        ));
    }

    #[test]
    fn conf_state_round_trips() {
        let payload = encode_conf_state(42, &[1, 2, 3], &[9]);
        let (index, voters, learners) =
            decode_conf_state(&payload).expect("a freshly encoded payload must decode");
        assert_eq!(index, 42);
        assert_eq!(voters, vec![1, 2, 3]);
        assert_eq!(learners, vec![9]);

        // Empty membership (e.g. the very first ConfChange is still a valid
        // set) must round-trip too rather than decode as a truncated payload.
        let empty = encode_conf_state(0, &[], &[]);
        let (index, voters, learners) =
            decode_conf_state(&empty).expect("empty member tables must decode");
        assert_eq!((index, voters.len(), learners.len()), (0, 0, 0));
    }

    #[test]
    fn decode_conf_state_rejects_truncated_payloads() {
        let payload = encode_conf_state(7, &[1, 2], &[3]);
        // Every proper prefix is either a short header or claims more ids than
        // remain; both must be a hard decode error, never a silent short read.
        for cut in 0..payload.len() {
            assert!(
                matches!(
                    decode_conf_state(&payload[..cut]),
                    Err(DecodeError::TooShort)
                ),
                "truncating to {cut} bytes must be rejected"
            );
        }
        // Trailing bytes are ignored (the member table is the last field), but
        // that must not be mistaken for success on a truncated prefix.
        assert!(decode_conf_state(&payload).is_ok());
    }
}
