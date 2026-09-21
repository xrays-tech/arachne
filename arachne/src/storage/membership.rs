//! The membership file: the durable [`ConfState`] that applying a ConfChange
//! produced (propsol v0.2.16 rev S).
//!
//! # Why this is not a WAL record
//!
//! Recovery auto-truncates a structural tear in the **last** segment only when
//! the torn record's estimated entry index lies beyond the committed window
//! (`estimated > commit + 1`). A torn record's *type* byte cannot be trusted —
//! it lives inside the CRC-protected body (propsol §5.5.3, the P2 lesson) — so
//! recovery must assume a torn tail could have been a newer `HardState` whose
//! term/vote is now lost; silently truncating that would permit a double vote
//! (INV7). Appending a new non-entry record after the committed entries would
//! therefore turn "crash while recording a membership change" — a routine
//! crash in the steady state, where `commit == last_index` — into a fail-start
//! that needs manual `force-recovery`.
//!
//! This file sidesteps that rule: it is replaced atomically, so a crash leaves
//! either the previous or the new membership, never a torn one, and the WAL
//! gains no new tail position.
//!
//! # Authority
//!
//! The log — and the snapshot covering its compacted prefix — is the authority.
//! This file is the seed that lets a restart skip re-applying ConfChange
//! entries already reflected in it (invariant I6: it is only ever written
//! *after* the entry it describes is durable, so it can never run ahead of the
//! log). A file at or below the snapshot index is ignored: the snapshot's
//! member table is authoritative for everything it covers (invariant I7), and
//! replay past [`crate::storage::Storage::conf_change_index`] re-derives
//! anything missing.
//!
//! # Layout
//!
//! ```text
//! [u32 magic][u32 format_version][u64 conf_change_index]
//! [member tables — identical to the snapshot's:
//!    u32 voters_len,  u64 voters…,
//!    u32 learners_len, u64 learners…]
//! [u32 crc32c]
//! ```
//!
//! The CRC covers every preceding byte. Writing is
//! `membership.tmp` → fsync → rename → fsync(dir), the same discipline as the
//! META file.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use arachne_seam::storage::ConfState;
use arachne_seam::types::LogIndex;

use crate::storage::crc32c::crc32c;
use crate::storage::format::{decode_conf_state, encode_conf_state};
use crate::storage::meta::fsync_dir;

/// The membership file's name inside the data directory.
pub const MEMBERSHIP_FILE: &str = "membership";

/// Magic identifying the membership file (`"ARMS"` little-endian).
const MEMBERSHIP_MAGIC: u32 = 0x534D_5241;

/// On-disk format version of the membership file.
const MEMBERSHIP_FORMAT_VERSION: u32 = 1;

/// A membership-file read or write failure.
#[derive(Debug)]
pub enum MembershipError {
    /// The underlying filesystem operation failed.
    Io(std::io::Error),
    /// The file exists but is not a valid membership file.
    ///
    /// A present-but-unreadable membership file is treated as damage, not as
    /// "absent": silently falling back to an older configuration would hide
    /// disk corruption. A *missing* file is normal (no ConfChange applied yet)
    /// and is reported as `Ok(None)`.
    Corrupt(String),
}

impl std::fmt::Display for MembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MembershipError::Io(e) => write!(f, "membership file I/O error: {e}"),
            MembershipError::Corrupt(detail) => write!(f, "corrupt membership file: {detail}"),
        }
    }
}

impl std::error::Error for MembershipError {}

impl From<std::io::Error> for MembershipError {
    fn from(e: std::io::Error) -> Self {
        MembershipError::Io(e)
    }
}

/// Where the membership file lives.
fn membership_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join(MEMBERSHIP_FILE)
}

/// Read the durable membership, if one was ever written.
///
/// Returns `Ok(None)` when the file does not exist.
///
/// # Errors
///
/// [`MembershipError::Corrupt`] if the file exists but its magic, version,
/// length or CRC is wrong; [`MembershipError::Io`] for any other I/O failure.
pub fn read_membership(
    data_dir: &Path,
) -> Result<Option<(LogIndex, ConfState)>, MembershipError> {
    let path = membership_path(data_dir);
    let buf = match fs::read(&path) {
        Ok(buf) => buf,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(MembershipError::Io(e)),
    };

    // magic + version + at least the index and two empty member tables + CRC.
    const MIN_LEN: usize = 4 + 4 + 8 + 4 + 4 + 4;
    if buf.len() < MIN_LEN {
        return Err(MembershipError::Corrupt(format!(
            "{} bytes is too short for a membership file",
            buf.len()
        )));
    }
    let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != MEMBERSHIP_MAGIC {
        return Err(MembershipError::Corrupt(format!(
            "bad magic {magic:#010x}"
        )));
    }
    let version = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if version != MEMBERSHIP_FORMAT_VERSION {
        return Err(MembershipError::Corrupt(format!(
            "unsupported version {version} (expected {MEMBERSHIP_FORMAT_VERSION})"
        )));
    }

    let body_len = buf.len() - 4;
    let expected = crc32c(&buf[..body_len]);
    let stored = u32::from_le_bytes([
        buf[body_len],
        buf[body_len + 1],
        buf[body_len + 2],
        buf[body_len + 3],
    ]);
    if expected != stored {
        return Err(MembershipError::Corrupt(format!(
            "crc mismatch (computed {expected:#010x}, stored {stored:#010x})"
        )));
    }

    let (index, voters, learners) =
        decode_conf_state(&buf[8..body_len]).map_err(|e| MembershipError::Corrupt(e.to_string()))?;
    Ok(Some((
        index,
        ConfState {
            voters,
            learners,
        },
    )))
}

/// Durably record the membership produced by the ConfChange entry at
/// `conf_change_index`.
///
/// The write is atomic: `membership.tmp` → fsync → rename → fsync(dir).
///
/// # Errors
///
/// [`MembershipError::Io`] if any step fails. The previous file (if any) is
/// still the durable membership when an error is returned.
pub fn write_membership(
    data_dir: &Path,
    conf_change_index: LogIndex,
    conf_state: &ConfState,
) -> Result<(), MembershipError> {
    let final_path = membership_path(data_dir);
    let tmp_path = data_dir.join("membership.tmp");

    let mut blob: Vec<u8> = Vec::with_capacity(
        16 + 8 * (conf_state.voters.len() + conf_state.learners.len()) + 4,
    );
    blob.extend_from_slice(&MEMBERSHIP_MAGIC.to_le_bytes());
    blob.extend_from_slice(&MEMBERSHIP_FORMAT_VERSION.to_le_bytes());
    blob.extend_from_slice(&encode_conf_state(
        conf_change_index,
        &conf_state.voters,
        &conf_state.learners,
    ));
    let crc = crc32c(&blob);
    blob.extend_from_slice(&crc.to_le_bytes());

    // Step 1: write the temp file.
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)?;
    file.write_all(&blob)?;

    // Step 2: fsync it, so the rename cannot expose a partial file.
    file.sync_all()?;
    drop(file);

    // Step 3: rename (atomic on POSIX).
    fs::rename(&tmp_path, &final_path)?;

    // Step 4: fsync the directory so the rename itself is durable.
    fsync_dir(data_dir)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "arachne-membership-test-{}-{}",
            std::process::id(),
            n
        ));
        fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    #[test]
    fn absent_file_reads_as_none() {
        let dir = temp_dir();
        assert!(read_membership(&dir).expect("absent file is not an error").is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn round_trips_and_latest_write_wins() {
        let dir = temp_dir();
        let first = ConfState {
            voters: vec![1, 2, 3],
            learners: vec![],
        };
        write_membership(&dir, 7, &first).expect("write");
        let (index, read) = read_membership(&dir)
            .expect("read")
            .expect("a written membership must be present");
        assert_eq!(index, 7);
        assert_eq!(read, first);

        // A second write replaces the first atomically.
        let second = ConfState {
            voters: vec![1, 2],
            learners: vec![3],
        };
        write_membership(&dir, 9, &second).expect("write 2");
        let (index, read) = read_membership(&dir)
            .expect("read 2")
            .expect("membership must still be present");
        assert_eq!(index, 9);
        assert_eq!(read, second);

        // No temp file is left behind by a successful write.
        assert!(!dir.join("membership.tmp").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_membership_round_trips() {
        let dir = temp_dir();
        let empty = ConfState {
            voters: vec![],
            learners: vec![],
        };
        write_membership(&dir, 0, &empty).expect("write");
        let (index, read) = read_membership(&dir)
            .expect("read")
            .expect("present");
        assert_eq!((index, read), (0, empty));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_corrupt_file_is_an_error_not_a_silent_fallback() {
        let dir = temp_dir();
        let conf = ConfState {
            voters: vec![1, 2, 3],
            learners: vec![],
        };
        write_membership(&dir, 4, &conf).expect("write");
        let path = membership_path(&dir);
        let mut bytes = fs::read(&path).expect("read raw");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff; // flip a CRC byte
        fs::write(&path, &bytes).expect("write raw");
        match read_membership(&dir) {
            Err(MembershipError::Corrupt(detail)) => {
                assert!(detail.contains("crc"), "unexpected detail: {detail}");
            }
            other => panic!("expected a CRC corruption error, got {other:?}"),
        }

        // A flipped member id (inside the CRC's coverage) is caught too.
        write_membership(&dir, 4, &conf).expect("write again");
        let mut bytes = fs::read(&path).expect("read raw");
        bytes[16] ^= 0xff;
        fs::write(&path, &bytes).expect("write raw");
        assert!(matches!(
            read_membership(&dir),
            Err(MembershipError::Corrupt(_))
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_stray_tmp_file_is_ignored() {
        // A crash between write and rename must leave the *previous* membership
        // as the durable one; the leftover temp file is not consulted.
        let dir = temp_dir();
        let conf = ConfState {
            voters: vec![1, 2, 3],
            learners: vec![],
        };
        write_membership(&dir, 4, &conf).expect("write");
        fs::write(dir.join("membership.tmp"), b"garbage from a crashed write")
            .expect("write tmp");
        let (index, read) = read_membership(&dir)
            .expect("a stray tmp file must not break the read")
            .expect("present");
        assert_eq!((index, read), (4, conf));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn short_and_bad_magic_files_are_rejected() {
        let dir = temp_dir();
        let path = membership_path(&dir);
        fs::write(&path, b"tiny").expect("write");
        assert!(matches!(
            read_membership(&dir),
            Err(MembershipError::Corrupt(_))
        ));

        // Long enough, but not a membership file.
        let mut blob = vec![0u8; 32];
        blob[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        fs::write(&path, &blob).expect("write");
        match read_membership(&dir) {
            Err(MembershipError::Corrupt(detail)) => assert!(detail.contains("magic")),
            other => panic!("expected a magic error, got {other:?}"),
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
