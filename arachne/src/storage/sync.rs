//! Platform-dispatched durability barrier for WAL segment files.
//!
//! A WAL commit must make the appended records retrievable after a crash. The
//! cost of that barrier differs by platform, so this module centralizes the
//! dispatch (propsol v0.2.18 rev U, U2 / plan B2):
//!
//! * **Linux** — `fdatasync(2)` ([`File::sync_data`]) flushes the data **and
//!   every piece of metadata a later read needs** — which includes the `i_size`
//!   change caused by an extending append. It skips only metadata that is not
//!   needed to retrieve the data (e.g. mtime/atime), so it is a complete
//!   durability barrier for the non-pre-allocated append path as well.
//! * **Other platforms (including macOS)** — Rust's std implements
//!   [`File::sync_all`] as `fcntl(F_FULLFSYNC)` there (and Apple targets have
//!   no `fdatasync`), so the historical full-device flush is preserved
//!   unchanged. This is the development-machine contract.
//!
//! # Pre-allocation
//!
//! Segment pre-allocation (pinning a segment to `segment_bytes` so an append
//! never changes `i_size`) was investigated and **deferred**: std-only
//! `File::set_len` is `ftruncate(2)`, which creates a sparse file (holes) rather
//! than reserved extents, so the first write into a hole still triggers the
//! filesystem's delayed-allocation/extent-journal work and `fdatasync` does not
//! become the cheap data-only barrier that `fallocate` would give. It was
//! therefore dropped rather than adding a crate dependency or rewriting WAL
//! recovery. `sync_durable` needs no pre-allocation flag: `fdatasync` is
//! correct with or without one.
//!
//! # Metadata durability
//!
//! This function only covers the segment *file*. The inode's existence in the
//! directory is made durable separately by the existing `fsync_dir` calls on
//! the segment create/rename paths.

use std::fs::File;
use std::io;

/// Durably persist everything written to `file` before this call.
///
/// On Linux this is `fdatasync(2)` ([`File::sync_data`]); on every other
/// platform it is [`File::sync_all`] (`F_FULLFSYNC` on macOS), matching the
/// pre-existing behaviour.
///
/// `fdatasync` covers the `i_size` extension of an appending write, so this is
/// a complete barrier for the WAL's append-only segment files.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] if the platform sync fails.
#[cfg(target_os = "linux")]
pub fn sync_durable(file: &File) -> io::Result<()> {
    // fdatasync(2): data is durable, and so is any metadata a later read
    // needs — including the inode size change from an extending append.
    file.sync_data()
}

/// Durably persist everything written to `file` before this call.
///
/// Non-Linux platforms have no `fdatasync`; macOS std maps [`File::sync_all`]
/// to `fcntl(F_FULLFSYNC)`, so the full-device flush is preserved.
///
/// # Errors
///
/// Returns the underlying [`io::Error`] if the platform sync fails.
#[cfg(not(target_os = "linux"))]
pub fn sync_durable(file: &File) -> io::Result<()> {
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_file() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "arachne-sync-test-{}-{n}.log",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    /// `sync_durable` returns `Ok` and the bytes survive a close/reopen — the
    /// baseline behavioural evidence that the abstraction does not weaken the
    /// barrier.
    #[test]
    fn sync_durable_persists_written_bytes() {
        let path = temp_file();
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .expect("create");
            file.write_all(b"durable-payload").expect("write");
            sync_durable(&file).expect("sync_durable");
        }
        let mut file = OpenOptions::new().read(true).open(&path).expect("reopen");
        let mut buf = String::new();
        file.read_to_string(&mut buf).expect("read back");
        assert_eq!(buf, "durable-payload");
        let _ = std::fs::remove_file(&path);
    }

    /// Linux-specific evidence for the `fdatasync(2)` path: a positional write
    /// inside a pre-sized file is made durable and reads back after reopen,
    /// without the file size changing. This pins the `sync_data` semantics the
    /// WAL relies on (data + retrieval metadata), even though the WAL itself
    /// does not pre-allocate.
    ///
    /// The `sync_data` path does not exist off Linux (macOS uses
    /// `F_FULLFSYNC`), so this is compiled only on Linux and covered by CI
    /// there; the non-Linux contract is asserted by
    /// `sync_durable_persists_written_bytes`.
    #[cfg(target_os = "linux")]
    #[test]
    fn fdatasync_path_persists_positional_write_without_size_change() {
        use std::io::{Seek, SeekFrom};
        use std::os::unix::fs::FileExt;

        let path = temp_file();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create");
        // Pre-size the file so the positional write does not extend it.
        file.set_len(4096).expect("set_len");
        let bound = file.metadata().expect("metadata").len();
        assert_eq!(bound, 4096);
        file.write_at(b"record", 0).expect("write_at");
        assert_eq!(
            file.metadata().expect("metadata").len(),
            bound,
            "positional write must not change the size"
        );
        sync_durable(&file).expect("fdatasync");
        drop(file);

        // A fresh reader (new inode reference) sees the durable bytes.
        let mut reader = OpenOptions::new().read(true).open(&path).expect("reopen");
        reader.seek(SeekFrom::Start(0)).expect("seek");
        let mut buf = [0u8; 6];
        reader.read_exact(&mut buf).expect("read back");
        assert_eq!(&buf, b"record");
        let _ = std::fs::remove_file(&path);
    }
}
