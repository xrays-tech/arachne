//! End-to-end: `ClusterConfig` profile / snapshot-threshold overrides reach the
//! facade assembly paths.
//!
//! These tests exercise the overrides through the public entry points rather
//! than the `open_storage` helper unit test:
//!
//! * peerless `Arachne::start` with a small threshold writes snapshot files;
//! * a large threshold leaves a small write set below the trigger;
//! * an invalid `ProfileConfig` is rejected with `InvalidConfig` before the
//!   data directory is created/opened;
//! * the peerless trailing-keep override changes segment retention (0 compacts,
//!   a huge window prevents pruning);
//! * `assemble_cluster` applies the same threshold override on the multi-node
//!   assembly path.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time/network imports there), so this file uses
//! `tokio::time` (allowed) and no `std::time` / `tokio::net`.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use arachne_kv::server::{Arachne, ArachneError, ClusterConfig, WalConfig};
use arachne_kv::storage::parse_segment_name;
use arachne_kv::storage::snapshot::parse_snapshot_file_name;
use arachne_kv::{Profile, ProfileConfig};

static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

/// The facade is a single global instance; serialize the tests that own it.
static FACADE_LOCK: Mutex<()> = Mutex::new(());

fn unique_tempdir(tag: &str) -> PathBuf {
    let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(env::temp_dir()).join(format!(
        "arachne-snap-e2e-{tag}-{0}-{seq}",
        std::process::id()
    ))
}

/// Snapshot indexes present in a data directory, sorted ascending.
fn snapshot_indexes(dir: &Path) -> Vec<u64> {
    let mut found: Vec<u64> = fs::read_dir(dir)
        .expect("read the data directory")
        .flatten()
        .filter_map(|entry| parse_snapshot_file_name(&entry.file_name().to_string_lossy()))
        .map(|(index, _term)| index)
        .collect();
    found.sort_unstable();
    found
}

/// Segment first-indexes present in a data directory, sorted ascending.
fn segment_indexes(dir: &Path) -> Vec<u64> {
    let mut found: Vec<u64> = fs::read_dir(dir)
        .expect("read the data directory")
        .flatten()
        .filter_map(|entry| parse_segment_name(&entry.file_name().to_string_lossy()).ok())
        .collect();
    found.sort_unstable();
    found
}

const MAX_ATTEMPTS: u32 = 400;

/// A large value keeps the write count (and therefore the fsync cost) low while
/// still crossing the tiny thresholds below several times.
const VALUE_BYTES: usize = 4096;
/// Small enough to fire after a few writes.
const SNAPSHOT_THRESHOLD: u64 = 16 * 1024;
/// Small enough that the same write set rolls several segments.
const SEGMENT_BYTES: u64 = 16 * 1024;
/// Enough writes to cross `SNAPSHOT_THRESHOLD` several times.
const SNAPSHOT_WRITES: u64 = 24;

async fn poll() {
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

fn value() -> Vec<u8> {
    vec![b'x'; VALUE_BYTES]
}

/// Poll the singleton until a linear read succeeds (i.e. it has self-elected).
async fn wait_for_leader() -> bool {
    for _ in 0..MAX_ATTEMPTS {
        if Arachne::get(b"health").await.is_ok() {
            return true;
        }
        poll().await;
    }
    false
}

/// Retry a facade write through transient leadership/back-pressure states.
async fn until_set(key: &[u8], value: &[u8]) {
    for _ in 0..MAX_ATTEMPTS {
        match Arachne::set(key, value).await {
            Ok(()) => return,
            Err(ArachneError::Timeout)
            | Err(ArachneError::Busy)
            | Err(ArachneError::QuorumUnavailable)
            | Err(ArachneError::NotLeader { .. }) => poll().await,
            Err(e) => panic!("unexpected write error: {e:?}"),
        }
    }
    panic!("write did not complete within the poll window");
}

async fn wait_for_snapshot(dir: &Path) -> Vec<u64> {
    for _ in 0..MAX_ATTEMPTS {
        let snapshots = snapshot_indexes(dir);
        if !snapshots.is_empty() {
            return snapshots;
        }
        poll().await;
    }
    snapshot_indexes(dir)
}

#[tokio::test]
async fn peerless_small_threshold_writes_a_snapshot() {
    let _guard = FACADE_LOCK.lock().unwrap();
    let dir = unique_tempdir("small");

    let cfg = ClusterConfig::single_node(1, &dir, WalConfig::default())
        .with_snapshot_threshold_bytes(SNAPSHOT_THRESHOLD);
    Arachne::start(cfg).expect("start with a small snapshot threshold");
    assert!(wait_for_leader().await, "singleton must self-elect");

    for i in 0..SNAPSHOT_WRITES {
        until_set(&key(i), &value()).await;
    }

    let snapshots = wait_for_snapshot(&dir).await;
    assert!(
        !snapshots.is_empty(),
        "a small threshold must produce at least one snapshot file"
    );

    Arachne::shutdown().expect("shutdown");
    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn peerless_large_threshold_does_not_snapshot_on_small_writes() {
    let _guard = FACADE_LOCK.lock().unwrap();
    let dir = unique_tempdir("large");

    let cfg = ClusterConfig::single_node(1, &dir, WalConfig::default())
        .with_snapshot_threshold_bytes(64 * 1024 * 1024);
    Arachne::start(cfg).expect("start with a large snapshot threshold");
    assert!(wait_for_leader().await, "singleton must self-elect");

    for i in 0..12u64 {
        until_set(&key(i), &value()).await;
    }
    // Give the actor several cycles to evaluate the trigger.
    for _ in 0..20 {
        poll().await;
    }

    assert!(
        snapshot_indexes(&dir).is_empty(),
        "small writes must stay below a 64 MiB threshold"
    );

    Arachne::shutdown().expect("shutdown");
    let _ = fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn invalid_profile_override_is_rejected_before_opening() {
    let _guard = FACADE_LOCK.lock().unwrap();
    let dir = unique_tempdir("invalid");

    let mut invalid = Profile::Lan.config();
    // `rpc_timeout_ms` must be strictly below `election_timeout_ms`.
    invalid.rpc_timeout_ms = invalid.election_timeout_ms;
    let cfg =
        ClusterConfig::single_node(1, &dir, WalConfig::default()).with_profile_config(invalid);

    match Arachne::start(cfg) {
        Err(ArachneError::InvalidConfig(message)) => {
            assert!(
                message.contains("invalid profile config"),
                "unexpected message: {message}"
            );
        }
        Err(e) => panic!("expected InvalidConfig, got {e:?}"),
        Ok(_) => panic!("expected InvalidConfig, got Ok"),
    }
    assert!(
        !dir.exists(),
        "an invalid config must not create/open the data directory"
    );
}

/// Start a peerless facade with a tiny threshold and segment size, write enough
/// to roll several segments and snapshot, then return the snapshot indexes and
/// the segment indexes left after compaction.
async fn peerless_segment_retention(tag: &str, keep_bytes: u64) -> (Vec<u64>, Vec<u64>) {
    let dir = unique_tempdir(tag);

    let mut profile: ProfileConfig = Profile::Lan.config();
    profile.snapshot_threshold_bytes = SNAPSHOT_THRESHOLD;
    profile.wal_trailing_keep_bytes = keep_bytes;
    profile.wal_segment_bytes = SEGMENT_BYTES;

    let cfg =
        ClusterConfig::single_node(1, &dir, WalConfig::default()).with_profile_config(profile);
    Arachne::start(cfg).expect("start with a segment-retention profile");
    assert!(wait_for_leader().await, "singleton must self-elect");

    for i in 0..SNAPSHOT_WRITES {
        until_set(&key(i), &value()).await;
    }
    let snapshots = wait_for_snapshot(&dir).await;
    assert!(!snapshots.is_empty(), "the tiny threshold must snapshot");

    // Compaction runs after each local snapshot; wait until either the prune is
    // visible (keep = 0) or the actor has had ample time (huge keep).
    for _ in 0..MAX_ATTEMPTS {
        if segment_indexes(&dir)
            .first()
            .is_some_and(|first| *first > 1)
        {
            break;
        }
        poll().await;
    }
    let segments = segment_indexes(&dir);

    Arachne::shutdown().expect("shutdown");
    let _ = fs::remove_dir_all(&dir);
    (snapshots, segments)
}

#[tokio::test]
async fn peerless_trailing_keep_e2e_controls_segment_retention() {
    let _guard = FACADE_LOCK.lock().unwrap();

    // keep = 0 is the escape hatch: compact everything the snapshot covers.
    let (zero_snapshots, zero_segments) = peerless_segment_retention("keep-zero", 0).await;
    assert!(!zero_snapshots.is_empty());
    assert!(
        zero_segments.first().is_some_and(|first| *first > 1),
        "keep = 0 must prune the oldest segment(s), found {zero_segments:?}"
    );

    // A huge window (larger than the whole test log) must prevent any pruning.
    let (huge_snapshots, huge_segments) =
        peerless_segment_retention("keep-huge", 1024 * 1024 * 1024).await;
    assert!(!huge_snapshots.is_empty());
    assert_eq!(
        huge_segments.first().copied(),
        Some(1),
        "a window larger than the log must retain the first segment, found {huge_segments:?}"
    );
}

#[cfg(feature = "transport-tonic")]
mod cluster {
    use super::*;

    use std::collections::HashMap;
    use std::net::SocketAddr;

    use arachne_kv::NodeId;
    use arachne_kv::client::Handle;
    use arachne_kv::server::assemble_cluster;

    fn prebind_port() -> SocketAddr {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind a free loopback port");
        listener.local_addr().expect("listener has an address")
    }

    async fn until_put(handle: &Handle, key: &[u8], value: &[u8]) {
        for _ in 0..MAX_ATTEMPTS {
            match handle.put(key, value).await {
                Ok(()) => return,
                Err(ArachneError::Timeout)
                | Err(ArachneError::Busy)
                | Err(ArachneError::QuorumUnavailable)
                | Err(ArachneError::NotLeader { .. }) => poll().await,
                Err(e) => panic!("unexpected write error: {e:?}"),
            }
        }
        panic!("write did not complete within the poll window");
    }

    #[tokio::test]
    async fn assemble_cluster_applies_snapshot_threshold_override() {
        let id = NodeId::new("n1");
        let port = prebind_port();
        let dir = unique_tempdir("cluster-snap");

        let mut profile = Profile::Lan.config();
        profile.snapshot_threshold_bytes = SNAPSHOT_THRESHOLD;

        let mut addresses = HashMap::new();
        addresses.insert(id.clone(), port);
        let cfg = ClusterConfig::member(
            "snap-e2e".to_string(),
            id.clone(),
            port,
            dir.clone(),
            vec![id.clone()],
            addresses,
        )
        .with_profile_config(profile);

        let node = assemble_cluster(cfg)
            .await
            .expect("assemble a single-member cluster node");

        let mut leader = false;
        for _ in 0..MAX_ATTEMPTS {
            if node.handle.get(b"health").await.is_ok() {
                leader = true;
                break;
            }
            poll().await;
        }
        assert!(leader, "the single-member cluster must self-elect");

        for i in 0..SNAPSHOT_WRITES {
            until_put(&node.handle, &key(i), &value()).await;
        }
        let snapshots = wait_for_snapshot(&dir).await;
        assert!(
            !snapshots.is_empty(),
            "assemble_cluster must apply the snapshot-threshold override"
        );

        node.tonic.shutdown().await;
        node.thread.shutdown();
        let _ = fs::remove_dir_all(&dir);
    }
}
