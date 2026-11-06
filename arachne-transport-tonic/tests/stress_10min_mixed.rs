//! Sustained 10-minute mixed read/write, high-concurrency fault stress test on
//! a real tonic 3-node cluster.
//!
//! Default duration is 600 s; export `ARACHNE_STRESS_SECS` for a smoke run:
//!
//! ```text
//! cargo test -p arachne-kv-transport-tonic --test stress_10min_mixed -- --ignored --nocapture
//! ARACHNE_STRESS_SECS=30 cargo test -p arachne-kv-transport-tonic --test stress_10min_mixed -- --ignored --nocapture
//! ```
//!
//! Faults scale with the configured duration: a follower is killed at 40 % and
//! the then-current leader at 70 %. After the latter the remaining node is a
//! minority of one: linearizable reads/writes fail with transient errors while
//! local stale reads and `wait_applied` keep working.
//!
//! Two writers and two readers run per node (plus one head publisher). Writers
//! own disjoint 4,000-key shards; readers mix linearizable `get`, local
//! `get_stale`, 256-key stale `multi_get`, and the Hydra materializer shape
//! (`get_stale_with_index(head)` → local `wait_applied` → stale `multi_get`).
//!
//! No in-process peer handles are registered, so follower writes/redirects use
//! the real tonic `Forward` path. Writer/reader/publisher tasks for a killed
//! node are allowed to end with `ShuttingDown`; anything else fatal is a test
//! failure.
//!
//! NOTE (entropy gates): `tests/` is scanned by `scripts/check-entropy.sh`
//! (Gate C forbids real-time imports there), so this file uses
//! `core::time::Duration` and `tokio::time::Instant`, never `std::time`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arachne_kv::client::Handle;
use arachne_kv::consensus::RaftNodeConfig;
use arachne_kv::runtime::{Runtime, RuntimeConfig};
use arachne_kv::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne_kv::{
    ArachneError, Metrics, NodeId, Profile, ProfileConfig, ReadConsistency, TransportFactory,
};
use arachne_kv_transport_tonic::TonicTransportFactory;
use slog::{Drain, Logger, o};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

const NODES: usize = 3;
const KEY_COUNT: usize = 24_000;
const WRITERS_PER_NODE: usize = 2;
const READERS_PER_NODE: usize = 2;
const SHARD_LEN: usize = KEY_COUNT / (NODES * WRITERS_PER_NODE);
const READ_CHUNK: usize = 256;
const HEAD_KEY: &[u8] = b"head";
const PUBLISH_INTERVAL_SECS: u64 = 5;
const HYDRA_INTERVAL_MS: u64 = 2_000;
const SAMPLE_CAP: usize = 200_000;
const WRITE_BATCH: usize = 8;
const VALUE_BYTES: usize = 200;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-stress10min-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn node_id(i: usize) -> NodeId {
    NodeId::from(format!("n{}", i + 1))
}

fn profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 200,
        rpc_timeout_ms: 100,
        ..Profile::Lan.config()
    }
}

fn entity_key(i: usize) -> Vec<u8> {
    format!("entity/{i:05}").into_bytes()
}

/// A ~200 B value with the required `v:<key>:<seq>` prefix, padded to a fixed
/// width so every seed/write payload has the same shape and size.
fn data_value(key: &[u8], seq: u64) -> Vec<u8> {
    let mut value = Vec::with_capacity(VALUE_BYTES);
    value.extend_from_slice(b"v:");
    value.extend_from_slice(key);
    value.push(b':');
    value.extend_from_slice(seq.to_string().as_bytes());
    assert!(
        value.len() <= VALUE_BYTES,
        "the v:<key>:<seq> prefix must fit in {VALUE_BYTES} bytes"
    );
    value.resize(VALUE_BYTES, b'.');
    value
}

/// Parse and validate `v:<key>:<seq><padding>` strictly against `key`.
fn value_seq(key: &[u8], value: &[u8]) -> Option<u64> {
    if value.len() < 2 + key.len() + 1 || &value[..2] != b"v:" {
        return None;
    }
    let after_v = &value[2..];
    if after_v.len() < key.len() + 1 || &after_v[..key.len()] != key || after_v[key.len()] != b':' {
        return None;
    }
    let tail = &after_v[key.len() + 1..];
    let digits_end = tail
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(tail.len());
    if digits_end == 0 || !tail[digits_end..].iter().all(|byte| *byte == b'.') {
        return None;
    }
    core::str::from_utf8(&tail[..digits_end])
        .ok()?
        .parse::<u64>()
        .ok()
}

fn head_value(generation: u64) -> Vec<u8> {
    format!("gen-{generation:010}").into_bytes()
}

fn is_transient(error: &ArachneError) -> bool {
    matches!(
        error,
        ArachneError::Timeout
            | ArachneError::QuorumUnavailable
            | ArachneError::NotLeader { .. }
            | ArachneError::Busy
            | ArachneError::SessionExpired
    )
}

// ---- statistics -----------------------------------------------------------

#[derive(Clone, Copy)]
enum OpKind {
    Write,
    Publish,
    Get,
    GetStale,
    MultiGet,
    HeadRead,
    WaitApplied,
}

const OP_KIND_COUNT: usize = 7;
const OP_NAMES: [&str; OP_KIND_COUNT] = [
    "write",
    "publish",
    "get",
    "get_stale",
    "multi_get",
    "head_read",
    "wait_applied",
];

struct OpStats {
    samples: Vec<Mutex<Vec<u32>>>,
    counts: [AtomicU64; OP_KIND_COUNT],
    total_micros: [AtomicU64; OP_KIND_COUNT],
}

impl OpStats {
    fn new() -> Self {
        Self {
            samples: (0..OP_KIND_COUNT).map(|_| Mutex::new(Vec::new())).collect(),
            counts: core::array::from_fn(|_| AtomicU64::new(0)),
            total_micros: core::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn record(&self, kind: OpKind, elapsed: core::time::Duration) {
        let micros = elapsed.as_micros().min(u32::MAX as u128) as u32;
        let index = kind as usize;
        let previous = self.counts[index].fetch_add(1, Ordering::Relaxed);
        self.total_micros[index].fetch_add(micros as u64, Ordering::Relaxed);
        let mut samples = self.samples[index].lock().expect("stats mutex");
        if samples.len() < SAMPLE_CAP {
            samples.push(micros);
        } else {
            samples[previous as usize % SAMPLE_CAP] = micros;
        }
    }

    fn count(&self, kind: OpKind) -> u64 {
        self.counts[kind as usize].load(Ordering::Relaxed)
    }

    fn summary(&self, kind: OpKind) -> (u64, u32, u32, u64) {
        let index = kind as usize;
        let count = self.counts[index].load(Ordering::Relaxed);
        if count == 0 {
            return (0, 0, 0, 0);
        }
        let mut samples = self.samples[index].lock().expect("stats mutex").clone();
        samples.sort_unstable();
        let pick = |fraction: f64| {
            let position = ((samples.len() as f64 - 1.0) * fraction).round() as usize;
            samples[position]
        };
        let mean = self.total_micros[index].load(Ordering::Relaxed) / count;
        (count, pick(0.50), pick(0.99), mean)
    }
}

// ---- error accounting -----------------------------------------------------

const ERROR_KIND_COUNT: usize = 10;
const ERROR_NAMES: [&str; ERROR_KIND_COUNT] = [
    "not_leader",
    "quorum_unavailable",
    "timeout",
    "busy",
    "session_expired",
    "shutting_down",
    "unrecoverable",
    "invalid_argument",
    "unsupported",
    "other",
];

fn error_kind(error: &ArachneError) -> usize {
    match error {
        ArachneError::NotLeader { .. } => 0,
        ArachneError::QuorumUnavailable => 1,
        ArachneError::Timeout => 2,
        ArachneError::Busy => 3,
        ArachneError::SessionExpired => 4,
        ArachneError::ShuttingDown => 5,
        ArachneError::Unrecoverable(_) => 6,
        ArachneError::InvalidArgument(_) => 7,
        ArachneError::Unsupported(_) => 8,
        _ => 9,
    }
}

const PHASE_NAMES: [&str; 3] = ["healthy", "one_follower_down", "quorum_lost"];

struct ErrorTracker {
    per_phase: Mutex<[[u64; ERROR_KIND_COUNT]; 3]>,
    transient: [AtomicU64; 3],
    fatal: [AtomicU64; 3],
    fatal_total: AtomicU64,
    unrecoverable: AtomicU64,
    unexpected_shutdown: AtomicU64,
}

impl ErrorTracker {
    fn new() -> Self {
        Self {
            per_phase: Mutex::new([[0; ERROR_KIND_COUNT]; 3]),
            transient: core::array::from_fn(|_| AtomicU64::new(0)),
            fatal: core::array::from_fn(|_| AtomicU64::new(0)),
            fatal_total: AtomicU64::new(0),
            unrecoverable: AtomicU64::new(0),
            unexpected_shutdown: AtomicU64::new(0),
        }
    }

    fn record(&self, error: &ArachneError, phase: usize, killed: bool) {
        let phase = phase.min(2);
        let kind = error_kind(error);
        {
            let mut per_phase = self.per_phase.lock().expect("error mutex");
            per_phase[phase][kind] += 1;
        }
        if matches!(error, ArachneError::ShuttingDown) {
            if !killed {
                self.unexpected_shutdown.fetch_add(1, Ordering::Relaxed);
                self.fatal[phase].fetch_add(1, Ordering::Relaxed);
                self.fatal_total.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        if matches!(error, ArachneError::Unrecoverable(_)) {
            self.unrecoverable.fetch_add(1, Ordering::Relaxed);
            self.fatal[phase].fetch_add(1, Ordering::Relaxed);
            self.fatal_total.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if is_transient(error) {
            self.transient[phase].fetch_add(1, Ordering::Relaxed);
        } else {
            self.fatal[phase].fetch_add(1, Ordering::Relaxed);
            self.fatal_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn report(&self) {
        for phase in 0..3 {
            println!(
                "[stress] errors phase={} transient={} fatal={}",
                PHASE_NAMES[phase],
                self.transient[phase].load(Ordering::Relaxed),
                self.fatal[phase].load(Ordering::Relaxed),
            );
            let per_phase = self.per_phase.lock().expect("error mutex");
            for kind in 0..ERROR_KIND_COUNT {
                let count = per_phase[phase][kind];
                if count > 0 {
                    println!(
                        "[stress]   phase={} variant={} count={count}",
                        PHASE_NAMES[phase], ERROR_NAMES[kind]
                    );
                }
            }
        }
        println!(
            "[stress] errors total: fatal={} unrecoverable={} unexpected_shutting_down={}",
            self.fatal_total.load(Ordering::Relaxed),
            self.unrecoverable.load(Ordering::Relaxed),
            self.unexpected_shutdown.load(Ordering::Relaxed),
        );
    }
}

// ---- shared state ---------------------------------------------------------

#[derive(Clone)]
struct Shared {
    keys: Arc<Vec<Vec<u8>>>,
    last_acked_index: Arc<Vec<AtomicU64>>,
    last_acked_seq: Arc<Vec<AtomicU64>>,
    stats: Arc<OpStats>,
    errors: Arc<ErrorTracker>,
    phase: Arc<AtomicU8>,
    stop: Arc<AtomicBool>,
    killed: Arc<Vec<AtomicBool>>,
    alive: Arc<Vec<AtomicBool>>,
    corruptions: Arc<AtomicU64>,
    missing: Arc<AtomicU64>,
    applied_violations: Arc<AtomicU64>,
    key_writes: Arc<AtomicU64>,
}

/// Record `error` and report whether the caller should keep running. Killed
/// nodes' tasks may end with `ShuttingDown`; every other non-transient error is
/// fatal and ends the task (the final assertions reject it).
fn note_error(error: &ArachneError, node: usize, shared: &Shared) -> bool {
    let killed = shared.killed[node].load(Ordering::Relaxed);
    let phase = shared.phase.load(Ordering::Relaxed) as usize;
    shared.errors.record(error, phase, killed);
    if matches!(
        error,
        ArachneError::ShuttingDown | ArachneError::Unrecoverable(_)
    ) {
        return false;
    }
    is_transient(error)
}

fn record_value(key: &[u8], value: &[u8], shared: &Shared) -> Option<u64> {
    match value_seq(key, value) {
        Some(seq) => Some(seq),
        None => {
            shared.corruptions.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[stress] CORRUPT value for key {:?}: {:?}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value)
            );
            None
        }
    }
}

fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn pick_alive(alive: &[AtomicBool], cursor: &mut usize) -> Option<usize> {
    let count = alive.len();
    for _ in 0..count {
        let node = *cursor % count;
        *cursor = cursor.wrapping_add(1);
        if alive[node].load(Ordering::Relaxed) {
            return Some(node);
        }
    }
    None
}

// ---- workload tasks -------------------------------------------------------

/// One writer owns a contiguous 4,000-key shard, writes every key in a loop,
/// and remembers the last acknowledged log index and sequence per key.
async fn writer_task(node: usize, shard_start: usize, handle: Handle, shared: Shared) {
    let mut seqs = vec![0u64; SHARD_LEN];
    while !shared.stop.load(Ordering::Relaxed) {
        for batch_start in (0..SHARD_LEN).step_by(WRITE_BATCH) {
            if shared.stop.load(Ordering::Relaxed) {
                return;
            }
            let batch_len = (batch_start + WRITE_BATCH).min(SHARD_LEN) - batch_start;
            let next_seqs: Vec<u64> = (0..batch_len)
                .map(|offset| seqs[batch_start + offset] + 1)
                .collect();
            let values: Vec<Vec<u8>> = (0..batch_len)
                .map(|offset| {
                    data_value(
                        &shared.keys[shard_start + batch_start + offset],
                        next_seqs[offset],
                    )
                })
                .collect();
            let entries: Vec<(&[u8], &[u8])> = (0..batch_len)
                .map(|offset| {
                    let key_index = shard_start + batch_start + offset;
                    (shared.keys[key_index].as_slice(), values[offset].as_slice())
                })
                .collect();
            loop {
                if shared.stop.load(Ordering::Relaxed) {
                    return;
                }
                let started = Instant::now();
                match handle.multi_put_with_index(&entries).await {
                    Ok(index) => {
                        shared.stats.record(OpKind::Write, started.elapsed());
                        shared
                            .key_writes
                            .fetch_add(batch_len as u64, Ordering::Relaxed);
                        for offset in 0..batch_len {
                            let key_index = shard_start + batch_start + offset;
                            shared.last_acked_index[key_index].store(index, Ordering::Relaxed);
                            shared.last_acked_seq[key_index]
                                .store(next_seqs[offset], Ordering::Relaxed);
                            seqs[batch_start + offset] = next_seqs[offset];
                        }
                        break;
                    }
                    Err(error) => {
                        if !note_error(&error, node, &shared) {
                            return;
                        }
                        let backoff = if matches!(error, ArachneError::Timeout) {
                            10
                        } else {
                            20
                        };
                        tokio::time::sleep(core::time::Duration::from_millis(backoff)).await;
                    }
                }
            }
            // Aim each 8-key batch at a sustainable rate: 6 writers x ~50
            // batches/s is ~1,500 key-writes/s in the healthy phase, never the
            // multi-10k/s the raw batch path could sustain.
            tokio::time::sleep(core::time::Duration::from_millis(20)).await;
        }
    }
}

async fn read_chunk(
    node: usize,
    handle: &Handle,
    shared: &Shared,
    rng: &mut u64,
    previous_applied: &mut u64,
) -> bool {
    let range = KEY_COUNT - READ_CHUNK;
    let start = (next_rand(rng) % (range as u64 + 1)) as usize;
    let key_refs: Vec<&[u8]> = (start..start + READ_CHUNK)
        .map(|index| shared.keys[index].as_slice())
        .collect();
    let started = Instant::now();
    match handle.multi_get(&key_refs, ReadConsistency::Stale).await {
        Ok(result) => {
            shared.stats.record(OpKind::MultiGet, started.elapsed());
            if result.values.len() != READ_CHUNK {
                shared.corruptions.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "[stress] multi_get returned {} values for {READ_CHUNK} keys",
                    result.values.len()
                );
                return true;
            }
            for (offset, value) in result.values.iter().enumerate() {
                match value {
                    Some(value) => {
                        record_value(&shared.keys[start + offset], value, shared);
                    }
                    None => {
                        shared.missing.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            if result.applied_index < *previous_applied {
                shared.applied_violations.fetch_add(1, Ordering::Relaxed);
            } else {
                *previous_applied = result.applied_index;
            }
            true
        }
        Err(error) => note_error(&error, node, shared),
    }
}

/// The Hydra materializer shape: read the current head locally, wait for it to
/// apply locally, then materialize a 256-key chunk with one stale command.
async fn hydra_round(
    node: usize,
    handle: &Handle,
    shared: &Shared,
    rng: &mut u64,
    previous_applied: &mut u64,
) -> bool {
    let started = Instant::now();
    match handle.get_stale_with_index(HEAD_KEY).await {
        Ok(Some((_head, head_index))) => {
            shared.stats.record(OpKind::HeadRead, started.elapsed());
            let wait_started = Instant::now();
            match tokio::time::timeout(
                core::time::Duration::from_secs(3),
                handle.wait_applied(head_index, core::time::Duration::from_secs(2)),
            )
            .await
            {
                Ok(Ok(observed)) => {
                    shared
                        .stats
                        .record(OpKind::WaitApplied, wait_started.elapsed());
                    if observed < head_index || observed < *previous_applied {
                        shared.applied_violations.fetch_add(1, Ordering::Relaxed);
                    } else {
                        *previous_applied = observed;
                    }
                }
                Ok(Err(error)) => {
                    if !note_error(&error, node, shared) {
                        return false;
                    }
                }
                Err(_) => {
                    let error = ArachneError::Timeout;
                    if !note_error(&error, node, shared) {
                        return false;
                    }
                }
            }
            read_chunk(node, handle, shared, rng, previous_applied).await
        }
        Ok(None) => {
            shared.missing.fetch_add(1, Ordering::Relaxed);
            true
        }
        Err(error) => note_error(&error, node, shared),
    }
}

async fn reader_task(node: usize, task_id: usize, handle: Handle, shared: Shared) {
    let mut rng = (node as u64 + 1)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(task_id as u64 + 0xDEAD_BEEF);
    if rng == 0 {
        rng = 1;
    }
    let mut previous_applied = 0u64;
    let mut last_hydra = Instant::now();

    while !shared.stop.load(Ordering::Relaxed) {
        if last_hydra.elapsed() >= core::time::Duration::from_millis(HYDRA_INTERVAL_MS) {
            last_hydra = Instant::now();
            if !hydra_round(node, &handle, &shared, &mut rng, &mut previous_applied).await {
                return;
            }
        }

        match next_rand(&mut rng) % 3 {
            0 => {
                let key_index = (next_rand(&mut rng) % KEY_COUNT as u64) as usize;
                let started = Instant::now();
                match handle.get(&shared.keys[key_index]).await {
                    Ok(Some(value)) => {
                        shared.stats.record(OpKind::Get, started.elapsed());
                        record_value(&shared.keys[key_index], &value, &shared);
                    }
                    Ok(None) => {
                        shared.stats.record(OpKind::Get, started.elapsed());
                        shared.missing.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        if !note_error(&error, node, &shared) {
                            return;
                        }
                    }
                }
            }
            1 => {
                let key_index = (next_rand(&mut rng) % KEY_COUNT as u64) as usize;
                let started = Instant::now();
                match handle.get_stale(&shared.keys[key_index]).await {
                    Ok(Some(value)) => {
                        shared.stats.record(OpKind::GetStale, started.elapsed());
                        record_value(&shared.keys[key_index], &value, &shared);
                    }
                    Ok(None) => {
                        shared.stats.record(OpKind::GetStale, started.elapsed());
                        shared.missing.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        if !note_error(&error, node, &shared) {
                            return;
                        }
                    }
                }
            }
            _ => {
                if !read_chunk(node, &handle, &shared, &mut rng, &mut previous_applied).await {
                    return;
                }
            }
        }

        tokio::time::sleep(core::time::Duration::from_millis(1)).await;
    }
}

/// Publishes a new head generation every ~5 s from whichever node is currently
/// alive. After quorum loss it keeps retrying (and counting transient errors)
/// until the deadline.
async fn publisher_task(handles: Arc<Vec<Handle>>, shared: Shared) {
    let mut generation = 2u64;
    let mut cursor = 0usize;
    while !shared.stop.load(Ordering::Relaxed) {
        tokio::time::sleep(core::time::Duration::from_secs(PUBLISH_INTERVAL_SECS)).await;
        if shared.stop.load(Ordering::Relaxed) {
            return;
        }
        let value = head_value(generation);
        loop {
            if shared.stop.load(Ordering::Relaxed) {
                return;
            }
            if shared.stop.load(Ordering::Relaxed) {
                return;
            }
            let Some(node) = pick_alive(&shared.alive, &mut cursor) else {
                return;
            };
            let started = Instant::now();
            match handles[node].put_with_index(HEAD_KEY, &value).await {
                Ok(_index) => {
                    shared.stats.record(OpKind::Publish, started.elapsed());
                    generation += 1;
                    break;
                }
                Err(error) => {
                    let killed = shared.killed[node].load(Ordering::Relaxed);
                    if matches!(&error, ArachneError::ShuttingDown) && killed {
                        // The node died under this attempt; another node is
                        // still alive, so retry there.
                        let phase = shared.phase.load(Ordering::Relaxed) as usize;
                        shared.errors.record(&error, phase, true);
                        tokio::time::sleep(core::time::Duration::from_millis(2)).await;
                        continue;
                    }
                    if !note_error(&error, node, &shared) {
                        return;
                    }
                    tokio::time::sleep(core::time::Duration::from_millis(50)).await;
                }
            }
        }
    }
}

// ---- diagnostics ----------------------------------------------------------

fn rss_kib() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(resident_pages.saturating_mul(4096) / 1024)
}

fn print_checkpoint(label: &str, metrics: &[Arc<Metrics>]) {
    println!("[stress] checkpoint {label}:");
    for (node, node_metrics) in metrics.iter().enumerate() {
        println!(
            "[stress]   n{} is_leader={} leader_id={} commit_index={} applied_index={} \
             multi_get_commands={}",
            node + 1,
            node_metrics.is_leader(),
            node_metrics.leader_id(),
            node_metrics.commit_index(),
            node_metrics.applied_index(),
            node_metrics.multi_get_commands(),
        );
    }
}

async fn wait_stale_value(handle: &Handle, key: &[u8], expected: &[u8]) {
    for _ in 0..700 {
        match handle.get_stale(key).await {
            Ok(Some(value)) if value == expected => return,
            Ok(_) => tokio::time::sleep(core::time::Duration::from_millis(10)).await,
            Err(error) if is_transient(&error) => {
                tokio::time::sleep(core::time::Duration::from_millis(10)).await
            }
            Err(error) => panic!("get_stale failed with a non-transient error: {error}"),
        }
    }
    panic!(
        "node {} never served key {:?}",
        handle.node_id(),
        String::from_utf8_lossy(key)
    );
}

async fn until_multi_put(handle: &Handle, entries: &[(&[u8], &[u8])]) {
    let mut last = String::new();
    for _ in 0..600 {
        match handle.multi_put(entries).await {
            Ok(()) => return,
            Err(error) if is_transient(&error) => {
                last = error.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(error) => panic!("seed multi_put failed with a non-transient error: {error}"),
        }
    }
    panic!("seed multi_put never committed (last transient: {last})");
}

async fn until_put_with_index(handle: &Handle, key: &[u8], value: &[u8]) -> u64 {
    let mut last = String::new();
    for _ in 0..600 {
        match handle.put_with_index(key, value).await {
            Ok(index) => return index,
            Err(error) if is_transient(&error) => {
                last = error.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(error) => panic!("put_with_index failed with a non-transient error: {error}"),
        }
    }
    panic!("put_with_index never committed (last transient: {last})");
}

async fn final_get_stale_with_index(handle: &Handle, key: &[u8]) -> (Vec<u8>, u64) {
    let mut last = String::new();
    for _ in 0..200 {
        match handle.get_stale_with_index(key).await {
            Ok(Some(pair)) => return pair,
            Ok(None) => {
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(error) if is_transient(&error) => {
                last = error.to_string();
                tokio::time::sleep(core::time::Duration::from_millis(5)).await;
            }
            Err(error) => panic!("final get_stale_with_index failed: {error}"),
        }
    }
    panic!(
        "final get_stale_with_index never observed key {:?} (last transient: {last})",
        String::from_utf8_lossy(key)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "10-minute sustained mixed fault stress (run with --ignored --nocapture)"]
async fn sustained_mixed_reads_and_writes_survive_two_faults() {
    let duration_secs = std::env::var("ARACHNE_STRESS_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(600);
    assert!(
        duration_secs >= 10,
        "ARACHNE_STRESS_SECS must be at least 10 so both faults occur"
    );
    let duration = core::time::Duration::from_secs(duration_secs);

    let profile = profile();
    let cluster_id = "stress10min";

    let mut addresses: HashMap<NodeId, SocketAddr> = HashMap::new();
    for i in 0..NODES {
        addresses.insert(node_id(i), "127.0.0.1:0".parse().expect("addr"));
    }

    let factory = TonicTransportFactory::new(cluster_id, 1, 0, Vec::new(), addresses.clone());
    factory.start().await.expect("start transport");

    let metrics: Vec<Arc<Metrics>> = (0..NODES).map(|_| Arc::new(Metrics::new())).collect();
    let mut handles: Vec<Handle> = Vec::new();
    let mut runtime_tasks: Vec<JoinHandle<()>> = Vec::new();
    let mut dirs = Vec::new();

    for i in 0..NODES {
        let dir = temp_dir(&format!("n{}", i + 1));
        let wal = WalStorage::open(
            &dir,
            WalOptions {
                cluster_id: cluster_id.into(),
                node_id: format!("n{}", i + 1),
                config: WalConfig {
                    fsync_policy: FsyncPolicy::Always,
                    segment_bytes: WalConfig::default().segment_bytes,
                },
                created_at_millis: 0,
                fsync_observer: None,
            },
        )
        .expect("open wal");

        let me = node_id(i);
        let (tx, rx) = factory.create(me.clone());
        let peers = (0..NODES)
            .filter(|j| *j != i)
            .map(|j| ((j + 1) as u64, node_id(j)))
            .collect();
        let config = RuntimeConfig {
            self_raft_id: (i + 1) as u64,
            self_node_id: me.clone(),
            peers,
            addresses: addresses.clone(),
            raft: RaftNodeConfig::from_profile(&profile),
            profile: profile.clone(),
            metrics: Arc::clone(&metrics[i]),
        };
        let (runtime, handle) = match Runtime::new(config, wal, tx, rx, &logger()) {
            Ok(pair) => pair,
            Err(error) => panic!("runtime build failed: {error}"),
        };
        factory.set_command_sink(me, runtime.command_sink());
        runtime_tasks.push(tokio::spawn(runtime.run()));
        handles.push(handle);
        dirs.push(dir);
    }

    // Deliberately no `register_peer`: follower writes and linearizable reads
    // must use the real tonic `Forward` path.
    let handles = Arc::new(handles);
    let metrics = Arc::new(metrics);
    let keys: Arc<Vec<Vec<u8>>> = Arc::new((0..KEY_COUNT).map(entity_key).collect());
    let last_acked_index: Arc<Vec<AtomicU64>> =
        Arc::new((0..KEY_COUNT).map(|_| AtomicU64::new(0)).collect());
    let last_acked_seq: Arc<Vec<AtomicU64>> =
        Arc::new((0..KEY_COUNT).map(|_| AtomicU64::new(0)).collect());
    let shared = Shared {
        keys: Arc::clone(&keys),
        last_acked_index,
        last_acked_seq,
        stats: Arc::new(OpStats::new()),
        errors: Arc::new(ErrorTracker::new()),
        phase: Arc::new(AtomicU8::new(0)),
        stop: Arc::new(AtomicBool::new(false)),
        killed: Arc::new((0..NODES).map(|_| AtomicBool::new(false)).collect()),
        alive: Arc::new((0..NODES).map(|_| AtomicBool::new(true)).collect()),
        corruptions: Arc::new(AtomicU64::new(0)),
        missing: Arc::new(AtomicU64::new(0)),
        applied_violations: Arc::new(AtomicU64::new(0)),
        key_writes: Arc::new(AtomicU64::new(0)),
    };

    let rss_before = rss_kib();
    let mut workload: JoinSet<()> = JoinSet::new();

    let result = tokio::time::timeout(duration + core::time::Duration::from_secs(120), async {
        // ---- election --------------------------------------------------
        let mut leader = None;
        for _ in 0..800 {
            if let Some(position) = metrics.iter().position(|m| m.is_leader())
                && metrics.iter().all(|m| m.leader_id() != 0)
            {
                leader = Some(position);
                break;
            }
            tokio::time::sleep(core::time::Duration::from_millis(20)).await;
        }
        let initial_leader = leader.expect("the cluster must elect a known leader");
        println!("[stress] initial leader=n{}", initial_leader + 1);

        // ---- seed 24k entities + generation-1 head ---------------------
        let seed_started = Instant::now();
        for start in (0..KEY_COUNT).step_by(512) {
            let end = (start + 512).min(KEY_COUNT);
            let seed_values: Vec<Vec<u8>> = (start..end)
                .map(|index| data_value(&keys[index], 0))
                .collect();
            let entries: Vec<(&[u8], &[u8])> = (start..end)
                .map(|i| (keys[i].as_slice(), seed_values[i - start].as_slice()))
                .collect();
            until_multi_put(&handles[initial_leader], &entries).await;
        }
        let head_1_index =
            until_put_with_index(&handles[initial_leader], HEAD_KEY, &head_value(1)).await;
        println!(
            "[stress] seeded {KEY_COUNT} entities in {} batches in {:?}; head_index={head_1_index}",
            KEY_COUNT.div_ceil(512),
            seed_started.elapsed()
        );

        let sample: [usize; 8] = [0, 1024, 4096, 8192, 12288, 16384, 20480, KEY_COUNT - 1];
        for node in 0..NODES {
            wait_stale_value(&handles[node], HEAD_KEY, &head_value(1)).await;
            for &index in &sample {
                wait_stale_value(&handles[node], &keys[index], &data_value(&keys[index], 0)).await;
            }
        }
        println!(
            "[stress] all 3 nodes converged on head + {} sampled entities",
            sample.len()
        );
        print_checkpoint("before-workload", &metrics);

        // ---- spawn writers, readers, publisher -------------------------
        for node in 0..NODES {
            for writer in 0..WRITERS_PER_NODE {
                let shard_start = (node * WRITERS_PER_NODE + writer) * SHARD_LEN;
                workload.spawn(writer_task(
                    node,
                    shard_start,
                    handles[node].clone(),
                    shared.clone(),
                ));
            }
            for reader in 0..READERS_PER_NODE {
                workload.spawn(reader_task(
                    node,
                    reader,
                    handles[node].clone(),
                    shared.clone(),
                ));
            }
        }
        workload.spawn(publisher_task(Arc::clone(&handles), shared.clone()));

        let workload_start = Instant::now();
        let follower_fault_at = workload_start + duration.mul_f64(0.4);
        let leader_fault_at = workload_start + duration.mul_f64(0.7);
        let deadline = workload_start + duration;

        // ---- 40 %: kill a follower ------------------------------------
        let now = Instant::now();
        if follower_fault_at > now {
            tokio::time::sleep(follower_fault_at - now).await;
        }
        let current_leader = (0..NODES)
            .find(|node| shared.alive[*node].load(Ordering::Relaxed) && metrics[*node].is_leader())
            .expect("a live leader must exist before the follower fault");
        let killed_follower = (0..NODES)
            .find(|node| shared.alive[*node].load(Ordering::Relaxed) && *node != current_leader)
            .expect("a live follower must exist before the follower fault");
        assert_ne!(killed_follower, current_leader);
        shared.killed[killed_follower].store(true, Ordering::SeqCst);
        shared.alive[killed_follower].store(false, Ordering::SeqCst);
        runtime_tasks[killed_follower].abort();
        shared.phase.store(1, Ordering::SeqCst);
        println!(
            "[stress] fault 40%: killed follower n{} (leader n{} stays)",
            killed_follower + 1,
            current_leader + 1
        );
        print_checkpoint("after-follower-fault", metrics.as_slice());

        // ---- 70 %: kill the current leader -----------------------------
        let now = Instant::now();
        if leader_fault_at > now {
            tokio::time::sleep(leader_fault_at - now).await;
        }
        let mut live_leader = None;
        for _ in 0..250 {
            if let Some(node) = (0..NODES).find(|node| {
                shared.alive[*node].load(Ordering::Relaxed) && metrics[*node].is_leader()
            }) {
                live_leader = Some(node);
                break;
            }
            tokio::time::sleep(core::time::Duration::from_millis(20)).await;
        }
        let killed_leader = live_leader.expect("a live leader must exist before the leader fault");
        shared.killed[killed_leader].store(true, Ordering::SeqCst);
        shared.alive[killed_leader].store(false, Ordering::SeqCst);
        runtime_tasks[killed_leader].abort();
        shared.phase.store(2, Ordering::SeqCst);
        let alive_after: Vec<usize> = (0..NODES)
            .filter(|node| shared.alive[*node].load(Ordering::Relaxed))
            .collect();
        assert_eq!(
            alive_after.len(),
            1,
            "the second fault must leave a minority of one"
        );
        let survivor = alive_after[0];
        assert_ne!(survivor, killed_follower);
        assert_ne!(survivor, killed_leader);
        println!(
            "[stress] fault 70%: killed leader n{}; lone survivor n{} (minority, no quorum)",
            killed_leader + 1,
            survivor + 1
        );
        print_checkpoint("after-leader-fault", metrics.as_slice());
        // ---- run until the deadline, then stop and drain --------------
        let now = Instant::now();
        if deadline > now {
            tokio::time::sleep(deadline - now).await;
        }
        shared.stop.store(true, Ordering::SeqCst);
        let actual_duration = workload_start.elapsed();

        let drain = tokio::time::timeout(core::time::Duration::from_secs(15), async {
            while let Some(joined) = workload.join_next().await {
                if let Err(error) = joined
                    && !error.is_cancelled()
                {
                    panic!("a workload task panicked: {error}");
                }
            }
        })
        .await;
        if drain.is_err() {
            workload.abort_all();
            while workload.join_next().await.is_some() {}
            panic!("workload tasks did not stop within 15 s of the deadline");
        }
        println!("[stress] workload stopped after {actual_duration:?} (configured {duration:?})");
        print_checkpoint("after-workload", metrics.as_slice());

        // ---- settle and verify the lone survivor ----------------------
        let survivor_handle = &handles[survivor];
        let mut applied = metrics[survivor].applied_index();
        let mut stable_since = Instant::now();
        for _ in 0..200 {
            tokio::time::sleep(core::time::Duration::from_millis(50)).await;
            let next = metrics[survivor].applied_index();
            if next != applied {
                applied = next;
                stable_since = Instant::now();
            } else if stable_since.elapsed() >= core::time::Duration::from_secs(1) {
                break;
            }
        }
        let survivor_applied = metrics[survivor].applied_index();
        println!(
            "[stress] survivor n{} settled at applied_index={survivor_applied} \
                 (commit_index={})",
            survivor + 1,
            metrics[survivor].commit_index()
        );

        // No acknowledged write visible below the survivor's watermark may
        // have been lost.
        let mut acked_checked = 0u64;
        for key_index in 0..KEY_COUNT {
            let ack_index = shared.last_acked_index[key_index].load(Ordering::Relaxed);
            if ack_index == 0 || ack_index > survivor_applied {
                continue;
            }
            let ack_seq = shared.last_acked_seq[key_index].load(Ordering::Relaxed);
            let (value, _origin) =
                final_get_stale_with_index(survivor_handle, &keys[key_index]).await;
            let seq = value_seq(&keys[key_index], &value).unwrap_or_else(|| {
                panic!(
                    "survivor returned an invalid value for acknowledged key {:?}",
                    String::from_utf8_lossy(&keys[key_index])
                )
            });
            assert!(
                seq >= ack_seq,
                "survivor lost acknowledged write for key {:?}: saw seq {seq}, acked {ack_seq} \
                     (write index {ack_index} <= applied {survivor_applied})",
                String::from_utf8_lossy(&keys[key_index])
            );
            acked_checked += 1;
        }
        println!("[stress] acknowledged-write check: {acked_checked} keys verified");

        // The whole 24k entity set must still be readable and valid on the
        // survivor (bounded recovery/settle retry).
        let mut verified_keys = 0usize;
        for attempt in 0..300 {
            let mut all_valid = true;
            let mut seen = 0usize;
            for start in (0..KEY_COUNT).step_by(512) {
                let end = (start + 512).min(KEY_COUNT);
                let key_refs: Vec<&[u8]> =
                    (start..end).map(|index| keys[index].as_slice()).collect();
                let result = survivor_handle
                    .multi_get(&key_refs, ReadConsistency::Stale)
                    .await
                    .unwrap_or_else(|error| panic!("final survivor multi_get failed: {error}"));
                assert_eq!(
                    result.values.len(),
                    end - start,
                    "final survivor multi_get returned the wrong arity"
                );
                for (offset, value) in result.values.iter().enumerate() {
                    match value {
                        Some(value) => {
                            if value_seq(&keys[start + offset], value).is_some() {
                                seen += 1;
                            } else {
                                all_valid = false;
                                shared.corruptions.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        None => {
                            all_valid = false;
                            shared.missing.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            if all_valid && seen == KEY_COUNT {
                verified_keys = seen;
                break;
            }
            if attempt % 10 == 0 {
                println!("[stress] final multi_get retry {attempt}: {seen}/{KEY_COUNT} valid keys");
            }
            tokio::time::sleep(core::time::Duration::from_millis(100)).await;
        }
        assert_eq!(
            verified_keys, KEY_COUNT,
            "the survivor must serve every one of the {KEY_COUNT} entities"
        );
        println!("[stress] final entity check: {verified_keys}/{KEY_COUNT} keys valid");

        // ---- report ---------------------------------------------------
        print_checkpoint("final", metrics.as_slice());
        shared.errors.report();
        for kind in [
            OpKind::Write,
            OpKind::Publish,
            OpKind::Get,
            OpKind::GetStale,
            OpKind::MultiGet,
            OpKind::HeadRead,
            OpKind::WaitApplied,
        ] {
            let (count, p50, p99, mean) = shared.stats.summary(kind);
            println!(
                "[stress] op={} count={count} p50={p50}us p99={p99}us mean={mean}us",
                OP_NAMES[kind as usize]
            );
        }

        let writes = shared.key_writes.load(Ordering::Relaxed);
        let publishes = shared.stats.count(OpKind::Publish);
        let reads = shared.stats.count(OpKind::Get)
            + shared.stats.count(OpKind::GetStale)
            + shared.stats.count(OpKind::MultiGet);
        let multi_gets = shared.stats.count(OpKind::MultiGet);
        let seconds = actual_duration.as_secs_f64().max(0.001);
        let total_ops = writes + publishes + reads;
        println!(
            "[stress] totals: writes={writes} publishes={publishes} reads={reads} \
                 multi_get_calls={multi_gets} total_ops={total_ops} ops/s={:.1} \
                 write_ops/s={:.1}",
            total_ops as f64 / seconds,
            (writes + publishes) as f64 / seconds,
        );

        let rss_after = rss_kib();
        match (rss_before, rss_after) {
            (Some(before), Some(after)) => println!(
                "[stress] rss before={} KiB ({:.1} MiB) after={} KiB ({:.1} MiB) delta={} KiB",
                before,
                before as f64 / 1024.0,
                after,
                after as f64 / 1024.0,
                after as i64 - before as i64,
            ),
            _ => println!("[stress] rss: unavailable"),
        }

        assert_eq!(
            shared.corruptions.load(Ordering::Relaxed),
            0,
            "no read may return a value belonging to another key"
        );
        assert_eq!(
            shared.missing.load(Ordering::Relaxed),
            0,
            "every seeded entity must be present to every live reader"
        );
        assert_eq!(
            shared.applied_violations.load(Ordering::Relaxed),
            0,
            "per-node applied_index observations must be monotonic/coherent"
        );
        assert_eq!(
            shared.errors.unrecoverable.load(Ordering::Relaxed),
            0,
            "no Unrecoverable errors are allowed"
        );
        assert_eq!(
            shared.errors.fatal_total.load(Ordering::Relaxed),
            0,
            "only transient errors are allowed (fatal errors are test failures)"
        );
        assert_eq!(
            shared.errors.unexpected_shutdown.load(Ordering::Relaxed),
            0,
            "ShuttingDown is only expected from killed nodes' tasks"
        );
        println!(
            "[stress] PASS: survivor n{} served all checks",
            survivor + 1
        );
    })
    .await;

    // If the outer timeout fired, the timed future was dropped; make sure no
    // detached workload tasks survive.
    workload.abort_all();
    while workload.join_next().await.is_some() {}
    for task in &runtime_tasks {
        task.abort();
    }
    tokio::time::sleep(core::time::Duration::from_millis(100)).await;
    factory.shutdown().await;
    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }

    result.expect("the mixed stress test must finish within duration + 120 s");
}
