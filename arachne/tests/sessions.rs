//! Integration: session lifetime and idempotency (propsol §2.2 G1, §7, M3 ④⑤;
//! v0.2.15 R1).
//!
//! The state machine dedups by `(client_id, seq_no)` and always has. What R1
//! adds is a *lifetime* around that:
//!
//! * inside the TTL a retry is proposed and deduped — exactly once;
//! * past the TTL but inside the grace window it is answered `SessionExpired`
//!   ("result unknown") and **not proposed at all**, which is what bounds a
//!   duplicate's effect to `ttl + grace`;
//! * past both, this leader treats it as a new session again — still safe here
//!   because the state machine keeps the outcome (GC, which removes it, lands
//!   in R2).
//!
//! The retries go through the real actor with an explicit session
//! (`Handle::propose_raw`, feature `fault-injection`), because `put` mints a new
//! `seq_no` per call and so cannot express a retry.
#![cfg(feature = "fault-injection")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::runtime::{Runtime, RuntimeConfig, RuntimeThread};
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{ArachneError, Metrics, NodeId, Profile, ProfileConfig, TransportFactory};
use arachne_testsupport::{InMemoryTransportFactory, ManualClock};
use slog::{o, Drain, Logger};

const TTL_MS: u64 = 100;
const GRACE_MS: u64 = 500;
const CLIENT: u64 = 7;
const SEQ: u64 = 1;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-sessions-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn profile() -> ProfileConfig {
    ProfileConfig {
        heartbeat_interval_ms: 10,
        election_timeout_ms: 600,
        rpc_timeout_ms: 300,
        session_ttl_ms: TTL_MS,
        session_grace_period_ms: GRACE_MS,
        ..Profile::Lan.config()
    }
}

async fn until_leader(metrics: &Metrics) {
    for _ in 0..400 {
        if metrics.is_leader() {
            return;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    panic!("the single node must elect itself");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_bands_dedup_then_expire_then_forget() {
    let dir = temp_dir();
    let profile = profile();
    let wal = WalStorage::open(
        &dir,
        WalOptions {
            cluster_id: "sessions".into(),
            node_id: "n1".into(),
            config: WalConfig {
                fsync_policy: FsyncPolicy::Always,
                segment_bytes: 1 << 20,
            },
            created_at_millis: 0,
            fsync_observer: None,
        },
    )
    .expect("open wal");
    let factory = InMemoryTransportFactory::new();
    let (tx, rx) = factory.create(NodeId::from("n1"));
    let metrics = Arc::new(Metrics::new());
    let config = RuntimeConfig {
        self_raft_id: 1,
        self_node_id: NodeId::from("n1"),
        peers: HashMap::new(),
        addresses: HashMap::new(),
        raft: arachne::consensus::RaftNodeConfig::from_profile(&profile),
        profile: profile.clone(),
        metrics: Arc::clone(&metrics),
    };
    // A manual clock, so `ttl`/`grace` are driven deterministically.
    let clock = Arc::new(ManualClock::new(1_000_000));
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
    let runtime = runtime.with_session_clock(Arc::clone(&clock) as Arc<dyn arachne::Clock>);
    let thread: RuntimeThread = runtime.spawn_dedicated().expect("spawn");
    until_leader(&metrics).await;

    let write = |value: &'static [u8]| KvStateMachine::encode_put(CLIENT, SEQ, b"k", value);

    // 1. The write itself.
    handle
        .propose_raw(write(b"v1"), CLIENT, SEQ)
        .await
        .expect("the first proposal commits");
    assert_eq!(
        handle.get_stale(b"k").await.expect("read"),
        Some(b"v1".to_vec())
    );

    // 2. Inside the TTL: the retry is proposed and deduped. Proving that means
    //    sending a *different value* under the same session and requiring the
    //    original to survive — exactly-once, not last-write-wins.
    clock.advance(TTL_MS / 2);
    handle
        .propose_raw(write(b"v2"), CLIENT, SEQ)
        .await
        .expect("a retry inside the TTL is accepted (and deduped)");
    assert_eq!(
        handle.get_stale(b"k").await.expect("read"),
        Some(b"v1".to_vec()),
        "a retry inside the TTL must not re-apply the command"
    );

    // 3. Past the TTL, inside the grace window: `SessionExpired`, and — the part
    //    that matters — nothing is appended to the log for it. (The TTL boundary
    //    itself counts as live, so step past it rather than onto it.)
    clock.advance(TTL_MS + 10);
    let applied_before = metrics.applied_index();
    let commit_before = metrics.commit_index();
    match handle.propose_raw(write(b"v3"), CLIENT, SEQ).await {
        Err(ArachneError::SessionExpired) => {}
        other => panic!("expected SessionExpired inside the grace window, got {other:?}"),
    }
    tokio::time::sleep(core::time::Duration::from_millis(50)).await;
    assert_eq!(
        metrics.applied_index(),
        applied_before,
        "an expired retry must not reach the log"
    );
    assert_eq!(metrics.commit_index(), commit_before);
    assert_eq!(
        handle.get_stale(b"k").await.expect("read"),
        Some(b"v1".to_vec())
    );

    // 4. Past the grace window: this leader treats it as a fresh session, so the
    //    proposal is accepted again — and the state machine, which still holds
    //    the outcome (GC lands in R2), keeps the effect once.
    clock.advance(GRACE_MS + TTL_MS);
    handle
        .propose_raw(write(b"v4"), CLIENT, SEQ)
        .await
        .expect("past the grace window the proposal is accepted again");
    assert_eq!(
        handle.get_stale(b"k").await.expect("read"),
        Some(b"v1".to_vec()),
        "the session table must still dedup, so the effect stays once"
    );

    drop(handle);
    thread.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
