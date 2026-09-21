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
    // Bounded retry on `Timeout` only, for the same reason `m2_wal_faults`
    // retries: the write deadline is a latency bound, and this test is about
    // session bands, not latency — a loaded CI runner must not turn a correct
    // acceptance into a failure. Retrying the *same session* is exactly what
    // the idempotency machinery exists for: if the first attempt did land, the
    // state machine dedups it and the value assertions below still hold.
    let mut accepted = false;
    let mut last = None;
    for _ in 0..50 {
        match handle.propose_raw(write(b"v4"), CLIENT, SEQ).await {
            Ok(()) => {
                accepted = true;
                break;
            }
            Err(ArachneError::Timeout) => {
                last = Some(ArachneError::Timeout);
                tokio::time::sleep(core::time::Duration::from_millis(20)).await;
            }
            Err(e) => panic!("past the grace window the proposal must be accepted, got {e}"),
        }
    }
    assert!(
        accepted,
        "past the grace window the proposal is accepted again: {last:?}"
    );
    assert_eq!(
        handle.get_stale(b"k").await.expect("read"),
        Some(b"v1".to_vec()),
        "the session table must still dedup, so the effect stays once"
    );

    drop(handle);
    thread.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// R2: GC prunes exactly what the leader listed, and that is what makes
/// `max_sessions` recoverable — the reason the cap and GC had to land together.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_gc_prunes_and_relieves_the_session_cap() {
    const CAP: u64 = 4;
    let dir = temp_dir();
    let mut profile = profile();
    profile.max_sessions = CAP;
    let wal = WalStorage::open(
        &dir,
        WalOptions {
            cluster_id: "sessions-gc".into(),
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
        profile,
        metrics: Arc::clone(&metrics),
    };
    let clock = Arc::new(ManualClock::new(2_000_000));
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
    let runtime = runtime.with_session_clock(Arc::clone(&clock) as Arc<dyn arachne::Clock>);
    let thread: RuntimeThread = runtime.spawn_dedicated().expect("spawn");
    until_leader(&metrics).await;

    // Fill the table to the cap with distinct sessions.
    for client in 1..=CAP {
        handle
            .propose_raw(
                KvStateMachine::encode_put(client, 1, format!("k{client}").as_bytes(), b"v"),
                client,
                1,
            )
            .await
            .expect("the table is below the cap");
    }
    for _ in 0..200 {
        if metrics.session_count() >= CAP {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert_eq!(metrics.session_count(), CAP, "the table reached the cap");

    // A *new* session is turned away before it is proposed.
    match handle
        .propose_raw(
            KvStateMachine::encode_put(CAP + 1, 1, b"overflow", b"v"),
            CAP + 1,
            1,
        )
        .await
    {
        Err(ArachneError::SessionTableFull) => {}
        other => panic!("expected SessionTableFull at the cap, got {other:?}"),
    }

    // Past `ttl + grace` the leader collects what expired, and the cap lifts.
    clock.advance(TTL_MS + GRACE_MS + 10);
    for _ in 0..400 {
        if metrics.session_count() == 0 {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        metrics.session_count(),
        0,
        "GC must prune the expired sessions on every replica"
    );
    handle
        .propose_raw(
            KvStateMachine::encode_put(CAP + 2, 1, b"after-gc", b"v"),
            CAP + 2,
            1,
        )
        .await
        .expect("a new session fits again once GC has pruned");

    // GC prunes sessions, not data.
    assert_eq!(
        handle.get_stale(b"k1").await.expect("read"),
        Some(b"v".to_vec()),
        "garbage-collecting a session must not touch the key/value state"
    );

    drop(handle);
    thread.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// INV5 scenarios: S07 (retry storm), S10 (GC vs retry race), S14 (clock jump)
// ---------------------------------------------------------------------------

/// A single-node runtime with a manual clock, for the scenario tests below.
async fn single_node(
    tag: &str,
    max_sessions: u64,
) -> (
    arachne::client::Handle,
    Arc<Metrics>,
    Arc<ManualClock>,
    RuntimeThread,
    PathBuf,
) {
    let dir = std::env::temp_dir().join(format!(
        "arachne-sessions-{tag}-{}-{}",
        std::process::id(),
        DIR_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let mut profile = profile();
    profile.max_sessions = max_sessions;
    let wal = WalStorage::open(
        &dir,
        WalOptions {
            cluster_id: "sessions-scenario".into(),
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
        profile,
        metrics: Arc::clone(&metrics),
    };
    let clock = Arc::new(ManualClock::new(5_000_000));
    let (runtime, handle) = Runtime::new(config, wal, tx, rx, &logger()).expect("runtime");
    let runtime = runtime.with_session_clock(Arc::clone(&clock) as Arc<dyn arachne::Clock>);
    let thread = runtime.spawn_dedicated().expect("spawn");
    until_leader(&metrics).await;
    (handle, metrics, clock, thread, dir)
}

/// S07: a storm of concurrent retries of one session must settle on exactly one
/// effect — and stay there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s07_a_retry_storm_has_exactly_one_effect() {
    let (handle, _metrics, _clock, thread, dir) = single_node("s07", 0).await;
    const STORM: usize = 24;

    // Every attempt carries the *same* session and a different value.
    let mut attempts = Vec::new();
    for i in 0..STORM {
        let handle = handle.clone();
        attempts.push(tokio::spawn(async move {
            handle
                .propose_raw(
                    KvStateMachine::encode_put(CLIENT, SEQ, b"storm", format!("v{i}").as_bytes()),
                    CLIENT,
                    SEQ,
                )
                .await
        }));
    }
    let mut ok = 0usize;
    for attempt in attempts {
        if matches!(attempt.await.expect("task"), Ok(())) {
            ok += 1;
        }
    }
    assert_eq!(ok, STORM, "every attempt inside the TTL is accepted and deduped");

    // Whichever attempt landed first, the state is one of them and it is stable:
    // further retries must not move it.
    let settled = handle
        .get_stale(b"storm")
        .await
        .expect("read")
        .expect("one of the attempts applied");
    assert!(
        settled.starts_with(b"v"),
        "the value must come from the storm, got {settled:?}"
    );
    for _ in 0..10 {
        handle
            .propose_raw(
                KvStateMachine::encode_put(CLIENT, SEQ, b"storm", b"late"),
                CLIENT,
                SEQ,
            )
            .await
            .expect("a retry is accepted");
    }
    assert_eq!(
        handle.get_stale(b"storm").await.expect("read"),
        Some(settled),
        "a retry storm must have exactly one effect"
    );

    drop(handle);
    thread.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// S10: the race between GC and a retry. Before the window closes a retry is
/// refused and changes nothing; only after `ttl + grace` can a duplicate take
/// effect — which is exactly the bound INV5 asks for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s10_a_duplicate_can_only_take_effect_after_the_grace_window() {
    let (handle, metrics, clock, thread, dir) = single_node("s10", 0).await;

    handle
        .propose_raw(
            KvStateMachine::encode_put(CLIENT, SEQ, b"k", b"first"),
            CLIENT,
            SEQ,
        )
        .await
        .expect("the first write commits");

    // Just inside the grace window: refused, and nothing reaches the log.
    clock.advance(TTL_MS + GRACE_MS / 2);
    let applied_before = metrics.applied_index();
    match handle
        .propose_raw(
            KvStateMachine::encode_put(CLIENT, SEQ, b"k", b"second"),
            CLIENT,
            SEQ,
        )
        .await
    {
        Err(ArachneError::SessionExpired) => {}
        other => panic!("inside the grace window a retry must not re-execute, got {other:?}"),
    }
    assert_eq!(
        handle.get_stale(b"k").await.expect("read"),
        Some(b"first".to_vec())
    );
    assert_eq!(metrics.applied_index(), applied_before);

    // Past the window: GC prunes the session, and the duplicate is a new command
    // — the effect happens again, which is the documented cost of retrying past
    // `ttl + grace`, not a violation.
    clock.advance(GRACE_MS);
    for _ in 0..400 {
        if metrics.session_count() == 0 {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert_eq!(metrics.session_count(), 0, "GC pruned the expired session");
    handle
        .propose_raw(
            KvStateMachine::encode_put(CLIENT, SEQ, b"k", b"second"),
            CLIENT,
            SEQ,
        )
        .await
        .expect("past ttl+grace the command is accepted as a new session");
    assert_eq!(
        handle.get_stale(b"k").await.expect("read"),
        Some(b"second".to_vec()),
        "and only now can a duplicate take effect"
    );

    drop(handle);
    thread.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// S14: a large forward clock jump must not break anything — the session is
/// simply long expired, and the node keeps serving new sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s14_a_forward_clock_jump_is_survivable() {
    let (handle, metrics, clock, thread, dir) = single_node("s14", 0).await;

    handle
        .propose_raw(
            KvStateMachine::encode_put(CLIENT, SEQ, b"k", b"before"),
            CLIENT,
            SEQ,
        )
        .await
        .expect("the first write commits");

    // A jump far past ttl + grace (the manual clock is monotonic, so this is a
    // forward jump — the only kind the seam can produce).
    clock.advance(60 * 60 * 1_000);
    for _ in 0..400 {
        if metrics.session_count() == 0 {
            break;
        }
        tokio::time::sleep(core::time::Duration::from_millis(5)).await;
    }
    assert_eq!(metrics.session_count(), 0, "the jump expired everything");

    // The node is unharmed: the data is intact and a fresh session works.
    assert_eq!(
        handle.get_stale(b"k").await.expect("read"),
        Some(b"before".to_vec()),
        "a clock jump must not touch the data"
    );
    handle
        .propose_raw(
            KvStateMachine::encode_put(CLIENT + 1, 1, b"after", b"v"),
            CLIENT + 1,
            1,
        )
        .await
        .expect("a fresh session is accepted after the jump");

    drop(handle);
    thread.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
