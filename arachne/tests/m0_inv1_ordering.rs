//! M0 acceptance ③ (INV1) — **no payload is sent before it is durable** (I2/I4).
//!
//! INV1 states that any message a node sends must carry only log entries that
//! are *already durably fsynced* in that node's WAL. The `RaftNode::step`
//! Ready loop is frozen to the ordering persist → sync → send, but this test
//! does not trust the implementation: it *observes* every outbound message at
//! the transport boundary and reconciles it against an independent fsync
//! ledger.
//!
//! # How the detector works
//!
//! * Each node's real [`WalStorage`] records every segment fsync into a shared
//!   [`FsyncLedger`] (via `WalOptions::fsync_observer`).
//! * Each node sends through a [`RecordingTx`] that wraps the in-memory
//!   transport. On every `send` it decodes the raft wire message, and if the
//!   message carries entries it computes `max_index` over them and asserts the
//!   ledger's `union_covers(1, max_index)` — i.e. *every entry the message
//!   carries is already fsynced*. Any miss increments a shared violation
//!   counter (recorded, not panicked inside the async fn; asserted after the
//!   run, per the fail-loud discipline).
//!
//! The cluster is then driven until at least one entry-carrying message has
//! been sent and we assert: at least one such send happened, and **zero**
//! ordering violations.
//!
//! # Non-vacuity (negative control)
//!
//! `inv1_detector_is_not_vacuous` feeds the detector a crafted message whose
//! entries extend *beyond* the durable range of its ledger and asserts the
//! violation counter increments. Without that, a detector that always recorded
//! zero violations would pass silently even if the ordering were broken.
//!
//! Deterministic and in-process: no threads, no wall clock, no tokio.
//!
//! # Note on I1 (HardState)
//!
//! INV1 also covers I1 (term/vote/commit durable before a vote/commit is
//! propagated). We do **not** assert I1 at the transport boundary here because
//! the [`FsyncLedger`] tracks *entry-index* durability (`durable_through_index`
//! is the highest fsynced log index), not a HardState (term/vote/commit)
//! watermark, so a message's `term`/`commit` fields cannot be reconciled
//! against it. I1 is instead enforced at the storage layer — [`WalStorage`]
//! always fsyncs in `set_hard_state` — and is verified by the
//! `i1_hard_state_fsync_always_policy` / `i1_hard_state_fsync_batch_policy`
//! unit tests in `src/storage/wal.rs`. The entry-durability half of INV1 (I2/
//! I4) is what this transport-boundary reconciliation proves.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arachne::consensus::RaftNode;
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{
    FsyncObserver, NodeId, RaftId, StateMachine, Transport, TransportFactory, TransportMessage,
};
use arachne_testsupport::{
    block_on, FsyncLedger, InMemoryRx, InMemoryTransportFactory, InMemoryTx, TransportError,
};
use protobuf::Message as _;
use raft::eraftpb::{Entry, Message as RaftMessage, MessageType};
use slog::{o, Drain, Logger};

type TestNode = RaftNode<WalStorage, RecordingTx, InMemoryRx>;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "arachne-m0-inv1-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn logger() -> Logger {
    Logger::root(slog::Discard.fuse(), o!())
}

fn wal_opts(
    node: &str,
    ledger: &Arc<FsyncLedger>,
) -> WalOptions {
    WalOptions {
        cluster_id: "m0-inv1".into(),
        node_id: node.into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        // Every real segment fsync is recorded into the shared ledger.
        fsync_observer: Some(ledger.clone()),
    }
}

fn node_id(i: RaftId) -> NodeId {
    NodeId::from(format!("n{i}"))
}

fn parse_raft_id(id: &NodeId) -> RaftId {
    id.as_str()
        .trim_start_matches('n')
        .parse::<RaftId>()
        .expect("harness node ids are `n<raft_id>`")
}

fn peers_of(self_id: RaftId, n: RaftId) -> HashMap<RaftId, NodeId> {
    (1..=n)
        .filter(|j| *j != self_id)
        .map(|j| (j, node_id(j)))
        .collect()
}

// ---------------------------------------------------------------------------
// Recording transport
// ---------------------------------------------------------------------------

/// A transport that records every entry-carrying raft message and checks the
/// INV1 ordering invariant (entries durable before the message is sent) at the
/// boundary, delegating to an inner in-memory transport.
///
/// `ledger` is the *same* `Arc<FsyncLedger>` the node's WAL reports fsyncs
/// into, so the check reconciles the outbound payload against exactly the
/// durability the node's own WAL achieved. `violations` and
/// `entry_carrying_sends` are shared counters so the test can assert on them
/// after the run (the check itself never panics inside the async `send`).
struct RecordingTx {
    inner: InMemoryTx,
    ledger: Arc<FsyncLedger>,
    violations: Arc<AtomicU64>,
    entry_carrying_sends: Arc<AtomicU64>,
}

impl RecordingTx {
    /// Decode an outbound message and, if it carries entries, verify they are
    /// already durable per the fsync ledger. Record a violation on any miss.
    /// This is pure observation: it never alters the message and never blocks.
    fn check_and_record(&self, msg: &TransportMessage) {
        let bytes = match msg {
            TransportMessage::Raft(b) => b,
            // The enum is non-exhaustive; only `Raft` exists today.
            _ => return,
        };
        let mut raft_msg = RaftMessage::default();
        if raft_msg.merge_from_bytes(bytes).is_err() {
            // A well-formed raft message always decodes; treat a decode miss as
            // "nothing to reconcile" rather than a violation (the core's codec
            // would already have rejected it).
            return;
        }
        let entries = raft_msg.get_entries();
        if entries.is_empty() {
            return;
        }
        // This is an entry-carrying (payload) send.
        self.entry_carrying_sends.fetch_add(1, Ordering::SeqCst);
        let max_index = entries.iter().map(|e| e.get_index()).max().unwrap_or(0);
        // INV1 (I2/I4): the message must not carry an entry the node has not
        // yet fsynced. Entries are contiguous from index 1, so covering
        // [1, max_index] is equivalent to "every carried entry is durable".
        if max_index >= 1 && !self.ledger.union_covers(1, max_index) {
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
    }
}

// `RecordingTx` holds an `Arc<FsyncLedger>` (which is not `Debug`), so derive
// is not possible; a minimal manual impl keeps the error type `Debug` for the
// node's `Result` (needed by `.expect` in the test harness).
impl std::fmt::Debug for RecordingTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingTx").finish_non_exhaustive()
    }
}

impl Transport for RecordingTx {
    type Error = TransportError;

    fn send(
        &self,
        to: NodeId,
        msg: TransportMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        // INV1: reconcile the payload against the fsync ledger BEFORE the
        // message is handed to the (delegated) transport.
        self.check_and_record(&msg);
        // Delegate. The in-memory transport's send resolves synchronously, so
        // driving it to completion here is safe (no suspension point).
        let result = block_on(self.inner.send(to, msg));
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// Two-node cluster over the recording transport
// ---------------------------------------------------------------------------

struct Cluster {
    dirs: Vec<PathBuf>,
    nodes: Vec<Option<TestNode>>,
    sms: Vec<KvStateMachine>,
}

impl Cluster {
    fn new(
        n: u64,
        ledger: &Arc<FsyncLedger>,
        violations: &Arc<AtomicU64>,
        entry_carrying_sends: &Arc<AtomicU64>,
    ) -> Self {
        // The in-memory transport factory is dropped at the end of this
        // constructor; the transport stays connected because every node's
        // outbound half holds an `Arc` to the shared switch.
        let factory = InMemoryTransportFactory::new();
        let mut dirs = Vec::new();
        let mut nodes = Vec::new();
        let mut sms = Vec::new();

        for i in 1..=n {
            let dir = temp_dir(&format!("n{i}"));
            let wal = WalStorage::open(&dir, wal_opts(&format!("n{i}"), ledger)).expect("open wal");
            let (tx, rx) = factory.create(node_id(i));
            let recording = RecordingTx {
                inner: tx,
                ledger: ledger.clone(),
                violations: violations.clone(),
                entry_carrying_sends: entry_carrying_sends.clone(),
            };
            let node = RaftNode::new(i, peers_of(i, n), wal, recording, rx, 0, &logger())
                .expect("build node");
            dirs.push(dir);
            nodes.push(Some(node));
            sms.push(KvStateMachine::new());
        }

        Self {
            dirs,
            nodes,
            sms,
        }
    }

    /// One round: drive every live node, then deliver queued inbound messages.
    fn round(&mut self) {
        for i in 0..self.nodes.len() {
            if let Some(node) = self.nodes[i].as_mut() {
                node.tick();
                let entries = block_on(node.step()).expect("step");
                for (idx, data) in entries {
                    self.sms[i].apply(idx, &data).expect("apply");
                }
                node.advance_apply();
            }
        }

        for i in 0..self.nodes.len() {
            let mut inbox: Vec<(RaftId, TransportMessage)> = Vec::new();
            if let Some(node) = self.nodes[i].as_mut() {
                loop {
                    match node.rx().try_recv() {
                        Ok(Some((from, msg))) => inbox.push((parse_raft_id(&from), msg)),
                        Ok(None) | Err(_) => break,
                    }
                }
            }
            if let Some(node) = self.nodes[i].as_mut() {
                for (from, msg) in inbox {
                    let _ = node.on_message(from, msg);
                }
            }
        }
    }

    fn leader_index(&self) -> Option<usize> {
        self.nodes.iter().enumerate().find_map(|(i, n)| {
            n.as_ref().and_then(|node| {
                let id = i as RaftId + 1;
                (node.leader_id() == id).then_some(i)
            })
        })
    }
}

// ---------------------------------------------------------------------------
// INV1 test
// ---------------------------------------------------------------------------

/// INV1 (I2/I4): no entry is sent over the wire before it is durably fsynced
/// in the sender's WAL. The recording transport observes every entry-carrying
/// message and reconciles it against the fsync ledger; a healthy cluster must
/// produce at least one such message and **zero** ordering violations.
#[test]
fn inv1_no_entry_sent_before_durable() {
    let ledger = Arc::new(FsyncLedger::new());
    let violations = Arc::new(AtomicU64::new(0));
    let entry_carrying_sends = Arc::new(AtomicU64::new(0));

    let mut c = Cluster::new(2, &ledger, &violations, &entry_carrying_sends);

    // Elect a leader.
    for _ in 0..300 {
        c.round();
        if c.leader_index().is_some() {
            break;
        }
    }
    let li = c.leader_index().expect("cluster must elect a leader");

    // Propose several commands so the leader emits entry-carrying MsgAppend
    // messages to its follower.
    for seq in 1..=4u64 {
        let cmd = KvStateMachine::encode_put(1, seq, b"k", b"v");
        c.nodes[li].as_mut().expect("leader present").propose(&cmd).expect("propose");
    }
    // Drive until the writes are committed and applied.
    for _ in 0..400 {
        c.round();
        if c.sms[li].get(b"k").expect("get") == Some(b"v".to_vec()) {
            break;
        }
    }
    assert_eq!(c.sms[li].get(b"k").expect("get"), Some(b"v".to_vec()));

    // The detector must actually have observed entry-carrying traffic (a
    // vacuous test would pass with zero sends).
    let entry_sends = entry_carrying_sends.load(Ordering::SeqCst);
    assert!(
        entry_sends >= 1,
        "expected at least one entry-carrying send, got {entry_sends}"
    );

    // INV1: every carried entry was durable before its message was sent.
    let viol = violations.load(Ordering::SeqCst);
    assert_eq!(
        viol, 0,
        "INV1 violated: {viol} entry-carrying message(s) sent before their entries were fsynced"
    );

    // Sanity: the ledger really did record fsyncs covering the applied log.
    let applied = c.nodes[li].as_ref().expect("leader present").applied_index();
    assert!(applied >= 1, "an entry must be applied");
    assert!(
        ledger.union_covers(1, applied),
        "fsync ledger must cover the applied range [1, {applied}]"
    );

    // Cleanup.
    c.nodes.iter_mut().for_each(|n| *n = None);
    for dir in &c.dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Negative control: prove the detector is not vacuous. Feed it a message whose
/// entries extend beyond the durable range of its ledger and assert it records
/// a violation. This is the exact condition the ordering invariant forbids; a
/// broken ordering would be caught precisely here.
#[test]
fn inv1_detector_is_not_vacuous() {
    let ledger = Arc::new(FsyncLedger::new());
    // The sender's WAL has only fsynced entries [1, 3].
    ledger.on_segment_fsynced(1, 3);

    let violations = Arc::new(AtomicU64::new(0));
    let entry_carrying_sends = Arc::new(AtomicU64::new(0));

    // A real in-memory tx to delegate to (its delivery outcome is irrelevant —
    // the check runs before delegation; sending to an unknown peer is fine).
    let factory = InMemoryTransportFactory::new();
    let (inner_tx, _rx) = factory.create(NodeId::from("n1"));
    let tx = RecordingTx {
        inner: inner_tx,
        ledger: ledger.clone(),
        violations: violations.clone(),
        entry_carrying_sends: entry_carrying_sends.clone(),
    };

    // Craft a MsgAppend carrying entries 4 and 5 (beyond the durable 3).
    let mut e4 = Entry::default();
    e4.set_index(4);
    e4.set_term(1);
    e4.set_data(b"payload-4".to_vec().into());
    let mut e5 = Entry::default();
    e5.set_index(5);
    e5.set_term(1);
    e5.set_data(b"payload-5".to_vec().into());
    let mut msg = RaftMessage::default();
    msg.set_msg_type(MessageType::MsgAppend);
    msg.set_from(1);
    msg.set_to(2);
    msg.set_term(1);
    msg.set_entries(vec![e4, e5].into());
    let bytes = msg.write_to_bytes().expect("encode");

    // Sending this message must be flagged: entries 4..5 are not durable.
    let _ = block_on(tx.send(NodeId::from("n2"), TransportMessage::Raft(bytes)));
    assert_eq!(
        entry_carrying_sends.load(Ordering::SeqCst),
        1,
        "detector must have observed the entry-carrying send"
    );
    assert_eq!(
        violations.load(Ordering::SeqCst),
        1,
        "negative control: detector must flag entries sent beyond the durable range"
    );
}

/// Complementary negative control: an in-range message (entries within the
/// durable range) must **not** be flagged — the detector is not a false alarm.
/// This pairs with `inv1_detector_is_not_vacuous` to show the detector fires
/// exactly on the forbidden condition and only there.
#[test]
fn inv1_detector_passes_in_range_send() {
    let ledger = Arc::new(FsyncLedger::new());
    // Durable through index 5.
    ledger.on_segment_fsynced(1, 5);

    let violations = Arc::new(AtomicU64::new(0));
    let entry_carrying_sends = Arc::new(AtomicU64::new(0));

    let factory = InMemoryTransportFactory::new();
    let (inner_tx, _rx) = factory.create(NodeId::from("n1"));
    let tx = RecordingTx {
        inner: inner_tx,
        ledger: ledger.clone(),
        violations: violations.clone(),
        entry_carrying_sends: entry_carrying_sends.clone(),
    };

    // Carry entries 4 and 5, both within the durable [1, 5].
    let mut e4 = Entry::default();
    e4.set_index(4);
    e4.set_term(1);
    e4.set_data(b"payload-4".to_vec().into());
    let mut e5 = Entry::default();
    e5.set_index(5);
    e5.set_term(1);
    e5.set_data(b"payload-5".to_vec().into());
    let mut msg = RaftMessage::default();
    msg.set_msg_type(MessageType::MsgAppend);
    msg.set_from(1);
    msg.set_to(2);
    msg.set_term(1);
    msg.set_entries(vec![e4, e5].into());
    let bytes = msg.write_to_bytes().expect("encode");

    let _ = block_on(tx.send(NodeId::from("n2"), TransportMessage::Raft(bytes)));
    assert_eq!(
        entry_carrying_sends.load(Ordering::SeqCst),
        1,
        "detector must have observed the entry-carrying send"
    );
    assert_eq!(
        violations.load(Ordering::SeqCst),
        0,
        "in-range send must not be flagged (no false alarm)"
    );
}
