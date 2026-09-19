//! A single-node Arachne node: the minimal complete embedder.
//!
//! Opens a real [`WalStorage`] in a temporary directory, assembles a
//! [`RaftNode`] over a peerless inline transport, drives the Ready loop until
//! the node elects itself leader, commits a `Put` via
//! [`KvStateMachine::encode_put`], and prints the applied value. Exits 0.
//!
//! No test scaffolding, no async runtime: the transport and the `block_on`
//! helper are defined inline (examples must not use `arachne-testsupport`).

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use arachne::consensus::RaftNode;
use arachne::state_machine::KvStateMachine;
use arachne::storage::{FsyncPolicy, WalConfig, WalOptions, WalStorage};
use arachne::{NodeId, StateMachine, Transport, TransportMessage, TransportRx};
use slog::{o, Drain, Logger};

type Node = RaftNode<WalStorage, NullTx, NullRx>;

// ---- Inline peerless transport (a single-node cluster has no peers) -------

/// No peers exist in this single-node cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NoPeer;

impl std::fmt::Display for NoPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("no peers (single-node cluster)")
    }
}

impl std::error::Error for NoPeer {}

/// Outbound half: every send is undeliverable (there are no peers).
#[derive(Debug, Default)]
struct NullTx;

impl Transport for NullTx {
    type Error = NoPeer;

    fn send(
        &self,
        _to: NodeId,
        _msg: TransportMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Err(NoPeer))
    }
}

/// Inbound half: never yields (there are no peers).
#[derive(Debug, Default)]
struct NullRx;

impl TransportRx for NullRx {
    fn recv(&mut self) -> impl Future<Output = Option<(NodeId, TransportMessage)>> + Send {
        std::future::pending()
    }
}

// ---- Minimal inline block_on (no-op waker; the Ready loop never suspends) --

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
    fn wake_by_ref(self: &Arc<Self>) {}
}

fn block_on<F: Future + Send>(future: F) -> F::Output {
    const MAX_POLLS: u64 = 10_000;
    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);
    let mut fut = Box::pin(future);
    let polls = AtomicU64::new(0);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => {
                let n = polls.fetch_add(1, Ordering::Relaxed) + 1;
                assert!(
                    n < MAX_POLLS,
                    "block_on: future still Pending after {MAX_POLLS} polls"
                );
            }
        }
    }
}

// ---- Assembly and drive loop ------------------------------------------------

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("arachne-example-{n}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn wal_opts() -> WalOptions {
    WalOptions {
        cluster_id: "example".into(),
        node_id: "n1".into(),
        config: WalConfig {
            fsync_policy: FsyncPolicy::Always,
            segment_bytes: 1 << 20,
        },
        created_at_millis: 1_700_000_000_000,
        fsync_observer: None,
    }
}

/// One Ready cycle: tick, step, apply committed entries, advance apply.
fn drive(node: &mut Node, sm: &mut KvStateMachine) {
    node.tick();
    let entries = block_on(node.step()).expect("Ready cycle must succeed").committed;
    for (index, data) in entries {
        sm.apply(index, &data).expect("applying a committed entry must succeed");
    }
    node.advance_apply();
}

fn main() {
    let dir = temp_dir();
    let wal = WalStorage::open(&dir, wal_opts()).expect("WAL must open");
    let mut node = RaftNode::new(
        1,
        HashMap::new(), // single node: the bootstrap voter set is {self}
        wal,
        NullTx,
        NullRx,
        0,
        &Logger::root(slog::Discard.fuse(), o!()),
    )
    .expect("node must assemble");
    let mut sm = KvStateMachine::new();

    // Wait for the single node to elect itself leader.
    for _ in 0..500 {
        drive(&mut node, &mut sm);
        if node.leader_id() == 1 {
            break;
        }
    }
    assert_eq!(node.leader_id(), 1, "the single node must elect itself leader");

    // Propose a Put and drive until it is committed and applied.
    let cmd = KvStateMachine::encode_put(1, 1, b"key", b"value");
    node.propose(&cmd).expect("propose must succeed on the leader");

    for _ in 0..500 {
        drive(&mut node, &mut sm);
        if sm.get(b"key").expect("read must succeed") == Some(b"value".to_vec()) {
            break;
        }
    }

    let value = sm
        .get(b"key")
        .expect("read must succeed")
        .expect("the committed Put must be applied");
    println!(
        "key \"key\" => \"{}\" (applied_index={})",
        String::from_utf8(value).expect("value is utf-8"),
        sm.applied_index()
    );

    // Drop the node (releasing the WAL lock), then clean up the temp dir.
    drop(node);
    std::fs::remove_dir_all(&dir).expect("temp dir must be removable");
}
