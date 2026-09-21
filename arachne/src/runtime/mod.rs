//! The node runtime: an actor that owns the raft node and the state machine.
//!
//! The runtime is the API layer's engine (propsol §4). It owns:
//! * the [`RaftNode`] (consensus + durable storage + transport),
//! * the [`KvStateMachine`] (applied state),
//! * the [`Metrics`] registry,
//!
//! and runs a biased `select!` event loop over three event sources:
//! * a **tick** interval (drives elections/heartbeats/retries),
//! * **inbound** transport messages (peer raft traffic),
//! * a **command channel** from client [`Handle`]s.
//!
//! Commands carry a `oneshot` reply. A `Propose` is answered only once the
//! command is committed **and applied** (detected via the session table), so a
//! successful reply means the write is durable and visible. Bounded by the
//! caller's deadline (`Timeout`).
//!
//! Applying entries happens on a **separate task** that owns the state machine
//! (propsol §347 / v0.2.11 N): a saturated apply path must not delay raft's
//! ticks, inbound peer messages, or the ReadIndex round, which is what dragged
//! read latency down under a write storm (risk R6). The actor hands committed
//! work to the task over a bounded channel and learns progress over a `watch`;
//! weak reads have their own channel so they never queue behind the write
//! backlog, and proposals are backpressured with `Busy` on the byte bound Q7.
//!
//! The runtime is **generic over the transport** (`T`/`Tr`) so the same actor
//! drives the in-memory transport in tests and the tonic transport in
//! production; it names no concrete transport type.
//!
//! The actor performs blocking durable-storage I/O (`fsync`), so production
//! runs it on a **dedicated OS thread** ([`Runtime::spawn_dedicated`]) rather
//! than on a shared async worker thread; `run` stays available for embedders
//! and tests that want to place it themselves.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use slog::Logger;
use tokio::sync::{mpsc, oneshot, watch};

use crate::client::{ArachneError, Handle};
use crate::consensus::{
    CommittedEntry, HeldSnapshot, NodeError, RaftNode, RaftNodeConfig, conf_change_identity,
    learner_caught_up,
};
use crate::metrics::Metrics;
use crate::profile::ProfileConfig;
use crate::state_machine::KvStateMachine;
use crate::storage::WalStorage;
use crate::types::Timestamp;
use crate::{
    Clock, LogIndex, NodeId, RaftId, StateMachine, Transport, TransportMessage, TransportRx,
};
use arachne_seam::storage::{
    EntryType as SeamEntryType, Snapshot as SeamSnapshot, Storage as _,
};
use raft::eraftpb::ConfChangeType;

/// Approximate per-entry durable overhead (record header, type byte, and the
/// index/term fields) used to size the snapshot trigger.
const ENTRY_FRAMING_BYTES: u64 = 24;

/// How often the physical WAL size is sampled for the `wal_bytes` metric.
/// Sizing the log is a `read_dir` + `stat` walk, so it must not run on every
/// drive cycle: under a write storm that would be thousands of syscalls a
/// second, exactly when latency matters most. The snapshot *trigger* does not
/// depend on this sample — it uses the logical growth counter.
const WAL_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// A request from a client [`Handle`] to the node runtime.
pub enum Command {
    /// Propose a single-step membership change (propsol v0.2.16 rev S).
    ///
    /// Replies once the change is committed **and applied** — i.e. once the new
    /// configuration is durable, not merely in the log.
    ConfChange {
        /// Which kind of change (add learner, promote, remove).
        change_type: ConfChangeType,
        /// The target node's raft id.
        node_id: RaftId,
        /// Reply channel.
        ack: oneshot::Sender<Result<(), ArachneError>>,
    },
    /// Report the applied membership configuration (rev S S5, ops surface).
    ///
    /// Answered from local durable state, so any node can serve it — an
    /// operator inspecting the cluster does not have to find the leader first,
    /// and a node that is partitioned away reports what it believes rather
    /// than hanging.
    Membership {
        /// Reply channel: `(voters, learners)`.
        ack: oneshot::Sender<Result<(Vec<RaftId>, Vec<RaftId>), ArachneError>>,
    },
    /// Move leadership to another voter (propsol §5.3; Q3 public API).
    ///
    /// `target: None` means "any other voter", which is what the automatic
    /// leader removal needs: the client asking for the removal does not know
    /// (and should not have to know) who else is in the configuration.
    /// Replies when the leadership has **actually moved**.
    TransferLeader {
        /// The intended new leader, or `None` for any other voter.
        target: Option<RaftId>,
        /// Reply channel.
        ack: oneshot::Sender<Result<(), ArachneError>>,
    },
    /// Propose a command and reply once it is committed and applied.
    Propose {
        /// The encoded state-machine command.
        cmd: Vec<u8>,
        /// The session client id (propsol §2.3).
        client_id: u64,
        /// The session sequence number.
        seq_no: u64,
        /// Reply channel: `Ok(())` once applied, or an error.
        ack: oneshot::Sender<Result<(), ArachneError>>,
    },
    /// Read this node's applied state (stale read, propsol N1).
    GetStale {
        /// The key to read.
        key: Vec<u8>,
        /// Reply channel with the local applied value.
        ack: oneshot::Sender<Result<Option<Vec<u8>>, ArachneError>>,
    },
    /// Linearizable read via ReadIndex (propsol §5.4). The leader registers a
    /// quorum-confirmed read and replies once the read index is applied; a
    /// non-leader replies `NotLeader{hint}` for the client to redirect.
    Read {
        /// The key to read.
        key: Vec<u8>,
        /// Reply channel with the value once the read is confirmed, or an error.
        ack: oneshot::Sender<Result<Option<Vec<u8>>, ArachneError>>,
    },
    /// Report the current leader hint.
    LeaderHint {
        /// Reply channel with `(leader id, address)` if a leader is known.
        ack: oneshot::Sender<Option<(NodeId, SocketAddr)>>,
    },
}

/// Configuration for a [`Runtime`].
pub struct RuntimeConfig {
    /// This node's raft id.
    pub self_raft_id: RaftId,
    /// This node's cluster identity.
    pub self_node_id: NodeId,
    /// The bootstrap peers (`raft id -> node id`), excluding self.
    pub peers: HashMap<RaftId, NodeId>,
    /// Every member's address (`node id -> socket addr`), used for hints.
    pub addresses: HashMap<NodeId, SocketAddr>,
    /// Raft tick/size configuration.
    pub raft: RaftNodeConfig,
    /// Profile limits (timeouts, key/value caps).
    pub profile: ProfileConfig,
    /// The shared metrics registry.
    pub metrics: Arc<Metrics>,
}

/// Where a proposal's session is in its life (propsol v0.2.15 R1).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SessionBand {
    /// Inside the TTL: propose; the state machine dedups.
    Live,
    /// Inside the grace window after it: answer `SessionExpired`, do not propose.
    Grace,
    /// Past both: a new session as far as this leader is concerned.
    Fresh,
}

/// A proposal awaiting commit+apply.
struct Pending {
    client_id: u64,
    seq_no: u64,
    ack: Option<oneshot::Sender<Result<(), ArachneError>>>,
    deadline: Instant,
    /// The log index carrying this proposal's entry, once the actor has handed
    /// it to the apply task. `None` until then; the reply follows the applied
    /// index (propsol v0.2.11 N).
    index: Option<LogIndex>,
}

/// A linearizable (ReadIndex) read awaiting quorum confirmation and apply
/// (propsol §5.4).
struct PendingRead {
    /// The 8-byte token registered with raft's ReadIndex round (big-endian).
    token: u64,
    /// The key to read once the read index is applied.
    key: Vec<u8>,
    /// Reply channel, taken exactly once on resolution.
    ack: Option<oneshot::Sender<Result<Option<Vec<u8>>, ArachneError>>>,
    /// Deadline for the current wait (quorum round or apply window).
    deadline: Instant,
    /// The quorum-confirmed read index, once raft reports it (`None` until then).
    read_index: Option<LogIndex>,
    /// How many times this read has been issued (`1` initially; one retry allowed).
    attempts: u8,
}

/// Upper bound on concurrent pending ReadIndex reads (propsol §4.1/§7: 4096).
/// Beyond this, new reads are rejected with `Busy`.
const MAX_PENDING_READS: usize = 4096;

/// How many apply batches may sit in the channel in front of the apply task.
///
/// This is the hard safety net, not the operative bound: the byte bound
/// (`proposal_queue_bytes`, Q7) stops proposals long before 256 batches could
/// pile up, so the actor rarely has to defer a batch at all.
const APPLY_QUEUE_DEPTH: usize = 256;

/// How many weak reads may be queued for the apply task.
const READ_QUEUE_DEPTH: usize = MAX_PENDING_READS;

/// How many queued weak reads the apply task serves before taking one unit of
/// write work, so a read flood cannot starve apply.
const READ_BURST: usize = 64;

/// How many sessions one GC entry may name (propsol v0.2.15 R2). The list has to
/// fit in a log entry, so the leader sweeps in bounded batches.
const SESSION_GC_BATCH: usize = 1_000;

/// The snapshot duration budget (propsol §8.2 Q4): creating a snapshot blocks
/// apply, so a longer one is a capacity signal worth warning about.
const SNAPSHOT_DURATION_BUDGET_MS: u64 = 1_000;

/// How many already-queued events the actor folds into one durability cycle.
///
/// Every cycle costs two real `fsync`s — the entries, then the commit
/// `HardState` — so under a write storm what the storm costs is the *number of
/// cycles*, not the number of messages. Draining what has already arrived lets
/// raft build a bigger `Ready` and lets several commit advances share one
/// flush (propsol v0.2.12 O).
const CYCLE_BURST: usize = 64;

/// Committed entries for the apply task, in log order.
///
/// An installed snapshot and the entries that follow it travel in **one**
/// batch: the task must never observe them out of order, and the entries start
/// at `snapshot.index + 1`.
struct ApplyBatch {
    /// A snapshot to `restore` before the entries, if one was installed.
    snapshot: Option<SeamSnapshot>,
    /// Committed entries `(index, data)`, ascending.
    entries: Vec<(LogIndex, Vec<u8>)>,
}

/// A membership change awaiting apply (propsol v0.2.16 rev S, S1b).
///
/// ConfChange entries never reach the state machine: they carry a ConfChange
/// protobuf, and applying them needs `&mut RawNode`, which only the actor has.
struct PendingConf {
    change_type: ConfChangeType,
    node_id: RaftId,
    ack: Option<oneshot::Sender<Result<(), ArachneError>>>,
    deadline: Instant,
    /// The log index carrying this change, once the actor has seen the entry.
    index: Option<LogIndex>,
}

/// A streamed snapshot transfer in flight (propsol rev T, T2b).
struct SnapshotFetch {
    /// The message that was held back, to be stepped once the bytes are in.
    held: HeldSnapshot,
    /// Where the fetch task is writing.
    dest: std::path::PathBuf,
}

/// A finished background snapshot fetch.
struct SnapshotFetchDone {
    /// Which snapshot this was for, so a stale completion (the peer sent a newer
    /// snapshot while this one was still transferring) is ignored instead of
    /// installing the wrong bytes.
    from: RaftId,
    index: LogIndex,
    /// Where the bytes landed.
    dest: std::path::PathBuf,
    /// `Ok(None)` = this transport cannot stream (never "empty snapshot").
    result: Result<Option<u64>, String>,
}

/// A leadership transfer awaiting its outcome (propsol v0.2.16 rev S).
///
/// `RawNode::transfer_leader` only sends a message: the reply has to wait for
/// the leadership to actually move, which the actor observes from raft.
struct PendingTransfer {
    target: RaftId,
    ack: Option<oneshot::Sender<Result<(), ArachneError>>>,
    deadline: Instant,
}

/// Requests the actor sends to the apply task, in order.
enum ApplyRequest {
    /// Apply a batch.
    Batch(ApplyBatch),
    /// Serialize the applied state for a local snapshot. Sent on this channel
    /// so the snapshot index is exactly the applied index at that point — and
    /// so serializing blocks apply, which is what Q4 accepted.
    Snapshot {
        ack: oneshot::Sender<Result<(LogIndex, Vec<u8>), String>>,
    },
}

/// A weak read (`get_stale`, propsol N1), on its **own** channel so it never
/// waits behind the write backlog.
struct ReadRequest {
    key: Vec<u8>,
    ack: oneshot::Sender<Result<Option<Vec<u8>>, ArachneError>>,
}

/// The apply task's published state.
///
/// A `watch` keeps only the newest value and never blocks the apply task on a
/// slow actor; `applied_bytes_total` is cumulative, so the actor derives the
/// backlog by subtracting what it has sent (propsol v0.2.11 N).
#[derive(Clone, Default)]
struct ApplyProgress {
    /// Highest applied log index.
    applied_index: LogIndex,
    /// Cumulative bytes of entries applied.
    applied_bytes_total: u64,
    /// Sticky fail-stop reason: once set, the node stops.
    failed: Option<String>,
    /// Sessions currently in the state machine's table (propsol §8
    /// `session_count`); the leader uses it to enforce `max_sessions`.
    sessions: u64,
}

/// Owns the state machine and applies committed work off the actor's loop.
struct ApplyTask {
    sm: KvStateMachine,
    applies: mpsc::Receiver<ApplyRequest>,
    reads: mpsc::Receiver<ReadRequest>,
    progress: watch::Sender<ApplyProgress>,
    applied_bytes_total: u64,
    failed: Option<String>,
}

impl ApplyTask {
    /// Serve queued weak reads, then one unit of write work, then wait.
    ///
    /// Reads jump the queue (a weak read must not wait for the write backlog)
    /// but are served in bounded bursts, so they cannot starve apply.
    async fn run(mut self) {
        let mut reads_open = true;
        loop {
            if reads_open {
                for _ in 0..READ_BURST {
                    match self.reads.try_recv() {
                        Ok(req) => self.serve_read(req),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            reads_open = false;
                            break;
                        }
                    }
                }
            }

            match self.applies.try_recv() {
                Ok(req) => {
                    if !self.handle(req) {
                        break;
                    }
                    continue;
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }

            tokio::select! {
                biased;
                maybe = self.reads.recv(), if reads_open => {
                    match maybe {
                        Some(req) => self.serve_read(req),
                        // The actor dropped its read sender; keep applying.
                        None => reads_open = false,
                    }
                }
                maybe = self.applies.recv() => {
                    match maybe {
                        Some(req) => {
                            if !self.handle(req) {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    }

    fn serve_read(&self, req: ReadRequest) {
        let value = self.sm.get(&req.key).map_err(|e| {
            ArachneError::Unrecoverable(format!("state machine read failed: {e}"))
        });
        let _ = req.ack.send(value);
    }

    /// Returns `false` when the task must stop.
    fn handle(&mut self, req: ApplyRequest) -> bool {
        match req {
            ApplyRequest::Batch(batch) => {
                if let Some(snapshot) = batch.snapshot
                    && let Err(e) = self.sm.restore(&snapshot.data)
                {
                    return self.fail(format!(
                        "state machine restore failed for the snapshot at {}: {e}",
                        snapshot.meta.index
                    ));
                }
                for (index, data) in batch.entries {
                    if let Err(e) = self.sm.apply(index, &data) {
                        return self.fail(format!("state machine apply failed: {e}"));
                    }
                    self.applied_bytes_total += data.len() as u64 + ENTRY_FRAMING_BYTES;
                }
                self.publish();
                true
            }
            ApplyRequest::Snapshot { ack } => {
                let result = self
                    .sm
                    .snapshot()
                    .map(|data| (self.sm.applied_index(), data))
                    .map_err(|e| e.to_string());
                let _ = ack.send(result);
                true
            }
        }
    }

    fn fail(&mut self, reason: String) -> bool {
        self.failed = Some(reason);
        self.publish();
        false
    }

    /// Publish progress, but only when the value actually changes: a `watch`
    /// send is a wakeup for the actor, and waking it for an unchanged value
    /// would be a busy loop.
    fn publish(&self) {
        let next = ApplyProgress {
            applied_index: self.sm.applied_index(),
            applied_bytes_total: self.applied_bytes_total,
            failed: self.failed.clone(),
            sessions: self.sm.session_count() as u64,
        };
        self.progress.send_if_modified(|current| {
            if current.applied_index == next.applied_index
                && current.applied_bytes_total == next.applied_bytes_total
                && current.failed == next.failed
                && current.sessions == next.sessions
            {
                false
            } else {
                *current = next;
                true
            }
        });
    }
}

enum Outcome {
    Tick,
    Inbound(Option<(NodeId, TransportMessage)>),
    /// The apply task published new progress (or stopped).
    Progress(Result<(), watch::error::RecvError>),
    /// The owner asked the actor to stop (`RuntimeThread::shutdown`).
    Stop,
    /// A durable-storage flush completed (propsol v0.2.13 P). The completion
    /// itself is picked up by the next `drive_cycle`; this event is what wakes
    /// the actor instead of making it wait for a tick.
    Durability,
    Command(Option<Command>),
    /// A background snapshot fetch finished (rev T).
    SnapshotFetched(Option<SnapshotFetchDone>),
}

/// The node runtime actor.
pub struct Runtime<T: Transport + Clone, Tr: TransportRx> {
    node: RaftNode<WalStorage, T, Tr>,
    /// The apply task, handed to `run` to spawn. `None` once running.
    apply_task: Option<ApplyTask>,
    /// Committed work for the apply task (bounded; see [`APPLY_QUEUE_DEPTH`]).
    applies: mpsc::Sender<ApplyRequest>,
    /// Weak reads, on their own channel so they never queue behind writes.
    reads: mpsc::Sender<ReadRequest>,
    /// The apply task's published progress.
    progress: watch::Receiver<ApplyProgress>,
    /// Woken by the storage when an offloaded flush completes (propsol P).
    durability: Arc<tokio::sync::Notify>,
    /// Fired by [`RuntimeThread::shutdown`] to stop the actor (and release its
    /// storage) without waiting for a transport to close.
    stop: Arc<tokio::sync::Notify>,
    /// A batch that did not fit the apply channel, retried next cycle. At most
    /// one, so a full channel cannot grow the actor's memory without bound.
    deferred: Option<ApplyBatch>,
    /// A local snapshot the apply task is serializing.
    snapshot_wait: Option<oneshot::Receiver<Result<(LogIndex, Vec<u8>), String>>>,
    /// A second handle to the transport, for the background snapshot fetch
    /// (rev T): a fetch can take seconds, so it cannot run on the actor.
    transport: T,
    /// A streamed snapshot being fetched: the held message plus where the bytes
    /// are landing (rev T).
    snapshot_fetch: Option<SnapshotFetch>,
    /// Where the fetch task reports back.
    snapshot_fetches: mpsc::Sender<SnapshotFetchDone>,
    snapshot_fetch_rx: mpsc::Receiver<SnapshotFetchDone>,
    /// The last confirmed applied index (from the apply task).
    applied_index: LogIndex,
    /// Cumulative bytes confirmed applied.
    applied_bytes_total: u64,
    /// Cumulative bytes handed to the apply task (for backlog accounting).
    sent_bytes_total: u64,
    /// Byte bound on committed-but-unapplied work (Q7 `proposal_queue_bytes`).
    proposal_queue_bytes: u64,
    /// How far a learner may lag and still be promotable (§5.3, rev S).
    promote_lag_entries: u64,
    metrics: Arc<Metrics>,
    /// Where the Q4 snapshot-budget warning goes.
    logger: Logger,
    raft_id: RaftId,
    self_node: NodeId,
    /// `node id -> raft id`, for tagging inbound messages.
    node_to_raft: HashMap<String, RaftId>,
    /// `raft id -> node id`, for building leader hints.
    raft_to_node: HashMap<RaftId, NodeId>,
    addresses: HashMap<NodeId, SocketAddr>,
    commands: mpsc::Receiver<Command>,
    tick: tokio::time::Interval,
    pending: Vec<Pending>,
    /// Committed entries not yet dispatched, in log order. Every entry waits
    /// behind a preceding ConfChange so that the state machine and the
    /// membership view are applied in the same order as the log (rev S S1b).
    committed_queue: VecDeque<CommittedEntry>,
    /// Membership changes awaiting commit + apply (rev S S1b).
    pending_conf: Vec<PendingConf>,
    /// Leadership transfers awaiting their outcome (rev S S2).
    pending_transfer: Vec<PendingTransfer>,
    /// Linearizable reads awaiting quorum confirmation / apply (propsol §5.4).
    pending_reads: Vec<PendingRead>,
    /// Wait timeout for a ReadIndex round / apply window (`2 × election_timeout`).
    read_index_timeout: Duration,
    /// Monotonic source of ReadIndex tokens (8-byte big-endian `ctx`).
    next_read_token: u64,
    propose_timeout: Duration,
    /// The clock session TTLs are measured on (propsol v0.2.15 R1). `None`
    /// disables expiry entirely, which is what every embedder that does not set
    /// one gets — i.e. the behaviour before sessions had a lifetime.
    session_clock: Option<Arc<dyn Clock>>,
    /// Leader-local `(client_id, seq_no) -> last used` (monotonic ms). Not
    /// replicated and not in any snapshot: it only decides when this leader
    /// stops accepting a retry, and a new leader simply starts the window over,
    /// which keeps sessions alive *longer* — the safe direction.
    sessions: HashMap<(u64, u64), Timestamp>,
    /// When to sweep expired entries out of `sessions`.
    next_session_sweep: Timestamp,
    /// `session_ttl_ms` from the profile.
    session_ttl_ms: u64,
    /// `session_grace_period_ms` from the profile.
    session_grace_ms: u64,
    /// `max_sessions` from the profile.
    max_sessions: u64,
    /// Sessions in the state machine's table, as published by the apply task.
    live_sessions: u64,
    /// When to propose the next session-GC entry.
    next_session_gc: Timestamp,
    /// Log growth that triggers a local snapshot and compaction
    /// (`snapshot_threshold`, propsol §7). 0 disables the trigger.
    snapshot_threshold_bytes: u64,
    /// When the physical WAL size was last sampled (see
    /// [`WAL_SAMPLE_INTERVAL`]).
    last_wal_sample: Instant,
    /// Applied bytes at the last snapshot — the trigger counts growth since
    /// then. This is the *logical* growth of the log, not the physical size of
    /// the segment files: v1 compacts whole segments, so a segment holding a
    /// compacted prefix keeps its bytes until it rolls over, and using
    /// physical size would re-fire the trigger on every entry.
    applied_bytes_at_snapshot: u64,
    /// The index of the newest snapshot this node created or installed. Guards
    /// against re-taking a snapshot at an index that is already covered.
    snapshot_index: LogIndex,
}

impl<T: Transport + Clone, Tr: TransportRx> Runtime<T, Tr> {
    /// Assemble the runtime and its local [`Handle`].
    pub fn new(
        config: RuntimeConfig,
        storage: WalStorage,
        transport: T,
        rx: Tr,
        logger: &Logger,
    ) -> Result<(Self, Handle), NodeError<T>> {
        // Rebuild the applied state from the durable snapshot before raft
        // starts delivering entries. raft derives its applied index from
        // `first_index - 1`, which is exactly the snapshot index, so the two
        // stay in step and the log tail replays on top of the restored state
        // (propsol §5.5.3 steps 3 and 7).
        let mut sm = KvStateMachine::new();
        let (snapshot_index, snapshot_data) = match storage.snapshot() {
            Ok(Some(snapshot)) => (snapshot.meta.index, Some(snapshot.data)),
            Ok(None) => (0, None),
            Err(e) => {
                return Err(NodeError::Storage(format!(
                    "cannot read the durable snapshot: {e}"
                )));
            }
        };
        if let Some(data) = &snapshot_data
            && let Err(e) = sm.restore(data)
        {
            return Err(NodeError::StateMachine(format!(
                "cannot restore the state machine from the snapshot at {snapshot_index}: {e}"
            )));
        }

        // The apply task owns the state machine from here on (propsol N).
        let applied_index = sm.applied_index();
        let (applies, applies_rx) = mpsc::channel(APPLY_QUEUE_DEPTH);
        let (reads, reads_rx) = mpsc::channel(READ_QUEUE_DEPTH);
        let (progress_tx, progress) = watch::channel(ApplyProgress {
            applied_index,
            applied_bytes_total: 0,
            failed: None,
            sessions: 0,
        });
        let apply_task = ApplyTask {
            sm,
            applies: applies_rx,
            reads: reads_rx,
            progress: progress_tx,
            applied_bytes_total: 0,
            failed: None,
        };

        // The transport's own answer decides how snapshots travel (rev T): a
        // transport that carries them inside the raft message keeps the
        // pre-streaming path, and one that cannot (a message-size cap a
        // snapshot will not fit through) streams the bytes separately.
        let streamed_snapshots = transport.supports_snapshot_streaming();
        let mut raft_config = config.raft.clone();
        raft_config.streamed_snapshots = streamed_snapshots;
        // A second handle for background fetches: a transfer can take seconds,
        // so it cannot run on the actor.
        let fetch_transport = transport.clone();
        let node = RaftNode::new_with_config(
            config.self_raft_id,
            config.peers.clone(),
            storage,
            transport,
            rx,
            0,
            raft_config,
            logger,
        )?;

        let node_to_raft = config
            .peers
            .iter()
            .map(|(raft, node)| (node.as_str().to_string(), *raft))
            .collect();
        let raft_to_node = config
            .peers
            .iter()
            .map(|(raft, node)| (*raft, node.clone()))
            .collect();

        let (tx, commands) = mpsc::channel(1024);
        let handle = Handle::new_local(config.self_node_id.clone(), tx, &config.profile);
        // One fetch at a time: raft hands one snapshot at a time, so a deep
        // queue would only hide a bug.
        let (snapshot_fetches, snapshot_fetch_rx) = mpsc::channel::<SnapshotFetchDone>(1);

        let period = Duration::from_millis(config.profile.heartbeat_interval_ms.max(1));
        let durability = node.durability_notifier();
        let stop = Arc::new(tokio::sync::Notify::new());
        let runtime = Self {
            node,
            apply_task: Some(apply_task),
            applies,
            reads,
            progress,
            durability,
            stop: Arc::clone(&stop),
            deferred: None,
            snapshot_wait: None,
            transport: fetch_transport,
            snapshot_fetch: None,
            snapshot_fetches,
            snapshot_fetch_rx,
            applied_index,
            applied_bytes_total: 0,
            sent_bytes_total: 0,
            proposal_queue_bytes: config.profile.proposal_queue_bytes,
            promote_lag_entries: config.profile.promote_lag_entries,
            metrics: config.metrics,
            logger: logger.clone(),
            raft_id: config.self_raft_id,
            self_node: config.self_node_id,
            node_to_raft,
            raft_to_node,
            addresses: config.addresses,
            commands,
            tick: tokio::time::interval(period),
            pending: Vec::new(),
            committed_queue: VecDeque::new(),
            pending_conf: Vec::new(),
            pending_transfer: Vec::new(),
            pending_reads: Vec::new(),
            read_index_timeout: Duration::from_millis(
                config.profile.read_index_timeout_ms.max(1),
            ),
            next_read_token: 0,
            propose_timeout: Duration::from_millis(config.profile.election_timeout_ms.max(1)),
            session_clock: None,
            sessions: HashMap::new(),
            next_session_sweep: 0,
            session_ttl_ms: config.profile.session_ttl_ms,
            session_grace_ms: config.profile.session_grace_period_ms,
            max_sessions: config.profile.max_sessions,
            live_sessions: 0,
            next_session_gc: 0,
            snapshot_threshold_bytes: config.profile.snapshot_threshold_bytes,
            last_wal_sample: Instant::now(),
            applied_bytes_at_snapshot: 0,
            snapshot_index,
        };
        runtime.refresh_metrics();
        Ok((runtime, handle))
    }

    /// Run the actor until the transport closes or the command channel ends.
    pub async fn run(mut self) {
        if let Some(task) = self.apply_task.take() {
            tokio::spawn(task.run());
        }
        loop {
            let outcome = tokio::select! {
                biased;
                _ = self.stop.notified() => Outcome::Stop,
                _ = self.tick.tick() => Outcome::Tick,
                progress = self.progress.changed() => Outcome::Progress(progress),
                _ = self.durability.notified() => Outcome::Durability,
                cmd = self.commands.recv() => Outcome::Command(cmd),
                msg = self.node.rx().recv() => Outcome::Inbound(msg),
                fetch = self.snapshot_fetch_rx.recv() => Outcome::SnapshotFetched(fetch),
            };
            match outcome {
                Outcome::Stop => break,
                Outcome::Tick => self.node.tick(),
                Outcome::Inbound(Some((from, msg))) => {
                    self.route_inbound(from, msg);
                    // Fold the rest of the burst in: one cycle then carries a
                    // bigger `Ready` and one flush can cover more work.
                    for _ in 1..CYCLE_BURST {
                        let next = self.node.rx().try_recv();
                        match next {
                            Some((from, msg)) => self.route_inbound(from, msg),
                            None => break,
                        }
                    }
                }
                Outcome::Inbound(None) => break,
                Outcome::SnapshotFetched(Some(done)) => self.finish_snapshot_fetch(done),
                // The fetch channel is only closed when the runtime is going
                // away; there is nothing to finish.
                Outcome::SnapshotFetched(None) => {}
                // Durability completions are consumed by `drive_cycle` (which
                // runs after every event); waking is the whole point.
                Outcome::Durability => {}
                // Progress is absorbed at the top of `drive_cycle`; this event
                // just wakes the loop so replies and reads see it promptly.
                Outcome::Progress(Ok(())) => {}
                Outcome::Progress(Err(_)) => {
                    // The apply task is gone. It publishes a failure reason
                    // before stopping, so prefer that.
                    let reason = self
                        .progress
                        .borrow()
                        .failed
                        .clone()
                        .unwrap_or_else(|| "the apply task stopped".to_string());
                    self.fail_all_pending(&reason);
                    self.metrics.set_is_leader(false);
                    break;
                }
                Outcome::Command(Some(cmd)) => {
                    self.handle_command(cmd);
                    // Same for proposals: they accumulate in raft's log and go
                    // out in one `Ready`, so several writes share one flush
                    // pair instead of paying their own (propsol v0.2.12 O).
                    for _ in 1..CYCLE_BURST {
                        match self.commands.try_recv() {
                            Ok(cmd) => self.handle_command(cmd),
                            Err(_) => break,
                        }
                    }
                }
                Outcome::Command(None) => break,
            }
            if !self.drive_cycle().await {
                break;
            }
        }
        self.fail_all_pending("runtime stopped");
    }

    /// Drive one `Ready` cycle: apply committed entries, register quorum
    /// read-states, then resolve reads and reply to pendings. Returns `false`
    /// on a fatal (fail-stop) error.
    async fn drive_cycle(&mut self) -> bool {
        // Apply progress first: replies to proposals and the resolution of
        // ReadIndex reads both depend on the applied index (propsol N).
        if !self.absorb_progress() || !self.poll_snapshot() {
            return false;
        }

        let outcome = match self.node.step().await {
            Ok(outcome) => outcome,
            Err(e) => {
                self.fail_all_pending(&format!("raft step failed: {e}"));
                self.metrics.set_is_leader(false);
                return false;
            }
        };

        // Hand committed work to the apply task, in log order: normal entries
        // go to the state machine, ConfChange entries are applied here on the
        // actor, and nothing behind a not-yet-applied ConfChange is dispatched
        // (rev S S1b).
        if !self.stage_committed(outcome.snapshot, outcome.committed) {
            self.metrics.set_is_leader(false);
            return false;
        }

        // Quorum-confirmed read states (propsol §5.4 step 2): record the read
        // index and open the wait-for-apply window. The ctx is the 8-byte
        // big-endian token issued by the `Read` command; unknown tokens (already
        // resolved or dropped) are ignored.
        for (ctx, index) in &outcome.read_states {
            let Ok(token_bytes) = ctx.as_slice().try_into() else {
                continue;
            };
            let token = u64::from_be_bytes(token_bytes);
            if let Some(r) = self.pending_reads.iter_mut().find(|r| r.token == token) {
                r.read_index = Some(*index);
                r.deadline = Instant::now() + self.read_index_timeout;
            }
        }

        self.resolve_transfers();
        self.resolve_reads();
        self.reply_pendings();
        self.maybe_collect_sessions();
        if !self.maybe_snapshot() {
            return false;
        }
        self.refresh_metrics();
        true
    }

    /// Measure session TTLs on this clock (propsol v0.2.15 R1).
    ///
    /// Set as a builder rather than as a `RuntimeConfig` field so that adding it
    /// does not touch every construction site; an embedder that never calls it
    /// keeps the behaviour from before sessions had a lifetime — they never
    /// expire.
    pub fn with_session_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.session_clock = Some(clock);
        self
    }

    /// Apply the task's progress to the actor's view, and advance raft's
    /// applied index to match (propsol N). Returns `false` on a fail-stop.
    fn absorb_progress(&mut self) -> bool {
        let progress = self.progress.borrow().clone();
        self.applied_index = progress.applied_index;
        self.applied_bytes_total = progress.applied_bytes_total;
        self.live_sessions = progress.sessions;
        if let Some(reason) = progress.failed {
            self.fail_all_pending(&reason);
            self.metrics.set_is_leader(false);
            return false;
        }
        // raft's own `applied` is advanced by `RawNode::advance` when the
        // entries are *handed over* (`commit_since_index`), so it legitimately
        // leads the state machine by whatever the apply task still has queued.
        // Moving it here would be moving it backwards — raft-rs treats that as
        // fatal — and it is not needed: everything that depends on "what the
        // state machine has really applied" (snapshots, reads, replies) goes
        // through `self.applied_index` (propsol v0.2.11 N).
        true
    }

    /// Bytes committed but not yet applied — the backpressure signal (Q7).
    fn apply_backlog_bytes(&self) -> u64 {
        self.sent_bytes_total.saturating_sub(self.applied_bytes_total)
    }

    /// Stage committed entries and dispatch as much as log order allows.
    ///
    /// An installed snapshot goes to the apply task first (it supersedes every
    /// queued entry at or below its index), then entries are dispatched from
    /// the front of the queue: runs of normal entries as batches, and a
    /// ConfChange entry only once the apply task has applied everything before
    /// it. That barrier is what keeps the KV state machine's view and the
    /// membership view consistent with the log — without it the actor could
    /// apply a membership change while the state machine was still catching up
    /// on commands that preceded it (rev S S1b).
    ///
    /// Returns `false` on a fail-stop.
    fn stage_committed(
        &mut self,
        snapshot: Option<SeamSnapshot>,
        entries: Vec<CommittedEntry>,
    ) -> bool {
        if let Some(snapshot) = &snapshot {
            let index = snapshot.meta.index;
            self.committed_queue.retain(|(i, _, _)| *i > index);
        }
        // Attribute membership changes to their waiting proposals before the
        // queue owns them. A ConfChange entry carries no session key, so its
        // identity is decoded instead; the single-flight rule keeps
        // `(change_type, node_id)` unique among outstanding changes.
        for (index, kind, data) in &entries {
            if matches!(kind, SeamEntryType::Entry) {
                continue;
            }
            let Ok((change_type, node_id)) = conf_change_identity(*kind, data) else {
                // Not decodable as a ConfChange: leave it unattributed and let
                // `apply_conf_change` report the failure with its own error.
                continue;
            };
            if let Some(p) = self.pending_conf.iter_mut().find(|p| {
                p.index.is_none() && p.node_id == node_id && p.change_type == change_type
            }) {
                p.index = Some(*index);
            }
        }
        self.committed_queue.extend(entries);

        if let Some(snapshot) = snapshot
            && !self.enqueue_apply(Some(snapshot), Vec::new())
        {
            self.fail_all_pending("the apply task stopped");
            return false;
        }
        self.pump_committed()
    }

    /// Dispatch queued entries from the front while ordering permits.
    fn pump_committed(&mut self) -> bool {
        loop {
            let Some((index, kind, _)) = self.committed_queue.front() else {
                return true;
            };
            let (index, kind) = (*index, *kind);

            if matches!(kind, SeamEntryType::Entry) {
                // Take the whole run of normal entries so batching is preserved.
                let mut batch: Vec<(LogIndex, Vec<u8>)> = Vec::new();
                while let Some((_, k, _)) = self.committed_queue.front() {
                    if !matches!(k, SeamEntryType::Entry) {
                        break;
                    }
                    let (index, _, data) = self
                        .committed_queue
                        .pop_front()
                        .expect("front was just observed");
                    batch.push((index, data));
                }
                if !self.enqueue_apply(None, batch) {
                    self.fail_all_pending("the apply task stopped");
                    return false;
                }
                continue;
            }

            // Ordering barrier: every entry before this one must be applied
            // before membership may change.
            if self.applied_index + 1 < index {
                return true;
            }
            let (_, _, data) = self
                .committed_queue
                .pop_front()
                .expect("front was just observed");

            // The state machine must still *see* this index or its applied
            // watermark would grow a hole and it would fail-stop on the next
            // entry (it requires `applied + 1`). An empty payload is already its
            // no-op: the index advances, nothing is interpreted. Enqueueing it
            // before the change also puts it ahead of every entry behind the
            // change on the same channel, which is what keeps the state
            // machine's order equal to the log's.
            if !self.enqueue_apply(None, vec![(index, Vec::new())]) {
                self.fail_all_pending("the apply task stopped");
                return false;
            }

            match self.node.apply_conf_change(index, kind, &data) {
                Ok(_applied) => {
                    // The state machine never sees this index, so the actor's
                    // own view has to move past it: the barrier, the read
                    // resolution and the backlog metric all read it.
                    self.applied_index = self.applied_index.max(index);
                }
                Err(e) => {
                    self.fail_all_pending(&format!("applying a ConfChange failed: {e}"));
                    return false;
                }
            }
        }
    }

    /// Hand committed work to the apply task, attributing entries to waiting
    /// proposals. Returns `false` if the apply task is gone.
    fn enqueue_apply(
        &mut self,
        snapshot: Option<SeamSnapshot>,
        entries: Vec<(LogIndex, Vec<u8>)>,
    ) -> bool {
        // Nothing committed and nothing installed: sending an empty batch
        // would make the apply task publish progress the actor has already
        // seen, waking it in a tight loop. (Harmless on a multi-thread runtime,
        // fatal on a single-threaded one — the turmoiled L2 cluster stopped
        // electing.)
        if snapshot.is_none() && entries.is_empty() && self.deferred.is_none() {
            return true;
        }
        if let Some(snapshot) = &snapshot {
            // Storage already persisted it; the apply task restores it in order
            // with the entries that follow, and a restore failure is a
            // fail-stop reported as progress.
            self.snapshot_index = self.snapshot_index.max(snapshot.meta.index);
            // The snapshot covers everything applied so far, so the local
            // trigger starts counting from here.
            self.applied_bytes_at_snapshot = self.applied_bytes_total;
            self.metrics.inc_snapshots_installed();
        }

        for (index, data) in &entries {
            // Attribute before the data moves: the actor knows which session
            // each entry carries, so a reply needs only the applied index.
            if let Some((client_id, seq_no)) = KvStateMachine::command_session(data) {
                if let Some(p) = self
                    .pending
                    .iter_mut()
                    .find(|p| p.index.is_none() && p.client_id == client_id && p.seq_no == seq_no)
                {
                    p.index = Some(*index);
                }
                // Only once the entry is committed and on its way to the state
                // machine: a proposal that never made it must not extend the
                // session, or a retry would be answered `SessionExpired` for an
                // operation that never happened.
                self.note_session(client_id, seq_no);
            }
            self.sent_bytes_total += data.len() as u64 + ENTRY_FRAMING_BYTES;
        }

        let batch = match self.deferred.take() {
            Some(mut waiting) => {
                if let Some(snapshot) = snapshot {
                    // A newer snapshot supersedes what is still waiting: the
                    // deferred entries are at or below its index.
                    waiting = ApplyBatch {
                        snapshot: Some(snapshot),
                        entries,
                    };
                } else {
                    waiting.entries.extend(entries);
                }
                waiting
            }
            None => ApplyBatch { snapshot, entries },
        };
        self.deferred = Some(batch);
        self.flush_deferred()
    }

    /// Move the deferred batch into the apply channel if there is room. The
    /// actor never awaits here: blocking on apply would hand the apply path's
    /// backpressure to raft's ticks and to reads, which is the opposite of the
    /// point (propsol N). Returns `false` if the task is gone.
    fn flush_deferred(&mut self) -> bool {
        let Some(batch) = self.deferred.take() else {
            return true;
        };
        match self.applies.try_reserve() {
            Ok(permit) => {
                permit.send(ApplyRequest::Batch(batch));
                true
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                // Keep it for the next cycle; the byte bound has already
                // stopped new proposals from deepening the backlog.
                self.deferred = Some(batch);
                true
            }
            Err(mpsc::error::TrySendError::Closed(())) => false,
        }
    }

    /// Propose a session-GC entry for sessions past `ttl + grace`
    /// (propsol v0.2.15 R2).
    ///
    /// Only the leader proposes, and only the sessions it has not seen for the
    /// whole window: the state machine prunes exactly the listed ones, so every
    /// replica ends up with the same table. Nothing is proposed unless something
    /// expired, so an idle cluster stays silent.
    fn maybe_collect_sessions(&mut self) {
        let Some(clock) = self.session_clock.clone() else {
            return;
        };
        if self.session_ttl_ms == 0 || self.node.leader_id() != self.raft_id {
            return;
        }
        let now = clock.now_millis();
        if now < self.next_session_gc {
            return;
        }
        self.next_session_gc = now.saturating_add(self.session_ttl_ms.max(1));
        let window = self.session_ttl_ms.saturating_add(self.session_grace_ms);
        let expired: Vec<(u64, u64)> = self
            .sessions
            .iter()
            .filter(|(_, last)| now.saturating_sub(**last) > window)
            .map(|(session, _)| *session)
            .take(SESSION_GC_BATCH)
            .collect();
        if expired.is_empty() {
            return;
        }
        let cmd = KvStateMachine::encode_session_gc(&expired);
        if self.node.propose(&cmd).is_ok() {
            // On its way to every replica: forget them here so the next round
            // does not propose the same list again.
            for session in &expired {
                self.sessions.remove(session);
            }
        }
    }

    /// Take a local snapshot and compact the log once the WAL outgrows
    /// `snapshot_threshold` (propsol §5.5.4, §7). Returns `false` on a
    /// fail-stop error.
    ///
    /// Two guards keep this from firing in a loop: the threshold itself, and
    /// [`Self::snapshot_index`] — a snapshot is only ever taken at an index
    /// strictly newer than the one already covered.
    fn maybe_snapshot(&mut self) -> bool {
        let applied = self.applied_index;
        let now = Instant::now();
        if now.saturating_duration_since(self.last_wal_sample) >= WAL_SAMPLE_INTERVAL {
            self.last_wal_sample = now;
            match self.node.log_bytes() {
                Ok(bytes) => self.metrics.set_wal_bytes(bytes),
                Err(e) => {
                    self.fail_all_pending(&format!("cannot size the durable log: {e}"));
                    self.metrics.set_is_leader(false);
                    return false;
                }
            }
        }

        let grew = self
            .applied_bytes_total
            .saturating_sub(self.applied_bytes_at_snapshot);
        if self.snapshot_threshold_bytes == 0
            || applied <= self.snapshot_index
            || grew < self.snapshot_threshold_bytes
            || self.snapshot_wait.is_some()
        {
            return true;
        }

        // Ask the apply task to serialize the applied state. The request rides
        // the apply channel so its index is exactly the applied index at that
        // point, and serializing blocks apply — which is what Q4 accepted
        // (propsol N).
        let (ack, rx) = oneshot::channel();
        match self.applies.try_send(ApplyRequest::Snapshot { ack }) {
            Ok(()) => {
                self.snapshot_wait = Some(rx);
                true
            }
            // No room right now: retry on a later cycle.
            Err(mpsc::error::TrySendError::Full(_)) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.fail_all_pending("the apply task stopped");
                self.metrics.set_is_leader(false);
                false
            }
        }
    }

    /// Finish a local snapshot once the apply task has serialized it.
    ///
    /// The task reports the index it serialized at, so compaction can never
    /// drop an entry the snapshot does not cover. Returns `false` on a
    /// fail-stop.
    fn poll_snapshot(&mut self) -> bool {
        let Some(mut rx) = self.snapshot_wait.take() else {
            return true;
        };
        let result = match rx.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {
                self.snapshot_wait = Some(rx);
                return true;
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                self.fail_all_pending("the apply task stopped while serializing a snapshot");
                self.metrics.set_is_leader(false);
                return false;
            }
            Ok(result) => result,
        };
        let (index, data) = match result {
            Ok(pair) => pair,
            Err(e) => {
                self.fail_all_pending(&format!("state machine snapshot failed: {e}"));
                self.metrics.set_is_leader(false);
                return false;
            }
        };
        let size_bytes = data.len() as u64;
        let term = match self.node.term_at(index) {
            Ok(term) => term,
            Err(e) => {
                self.fail_all_pending(&format!("cannot resolve the term at {index}: {e}"));
                self.metrics.set_is_leader(false);
                return false;
            }
        };
        // The live configuration, learners included (rev S S4). The static
        // voter list this used to pass would drop every learner, and every
        // membership change, from the snapshot that is supposed to rebuild a
        // node.
        let conf_state = match self.node.applied_conf_state() {
            Ok(conf_state) => conf_state,
            Err(e) => {
                self.fail_all_pending(&format!("cannot read the applied membership: {e}"));
                self.metrics.set_is_leader(false);
                return false;
            }
        };
        // Q4: creation blocks apply (in the apply task). It is measured every
        // time so the >1s budget alarm has data (propsol §8.2).
        let started = Instant::now();
        if let Err(e) = self.node.create_snapshot(index, term, conf_state, data) {
            self.fail_all_pending(&format!("snapshot creation failed: {e}"));
            self.metrics.set_is_leader(false);
            return false;
        }
        let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        // Q4 budget alarm (propsol §8.2): snapshot creation blocks apply, so
        // exceeding the budget is the signal that motivated the double-buffer
        // revisit in v1.1.
        if elapsed_ms > SNAPSHOT_DURATION_BUDGET_MS {
            self.metrics.inc_snapshot_slow();
            slog::warn!(
                self.logger,
                "snapshot creation exceeded its budget";
                "duration_ms" => elapsed_ms,
                "index" => index,
                "size_bytes" => size_bytes,
                "budget_ms" => SNAPSHOT_DURATION_BUDGET_MS,
            );
        }
        self.snapshot_index = index;
        self.applied_bytes_at_snapshot = self.applied_bytes_total;
        self.metrics.set_snapshot_last(elapsed_ms, size_bytes);
        self.metrics.inc_snapshots_created();
        true
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Propose {
                cmd,
                client_id,
                seq_no,
                ack,
            } => {
                if self.node.leader_id() != self.raft_id {
                    let _ = ack.send(Err(ArachneError::NotLeader {
                        leader_hint: self.hint(),
                    }));
                    return;
                }
                // Q7: the bound is on committed-but-unapplied bytes — the log
                // the client cannot see yet — so a lagging apply path sheds
                // proposals instead of growing without bound (propsol N).
                if self.apply_backlog_bytes() >= self.proposal_queue_bytes {
                    self.metrics.inc_proposal_busy();
                    let _ = ack.send(Err(ArachneError::Busy));
                    return;
                }
                // Session bands (propsol v0.2.15 R1): a retry past the TTL but
                // inside the grace window is answered "result unknown" instead
                // of being proposed again.
                let band = self.session_band(client_id, seq_no);
                if band == SessionBand::Grace {
                    let _ = ack.send(Err(ArachneError::SessionExpired));
                    return;
                }
                // `max_sessions` (propsol v0.2.15 R2): only a *new* session can be
                // turned away, and only before proposing — a committed entry
                // cannot be refused by the state machine without breaking
                // replica agreement, and refusing to record it would turn a
                // retry into a fresh command. GC is what makes the cap
                // recoverable, which is why the two land together.
                if band == SessionBand::Fresh
                    && self.max_sessions > 0
                    && self.live_sessions >= self.max_sessions
                {
                    let _ = ack.send(Err(ArachneError::SessionTableFull));
                    return;
                }
                match self.node.propose(&cmd) {
                    Ok(()) => self.pending.push(Pending {
                        client_id,
                        seq_no,
                        ack: Some(ack),
                        deadline: Instant::now() + self.propose_timeout,
                        index: None,
                    }),
                    Err(_) => {
                        // A dropped proposal means we are (effectively) not the
                        // leader any more; never hang — report and let the
                        // client redirect.
                        let _ = ack.send(Err(ArachneError::NotLeader {
                            leader_hint: self.hint(),
                        }));
                    }
                }
            }
            Command::Membership { ack } => {
                let result = self
                    .node
                    .applied_conf_state()
                    .map(|conf| (conf.voters, conf.learners))
                    .map_err(|e| {
                        ArachneError::Unrecoverable(format!("reading the membership failed: {e}"))
                    });
                let _ = ack.send(result);
            }
            Command::TransferLeader { target, ack } => {
                if self.node.leader_id() != self.raft_id {
                    let _ = ack.send(Err(ArachneError::NotLeader {
                        leader_hint: self.hint(),
                    }));
                    return;
                }
                // Another transfer is already in flight: report it rather than
                // queueing a second campaign.
                if !self.pending_transfer.is_empty() {
                    let _ = ack.send(Err(ArachneError::ConfChangePending));
                    return;
                }
                let target = match target {
                    Some(target) => target,
                    None => match self.choose_transferee() {
                        Ok(target) => target,
                        Err(e) => {
                            let _ = ack.send(Err(e));
                            return;
                        }
                    },
                };
                self.node.transfer_leader(target);
                self.pending_transfer.push(PendingTransfer {
                    target,
                    ack: Some(ack),
                    deadline: Instant::now() + self.propose_timeout,
                });
            }
            Command::ConfChange {
                change_type,
                node_id,
                ack,
            } => {
                if self.node.leader_id() != self.raft_id {
                    let _ = ack.send(Err(ArachneError::NotLeader {
                        leader_hint: self.hint(),
                    }));
                    return;
                }
                // Hard constraint 3 (propsol §5.3): a leader is never removed
                // directly. The transfer cannot be done *here* either — once
                // leadership moves, this node can no longer propose — so the
                // caller is told to drive the two-step sequence, and
                // `Handle::remove_member` does exactly that.
                if change_type == ConfChangeType::RemoveNode && node_id == self.raft_id {
                    let _ = ack.send(Err(ArachneError::LeaderRemovalRequiresTransfer));
                    return;
                }
                // Hard constraint 2: a learner is promoted only once it is
                // online and caught up. Doing this before the proposal keeps a
                // voter with an incomplete log out of the quorum — and unlike a
                // post-hoc check it cannot be raced by the entry committing.
                if change_type == ConfChangeType::AddNode {
                    match self.node.is_learner(node_id) {
                        Ok(true) => {}
                        Ok(false) => {
                            let _ = ack.send(Err(ArachneError::InvalidArgument(format!(
                                "node {node_id} is not a learner in the applied \
                                 configuration; add it first with add_learner"
                            ))));
                            return;
                        }
                        Err(e) => {
                            let _ = ack.send(Err(ArachneError::Unrecoverable(format!(
                                "reading the membership failed: {e}"
                            ))));
                            return;
                        }
                    }
                    let (behind, has_acked) = self
                        .node
                        .learner_progress(node_id)
                        .unwrap_or((u64::MAX, false));
                    if !learner_caught_up(behind, has_acked, self.promote_lag_entries) {
                        let _ = ack.send(Err(ArachneError::LearnerNotCaughtUp {
                            behind,
                            threshold: self.promote_lag_entries,
                        }));
                        return;
                    }
                }
                // Hard constraint 1: at most one outstanding membership change.
                // Ordinary writes are a different command and are unaffected.
                if !self.pending_conf.is_empty() {
                    let _ = ack.send(Err(ArachneError::ConfChangePending));
                    return;
                }
                match self.node.propose_conf_change(change_type, node_id) {
                    Ok(()) => self.pending_conf.push(PendingConf {
                        change_type,
                        node_id,
                        ack: Some(ack),
                        deadline: Instant::now() + self.propose_timeout,
                        index: None,
                    }),
                    Err(_) => {
                        let _ = ack.send(Err(ArachneError::NotLeader {
                            leader_hint: self.hint(),
                        }));
                    }
                }
            }
            Command::GetStale { key, ack } => {
                // The apply task owns the state machine. This rides its own
                // channel, so a weak read never queues behind the write
                // backlog (propsol N1 / v0.2.11 N).
                if let Err(err) = self.reads.try_send(ReadRequest { key, ack }) {
                    match err {
                        mpsc::error::TrySendError::Full(req) => {
                            let _ = req.ack.send(Err(ArachneError::Busy));
                        }
                        mpsc::error::TrySendError::Closed(req) => {
                            let _ = req.ack.send(Err(ArachneError::ShuttingDown));
                        }
                    }
                }
            }
            Command::Read { key, ack } => {
                // Only the leader can serve a linearizable read (propsol §5.4
                // step 5); a non-leader redirects.
                if self.node.leader_id() != self.raft_id {
                    let _ = ack.send(Err(ArachneError::NotLeader {
                        leader_hint: self.hint(),
                    }));
                    return;
                }
                // Backpressure: bound the read-wait queue (propsol §4.1).
                if self.pending_reads.len() >= MAX_PENDING_READS {
                    let _ = ack.send(Err(ArachneError::Busy));
                    return;
                }
                // Register a ReadIndex round. The token is the 8-byte big-endian
                // `ctx` raft echoes back in `StepOutcome::read_states`.
                let token = self.next_read_token;
                self.next_read_token += 1;
                self.pending_reads.push(PendingRead {
                    token,
                    key,
                    ack: Some(ack),
                    deadline: Instant::now() + self.read_index_timeout,
                    read_index: None,
                    attempts: 1,
                });
                self.node.read_index(token.to_be_bytes().to_vec());
            }
            Command::LeaderHint { ack } => {
                let _ = ack.send(self.hint());
            }
        }
    }

    /// Reply to proposals that have been applied, and time out overdue ones.
    fn reply_pendings(&mut self) {
        let now = Instant::now();
        // Membership changes reply on the same rule as proposals: applied, or
        // timed out. `applied_index` is bumped past a ConfChange index when the
        // actor applies it, so this covers both kinds of entry.
        let mut still_conf = Vec::with_capacity(self.pending_conf.len());
        for mut p in self.pending_conf.drain(..) {
            let applied = matches!(p.index, Some(index) if self.applied_index >= index);
            if applied {
                if let Some(ack) = p.ack.take() {
                    let _ = ack.send(Ok(()));
                }
            } else if now >= p.deadline {
                if let Some(ack) = p.ack.take() {
                    let _ = ack.send(Err(ArachneError::Timeout));
                }
            } else {
                still_conf.push(p);
            }
        }
        self.pending_conf = still_conf;
        let mut still = Vec::with_capacity(self.pending.len());
        for mut p in self.pending.drain(..) {
            // The entry the proposal was attributed to is applied once the
            // applied index reaches it (propsol v0.2.11 N). A proposal whose
            // entry never reached the apply task keeps `None` and times out.
            let applied = matches!(p.index, Some(index) if self.applied_index >= index);
            if applied {
                if let Some(ack) = p.ack.take() {
                    let _ = ack.send(Ok(()));
                }
            } else if now >= p.deadline {
                if let Some(ack) = p.ack.take() {
                    let _ = ack.send(Err(ArachneError::Timeout));
                }
            } else {
                still.push(p);
            }
        }
        self.pending = still;
    }

    /// Pick a voter to hand leadership to, excluding this node (rev S).
    ///
    /// The lowest id wins so the choice is deterministic and testable.
    /// `LeaderRemovalRequiresTransfer` is the right rejection when there is
    /// nowhere to go: removing the only voter would leave an empty
    /// configuration, which raft cannot represent.
    fn choose_transferee(&self) -> Result<RaftId, ArachneError> {
        let voters = self
            .node
            .voter_ids()
            .map_err(|e| ArachneError::Unrecoverable(format!("reading the voter set failed: {e}")))?;
        voters
            .into_iter()
            .filter(|v| *v != self.raft_id)
            .min()
            .ok_or(ArachneError::LeaderRemovalRequiresTransfer)
    }

    /// Resolve pending leadership transfers (rev S S2).
    ///
    /// Success is the leadership *actually* moving to the target — raft
    /// accepting the transfer message proves nothing. A leadership that landed
    /// on a third node, or a timeout, is a failure. A brief `None` (the old
    /// leader has stepped down but has not yet learned the new one) is not a
    /// verdict: it waits for the deadline.
    fn resolve_transfers(&mut self) {
        // Nothing in flight: return before reading the leader and building a
        // hint, because this runs on every drive cycle.
        if self.pending_transfer.is_empty() {
            return;
        }
        let now = Instant::now();
        let leader = self.node.leader_id();
        // Read the hint before draining: `self.hint()` needs `&self`, which the
        // drain borrow rules out for the rest of the loop.
        let hint = self.hint();
        let mut still = Vec::with_capacity(self.pending_transfer.len());
        for mut t in self.pending_transfer.drain(..) {
            if leader == t.target {
                if let Some(ack) = t.ack.take() {
                    let _ = ack.send(Ok(()));
                }
            } else if leader != 0 && leader != self.raft_id && leader != t.target {
                // Leadership went somewhere else entirely (0 means "not known
                // yet", which is why it is not a verdict here).
                if let Some(ack) = t.ack.take() {
                    let _ = ack.send(Err(ArachneError::NotLeader {
                        leader_hint: hint.clone(),
                    }));
                }
            } else if now >= t.deadline {
                if let Some(ack) = t.ack.take() {
                    let _ = ack.send(Err(ArachneError::Timeout));
                }
            } else {
                still.push(t);
            }
        }
        self.pending_transfer = still;
    }

    /// Resolve pending ReadIndex reads (propsol §5.4 steps 3–4).
    ///
    /// For each pending read:
    /// * if leadership was lost, reply `NotLeader{hint}` (step-down clears
    ///   waiters);
    /// * else if the read index is applied, reply the local value;
    /// * else if the wait timed out, retry once (re-issue the ReadIndex round
    ///   with a fresh token) or reply `Timeout`;
    /// * otherwise keep it pending.
    fn resolve_reads(&mut self) {
        let now = Instant::now();
        // Drain into an owned vec first so the `&mut self.pending_reads` borrow
        // is released before we call `&self` / `&mut self.node` methods.
        let drained: Vec<PendingRead> = self.pending_reads.drain(..).collect();
        let mut still = Vec::with_capacity(drained.len());
        for mut r in drained {
            if self.node.leader_id() != self.raft_id {
                let _ = r.ack.take().map(|ack| {
                    ack.send(Err(ArachneError::NotLeader {
                        leader_hint: self.hint(),
                    }))
                });
                continue;
            }

            let applied = matches!(
                r.read_index,
                Some(idx) if self.applied_index >= idx
            );
            if applied {
                if let Some(ack) = r.ack.take() {
                    // Fetch through the apply task (it owns the state machine)
                    // and let it reply directly to the caller.
                    if let Err(err) = self.reads.try_send(ReadRequest { key: r.key, ack }) {
                        match err {
                            mpsc::error::TrySendError::Full(req) => {
                                let _ = req.ack.send(Err(ArachneError::Busy));
                            }
                            mpsc::error::TrySendError::Closed(req) => {
                                let _ = req.ack.send(Err(ArachneError::ShuttingDown));
                            }
                        }
                    }
                }
                continue;
            }

            if now >= r.deadline {
                if r.attempts <= 1 {
                    // One retry: re-issue the ReadIndex round with a fresh token.
                    r.attempts += 1;
                    r.read_index = None;
                    r.deadline = now + self.read_index_timeout;
                    let new_token = self.next_read_token;
                    self.next_read_token += 1;
                    r.token = new_token;
                    self.node.read_index(new_token.to_be_bytes().to_vec());
                    still.push(r);
                } else {
                    self.metrics.inc_read_index_timeout();
                    let _ = r.ack.take().map(|ack| ack.send(Err(ArachneError::Timeout)));
                }
                continue;
            }

            still.push(r);
        }
        self.pending_reads = still;
    }

    fn fail_all_pending(&mut self, message: &str) {
        for mut p in self.pending_conf.drain(..) {
            if let Some(ack) = p.ack.take() {
                let _ = ack.send(Err(ArachneError::Unrecoverable(message.to_string())));
            }
        }
        for mut t in self.pending_transfer.drain(..) {
            if let Some(ack) = t.ack.take() {
                let _ = ack.send(Err(ArachneError::Unrecoverable(message.to_string())));
            }
        }
        for mut p in self.pending.drain(..) {
            if let Some(ack) = p.ack.take() {
                let _ = ack.send(Err(ArachneError::Unrecoverable(message.to_string())));
            }
        }
        // Also fail all in-flight ReadIndex reads (a fatal error means the
        // process is failing stop).
        for mut r in self.pending_reads.drain(..) {
            if let Some(ack) = r.ack.take() {
                let _ = ack.send(Err(ArachneError::Unrecoverable(message.to_string())));
            }
        }
    }

    /// Which part of a session's life a proposal falls in (propsol v0.2.15 R1).
    ///
    /// * `Live` — inside the TTL: propose it; the state machine dedups, so the
    ///   effect is exactly once.
    /// * `Grace` — after the TTL but inside the grace window: answer
    ///   `SessionExpired` ("result unknown") and do **not** propose. This is
    ///   what bounds a duplicate's effect to `ttl + grace`.
    /// * `Fresh` — past both: treat it as a new session. Safe while the state
    ///   machine still holds the outcome (until GC, R2, removes it), and the
    ///   client was told not to retry this far out.
    fn session_band(&self, client_id: u64, seq_no: u64) -> SessionBand {
        let Some(clock) = &self.session_clock else {
            return SessionBand::Live;
        };
        if self.session_ttl_ms == 0 {
            return SessionBand::Live;
        }
        let Some(last) = self.sessions.get(&(client_id, seq_no)) else {
            return SessionBand::Fresh;
        };
        let elapsed = clock.now_millis().saturating_sub(*last);
        if elapsed <= self.session_ttl_ms {
            SessionBand::Live
        } else if elapsed <= self.session_ttl_ms.saturating_add(self.session_grace_ms) {
            SessionBand::Grace
        } else {
            SessionBand::Fresh
        }
    }

    /// Record that `(client_id, seq_no)` was used, sweeping expired entries out
    /// of the local table on a timer so it stays bounded by the grace window
    /// rather than by the write volume.
    fn note_session(&mut self, client_id: u64, seq_no: u64) {
        let Some(clock) = &self.session_clock else {
            return;
        };
        if self.session_ttl_ms == 0 {
            return;
        }
        let now = clock.now_millis();
        let window = self
            .session_ttl_ms
            .saturating_add(self.session_grace_ms)
            .max(1);
        if now >= self.next_session_sweep {
            self.next_session_sweep = now.saturating_add(window);
            self.sessions
                .retain(|_, last| now.saturating_sub(*last) <= window);
        }
        self.sessions.insert((client_id, seq_no), now);
    }

    /// Tag an inbound peer message with its raft id and hand it to raft.
    fn route_inbound(&mut self, from: NodeId, msg: TransportMessage) {
        if let Some(id) = self.node_to_raft.get(from.as_str()).copied() {
            let _ = self.node.on_message(id, msg);
            // rev T: a streamed snapshot message is held back by the node (raft
            // must not restore metadata the storage cannot honour). Fetch its
            // bytes now, off the actor.
            if let Some(held) = self.node.take_held_snapshot() {
                self.start_snapshot_fetch(held);
            }
        }
    }

    /// Start the background transfer for a held snapshot message (rev T).
    ///
    /// The fetch runs in its own task: a snapshot can be tens of megabytes and
    /// is paced, so awaiting it here would stall ticks, heartbeats and reads for
    /// the whole transfer.
    fn start_snapshot_fetch(&mut self, held: HeldSnapshot) {
        // A newer snapshot supersedes one still in flight; its completion is
        // ignored by the identity check in `finish_snapshot_fetch`, and its file
        // is dropped here.
        if let Some(previous) = self.snapshot_fetch.take() {
            let _ = std::fs::remove_file(&previous.dest);
        }
        let Some(node_id) = self.raft_to_node.get(&held.from).cloned() else {
            // The sender is not in this node's peer map: nothing to fetch from,
            // so report failure (raft retries) instead of stalling.
            self.node.report_snapshot(held.from, false);
            return;
        };
        let dest = std::env::temp_dir().join(format!(
            "arachne-snapshot-{}-{}-{}.fetch",
            held.from, held.index, self.raft_id
        ));
        let transport = self.transport.clone();
        let done_tx = self.snapshot_fetches.clone();
        let (from, index, term) = (held.from, held.index, held.term);
        let task_dest = dest.clone();
        tokio::spawn(async move {
            let result = transport
                .fetch_snapshot(node_id, index, term, &task_dest)
                .await
                .map_err(|e| e.to_string());
            // A full queue means the actor is gone; nothing to report to.
            let _ = done_tx
                .send(SnapshotFetchDone {
                    from,
                    index,
                    dest: task_dest,
                    result,
                })
                .await;
        });
        self.snapshot_fetch = Some(SnapshotFetch { held, dest });
    }

    /// Finish a streamed snapshot transfer: install, step, report (rev T).
    ///
    /// Nothing has reached raft while the transfer was in flight, so every
    /// failure path here is simply "report failure and let raft retry": no
    /// metadata was restored that the storage cannot back.
    fn finish_snapshot_fetch(&mut self, done: SnapshotFetchDone) {
        let is_current = self.snapshot_fetch.as_ref().is_some_and(|f| {
            f.held.from == done.from && f.held.index == done.index
        });
        if !is_current {
            // Superseded by a newer snapshot: drop the bytes and say nothing.
            let _ = std::fs::remove_file(&done.dest);
            return;
        }
        let fetch = self.snapshot_fetch.take().expect("checked just above");

        let Some(file_bytes) = (match done.result {
            Ok(Some(_)) => std::fs::read(&done.dest).ok(),
            // `Ok(None)` = this transport cannot stream; `Err` = the transfer
            // failed. Neither is an empty snapshot.
            _ => None,
        }) else {
            slog::warn!(
                self.logger,
                "streamed snapshot fetch failed";
                "from" => fetch.held.from,
                "index" => fetch.held.index,
            );
            self.node.report_snapshot(fetch.held.from, false);
            let _ = std::fs::remove_file(&done.dest);
            return;
        };

        let snapshot = match crate::storage::snapshot::decode_snapshot(&file_bytes) {
            Ok(snapshot) => snapshot,
            Err(e) => {
                // The file carries its own CRC, so this is a corrupt or
                // truncated transfer: fail it rather than install it (I9).
                slog::warn!(
                    self.logger,
                    "streamed snapshot failed to decode";
                    "error" => %e,
                    "index" => fetch.held.index,
                );
                self.node.report_snapshot(fetch.held.from, false);
                let _ = std::fs::remove_file(&done.dest);
                return;
            }
        };

        // Durable install first: raft must never restore a snapshot the storage
        // does not have. `install_snapshot` is idempotent, so the copy that
        // arrives through the next `Ready` is harmless.
        // Step it into raft **before** touching storage. The message now carries
        // the real bytes, so raft restores its log state and the ordinary
        // `Ready` path installs the snapshot and hands it to the state machine —
        // one code path for both routes. Installing first would be wrong: raft's
        // `restore` verifies the snapshot against the *storage*, and a storage
        // that had already rotated its log past that index makes `restore` fail
        // silently, leaving raft's log inconsistent with it (observed as a raft
        // `unstable.slice` out-of-bounds panic on the next AppendEntries).
        let data = snapshot.data.clone();
        if let Err(e) = self.node.step_held_snapshot(&fetch.held, data) {
            // A codec/raft failure: this node cannot make progress from here.
            self.fail_all_pending(&format!("stepping a fetched snapshot failed: {e}"));
            self.node.report_snapshot(fetch.held.from, false);
            let _ = std::fs::remove_file(&done.dest);
            return;
        }
        // raft keeps the follower in `Snapshot` state (and sends it nothing
        // else) until it hears how the transfer ended.
        self.node.report_snapshot(fetch.held.from, true);
        let _ = std::fs::remove_file(&done.dest);
        self.metrics.inc_snapshots_installed();
    }

    /// The current leader hint: `(node id, address)` if a leader is known.
    fn hint(&self) -> Option<(NodeId, SocketAddr)> {
        let leader = self.node.leader_id();
        if leader == 0 {
            return None;
        }
        let node = self.raft_to_node.get(&leader)?;
        let addr = self.addresses.get(node)?;
        Some((node.clone(), *addr))
    }

    fn refresh_metrics(&self) {
        let hs = self.node.hard_state();
        self.metrics.set_term(hs.term);
        self.metrics.set_commit_index(hs.commit);
        self.metrics.set_applied_index(self.applied_index);
        self.metrics.set_apply_backlog(
            hs.commit.saturating_sub(self.applied_index),
            self.apply_backlog_bytes(),
        );
        self.metrics.set_leader_id(self.node.leader_id());
        self.metrics
            .set_is_leader(self.node.leader_id() == self.raft_id);
        self.metrics.set_dropped_sends(self.node.dropped_send_count());
        self.metrics.set_read_index_pending(self.pending_reads.len() as u64);
        self.metrics.set_session_count(self.live_sessions);
    }

    /// This node's cluster identity.
    pub fn node_id(&self) -> &NodeId {
        &self.self_node
    }

    /// The highest applied log index (as reported by the apply task).
    pub fn applied_index(&self) -> LogIndex {
        self.applied_index
    }
}

/// A [`Runtime`] running on its own OS thread.
pub struct RuntimeThread {
    handle: Option<std::thread::JoinHandle<()>>,
    stop: Arc<tokio::sync::Notify>,
}

impl RuntimeThread {
    /// Ask the actor to stop and wait for its thread to end.
    ///
    /// Dropping the handle only detaches; this is what releases the storage
    /// (and its data-dir lock) without depending on a transport closing.
    pub fn shutdown(mut self) {
        // `notify_one` stores a permit, so a stop fired before the actor next
        // polls its stop branch is not lost (`notify_waiters` would only wake
        // waiters that already exist).
        self.stop.notify_one();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Ask the actor to stop without waiting for it.
    pub fn stop(&self) {
        self.stop.notify_one();
    }

    /// Wait for the actor to stop (it stops when its inbound stream and command
    /// channel close, on a fail-stop, or after [`stop`](Self::stop)).
    pub fn join(mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for RuntimeThread {
    fn drop(&mut self) {
        // Detached on drop: dropping the join handle leaves the thread running,
        // which is what an embedder that keeps its `Handle`s wants. Joining here
        // would block the (usually async) caller.
        self.handle.take();
    }
}

impl<T, Tr> Runtime<T, Tr>
where
    T: Transport + Clone + Send + 'static,
    Tr: TransportRx + Send + 'static,
{
    /// Run this actor on a **dedicated OS thread** with its own current-thread
    /// runtime.
    ///
    /// The consensus loop performs blocking durable-storage I/O: every
    /// `sync_entries` and every `set_hard_state` is a real `fsync` of a segment
    /// (and META updates flush the directory too). A full-durability flush of a
    /// device costs milliseconds, and running the loop as a task on a shared
    /// tokio runtime parks those flushes on worker threads that the transport
    /// and client tasks also need — so a write storm starves unrelated work
    /// (measured: ~10ms per flush, with everything client-facing queueing behind
    /// it).
    ///
    /// Blocking I/O belongs on its own thread: this keeps the async runtime for
    /// what it is good at (network and client work) while the actor keeps its
    /// synchronous storage seam. It does **not** remove the actor's own
    /// serialization on durability — a read still waits for the flush the actor
    /// is currently inside — so this bounds *worker* starvation, not the
    /// per-flush latency (see the handoff: group commit is the separate change
    /// for that).
    pub fn spawn_dedicated(self) -> Result<RuntimeThread, std::io::Error> {
        let name = format!("arachne-consensus-{}", self.self_node);
        let stop = Arc::clone(&self.stop);
        let handle = std::thread::Builder::new().name(name).spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                // Without a runtime the actor cannot run at all; it stops, and
                // the node's `shutdown` join observes the thread ending.
                Err(_) => return,
            };
            runtime.block_on(self.run());
        })?;
        Ok(RuntimeThread {
            handle: Some(handle),
            stop: Arc::clone(&stop),
        })
    }
}
