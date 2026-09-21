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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use slog::Logger;
use tokio::sync::{mpsc, oneshot, watch};

use crate::client::{ArachneError, Handle};
use crate::consensus::{NodeError, RaftNode, RaftNodeConfig};
use crate::metrics::Metrics;
use crate::profile::ProfileConfig;
use crate::state_machine::KvStateMachine;
use crate::storage::WalStorage;
use crate::{LogIndex, NodeId, RaftId, StateMachine, Transport, TransportMessage, TransportRx};
use arachne_seam::storage::{
    ConfState as SeamConfState, Snapshot as SeamSnapshot, Storage as _,
};

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
        };
        self.progress.send_if_modified(|current| {
            if current.applied_index == next.applied_index
                && current.applied_bytes_total == next.applied_bytes_total
                && current.failed == next.failed
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
}

/// The node runtime actor.
pub struct Runtime<T: Transport, Tr: TransportRx> {
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
    /// The last confirmed applied index (from the apply task).
    applied_index: LogIndex,
    /// Cumulative bytes confirmed applied.
    applied_bytes_total: u64,
    /// Cumulative bytes handed to the apply task (for backlog accounting).
    sent_bytes_total: u64,
    /// Byte bound on committed-but-unapplied work (Q7 `proposal_queue_bytes`).
    proposal_queue_bytes: u64,
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
    /// Linearizable reads awaiting quorum confirmation / apply (propsol §5.4).
    pending_reads: Vec<PendingRead>,
    /// Wait timeout for a ReadIndex round / apply window (`2 × election_timeout`).
    read_index_timeout: Duration,
    /// Monotonic source of ReadIndex tokens (8-byte big-endian `ctx`).
    next_read_token: u64,
    propose_timeout: Duration,
    /// The voting membership this node was started with. It travels into every
    /// local snapshot so that a node restored from one knows the configuration
    /// (propsol §5.5.4); ConfChange is M3.
    voters: Vec<RaftId>,
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

impl<T: Transport, Tr: TransportRx> Runtime<T, Tr> {
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
        });
        let apply_task = ApplyTask {
            sm,
            applies: applies_rx,
            reads: reads_rx,
            progress: progress_tx,
            applied_bytes_total: 0,
            failed: None,
        };

        let node = RaftNode::new_with_config(
            config.self_raft_id,
            config.peers.clone(),
            storage,
            transport,
            rx,
            0,
            config.raft,
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

        let mut voters: Vec<RaftId> = config.peers.keys().copied().collect();
        voters.push(config.self_raft_id);
        voters.sort_unstable();
        voters.dedup();

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
            applied_index,
            applied_bytes_total: 0,
            sent_bytes_total: 0,
            proposal_queue_bytes: config.profile.proposal_queue_bytes,
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
            pending_reads: Vec::new(),
            read_index_timeout: Duration::from_millis(
                config.profile.read_index_timeout_ms.max(1),
            ),
            next_read_token: 0,
            propose_timeout: Duration::from_millis(config.profile.election_timeout_ms.max(1)),
            voters,
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

        // Hand committed work to the apply task. It owns the state machine, so
        // a saturated apply path can no longer keep the actor from ticking or
        // from answering the ReadIndex round (propsol §347).
        if !self.enqueue_apply(outcome.snapshot, outcome.committed) {
            self.fail_all_pending("the apply task stopped");
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

        self.resolve_reads();
        self.reply_pendings();
        if !self.maybe_snapshot() {
            return false;
        }
        self.refresh_metrics();
        true
    }

    /// Apply the task's progress to the actor's view, and advance raft's
    /// applied index to match (propsol N). Returns `false` on a fail-stop.
    fn absorb_progress(&mut self) -> bool {
        let progress = self.progress.borrow().clone();
        self.applied_index = progress.applied_index;
        self.applied_bytes_total = progress.applied_bytes_total;
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
            if let Some((client_id, seq_no)) = KvStateMachine::command_session(data)
                && let Some(p) = self
                    .pending
                    .iter_mut()
                    .find(|p| p.index.is_none() && p.client_id == client_id && p.seq_no == seq_no)
            {
                p.index = Some(*index);
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
        let conf_state = SeamConfState {
            voters: self.voters.clone(),
            learners: Vec::new(),
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

    /// Tag an inbound peer message with its raft id and hand it to raft.
    fn route_inbound(&mut self, from: NodeId, msg: TransportMessage) {
        if let Some(id) = self.node_to_raft.get(from.as_str()).copied() {
            let _ = self.node.on_message(id, msg);
        }
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
    T: Transport + Send + 'static,
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
