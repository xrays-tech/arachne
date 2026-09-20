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
//! The runtime is **generic over the transport** (`T`/`Tr`) so the same actor
//! drives the in-memory transport in tests and the tonic transport in
//! production; it names no concrete transport type.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use slog::Logger;
use tokio::sync::{mpsc, oneshot};

/// Approximate per-entry durable overhead (record header, type byte, and the
/// index/term fields) used to size the snapshot trigger.
const ENTRY_FRAMING_BYTES: u64 = 24;

use crate::client::{ArachneError, Handle};
use crate::consensus::{NodeError, RaftNode, RaftNodeConfig};
use crate::metrics::Metrics;
use crate::profile::ProfileConfig;
use crate::state_machine::KvStateMachine;
use crate::storage::WalStorage;
use crate::{LogIndex, NodeId, RaftId, StateMachine, Transport, TransportMessage, TransportRx};
use arachne_seam::storage::{ConfState as SeamConfState, Storage as _};

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

enum Outcome {
    Tick,
    Inbound(Option<(NodeId, TransportMessage)>),
    Command(Option<Command>),
}

/// The node runtime actor.
pub struct Runtime<T: Transport, Tr: TransportRx> {
    node: RaftNode<WalStorage, T, Tr>,
    sm: KvStateMachine,
    metrics: Arc<Metrics>,
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
    /// Bytes of applied log written since the last snapshot — the trigger's
    /// input. This is the *logical* growth of the log, not the physical size
    /// of the segment files: v1 compacts whole segments, so a segment holding
    /// a compacted prefix keeps its bytes until it rolls over, and using
    /// physical size would re-fire the trigger on every entry.
    bytes_since_snapshot: u64,
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
        let runtime = Self {
            node,
            sm,
            metrics: config.metrics,
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
            bytes_since_snapshot: 0,
            snapshot_index,
        };
        runtime.refresh_metrics();
        Ok((runtime, handle))
    }

    /// Run the actor until the transport closes or the command channel ends.
    pub async fn run(mut self) {
        loop {
            let outcome = tokio::select! {
                biased;
                _ = self.tick.tick() => Outcome::Tick,
                msg = self.node.rx().recv() => Outcome::Inbound(msg),
                cmd = self.commands.recv() => Outcome::Command(cmd),
            };
            match outcome {
                Outcome::Tick => self.node.tick(),
                Outcome::Inbound(Some((from, msg))) => {
                    if let Some(id) = self.node_to_raft.get(from.as_str()).copied() {
                        let _ = self.node.on_message(id, msg);
                    }
                }
                Outcome::Inbound(None) => break,
                Outcome::Command(Some(cmd)) => self.handle_command(cmd),
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
        let outcome = match self.node.step().await {
            Ok(outcome) => outcome,
            Err(e) => {
                self.fail_all_pending(&format!("raft step failed: {e}"));
                self.metrics.set_is_leader(false);
                return false;
            }
        };

        if let Some(snapshot) = outcome.snapshot {
            // An installed snapshot replaces the *entire* state, including the
            // idempotency session table (propsol §5.5.4): a follower that
            // restored without it would re-apply retried writes. The storage
            // already persisted the snapshot and reset the log, so this is the
            // in-memory half of the install.
            if let Err(e) = self.sm.restore(&snapshot.data) {
                self.fail_all_pending(&format!(
                    "state machine restore failed for the snapshot at {}: {e}",
                    snapshot.meta.index
                ));
                self.metrics.set_is_leader(false);
                return false;
            }
            self.snapshot_index = self.snapshot_index.max(snapshot.meta.index);
            self.metrics.inc_snapshots_installed();
        }

        for (index, data) in outcome.committed {
            if let Err(e) = self.sm.apply(index, &data) {
                self.fail_all_pending(&format!("state machine apply failed: {e}"));
                self.metrics.set_is_leader(false);
                return false;
            }
            // Approximate the durable footprint of this entry (framing, type
            // byte, index/term header, payload) for the snapshot trigger.
            self.bytes_since_snapshot += data.len() as u64 + ENTRY_FRAMING_BYTES;
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

        self.node.advance_apply();
        self.resolve_reads();
        self.reply_pendings();
        if !self.maybe_snapshot() {
            return false;
        }
        self.refresh_metrics();
        true
    }

    /// Take a local snapshot and compact the log once the WAL outgrows
    /// `snapshot_threshold` (propsol §5.5.4, §7). Returns `false` on a
    /// fail-stop error.
    ///
    /// Two guards keep this from firing in a loop: the threshold itself, and
    /// [`Self::snapshot_index`] — a snapshot is only ever taken at an index
    /// strictly newer than the one already covered.
    fn maybe_snapshot(&mut self) -> bool {
        let applied = self.sm.applied_index();
        match self.node.log_bytes() {
            Ok(bytes) => self.metrics.set_wal_bytes(bytes),
            Err(e) => {
                self.fail_all_pending(&format!("cannot size the durable log: {e}"));
                self.metrics.set_is_leader(false);
                return false;
            }
        }

        if self.snapshot_threshold_bytes == 0
            || applied <= self.snapshot_index
            || self.bytes_since_snapshot < self.snapshot_threshold_bytes
        {
            return true;
        }

        // The snapshot is of the applied state, so its index must be the
        // applied index: a snapshot ahead of what this node applied would let
        // compaction drop entries it never wrote to the state machine.
        let term = match self.node.term_at(applied) {
            Ok(term) => term,
            Err(e) => {
                self.fail_all_pending(&format!("cannot resolve the term at {applied}: {e}"));
                self.metrics.set_is_leader(false);
                return false;
            }
        };
        let data = match self.sm.snapshot() {
            Ok(data) => data,
            Err(e) => {
                self.fail_all_pending(&format!("state machine snapshot failed: {e}"));
                self.metrics.set_is_leader(false);
                return false;
            }
        };
        let size_bytes = data.len() as u64;
        let conf_state = SeamConfState {
            voters: self.voters.clone(),
            learners: Vec::new(),
        };

        // Q4: snapshot creation blocks apply. It is measured every time so the
        // >1s budget alarm has data (propsol §8.2).
        let started = Instant::now();
        if let Err(e) = self.node.create_snapshot(applied, term, conf_state, data) {
            self.fail_all_pending(&format!("snapshot creation failed: {e}"));
            self.metrics.set_is_leader(false);
            return false;
        }
        let elapsed_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.snapshot_index = applied;
        self.bytes_since_snapshot = 0;
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
                match self.node.propose(&cmd) {
                    Ok(()) => self.pending.push(Pending {
                        client_id,
                        seq_no,
                        ack: Some(ack),
                        deadline: Instant::now() + self.propose_timeout,
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
                let _ = ack.send(self.read_local(&key));
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

    fn read_local(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ArachneError> {
        self.sm
            .get(key)
            .map_err(|e| ArachneError::Unrecoverable(format!("state machine read failed: {e}")))
    }

    /// Reply to proposals that have been applied, and time out overdue ones.
    fn reply_pendings(&mut self) {
        let now = Instant::now();
        let mut still = Vec::with_capacity(self.pending.len());
        for mut p in self.pending.drain(..) {
            if self.sm.applied_session(p.client_id, p.seq_no) {
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
                Some(idx) if self.sm.applied_index() >= idx
            );
            if applied {
                let _ = r.ack.take().map(|ack| ack.send(self.read_local(&r.key)));
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
        self.metrics.set_applied_index(self.sm.applied_index());
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

    /// The highest applied log index.
    pub fn applied_index(&self) -> LogIndex {
        self.sm.applied_index()
    }
}
