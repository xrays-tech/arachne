//! `RaftNode`: wraps `raft::RawNode` with the frozen persist/send ordering.
//!
//! # Ready loop (invariants I1–I4)
//!
//! Each [`RaftNode::step`] call drives exactly one `Ready` cycle:
//!
//! 1. `ready()` → obtain the [`Ready`];
//! 2. **persist** `entries` (then `sync_entries`, the I2/I4 durability barrier)
//!    and `hard_state` (I1: always fsyncs) and `snapshot` durably;
//! 3. **send** `ready.messages()` — the leader's fast-path messages;
//! 4. `advance(ready)` → confirm persistence, obtain the `LightReady`;
//! 5. **send** `ready.persisted_messages()` (now durable) and any messages the
//!    `advance` produced.
//!
//! **No message is sent before its payload is durable** (I2/I4): every send in
//! steps 3 and 5 happens after step 2 has made the corresponding entries and
//! hard state durable.
//!
//! The node persists through the *same* `RaftStorage` the `RawNode` reads from
//! (reached via `RawNode::mut_store`), so there is a single home for durable
//! state and no second cache that could diverge.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use protobuf::Message as _;
use raft::eraftpb::{ConfChange, ConfChangeType, ConfChangeV2, Message};
// For `RaftStorage`'s raft `Storage` impl (`initial_state` reads the applied
// membership), which is otherwise not in scope for method resolution.
use raft::storage::Storage as _;
use raft::ReadOnlyOption;
use raft::{Config as RaftConfig, RawNode};
use slog::Logger;

use arachne_seam::seam::{Transport, TransportMessage, TransportRx};
use arachne_seam::storage::{
    ConfState as SeamConfState, EntryType as SeamEntryType, FlushToken, FlushWaker,
    HardState as SeamHardState, LogEntry, PersistSubmit, RaftId, Snapshot as SeamSnapshot,
    SnapshotMeta as SeamSnapshotMeta, Storage as SeamStorage,
};
use arachne_seam::types::{LogIndex, NodeId, Term};

use super::raft_storage::RaftStorage;
use crate::profile::ProfileConfig;
use crate::storage::WalStorage;

/// Errors reported by the consensus node.
///
/// `Raft` covers the raft core and storage; `Transport` covers a failed send.
/// A failed send is surfaced (not swallowed) even though raft will re-send on
/// the next tick — the payload is already durable, so the fault is
/// non-fatal for consensus but must still be reported to the caller.
#[derive(Debug, thiserror::Error)]
pub enum NodeError<T: Transport> {
    /// An error from the raft core or the durable storage.
    #[error("{0}")]
    Raft(raft::Error),
    /// An error from the transport while delivering a message.
    #[error("{0}")]
    Transport(T::Error),
    /// A raft wire-message codec error (encode/decode of `Message` bytes).
    #[error("codec error: {0}")]
    Codec(String),
    /// A durable-storage failure outside the `Ready` loop: startup recovery or
    /// local snapshot creation. A fail-start condition (propsol §5.5.3).
    #[error("storage error: {0}")]
    Storage(String),
    /// The state machine could not be rebuilt from a durable snapshot. A
    /// fail-start condition: every later apply would be against wrong state.
    #[error("state machine error: {0}")]
    StateMachine(String),
}

/// Raft tick and flow-control timings for a [`RaftNode`].
///
/// These are the knobs that were previously hardcoded in [`RaftNode::new`].
/// A deployment derives them from a [`ProfileConfig`] (via
/// [`RaftNodeConfig::from_profile`]) or uses [`Default`] (the original
/// hardcoded values, kept so existing callers and tests are unchanged).
#[derive(Clone, Debug)]
pub struct RaftNodeConfig {
    /// Number of ticks for an election timeout.
    pub election_tick: u64,
    /// Number of ticks between heartbeats.
    pub heartbeat_tick: u64,
    /// Max total bytes in a single raft message (AppendEntries batching).
    pub max_size_per_msg: u64,
    /// Max messages in flight to a single follower (raft built-in flow control).
    pub max_inflight_msgs: u64,
    /// The voter set this node should start with, when it must differ from the
    /// transport peers it was configured with (rev S S5).
    ///
    /// `None` means "the configured peers plus this node", which is the right
    /// answer for a cluster that is being created. It is the **wrong** answer
    /// for a node joining a cluster that already exists: to send raft messages
    /// to the new member, every existing node must know its address and id, but
    /// knowing where a node is must not make it a voter. Without this field
    /// those two facts are conflated, and the existing members would bootstrap
    /// a configuration the rest of the cluster never agreed to.
    pub bootstrap_voters: Option<Vec<RaftId>>,
    /// Start this node as a **learner** of the peers it was given, rather than
    /// as a voter (propsol §5.3 hard constraint 2, rev S S5).
    ///
    /// A joining node has no durable membership yet, so its bootstrap
    /// configuration is a declaration: the configured peers are the voters and
    /// this node is a learner. That is the only way raft will let it receive
    /// the log without also letting it campaign — and it is exactly the
    /// configuration the leader's `add_learner` will then make official. Once
    /// a persisted configuration exists, it wins over this declaration
    /// (`RaftStorage::initial_state`).
    pub join_as_learner: bool,
}

impl Default for RaftNodeConfig {
    /// The values previously hardcoded in [`RaftNode::new`].
    fn default() -> Self {
        Self {
            election_tick: 10,
            heartbeat_tick: 1,
            max_size_per_msg: 1_048_576,
            max_inflight_msgs: 256,
            bootstrap_voters: None,
            join_as_learner: false,
        }
    }
}

impl RaftNodeConfig {
    /// Derive raft tick timings and flow-control limits from a
    /// [`ProfileConfig`] (propsol §7 → §4.2).
    ///
    /// `heartbeat_tick` is fixed at 1 (one tick == one heartbeat interval);
    /// `election_tick` is the election timeout expressed in heartbeat ticks,
    /// `election_timeout_ms / heartbeat_interval_ms`, clamped to >= 1 (10 for
    /// the Lan preset, 5 for the Wan preset). `max_size_per_msg` and
    /// `max_inflight_msgs` are taken from the profile's per-follower inflight
    /// limits.
    pub fn from_profile(profile: &ProfileConfig) -> Self {
        let heartbeat = profile.heartbeat_interval_ms.max(1);
        Self {
            heartbeat_tick: 1,
            election_tick: (profile.election_timeout_ms / heartbeat).max(1),
            max_size_per_msg: profile.max_inflight_bytes,
            max_inflight_msgs: profile.max_inflight_msgs,
            // Joining as a learner is a per-node role, not a profile limit;
            // `ProfileConfig` deliberately does not carry it (it would also
            // break the profile-knob gate's "every knob is a limit" reading).
            // An operator sets these two directly on the node config.
            bootstrap_voters: None,
            join_as_learner: false,
        }
    }
}

impl<T: Transport, Tr: TransportRx> RaftNode<WalStorage, T, Tr> {
    /// Bytes the durable WAL occupies on disk.
    ///
    /// This is the input to the snapshot trigger (propsol §5.5.4): the runtime
    /// does not need to know how segments are laid out, only how much space
    /// the log is holding.
    pub fn log_bytes(&self) -> Result<u64, NodeError<T>> {
        self.raw.store().log_bytes().map_err(NodeError::Raft)
    }
}

/// How many `Ready` cycles may have records written but not yet flushed.
///
/// One is not enough: while a device flush is in the air the node must still be
/// able to take a `Ready` for a ReadIndex round, or reads would queue behind
/// durability again (propsol v0.2.13 P).
const MAX_IN_FLIGHT_READIES: usize = 8;

/// A cycle whose records are written, waiting for durability (propsol P).
struct PendingPersist {
    /// raft's `Ready` number, for `on_persist_ready`.
    number: u64,
    /// The flush covering the records, if the storage offloaded it.
    token: Option<FlushToken>,
    /// Leader fast-path messages, still to be sent in the synchronous path
    /// (the offloaded path sends them at submit: raft classifies them as
    /// not requiring local durability).
    immediate: Vec<Message>,
    /// Messages that must not leave before their payload is durable.
    persisted: Vec<Message>,
    /// Committed entries to hand to the state machine or to the membership
    /// machinery, tagged with their raft entry type (rev S S1b).
    committed: Vec<CommittedEntry>,
    /// Quorum-confirmed read states.
    read_states: Vec<(Vec<u8>, LogIndex)>,
    /// An installed snapshot this cycle carried.
    snapshot: Option<SeamSnapshot>,
}

/// A committed log entry on its way out of [`RaftNode::step`]:
/// `(index, entry type, data)`.
///
/// The type matters because a ConfChange entry's payload is a ConfChange
/// protobuf, **not** a state-machine command: feeding it to the KV state
/// machine would fail-stop the node. Entry types were dropped here before M3
/// ConfChange (rev S).
pub type CommittedEntry = (LogIndex, SeamEntryType, Vec<u8>);

/// The result of one [`RaftNode::step`] `Ready` cycle.
///
/// `committed` are the entries to apply (normal commands to the state machine,
/// ConfChange entries to the membership machinery); `read_states` are
/// quorum-confirmed ReadIndex results `(request_ctx, read_index)` — a read may
/// be served locally once the applied index reaches `read_index`
/// (propsol §5.4). Both are empty when there was no pending work.
#[derive(Debug, Default)]
pub struct StepOutcome {
    /// Committed entries `(index, entry type, data)` in ascending index order.
    ///
    /// When `snapshot` is set, every entry here is strictly newer than it: an
    /// installed snapshot supersedes everything at or below its index.
    pub committed: Vec<CommittedEntry>,
    /// A snapshot this node installed during the cycle (it arrived from the
    /// leader). The caller **must** restore the state machine from
    /// `snapshot.data` before applying `committed`.
    pub snapshot: Option<SeamSnapshot>,
    /// Quorum-confirmed ReadIndex results `(request_ctx, read_index)`.
    pub read_states: Vec<(Vec<u8>, LogIndex)>,
}

/// The promotion rule of propsol §5.3, hard constraint 2: a learner may become
/// a voter once it has **answered at least once** and is within `threshold`
/// entries of the leader.
///
/// Kept as a free function so the boundary is testable without a cluster: the
/// two conditions fail for different reasons and both matter — a reachable
/// learner that is far behind would put an incomplete log into the quorum, and
/// a learner that has never answered has nothing to promote regardless of how
/// short the log is.
pub fn learner_caught_up(behind: u64, has_acked: bool, threshold: u64) -> bool {
    has_acked && behind <= threshold
}

/// Decode a ConfChange entry's identity without applying it:
/// `(change type, target node)`.
///
/// The runtime uses this to attribute a committed change to the proposal
/// waiting for it (rev S S1b). It is a free function because decoding needs
/// neither a node, a storage, nor a transport.
///
/// # Errors
///
/// A message if the payload does not decode, or if a `ConfChangeV2` carries
/// anything other than exactly one change: v1 is single-step by design
/// (propsol §5.3), so a multi-change entry is a bug or a foreign writer, not
/// something to guess about.
pub fn conf_change_identity(
    entry_type: SeamEntryType,
    data: &[u8],
) -> Result<(ConfChangeType, RaftId), String> {
    match entry_type {
        SeamEntryType::ConfChange => {
            let cc = ConfChange::parse_from_bytes(data)
                .map_err(|e| format!("ConfChange decode failed: {e}"))?;
            Ok((cc.get_change_type(), cc.get_node_id()))
        }
        SeamEntryType::ConfChangeV2 => {
            let cc = ConfChangeV2::parse_from_bytes(data)
                .map_err(|e| format!("ConfChangeV2 decode failed: {e}"))?;
            let changes = cc.get_changes();
            if changes.len() != 1 {
                return Err(format!(
                    "ConfChangeV2 with {} changes is not single-step",
                    changes.len()
                ));
            }
            let single = &changes[0];
            Ok((single.get_change_type(), single.get_node_id()))
        }
        SeamEntryType::Entry => Err("conf_change_identity called with a normal entry".into()),
    }
}

/// A raft consensus node that drives the `RawNode` Ready loop with the frozen
/// persist/send ordering.
///
/// * `S` — the durable seam storage (the WAL).
/// * `T` — the outbound transport half (used to send raft messages).
/// * `Tr` — the inbound transport half (held so the runtime can drive the
///   receive loop via [`RaftNode::rx`] and feed messages to
///   [`RaftNode::on_message`]).
pub struct RaftNode<S, T, Tr>
where
    S: SeamStorage,
    T: Transport,
    Tr: TransportRx,
{
    raw: RawNode<RaftStorage<S>>,
    transport: T,
    rx: Tr,
    /// Cycles whose records are written but whose flush has not completed, in
    /// submission order (propsol v0.2.13 P). Bounded by
    /// [`MAX_IN_FLIGHT_READIES`]: while a flush is in the air the node may keep
    /// stepping, which is what lets a ReadIndex round proceed instead of
    /// waiting for the device.
    pending: std::collections::VecDeque<PendingPersist>,
    /// Woken by the storage when an offloaded flush completes, so the actor
    /// does not have to wait for its next tick to notice durability.
    durability: Arc<tokio::sync::Notify>,
    /// Raft id → transport `NodeId` for outbound resolution. Peers not present
    /// here are not yet part of this node's membership and are skipped.
    peers: HashMap<RaftId, NodeId>,
    /// Outbound raft messages dropped because their transport `send` failed.
    /// A failed send is non-fatal (raft retransmits on a later tick), so it is
    /// counted — not surfaced — making a permanently dead transport visible as
    /// a rising gauge instead of a silent liveness loss (P4 gate, propsol §8).
    dropped_sends: u64,
}

impl<S, T, Tr> RaftNode<S, T, Tr>
where
    S: SeamStorage,
    T: Transport,
    Tr: TransportRx,
{
    /// Create a new `RaftNode` with the default tick/flow-control timings
    /// ([`RaftNodeConfig::default`]).
    ///
    /// `self_raft_id` must be non-zero and unique in the group. `applied` is
    /// the last index applied to the state machine (0 for a fresh node). The
    /// raft configuration enables PreVote, CheckQuorum, and quorum-confirmed
    /// linearizable reads (`Safe` ReadIndex, propsol §5.4): v1 confirms each
    /// read with a quorum heartbeat round, so read consistency does not depend
    /// on a clock-drift bound. Lease-based reads are forbidden in v1 (propsol
    /// §1) — they rely on a leader lease and would weaken linearizability under
    /// unbounded clock skew.
    pub fn new(
        self_raft_id: RaftId,
        peers: HashMap<RaftId, NodeId>,
        storage: S,
        transport: T,
        rx: Tr,
        applied: LogIndex,
        logger: &Logger,
    ) -> Result<Self, NodeError<T>> {
        Self::new_with_config(
            self_raft_id,
            peers,
            storage,
            transport,
            rx,
            applied,
            RaftNodeConfig::default(),
            logger,
        )
    }

    /// Create a new `RaftNode` with an explicit [`RaftNodeConfig`].
    ///
    /// This is the entry point for profile-driven deployments: derive the
    /// tick timings and flow-control limits from a validated
    /// [`ProfileConfig`] via [`RaftNodeConfig::from_profile`], then construct
    /// the node here. See [`Self::new`] for the parameter contract; the only
    /// addition is `config`, which controls the raft tick rates and per-follower
    /// inflight limits.
    // The signature is `new` plus one `config` argument (the mandated API), so
    // it sits one past the `too_many_arguments` threshold.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_config(
        self_raft_id: RaftId,
        peers: HashMap<RaftId, NodeId>,
        storage: S,
        transport: T,
        rx: Tr,
        applied: LogIndex,
        config: RaftNodeConfig,
        logger: &Logger,
    ) -> Result<Self, NodeError<T>> {
        if self_raft_id == 0 {
            return Err(NodeError::Raft(
                raft::Error::ConfigInvalid("node id must be non-zero".into()),
            ));
        }

        let raft_config = RaftConfig {
            id: self_raft_id,
            election_tick: config.election_tick as usize,
            heartbeat_tick: config.heartbeat_tick as usize,
            applied,
            max_size_per_msg: config.max_size_per_msg,
            max_inflight_msgs: config.max_inflight_msgs as usize,
            check_quorum: true,
            pre_vote: true,
            // Quorum-confirmed ReadIndex (propsol §5.4). `Safe` is the
            // linearizable read mode that does not trust a leader lease.
            read_only_option: ReadOnlyOption::Safe,
            ..Default::default()
        };
        raft_config.validate().map_err(NodeError::Raft)?;

        // Bootstrap membership: the declared peers plus this node. Without a
        // voter set containing this node, raft has no quorum and can never
        // elect a leader (propsol §5.7 `initial_cluster`). A persisted
        // configuration always wins over this declaration; it is only the
        // starting point of a fresh store.
        let mut voters: Vec<RaftId> = match &config.bootstrap_voters {
            Some(voters) => voters.clone(),
            None => peers.keys().copied().collect(),
        };
        let mut learners: Vec<RaftId> = Vec::new();
        if config.join_as_learner {
            // A joiner is not a voter yet: it starts as a learner of the
            // configured voters (rev S). Adding itself to the voter set would
            // make it campaign for an election it cannot win and must not enter.
            learners.push(self_raft_id);
        } else if config.bootstrap_voters.is_none() {
            // Fresh cluster: this node is one of the voters. (With an explicit
            // voter set it is already listed — or, for a joiner, deliberately
            // not.)
            voters.push(self_raft_id);
        }
        voters.sort_unstable();
        voters.dedup();
        learners.sort_unstable();
        learners.dedup();
        let bootstrap = SeamConfState { voters, learners };

        // The pipeline's wakeup: the storage calls this when an offloaded flush
        // completes, so the actor notices durability without waiting for a tick.
        let durability = Arc::new(tokio::sync::Notify::new());
        let mut storage = storage;
        storage.set_flush_waker(FlushWaker::new({
            let durability = Arc::clone(&durability);
            move || durability.notify_one()
        }));

        let store = RaftStorage::with_conf_state(storage, bootstrap);
        let raw = RawNode::new(&raft_config, store, logger).map_err(NodeError::Raft)?;

        Ok(Self {
            raw,
            transport,
            rx,
            pending: VecDeque::new(),
            durability,
            peers,
            dropped_sends: 0,
        })
    }

    /// The notifier the storage pings when an offloaded flush completes
    /// (propsol v0.2.13 P). Cloned rather than awaited so a caller can select on
    /// it while still driving the node.
    pub fn durability_notifier(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.durability)
    }

    /// Whether a cycle is waiting for durability.
    pub fn is_persisting(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Advance the internal logical clock by one tick.
    pub fn tick(&mut self) {
        self.raw.tick();
    }

    /// Drive one `Ready` cycle with the frozen persist/send ordering (I1–I4).
    ///
    /// Returns a [`StepOutcome`]: the committed entries to apply to the state
    /// machine, and any quorum-confirmed ReadIndex results. After applying the
    /// committed entries, call [`Self::advance_apply`] to update the apply
    /// progress. Returns an empty [`StepOutcome`] when there is no pending work.
    ///
    /// # Transport failures are non-fatal
    ///
    /// A message that cannot be delivered (e.g. its peer is currently down) is
    /// dropped, **not** surfaced as an error, and never prevents `advance()`:
    /// raft retransmits on a later tick. Aborting the `Ready` cycle on a send
    /// failure would let one unreachable peer stall this node's persistence
    /// pipeline, which is exactly the failure mode raft is designed to absorb.
    /// (`NodeError::Transport` remains for callers that send explicitly.) Every
    /// dropped send is counted in [`Self::dropped_send_count`] and exposed as the
    /// `arachne_dropped_sends` metric, so a permanently dead transport degrades
    /// to a visible, rising gauge instead of a silent liveness loss (P4 gate,
    /// propsol §8).
    ///
    /// # Forward note (M3)
    ///
    /// Committed entries are returned as `(index, data)`; the raft `EntryType`
    /// is dropped for now. When ConfChange is introduced (`propose_conf_change`),
    /// ConfChange entries must be routed to the raft membership machinery and
    /// **must not** be fed to the KV state machine (which would reject them as
    /// malformed).
    pub async fn step(&mut self) -> Result<StepOutcome, NodeError<T>> {
        // 1. Finish the oldest cycle whose flush has landed, if any.
        if let Some(outcome) = self.finish_persisted().await? {
            return Ok(outcome);
        }
        // 2. Otherwise submit the next `Ready`, if the pipeline has room and
        //    raft has work. Returning an empty outcome when a flush is in the
        //    air is the point: the caller keeps ticking, serving commands and
        //    answering ReadIndex rounds instead of waiting for the device.
        if self.pending.len() < MAX_IN_FLIGHT_READIES && self.raw.has_ready() {
            self.submit_ready().await?;
            if let Some(outcome) = self.finish_persisted().await? {
                return Ok(outcome);
            }
        }
        Ok(StepOutcome::default())
    }

    /// Take a `Ready`, write its records, and queue its flush (propsol P).
    async fn submit_ready(&mut self) -> Result<(), NodeError<T>> {
        let ready = self.raw.ready();
        let number = ready.number();

        // Everything raft hands over in `advance` has to be read first: the
        // call moves the data into raft's own state.
        let immediate: Vec<Message> = ready.messages().to_vec();
        let persisted: Vec<Message> = ready.persisted_messages().to_vec();
        let committed: Vec<CommittedEntry> = ready
            .committed_entries()
            .iter()
            .map(|e| {
                let entry = RaftStorage::<S>::from_raft_entry(e);
                (entry.index, entry.entry_type, entry.data)
            })
            .collect();
        let read_states: Vec<(Vec<u8>, LogIndex)> = ready
            .read_states()
            .iter()
            .map(|rs| (rs.request_ctx.clone(), rs.index))
            .collect();
        let snapshot = if ready.snapshot().is_empty() {
            None
        } else {
            Some(RaftStorage::<S>::from_raft_snapshot(ready.snapshot()))
        };
        let entries: Vec<LogEntry> = ready
            .entries()
            .iter()
            .map(RaftStorage::<S>::from_raft_entry)
            .collect();
        let hard_state = ready.hs().map(RaftStorage::<S>::to_seam_hard_state);

        // A snapshot is a separate file and stays synchronous: it is rare, and
        // it is what lets the log below it be released (propsol §5.5.4).
        if let Some(snapshot) = &snapshot {
            self.raw
                .mut_store()
                .install_snapshot(snapshot)
                .map_err(NodeError::Raft)?;
        }

        let submitted = self
            .raw
            .mut_store()
            .persist_ready_records(&entries, hard_state.as_ref())
            .map_err(NodeError::Raft)?;

        // The records are readable from the storage, which is all raft requires
        // before its own state may advance.
        self.raw.advance_append_async(ready);

        let offloaded = matches!(submitted, PersistSubmit::Offloaded(_));
        // A leader's fast-path messages are classified by raft itself as not
        // needing local durability, and sending them now is what keeps a
        // ReadIndex round from queueing behind the device. A follower's
        // messages are all "persisted messages" and wait below.
        if offloaded {
            for msg in &immediate {
                self.deliver(msg).await;
            }
        }
        self.pending.push_back(PendingPersist {
            number,
            token: match submitted {
                PersistSubmit::Durable => None,
                PersistSubmit::Offloaded(token) => Some(token),
            },
            immediate: if offloaded { Vec::new() } else { immediate },
            persisted,
            committed,
            read_states,
            snapshot,
        });
        Ok(())
    }

    /// Complete the oldest queued cycle if its flush has landed. `None` = it is
    /// still in the air.
    ///
    /// Cycles complete in submission order, which is what raft's
    /// `on_persist_ready` requires and what keeps a follower from acknowledging
    /// entries an earlier cycle has not yet made durable.
    async fn finish_persisted(&mut self) -> Result<Option<StepOutcome>, NodeError<T>> {
        let Some(number) = self.pending.front().map(|pending| pending.number) else {
            return Ok(None);
        };
        if let Some(token) = self.pending.front().and_then(|pending| pending.token) {
            match self
                .raw
                .mut_store()
                .poll_flush(&token)
                .map_err(NodeError::Raft)?
            {
                None => return Ok(None),
                Some(Err(e)) => {
                    return Err(NodeError::Storage(format!(
                        "offloaded flush failed: {e} (durability cannot be claimed)"
                    )));
                }
                Some(Ok(())) => {}
            }
        }

        // The records are durable: raft may treat everything up to this cycle as
        // persisted, and a commit advance it recorded since then is carried by
        // the hard state of a later `Ready` (writing it here as well would make
        // raft surface the same hard state twice, leaving a duplicate record on
        // disk for no gain).
        self.raw.on_persist_ready(number);
        // `advance` used to keep raft's applied index moving; the phased path
        // must do it explicitly now that the records are durable.
        self.raw.advance_apply();

        let Some(mut pending) = self.pending.pop_front() else {
            return Ok(None);
        };

        // INV2 crash injection (propsol v0.2.9 L): records durable, nothing sent.
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::check(crate::fault_injection::Stage::AfterPersist);

        for msg in &pending.immediate {
            self.deliver(msg).await;
        }
        for msg in &pending.persisted {
            self.deliver(msg).await;
        }

        let mut committed = pending.committed;
        if let Some(snapshot) = &pending.snapshot {
            // An installed snapshot supersedes everything at or below its index,
            // including entries already committed before this cycle.
            committed.retain(|(index, _, _)| *index > snapshot.meta.index);
        }
        // INV2 crash injection: messages are out, nothing has been applied yet.
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::check(crate::fault_injection::Stage::AfterDeliver);
        Ok(Some(StepOutcome {
            committed,
            snapshot: pending.snapshot.take(),
            read_states: pending.read_states,
        }))
    }

    /// Propose a command to the raft log.
    pub fn propose(&mut self, cmd: &[u8]) -> Result<(), NodeError<T>> {
        self.raw
            .propose(Vec::new(), cmd.to_vec())
            .map_err(NodeError::Raft)
    }

    /// Propose a single-step membership change (propsol §5.3, rev S).
    ///
    /// Single-step on purpose: v1 does not enter joint consensus. The change is
    /// committed by raft like any other entry, but its payload is a ConfChange
    /// protobuf rather than a state-machine command, so it must be routed to
    /// [`RaftNode::apply_conf_change`] and **never** to the state machine.
    pub fn propose_conf_change(
        &mut self,
        change_type: ConfChangeType,
        node_id: RaftId,
    ) -> Result<(), NodeError<T>> {
        let mut cc = ConfChange::default();
        cc.set_change_type(change_type);
        cc.set_node_id(node_id);
        self.raw
            .propose_conf_change(Vec::new(), cc)
            .map_err(NodeError::Raft)
    }

    /// Apply a committed ConfChange entry to the membership (propsol §5.3, rev S).
    ///
    /// The new configuration is persisted through the storage **before this
    /// returns** (invariant I6), so a restart can skip re-applying the entry and
    /// raft's progress tracker is rebuilt from it by `initial_state`.
    ///
    /// Returns `Ok(true)` when the change was applied and `Ok(false)` when it
    /// was skipped because the durable membership already reflects it — the
    /// normal case for the entries replayed after a restart (invariant I8 makes
    /// that skip sound: the membership is a cache, the log is the authority).
    ///
    /// # Errors
    ///
    /// [`NodeError::Codec`] if the payload is not a valid ConfChange for the
    /// entry type, [`NodeError::Raft`] if raft rejects the change, and
    /// [`NodeError::Storage`] if the new configuration cannot be persisted.
    pub fn apply_conf_change(
        &mut self,
        index: LogIndex,
        entry_type: SeamEntryType,
        data: &[u8],
    ) -> Result<bool, NodeError<T>> {
        if index <= self.raw.mut_store().conf_change_index() {
            return Ok(false);
        }
        // Validate the payload up front (single-step, decodable) so a bad entry
        // fails before raft's progress tracker is touched.
        conf_change_identity(entry_type, data).map_err(NodeError::Codec)?;
        let conf_state = match entry_type {
            SeamEntryType::ConfChange => {
                let cc = ConfChange::parse_from_bytes(data)
                    .map_err(|e| NodeError::Codec(format!("ConfChange decode failed: {e}")))?;
                self.raw.apply_conf_change(&cc).map_err(NodeError::Raft)?
            }
            SeamEntryType::ConfChangeV2 => {
                let cc = ConfChangeV2::parse_from_bytes(data)
                    .map_err(|e| NodeError::Codec(format!("ConfChangeV2 decode failed: {e}")))?;
                self.raw.apply_conf_change(&cc).map_err(NodeError::Raft)?
            }
            SeamEntryType::Entry => {
                // A routing bug, not a data problem: normal entries belong to
                // the state machine. Failing loudly beats mutating membership
                // from a command payload.
                return Err(NodeError::Codec(
                    "apply_conf_change called with a normal entry".into(),
                ));
            }
        };
        let seam_conf_state = RaftStorage::<S>::to_seam_conf_state(&conf_state);
        self.raw
            .mut_store()
            .save_conf_state(index, &seam_conf_state)
            .map_err(|e| NodeError::Storage(e.to_string()))?;
        Ok(true)
    }

    /// Ask raft to move leadership to `node_id` (propsol §5.3; Q3 makes this a
    /// public API because shutdown and leader removal both depend on it).
    ///
    /// This only *starts* the transfer: raft sends the transfer message and the
    /// target campaigns. Success is the leadership actually moving, which the
    /// caller observes through [`RaftNode::leader_id`] — a returned `Ok` here
    /// would prove nothing (rev S).
    pub fn transfer_leader(&mut self, node_id: RaftId) {
        self.raw.transfer_leader(node_id);
    }

    /// The voter set of the applied configuration (propsol §5.3).
    ///
    /// Read from the durable membership rather than from raft's in-flight
    /// progress tracker: this is the configuration the cluster has *agreed* on,
    /// which is what choosing a transferee requires.
    ///
    /// # Errors
    ///
    /// [`NodeError::Raft`] if the storage cannot report its state.
    pub fn voter_ids(&self) -> Result<Vec<RaftId>, NodeError<T>> {
        let state = self
            .raw
            .store()
            .initial_state()
            .map_err(NodeError::Raft)?;
        Ok(state.conf_state.get_voters().to_vec())
    }

    /// How far `node_id` is behind this node's log, and whether it has
    /// answered recently: `(behind_entries, active)` (propsol §5.3, rev S).
    ///
    /// `None` when raft tracks no progress for that node at all — a learner
    /// that was never added, or one whose membership has not been applied yet.
    ///
    /// `has_acked` is `matched > 0`: the learner has acknowledged at least one
    /// append, so it exists and is reachable. This is the *在线* half of the
    /// promotion rule — a node that has never answered is not promotable no
    /// matter how short the log is.
    ///
    /// # Why not `recent_active`
    ///
    /// It is tempting to read raft's `recent_active` here, but raft-rs sets it
    /// to `true` **when a node is first added** (`ProgressTracker::apply_conf`,
    /// deliberately, so a pending `CheckQuorum` cannot step the leader down
    /// before the new node has had a chance to talk to it). It therefore says
    /// nothing about whether the learner has ever answered, and a promotion
    /// gate built on it would let a nonexistent node become a voter.
    pub fn learner_progress(&self, node_id: RaftId) -> Option<(u64, bool)> {
        // `last_index` comes from the durable store: entries that are not
        // durable yet are not committed either, so they cannot widen the gap a
        // promotion decision should care about.
        let last_index = self.raw.store().last_index().ok()?;
        let status = self.raw.status();
        let progress = status.progress?.get(node_id)?;
        Some((
            last_index.saturating_sub(progress.matched),
            progress.matched > 0,
        ))
    }

    /// Whether `node_id` is a learner in the applied configuration.
    ///
    /// # Errors
    ///
    /// [`NodeError::Raft`] if the storage cannot report its state.
    pub fn is_learner(&self, node_id: RaftId) -> Result<bool, NodeError<T>> {
        let state = self.raw.store().initial_state().map_err(NodeError::Raft)?;
        Ok(state.conf_state.get_learners().contains(&node_id))
    }

    /// Issue a quorum-confirmed ReadIndex read (propsol §5.4).
    ///
    /// `ctx` is an opaque, caller-chosen token that raft echoes back in the
    /// matching [`StepOutcome::read_states`] entry. The caller is responsible
    /// for ensuring this node is the leader (a non-leader silently drops the
    /// request). The read is confirmed once a quorum heartbeats back, after
    /// which the caller may serve it locally once the applied index reaches
    /// the returned `read_index`.
    pub fn read_index(&mut self, ctx: Vec<u8>) {
        self.raw.read_index(ctx);
    }

    /// Process an inbound transport message from the peer `from`.
    ///
    /// The message bytes are a raft 0.7-serialized `Message`. The `from` field
    /// is set from the transport's sender tag (the authoritative source), not
    /// from the (untrusted) message contents.
    pub fn on_message(
        &mut self,
        from: RaftId,
        msg: TransportMessage,
    ) -> Result<(), NodeError<T>> {
        let bytes = match msg {
            TransportMessage::Raft(bytes) => bytes,
            // The enum is non-exhaustive; only `Raft` exists today.
            _ => return Ok(()),
        };
        let mut raft_msg = Message::default();
        raft_msg
            .merge_from_bytes(&bytes)
            .map_err(|e| NodeError::Codec(e.to_string()))?;
        raft_msg.set_from(from);
        self.raw.step(raft_msg).map_err(NodeError::Raft)
    }

    /// Advance the apply progress to the last committed entry.
    ///
    /// Call this after applying the entries returned by [`Self::step`] to the
    /// state machine.
    pub fn advance_apply(&mut self) {
        self.raw.advance_apply();
    }


    /// The highest log index applied so far (0 if none).
    pub fn applied_index(&self) -> LogIndex {
        self.raw.raft.raft_log.applied
    }

    /// The number of outbound raft messages dropped because their transport
    /// `send` failed (see the `step` docs). A dead transport shows up here as a
    /// rising count rather than a silent liveness loss; the node runtime
    /// exposes it as the `arachne_dropped_sends` metric.
    pub fn dropped_send_count(&self) -> u64 {
        self.dropped_sends
    }

    /// The current **in-memory** hard state (term, vote, commit).
    ///
    /// The `commit` watermark here is raft's current view and may run ahead of
    /// what is durable on disk between `Ready` cycles (raft re-derives commit on
    /// restart — see the M0 ① notes). For the durable commit watermark, read the
    /// WAL (`Storage::initial_state`).
    pub fn hard_state(&self) -> SeamHardState {
        RaftStorage::<S>::to_seam_hard_state(&self.raw.raft.hard_state())
    }

    /// The current leader's raft id (0 if unknown).
    pub fn leader_id(&self) -> RaftId {
        self.raw.raft.leader_id
    }

    /// The inbound transport half, for the runtime to drive the receive loop.
    ///
    /// The runtime polls `node.rx().recv()` and forwards each
    /// `(sender, message)` to [`Self::on_message`].
    pub fn rx(&mut self) -> &mut Tr {
        &mut self.rx
    }

    /// Persist a snapshot of the local state machine, then release the log it
    /// covers.
    ///
    /// The snapshot must be taken at the **applied** index, so that compaction
    /// can never drop an entry the snapshot does not already contain. Ordering
    /// is durability-first (propsol §5.5.4): the snapshot file and its META
    /// pointer are fsynced before `compact` removes anything.
    pub fn create_snapshot(
        &mut self,
        index: LogIndex,
        term: Term,
        conf_state: SeamConfState,
        data: Vec<u8>,
    ) -> Result<(), NodeError<T>> {
        let snapshot = SeamSnapshot {
            meta: SeamSnapshotMeta {
                index,
                term,
                conf_state,
            },
            data,
        };
        let store = self.raw.mut_store();
        store.save_snapshot(&snapshot).map_err(NodeError::Raft)?;
        store.compact(index).map_err(NodeError::Raft)?;
        Ok(())
    }

    /// The membership configuration the cluster has agreed on, learners
    /// included (propsol §5.5.4; rev S S4).
    ///
    /// This is what a snapshot must carry: a locally created snapshot that
    /// recorded only the bootstrap voters would lose every learner (and any
    /// membership change) the moment it was used to rebuild a node.
    ///
    /// # Errors
    ///
    /// [`NodeError::Raft`] if the storage cannot report its state.
    pub fn applied_conf_state(&self) -> Result<SeamConfState, NodeError<T>> {
        let state = self.raw.store().initial_state().map_err(NodeError::Raft)?;
        Ok(RaftStorage::<S>::to_seam_conf_state(&state.conf_state))
    }

    /// The term of the entry at `index`, answered from the durable snapshot
    /// once the entry itself has been compacted away.
    pub fn term_at(&mut self, index: LogIndex) -> Result<Term, NodeError<T>> {
        self.raw.mut_store().term(index).map_err(NodeError::Raft)
    }

    /// Deliver one raft message, counting any failure in `dropped_sends`.
    ///
    /// A failed send is non-fatal — raft retransmits on a later tick — so the
    /// error is dropped after being counted (P4 gate: a permanently dead
    /// transport degrades to a visible counter instead of a silent liveness
    /// loss). This is a `&mut self` method so the counting write and the
    /// `send_one` borrows of `self.transport`/`self.peers` never overlap.
    async fn deliver(&mut self, msg: &Message) {
        let self_id = self.raw.raft.id;
        let outcome = send_one(&self.transport, &self.peers, self_id, msg).await;
        if outcome.is_err() {
            self.dropped_sends += 1;
        }
    }

}

/// Encode and deliver a single raft message to its destination peer.
///
/// This is a free function (not a method) so the `step` future captures only
/// `&T` and `&HashMap` — both `Send` because `T: Send + Sync` — instead of
/// `&Self`, which would require `Self: Sync` (the inbound half need not be
/// `Sync`, e.g. a `std::sync::mpsc::Receiver`).
///
/// Local messages (`to == self_id`) are never sent. Unknown peers (not yet in
/// the membership) are skipped: raft re-sends on the next tick.
async fn send_one<T: Transport>(
    transport: &T,
    peers: &HashMap<RaftId, NodeId>,
    self_id: RaftId,
    msg: &Message,
) -> Result<(), NodeError<T>> {
    let to = msg.get_to();
    if to == self_id {
        return Ok(());
    }
    let Some(node_id) = peers.get(&to).cloned() else {
        return Ok(());
    };
    let bytes = msg
        .write_to_bytes()
        .map_err(|e| NodeError::Codec(e.to_string()))?;
    transport
        .send(node_id, TransportMessage::Raft(bytes))
        .await
        .map_err(NodeError::Transport)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arachne_seam::seam::{StateMachine, TransportFactory};
    use arachne_seam::storage::{
        HardState, LogEntry, RaftState, Snapshot, Storage, StorageError,
    };
    use arachne_seam::types::{LogIndex, NodeId};
    use arachne_testsupport::{
        block_on, InMemoryRx, InMemoryTransportFactory, InMemoryTx,
    };
    use crate::state_machine::KvStateMachine;
    use raft::eraftpb::{Message as RaftMsg, MessageType};
    use slog::{o, Drain};

    /// A no-op logger for tests (production code never reads ambient logs).
    fn logger() -> Logger {
        Logger::root(slog::Discard.fuse(), o!())
    }

    /// A simple in-memory seam storage for node tests.
    struct MemStore {
        entries: Vec<LogEntry>,
        hard_state: HardState,
        fsync_observer: Option<std::sync::Arc<dyn arachne_seam::FsyncObserver>>,
        /// Highest index durable so far (for observer callbacks).
        durable_through: LogIndex,
    }

    impl MemStore {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                hard_state: HardState::default(),
                fsync_observer: None,
                durable_through: 0,
            }
        }

        fn with_observer(observer: std::sync::Arc<dyn arachne_seam::FsyncObserver>) -> Self {
            let mut s = Self::new();
            s.fsync_observer = Some(observer);
            s
        }

        fn notify_fsynced(&self) {
            if let Some(observer) = &self.fsync_observer {
                observer.on_segment_fsynced(1, self.durable_through);
            }
        }
    }

    impl Storage for MemStore {
        fn initial_state(&self) -> Result<RaftState, StorageError> {
            Ok(RaftState {
                hard_state: self.hard_state.clone(),
                conf_state: Default::default(),
            })
        }

        fn entries(
            &self,
            low: LogIndex,
            high: LogIndex,
            _max_size: Option<u64>,
        ) -> Result<Vec<LogEntry>, StorageError> {
            if low >= high {
                return Ok(Vec::new());
            }
            let first = self.entries.first().map_or(1, |e| e.index);
            let last = self.entries.last().map_or(0, |e| e.index);
            if low < first || high > last + 1 {
                return Err(StorageError::Compacted);
            }
            Ok(self.entries[(low - first) as usize..(high - first) as usize].to_vec())
        }

        fn term(&self, index: LogIndex) -> Result<u64, StorageError> {
            let first = self.entries.first().map_or(1, |e| e.index);
            let last = self.entries.last().map_or(0, |e| e.index);
            if index < first || index > last {
                return Err(StorageError::Compacted);
            }
            Ok(self.entries[(index - first) as usize].term)
        }

        fn first_index(&self) -> Result<LogIndex, StorageError> {
            Ok(self.entries.first().map_or(1, |e| e.index))
        }

        fn last_index(&self) -> Result<LogIndex, StorageError> {
            Ok(self.entries.last().map_or(0, |e| e.index))
        }

        fn snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
            Ok(None)
        }

        fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
            self.entries.extend_from_slice(entries);
            Ok(())
        }

        fn set_hard_state(&mut self, hs: &HardState) -> Result<(), StorageError> {
            self.hard_state = hs.clone();
            // I1: durable before returning; notify observers (like the WAL).
            self.durable_through = self
                .entries
                .last()
                .map(|e| e.index)
                .unwrap_or(self.durable_through);
            self.notify_fsynced();
            Ok(())
        }

        fn sync_entries(&mut self) -> Result<(), StorageError> {
            // I2/I4: durability barrier.
            self.durable_through = self.entries.last().map_or(self.durable_through, |e| e.index);
            self.notify_fsynced();
            Ok(())
        }

        fn compact(&mut self, _to: LogIndex) -> Result<(), StorageError> {
            Ok(())
        }
    }

    fn make_node(store: MemStore) -> RaftNode<MemStore, InMemoryTx, InMemoryRx> {
        let factory = InMemoryTransportFactory::new();
        let (tx, rx) = factory.create(NodeId::from("node-1"));
        RaftNode::new(1, HashMap::new(), store, tx, rx, 0, &logger())
            .expect("single-node construction must succeed")
    }

    /// A node that becomes leader after enough ticks (a single node elects
    /// itself once the election timeout elapses).
    fn make_leader() -> (RaftNode<MemStore, InMemoryTx, InMemoryRx>, KvStateMachine) {
        let mut node = make_node(MemStore::new());
        let mut sm = KvStateMachine::new();
        for _ in 0..100 {
            node.tick();
            let entries = block_on(node.step()).expect("drive cycle must succeed").committed;
            for (idx, _kind, data) in entries {
                sm.apply(idx, &data).expect("apply must succeed");
            }
            node.advance_apply();
            if node.leader_id() == 1 {
                break;
            }
        }
        assert_eq!(node.leader_id(), 1, "single node must elect itself leader");
        (node, sm)
    }

    #[test]
    fn promotion_requires_an_online_caught_up_learner() {
        // Caught up and answering.
        assert!(learner_caught_up(0, true, 128));
        assert!(
            learner_caught_up(128, true, 128),
            "the threshold is inclusive"
        );
        // Online but too far behind: promoting it would put an incomplete log
        // into the quorum.
        assert!(!learner_caught_up(129, true, 128));
        // Never answered: not promotable however short the log is. (This is the
        // case `recent_active` gets wrong: raft marks a newly added node as
        // recently active, so a node that never existed would pass.)
        assert!(!learner_caught_up(0, false, 128));
        assert!(!learner_caught_up(0, false, u64::MAX));
        // A zero threshold still allows an exactly-caught-up learner.
        assert!(learner_caught_up(0, true, 0));
        assert!(!learner_caught_up(1, true, 0));
    }

    #[test]
    fn single_node_propose_commit_apply() {
        let (mut node, mut sm) = make_leader();

        let cmd = KvStateMachine::encode_put(1, 1, b"key", b"value");
        node.propose(&cmd).expect("propose to a leader must succeed");

        for _ in 0..100 {
            node.tick();
            let entries = block_on(node.step()).expect("drive cycle must succeed").committed;
            for (idx, _kind, data) in entries {
                sm.apply(idx, &data).expect("apply must succeed");
            }
            node.advance_apply();
            if sm.get(b"key").unwrap() == Some(b"value".to_vec()) {
                break;
            }
        }

        assert_eq!(sm.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert!(node.applied_index() >= 1, "applied index must advance");
    }

    #[test]
    fn read_index_yields_a_quorum_confirmed_read_state() {
        let (mut node, mut sm) = make_leader();

        // Propose, commit, and apply a value so there is a stable committed
        // index for the read to anchor on.
        let cmd = KvStateMachine::encode_put(1, 1, b"key", b"value");
        node.propose(&cmd).expect("propose to a leader must succeed");
        for _ in 0..100 {
            node.tick();
            let entries = block_on(node.step()).expect("drive cycle must succeed").committed;
            for (idx, _kind, data) in entries {
                sm.apply(idx, &data).expect("apply must succeed");
            }
            node.advance_apply();
            if sm.get(b"key").unwrap() == Some(b"value".to_vec()) {
                break;
            }
        }
        assert_eq!(sm.get(b"key").unwrap(), Some(b"value".to_vec()));

        // Issue a ReadIndex read with a known ctx and drive until it resolves.
        let ctx = b"read-ctx-1".to_vec();
        node.read_index(ctx.clone());
        let mut resolved: Option<(Vec<u8>, LogIndex)> = None;
        for _ in 0..100 {
            node.tick();
            let outcome = block_on(node.step()).expect("drive cycle must succeed");
            for (idx, _kind, data) in outcome.committed {
                sm.apply(idx, &data).expect("apply must succeed");
            }
            node.advance_apply();
            for (rctx, index) in &outcome.read_states {
                if *rctx == ctx {
                    resolved = Some((rctx.clone(), *index));
                }
            }
            if resolved.is_some() {
                break;
            }
        }

        let (got_ctx, index) =
            resolved.expect("the read state must resolve with our ctx");
        assert_eq!(got_ctx, ctx, "the request ctx must round-trip");
        assert!(
            index <= node.applied_index(),
            "read index {index} must be <= applied index {}",
            node.applied_index()
        );
    }

    #[test]
    fn i1_i2_persist_durable_before_step_returns() {
        let ledger = std::sync::Arc::new(arachne_testsupport::FsyncLedger::new());
        let observer: std::sync::Arc<dyn arachne_seam::FsyncObserver> = ledger.clone();
        let store = MemStore::with_observer(observer);
        let (mut node, mut sm) = (make_node(store), KvStateMachine::new());

        // Become leader.
        for _ in 0..100 {
            node.tick();
            let entries = block_on(node.step()).expect("drive must succeed").committed;
            for (idx, _kind, data) in entries {
                sm.apply(idx, &data).expect("apply must succeed");
            }
            node.advance_apply();
            if node.leader_id() == 1 {
                break;
            }
        }

        let cmd = KvStateMachine::encode_put(7, 1, b"k", b"v");
        node.propose(&cmd).expect("propose must succeed");
        for _ in 0..100 {
            node.tick();
            let entries = block_on(node.step()).expect("drive must succeed").committed;
            for (idx, _kind, data) in entries {
                sm.apply(idx, &data).expect("apply must succeed");
            }
            node.advance_apply();
            if sm.get(b"k").unwrap() == Some(b"v".to_vec()) {
                break;
            }
        }

        // I2/I4: every committed index is durable (fsynced) by the time the
        // step that committed it returned — the ledger covers the applied range.
        let applied = node.applied_index();
        assert!(applied >= 1, "an entry must be applied");
        assert!(
            ledger.union_covers(1, applied),
            "fsync ledger must cover the applied range [1, {applied}]"
        );
    }

    #[test]
    fn raft_message_round_trips_through_transport_message() {
        let mut msg = RaftMsg::default();
        msg.set_msg_type(MessageType::MsgAppend);
        msg.set_to(2);
        msg.set_from(1);
        msg.set_term(3);
        msg.set_index(10);
        msg.set_commit(9);

        let bytes = msg.write_to_bytes().expect("encode must succeed");
        let wire = TransportMessage::Raft(bytes.clone());
        let TransportMessage::Raft(decoded) = wire else {
            panic!("expected the Raft variant");
        };
        let mut back = RaftMsg::default();
        back.merge_from_bytes(&decoded).expect("decode must succeed");
        assert_eq!(back.get_msg_type(), MessageType::MsgAppend);
        assert_eq!(back.get_to(), 2);
        assert_eq!(back.get_from(), 1);
        assert_eq!(back.get_term(), 3);
        assert_eq!(back.get_index(), 10);
        assert_eq!(back.get_commit(), 9);
    }

    #[test]
    fn on_message_rejects_local_message_type() {
        let (mut node, _sm) = make_leader();
        // A local message (MsgBeat) arriving over the network must be rejected.
        let mut msg = RaftMsg::default();
        msg.set_msg_type(MessageType::MsgBeat);
        msg.set_from(1);
        let bytes = msg.write_to_bytes().unwrap();
        let err = node
            .on_message(1, TransportMessage::Raft(bytes))
            .expect_err("local msg type must be rejected");
        assert!(matches!(err, NodeError::Raft(raft::Error::StepLocalMsg)));
    }

    #[test]
    fn hard_state_reflects_elected_leader() {
        let (node, _sm) = make_leader();
        let hs = node.hard_state();
        assert_eq!(hs.vote, Some(1), "the single node votes for itself");
        assert!(hs.term >= 1);
    }

    #[test]
    fn dropped_send_counter_increments_when_a_send_fails() {
        // A two-voter cluster where the peer (raft id 2) is mapped to a NodeId
        // that is NOT registered with the transport switch, so every send to it
        // fails (`UnknownPeer`). The node keeps trying to reach that peer
        // (election / replication messages), so the dropped-send counter must
        // rise.
        let factory = InMemoryTransportFactory::new();
        let (tx, rx) = factory.create(NodeId::from("node-1"));
        let mut peers = HashMap::new();
        peers.insert(2u64, NodeId::from("ghost"));
        let node = RaftNode::new(1, peers, MemStore::new(), tx, rx, 0, &logger())
            .expect("two-voter node must construct");

        let mut node = node;
        let mut dropped = 0u64;
        for _ in 0..200 {
            node.tick();
            let _ = block_on(node.step()).expect("drive cycle must succeed");
            node.advance_apply();
            dropped = node.dropped_send_count();
            if dropped > 0 {
                break;
            }
        }
        assert!(
            dropped > 0,
            "a failed send to the unknown peer must be counted; got {dropped}"
        );
    }

    #[test]
    fn raft_node_config_default_matches_original_hardcoded_values() {
        let c = RaftNodeConfig::default();
        assert_eq!(c.election_tick, 10);
        assert_eq!(c.heartbeat_tick, 1);
        assert_eq!(c.max_size_per_msg, 1_048_576);
        assert_eq!(c.max_inflight_msgs, 256);
    }

    #[test]
    fn raft_node_config_from_profile_derives_ticks() {
        // Lan: 1000 / 100 = 10 ticks.
        let lan = RaftNodeConfig::from_profile(&crate::profile::Profile::Lan.config());
        assert_eq!(lan.heartbeat_tick, 1);
        assert_eq!(lan.election_tick, 10);
        assert_eq!(lan.max_size_per_msg, 4 * 1024 * 1024);
        assert_eq!(lan.max_inflight_msgs, 256);

        // Wan: 2500 / 500 = 5 ticks.
        let wan = RaftNodeConfig::from_profile(&crate::profile::Profile::Wan.config());
        assert_eq!(wan.heartbeat_tick, 1);
        assert_eq!(wan.election_tick, 5);
        assert_eq!(wan.max_size_per_msg, 1024 * 1024);
        assert_eq!(wan.max_inflight_msgs, 256);
    }
}
