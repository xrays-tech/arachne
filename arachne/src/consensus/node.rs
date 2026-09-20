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

use std::collections::HashMap;

use protobuf::Message as _;
use raft::eraftpb::Message;
use raft::raw_node::Ready;
use raft::ReadOnlyOption;
use raft::{Config as RaftConfig, RawNode};
use slog::Logger;

use arachne_seam::seam::{Transport, TransportMessage, TransportRx};
use arachne_seam::storage::{
    ConfState as SeamConfState, HardState as SeamHardState, LogEntry, RaftId,
    Snapshot as SeamSnapshot, SnapshotMeta as SeamSnapshotMeta, Storage as SeamStorage,
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
#[derive(Clone, Copy, Debug)]
pub struct RaftNodeConfig {
    /// Number of ticks for an election timeout.
    pub election_tick: u64,
    /// Number of ticks between heartbeats.
    pub heartbeat_tick: u64,
    /// Max total bytes in a single raft message (AppendEntries batching).
    pub max_size_per_msg: u64,
    /// Max messages in flight to a single follower (raft built-in flow control).
    pub max_inflight_msgs: u64,
}

impl Default for RaftNodeConfig {
    /// The values previously hardcoded in [`RaftNode::new`].
    fn default() -> Self {
        Self {
            election_tick: 10,
            heartbeat_tick: 1,
            max_size_per_msg: 1_048_576,
            max_inflight_msgs: 256,
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

/// The result of one [`RaftNode::step`] `Ready` cycle.
///
/// `committed` are the entries to apply to the state machine; `read_states`
/// are quorum-confirmed ReadIndex results `(request_ctx, read_index)` — a read
/// may be served locally once the applied index reaches `read_index`
/// (propsol §5.4). Both are empty when there was no pending work.
#[derive(Debug, Default)]
pub struct StepOutcome {
    /// Committed entries `(index, data)` to apply, in ascending index order.
    ///
    /// When `snapshot` is set, every entry here is strictly newer than it: an
    /// installed snapshot supersedes everything at or below its index.
    pub committed: Vec<(LogIndex, Vec<u8>)>,
    /// A snapshot this node installed during the cycle (it arrived from the
    /// leader). The caller **must** restore the state machine from
    /// `snapshot.data` before applying `committed`.
    pub snapshot: Option<SeamSnapshot>,
    /// Quorum-confirmed ReadIndex results `(request_ctx, read_index)`.
    pub read_states: Vec<(Vec<u8>, LogIndex)>,
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
        // elect a leader (propsol §5.7 `initial_cluster`). M3 ConfChange will
        // replace this with persisted, changeable membership.
        let mut voters: Vec<RaftId> = peers.keys().copied().collect();
        voters.push(self_raft_id);
        voters.sort_unstable();
        voters.dedup();
        let bootstrap = SeamConfState {
            voters,
            learners: Vec::new(),
        };

        let store = RaftStorage::with_conf_state(storage, bootstrap);
        let raw = RawNode::new(&raft_config, store, logger).map_err(NodeError::Raft)?;

        Ok(Self {
            raw,
            transport,
            rx,
            peers,
            dropped_sends: 0,
        })
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
        if !self.raw.has_ready() {
            return Ok(StepOutcome::default());
        }
        let ready = self.raw.ready();

        // I1/I2: persist entries, hard state, and snapshot durably BEFORE any
        // message is sent.
        let installed = self.persist_ready(&ready)?;
        // INV2 crash injection (propsol v0.2.9 L; test-only, compiled out by
        // default): the boundary where entries + HardState are durable but no
        // message has been sent yet.
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::check(crate::fault_injection::Stage::AfterPersist);

        // Send the immediate (pre-persist) messages: the leader's fast-path
        // replication messages, which do not depend on local durability.
        let immediate: Vec<Message> = ready.messages().to_vec();
        for msg in &immediate {
            self.deliver(msg).await;
        }

        // Persisted messages must go out only after their payload is durable.
        // Capture them before moving `ready` into `advance`.
        let persisted: Vec<Message> = ready.persisted_messages().to_vec();

        // Committed entries that were ALREADY durable and committed before this
        // `Ready` was produced (raft doc, step 3). `RawNode::advance`'s
        // `LightReady` (doc, step 7) only carries the entries that became
        // committed *by this* `Ready` (the ones just persisted above). The two
        // sets are disjoint and together form the complete set of newly-committed
        // entries; returning only the `LightReady` set silently drops the first
        // set — which is exactly the entries a follower/leader persists in one
        // round and commits in a later round. Capture before `ready` is moved.
        let ready_committed: Vec<(LogIndex, Vec<u8>)> = ready
            .committed_entries()
            .iter()
            .map(|e| (e.get_index(), e.get_data().to_vec()))
            .collect();

        // Quorum-confirmed ReadIndex results (propsol §5.4 step 2). Read states
        // appear only on the full `Ready`, not the `LightReady`, so they must be
        // captured before `ready` is moved into `advance` (exactly like
        // `ready_committed`). Each is `(request_ctx, read_index)`: the caller
        // may serve the read once the applied index reaches `read_index`.
        let read_states: Vec<(Vec<u8>, LogIndex)> = ready
            .read_states()
            .iter()
            .map(|rs| (rs.request_ctx.clone(), rs.index))
            .collect();

        // Advance: confirms persistence and yields the `LightReady` (committed
        // entries plus any messages generated during the advance).
        let light = self.raw.advance(ready);

        // Persist a **commit-index advance**. raft-rs surfaces a commit advance
        // on the `LightReady` (`LightReady::commit_index`), *not* on
        // `Ready::hs` — `Ready::hs` carries term/vote changes, and the
        // `LightReady` path also advances raft's internal `prev_hs.commit`. So
        // if this is not persisted here, the commit is never written again and
        // the durable HardState's `commit` stays stale forever. That stale
        // value is what `initial_state()` reports on restart and what
        // force-recovery (propsol §6.1, recovery point = committed/applied
        // index) would use as the truncation point — so a stale commit makes
        // force-recovery discard committed data. Persist it here, before any
        // committed entry is applied or acknowledged (I1: `set_hard_state`
        // always fsyncs), mirroring raft-rs's own example
        // (`examples/single_mem_node`).
        if let Some(commit) = light.commit_index() {
            let hs = self.raw.raft.hard_state();
            let mut seam_hs = RaftStorage::<S>::to_seam_hard_state(&hs);
            seam_hs.commit = commit;
            self.raw
                .mut_store()
                .set_hard_state(&seam_hs)
                .map_err(NodeError::Raft)?;
        }

        for msg in &persisted {
            self.deliver(msg).await;
        }
        for msg in light.messages() {
            self.deliver(msg).await;
        }

        // Committed entries for the state machine: the union of the entries that
        // were already committed before this `Ready` and those committed by it.
        // They are disjoint and in ascending index order (`ready_committed`
        // holds the lower indices), which the state machine requires.
        let mut committed = ready_committed;
        committed.extend(
            light
                .committed_entries()
                .iter()
                .map(|e| (e.get_index(), e.get_data().to_vec())),
        );
        if let Some(snapshot) = &installed {
            // An installed snapshot supersedes everything at or below its
            // index, including entries that were already committed before this
            // cycle. The state machine is restored to `snapshot.index` first,
            // so applying one of those would be an index violation.
            committed.retain(|(index, _)| *index > snapshot.meta.index);
        }
        // INV2 crash injection: messages have been sent, committed entries have
        // not been applied yet (the caller applies after `step` returns).
        #[cfg(feature = "fault-injection")]
        crate::fault_injection::check(crate::fault_injection::Stage::AfterDeliver);

        Ok(StepOutcome {
            committed,
            snapshot: installed,
            read_states,
        })
    }

    /// Propose a command to the raft log.
    pub fn propose(&mut self, cmd: &[u8]) -> Result<(), NodeError<T>> {
        self.raw
            .propose(Vec::new(), cmd.to_vec())
            .map_err(NodeError::Raft)
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

    /// The term of the entry at `index`, answered from the durable snapshot
    /// once the entry itself has been compacted away.
    pub fn term_at(&mut self, index: LogIndex) -> Result<Term, NodeError<T>> {
        self.raw.mut_store().term(index).map_err(NodeError::Raft)
    }

    /// Persist a `Ready`'s entries, hard state, and snapshot durably.
    ///
    /// Entries are appended then `sync_entries`ed (I2/I4 barrier); the hard
    /// state is persisted via `set_hard_state` (I1, always fsyncs).
    ///
    /// A snapshot received from the leader is installed **first**, because it
    /// replaces the whole log and `ready.entries()` (when present) starts at
    /// `snapshot.index + 1`. Returns the installed snapshot so the caller can
    /// restore the state machine from it.
    fn persist_ready(&mut self, ready: &Ready) -> Result<Option<SeamSnapshot>, NodeError<T>> {
        let store = self.raw.mut_store();

        let installed = if ready.snapshot().is_empty() {
            None
        } else {
            let snapshot = RaftStorage::<S>::from_raft_snapshot(ready.snapshot());
            // I3: durable before the log is released and before any message
            // acknowledging it goes out.
            store.install_snapshot(&snapshot).map_err(NodeError::Raft)?;
            Some(snapshot)
        };

        if !ready.entries().is_empty() {
            let entries: Vec<LogEntry> =
                ready.entries().iter().map(RaftStorage::<S>::from_raft_entry).collect();
            store.append(&entries).map_err(NodeError::Raft)?;
            // I2/I4: durability barrier — entries are durable after this.
            store.sync_entries().map_err(NodeError::Raft)?;
        }

        if let Some(hs) = ready.hs() {
            let seam_hs = RaftStorage::<S>::to_seam_hard_state(hs);
            store.set_hard_state(&seam_hs).map_err(NodeError::Raft)?;
        }

        Ok(installed)
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
            for (idx, data) in entries {
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
    fn single_node_propose_commit_apply() {
        let (mut node, mut sm) = make_leader();

        let cmd = KvStateMachine::encode_put(1, 1, b"key", b"value");
        node.propose(&cmd).expect("propose to a leader must succeed");

        for _ in 0..100 {
            node.tick();
            let entries = block_on(node.step()).expect("drive cycle must succeed").committed;
            for (idx, data) in entries {
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
            for (idx, data) in entries {
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
            for (idx, data) in outcome.committed {
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
            for (idx, data) in entries {
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
            for (idx, data) in entries {
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
