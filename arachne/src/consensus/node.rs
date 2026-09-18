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
    Storage as SeamStorage,
};
use arachne_seam::types::{LogIndex, NodeId};

use super::raft_storage::RaftStorage;

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
}

impl<S, T, Tr> RaftNode<S, T, Tr>
where
    S: SeamStorage,
    T: Transport,
    Tr: TransportRx,
{
    /// Create a new `RaftNode`.
    ///
    /// `self_raft_id` must be non-zero and unique in the group. `applied` is
    /// the last index applied to the state machine (0 for a fresh node). The
    /// raft configuration enables PreVote, CheckQuorum, and lease-based
    /// linearizable reads (`LeaseBased` requires `check_quorum`).
    pub fn new(
        self_raft_id: RaftId,
        peers: HashMap<RaftId, NodeId>,
        storage: S,
        transport: T,
        rx: Tr,
        applied: LogIndex,
        logger: &Logger,
    ) -> Result<Self, NodeError<T>> {
        if self_raft_id == 0 {
            return Err(NodeError::Raft(
                raft::Error::ConfigInvalid("node id must be non-zero".into()),
            ));
        }

        let config = RaftConfig {
            id: self_raft_id,
            election_tick: 10,
            heartbeat_tick: 1,
            applied,
            max_size_per_msg: 1_048_576,
            max_inflight_msgs: 256,
            check_quorum: true,
            pre_vote: true,
            read_only_option: ReadOnlyOption::LeaseBased,
            ..Default::default()
        };
        config.validate().map_err(NodeError::Raft)?;

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
        let raw = RawNode::new(&config, store, logger).map_err(NodeError::Raft)?;

        Ok(Self {
            raw,
            transport,
            rx,
            peers,
        })
    }

    /// Advance the internal logical clock by one tick.
    pub fn tick(&mut self) {
        self.raw.tick();
    }

    /// Drive one `Ready` cycle with the frozen persist/send ordering (I1–I4).
    ///
    /// Returns the committed entries `(index, data)` to apply to the state
    /// machine. After applying them, call [`Self::advance_apply`] to update the
    /// apply progress. Returns an empty vec when there is no pending work.
    ///
    /// # Transport failures are non-fatal
    ///
    /// A message that cannot be delivered (e.g. its peer is currently down) is
    /// dropped, **not** surfaced as an error, and never prevents `advance()`:
    /// raft retransmits on a later tick. Aborting the `Ready` cycle on a send
    /// failure would let one unreachable peer stall this node's persistence
    /// pipeline, which is exactly the failure mode raft is designed to absorb.
    /// (`NodeError::Transport` remains for callers that send explicitly.)
    pub async fn step(&mut self) -> Result<Vec<(LogIndex, Vec<u8>)>, NodeError<T>> {
        if !self.raw.has_ready() {
            return Ok(Vec::new());
        }
        let ready = self.raw.ready();
        let self_id = self.raw.raft.id;

        // I1/I2: persist entries, hard state, and snapshot durably BEFORE any
        // message is sent.
        self.persist_ready(&ready)?;

        // Send the immediate (pre-persist) messages: the leader's fast-path
        // replication messages, which do not depend on local durability.
        let immediate: Vec<Message> = ready.messages().to_vec();
        for msg in &immediate {
            let _ = send_one(&self.transport, &self.peers, self_id, msg).await;
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

        // Advance: confirms persistence and yields the `LightReady` (committed
        // entries plus any messages generated during the advance).
        let light = self.raw.advance(ready);

        for msg in &persisted {
            let _ = send_one(&self.transport, &self.peers, self_id, msg).await;
        }
        for msg in light.messages() {
            let _ = send_one(&self.transport, &self.peers, self_id, msg).await;
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
        Ok(committed)
    }

    /// Propose a command to the raft log.
    pub fn propose(&mut self, cmd: &[u8]) -> Result<(), NodeError<T>> {
        self.raw
            .propose(Vec::new(), cmd.to_vec())
            .map_err(NodeError::Raft)
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

    /// The current durable hard state (term, vote, commit).
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

    /// Persist a `Ready`'s entries, hard state, and snapshot durably.
    ///
    /// Entries are appended then `sync_entries`ed (I2/I4 barrier); the hard
    /// state is persisted via `set_hard_state` (I1, always fsyncs). A
    /// non-empty snapshot is a fault: this storage does not support snapshot
    /// transfer, so we fail loudly rather than silently drop it.
    fn persist_ready(&mut self, ready: &Ready) -> Result<(), NodeError<T>> {
        let store = self.raw.mut_store();

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

        if !ready.snapshot().is_empty() {
            return Err(NodeError::Raft(
                raft::Error::ConfigInvalid(
                    "snapshot transfer is not supported by this storage".into(),
                ),
            ));
        }

        Ok(())
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
            let entries = block_on(node.step()).expect("drive cycle must succeed");
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
            let entries = block_on(node.step()).expect("drive cycle must succeed");
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
    fn i1_i2_persist_durable_before_step_returns() {
        let ledger = std::sync::Arc::new(arachne_testsupport::FsyncLedger::new());
        let observer: std::sync::Arc<dyn arachne_seam::FsyncObserver> = ledger.clone();
        let store = MemStore::with_observer(observer);
        let (mut node, mut sm) = (make_node(store), KvStateMachine::new());

        // Become leader.
        for _ in 0..100 {
            node.tick();
            let entries = block_on(node.step()).expect("drive must succeed");
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
            let entries = block_on(node.step()).expect("drive must succeed");
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
}
