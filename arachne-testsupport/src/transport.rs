//! An in-memory transport switch: connects multiple `NodeId`s over channels.
//!
//! It is the test/simulator implementation of the transport seam. No sockets,
//! no real time, no external crates — just `std::sync::mpsc` channels wired
//! through a shared switch. It exists so the core can be driven deterministically
//! in tests and the simulator.
//!
//! # Behaviour
//! * Sending to a **connected** peer delivers the message to that peer's
//!   receiving queue, tagged with the sender's `NodeId`.
//! * Sending to an **unknown or disconnected** peer returns an error (never
//!   panics).
//! * A peer's [`recv`](InMemoryRx) yields its queued messages in FIFO order.
//!
//! # Caveat (by design)
//! This transport is *pull-based and synchronous*. The producer must enqueue a
//! message before the consumer's `recv` future is polled for it to be
//! delivered. The provided [`block_on`] helper relies on exactly this
//! ordering: it has no background thread, so it only completes futures that do
//! not need to be woken — which is always the case here once the message is
//! queued.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use arachne_seam::{NodeId, Transport, TransportFactory, TransportMessage, TransportRx};

/// Errors an in-memory transport can report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The target node is not (currently) registered with the switch.
    UnknownPeer(NodeId),
    /// The target's receiving channel has been closed (its `Rx` was dropped).
    Disconnected(NodeId),
    /// The internal switch lock was poisoned by a previously panicked thread.
    SwitchPoisoned,
    /// Assembling a streamed snapshot failed (propsol rev T).
    SnapshotWrite,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::UnknownPeer(id) => write!(f, "unknown peer: {id}"),
            TransportError::Disconnected(id) => write!(f, "peer is disconnected: {id}"),
            TransportError::SwitchPoisoned => write!(f, "transport switch lock is poisoned"),
            TransportError::SnapshotWrite => write!(f, "writing a streamed snapshot failed"),
        }
    }
}

impl std::error::Error for TransportError {}

/// A closed in-memory receiving half: every sender has been dropped and the
/// queue is drained, so no further messages can arrive.
///
/// This is the non-`Pending` counterpart of a drained, fully-disconnected
/// channel. It is reported by [`InMemoryRx::try_recv`] (and, as `None`, by
/// [`InMemoryRx::recv`]) once the transport can no longer deliver anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InMemoryClosed;

impl std::fmt::Display for InMemoryClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("in-memory transport is closed (all senders dropped, queue drained)")
    }
}

impl std::error::Error for InMemoryClosed {}

/// A message in the switch's queue: who sent it and the payload.
type Queued = (NodeId, TransportMessage);

/// The shared switch: maps each connected node to its delivery channel.
type Switch = Arc<Mutex<HashMap<NodeId, UnboundedSender<Queued>>>>;

/// Test/simulator factory that wires up in-memory transport halves.
///
/// Create one factory, then call [`create`](TransportFactory::create) once per
/// node. All nodes created from the same factory are connected to each other
/// and can exchange messages.
#[derive(Default)]
pub struct InMemoryTransportFactory {
    switch: Switch,
    /// Whether the transports this factory mints advertise snapshot streaming
    /// (propsol rev T). Off by default: an in-process transport delivers the
    /// snapshot inside the raft message, which is what the pre-rev-T code path
    /// expects, so a test that wants the streamed path asks for it.
    stream_snapshots: Arc<AtomicBool>,
    /// Per-peer snapshot bytes, keyed by the peer's node id: what this factory's
    /// transports can fetch when streaming is on. The closure answers
    /// `(index, term)`.
    snapshot_sources: SnapshotSources,
}

impl std::fmt::Debug for InMemoryTransportFactory {
    /// Hand-written: the snapshot sources are closures, which have no `Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryTransportFactory")
            .field(
                "stream_snapshots",
                &self.stream_snapshots.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

/// `peer -> (index, term) -> bytes`, shared by every transport of a factory.
pub type SnapshotSources = Arc<
    Mutex<HashMap<NodeId, Arc<dyn Fn(u64, u64) -> Option<Vec<u8>> + Send + Sync>>>,
>;

impl InMemoryTransportFactory {
    /// Create an empty switch with no nodes yet.
    pub fn new() -> Self {
        Self {
            switch: Arc::new(Mutex::new(HashMap::new())),
            stream_snapshots: Arc::new(AtomicBool::new(false)),
            snapshot_sources: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Make the transports this factory mints advertise snapshot streaming.
    ///
    /// A test that turns this on must also register a source for every peer it
    /// expects bytes from ([`Self::set_snapshot_source`]), exactly as the real
    /// transport needs a [`SnapshotProvider`] on the serving node.
    pub fn with_snapshot_streaming(self) -> Self {
        self.stream_snapshots.store(true, Ordering::Relaxed);
        self
    }

    /// Register the snapshot bytes this factory serves on behalf of `peer`.
    ///
    /// The closure is the test's stand-in for the serving node's snapshot
    /// provider; returning `None` means "this peer has no such snapshot", which
    /// is how a failed transfer is simulated.
    pub fn set_snapshot_source(
        &self,
        peer: NodeId,
        source: Arc<dyn Fn(u64, u64) -> Option<Vec<u8>> + Send + Sync>,
    ) {
        let mut guard = self
            .snapshot_sources
            .lock()
            .expect("snapshot-source lock must not be poisoned in tests");
        guard.insert(peer, source);
    }
}

impl TransportFactory for InMemoryTransportFactory {
    type Tx = InMemoryTx;
    type Rx = InMemoryRx;

    fn create(&self, me: NodeId) -> (Self::Tx, Self::Rx) {
        let (tx, rx) = mpsc::unbounded_channel::<Queued>();
        // Register this node's delivery channel in the switch. A poisoned lock
        // is unrecoverable from a non-`Result` return, and this is a test-only
        // crate, so we surface it loudly rather than hide it.
        let mut guard = self
            .switch
            .lock()
            .expect("in-memory transport switch lock must not be poisoned in tests");
        guard.insert(me.clone(), tx);
        (
            InMemoryTx {
                switch: Arc::clone(&self.switch),
                self_id: me.clone(),
                stream_snapshots: Arc::clone(&self.stream_snapshots),
                snapshot_sources: Arc::clone(&self.snapshot_sources),
            },
            InMemoryRx { receiver: rx },
        )
    }
}

/// The outbound half for one node.
#[derive(Clone)]
pub struct InMemoryTx {
    switch: Switch,
    self_id: NodeId,
    /// See [`InMemoryTransportFactory::with_snapshot_streaming`].
    stream_snapshots: Arc<AtomicBool>,
    /// See [`InMemoryTransportFactory::set_snapshot_source`].
    snapshot_sources: SnapshotSources,
}

impl std::fmt::Debug for InMemoryTx {
    /// Hand-written: the snapshot sources are closures, which have no `Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryTx")
            .field("self_id", &self.self_id)
            .field(
                "stream_snapshots",
                &self.stream_snapshots.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl Transport for InMemoryTx {
    type Error = TransportError;

    fn supports_snapshot_streaming(&self) -> bool {
        self.stream_snapshots.load(Ordering::Relaxed)
    }

    /// Serve the snapshot from the peer's registered source, writing it to
    /// `dest` (propsol rev T).
    ///
    /// In-process, so "streaming" is just a file write: the point of the test
    /// scaffolding is to exercise the *runtime's* streamed-snapshot path
    /// (intercept → fetch → install → report) without a network.
    async fn fetch_snapshot(
        &self,
        from: NodeId,
        index: u64,
        term: u64,
        dest: &std::path::Path,
    ) -> Result<Option<u64>, Self::Error> {
        if !self.supports_snapshot_streaming() {
            return Ok(None);
        }
        let source = {
            let guard = self
                .snapshot_sources
                .lock()
                .map_err(|_| TransportError::SwitchPoisoned)?;
            guard.get(&from).cloned()
        };
        let Some(source) = source else {
            // The peer serves no snapshots: report the transfer as failed
            // rather than as an empty snapshot.
            return Ok(None);
        };
        let Some(bytes) = source(index, term) else {
            return Ok(None);
        };
        let written = bytes.len() as u64;
        std::fs::write(dest, &bytes).map_err(|_| TransportError::SnapshotWrite)?;
        Ok(Some(written))
    }

    fn send(&self, to: NodeId, msg: TransportMessage) -> impl Future<Output = Result<(), Self::Error>> + Send {
        // Delivery is synchronous and cheap, so we perform it eagerly and return
        // an already-resolved future. This keeps the `MutexGuard` from living
        // across any suspension point.
        let result = {
            let guard = match self.switch.lock() {
                Ok(g) => g,
                // A poisoned lock means another thread panicked while holding
                // it; we report it rather than panic here.
                Err(_) => return std::future::ready(Err(TransportError::SwitchPoisoned)),
            };
            match guard.get(&to) {
                Some(peer_tx) => match peer_tx.send((self.self_id.clone(), msg)) {
                    Ok(()) => Ok(()),
                    // The peer's `Rx` was dropped, so its receiving half is gone.
                    Err(_) => Err(TransportError::Disconnected(to)),
                },
                None => Err(TransportError::UnknownPeer(to)),
            }
        };
        std::future::ready(result)
    }
}

/// The inbound half for one node.
#[derive(Debug)]
pub struct InMemoryRx {
    receiver: UnboundedReceiver<Queued>,
}

impl TransportRx for InMemoryRx {
    async fn recv(&mut self) -> Option<(NodeId, TransportMessage)> {
        // The channel registers this task's waker, so an inbound message wakes
        // the actor immediately instead of on its next tick.
        self.receiver.recv().await
    }

    fn try_recv(&mut self) -> Option<(NodeId, TransportMessage)> {
        self.receiver.try_recv().ok()
    }
}

/// Non-blocking receive, for deterministic single-threaded harness loops that
/// must not suspend when the queue happens to be empty.
///
/// * `Ok(Some(msg))` — a message was queued;
/// * `Ok(None)` — the queue is empty right now (try again later);
/// * `Err(InMemoryClosed)` — every sender is gone and the queue is drained.
///
/// `InMemoryClosed` itself is defined near the top of this module.
impl InMemoryRx {
    pub fn try_recv(
        &mut self,
    ) -> Result<Option<(NodeId, TransportMessage)>, InMemoryClosed> {
        match self.receiver.try_recv() {
            Ok(item) => Ok(Some(item)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(InMemoryClosed),
        }
    }
}


/// A minimal `Wake` implementation that does nothing on wake.
///
/// Used by [`block_on`] to satisfy the waker slot without any real signalling,
/// since the in-memory transport only needs a message to already be queued.
struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
    fn wake_by_ref(self: &Arc<Self>) {}
}

/// Drive a `Send` future to completion with a no-op waker.
///
/// This is a **test/simulator helper**. It has no background thread, so it
/// only works for futures that complete without needing to be woken — exactly
/// the in-memory transport futures, where the producer enqueues before the
/// consumer polls. If the future stays `Pending` past a generous poll cap, it
/// panics (fail fast) instead of spinning forever.
pub fn block_on<F: Future + Send>(future: F) -> F::Output {
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
                    "block_on: future still Pending after {MAX_POLLS} polls (in-memory transports should never suspend once a message is queued)"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arachne_seam::TransportMessage;

    fn raft(bytes: &[u8]) -> TransportMessage {
        TransportMessage::Raft(bytes.to_vec())
    }

    #[test]
    fn the_in_memory_transport_does_not_stream_snapshots() {
        // propsol rev T: the streaming path is opt-in. An in-process transport
        // hands the snapshot over inside the raft message, so it must report
        // "cannot stream" — the core uses exactly this answer to decide whether
        // a metadata-only snapshot is acceptable. Claiming `true` here would
        // leave a follower holding metadata and no data.
        let factory = InMemoryTransportFactory::new();
        let (tx, _rx) = factory.create(NodeId::from("a"));
        assert!(!tx.supports_snapshot_streaming());
        let dest = std::env::temp_dir().join("arachne-never-written.snap");
        let result = block_on(tx.fetch_snapshot(
            NodeId::from("b"),
            1,
            1,
            dest.as_path(),
        ));
        assert_eq!(
            result.map_err(|e| e.to_string()),
            Ok(None),
            "the default must say 'unsupported', never 'empty snapshot'"
        );
        assert!(!dest.exists(), "an unsupported fetch must not create a file");
    }

    #[test]
    fn the_streaming_scaffolding_serves_registered_snapshot_bytes() {
        // rev T: this is what a test needs to exercise the runtime's streamed
        // snapshot path in-process (intercept → fetch → install → report)
        // without a network. The bytes are served by the peer's registered
        // source, exactly as the real transport serves them from the serving
        // node's snapshot provider.
        let factory = InMemoryTransportFactory::new().with_snapshot_streaming();
        let (tx, _rx) = factory.create(NodeId::from("follower"));
        assert!(
            tx.supports_snapshot_streaming(),
            "the factory was asked to advertise streaming"
        );

        let payload = b"snapshot bytes".to_vec();
        let served = std::sync::Arc::new(payload.clone());
        factory.set_snapshot_source(
            NodeId::from("leader"),
            std::sync::Arc::new(move |index, term| {
                if index == 9 && term == 4 {
                    Some(served.as_ref().clone())
                } else {
                    None
                }
            }),
        );

        let dest = std::env::temp_dir().join("arachne-testsupport-snapshot-fetch.snap");
        let _ = std::fs::remove_file(&dest);
        let written = block_on(tx.fetch_snapshot(NodeId::from("leader"), 9, 4, &dest))
            .expect("the fetch must not fail")
            .expect("a registered source serves the snapshot");
        assert_eq!(written, payload.len() as u64);
        assert_eq!(
            std::fs::read(&dest).expect("read the fetched file"),
            payload,
            "the file must be exactly what the source served"
        );
        let _ = std::fs::remove_file(&dest);

        // A snapshot the peer does not have is `None` — a failed transfer the
        // runtime reports to raft — never an empty snapshot to install.
        let dest = std::env::temp_dir().join("arachne-testsupport-snapshot-missing.snap");
        let _ = std::fs::remove_file(&dest);
        assert_eq!(
            block_on(tx.fetch_snapshot(NodeId::from("leader"), 1, 1, &dest)),
            Ok(None)
        );
        assert!(!dest.exists(), "a failed fetch must not leave a file behind");

        // A peer with no registered source behaves the same way.
        assert_eq!(
            block_on(tx.fetch_snapshot(NodeId::from("stranger"), 9, 4, &dest)),
            Ok(None)
        );
        assert!(!dest.exists());
    }

    #[test]
    fn send_to_connected_peer_roundtrips() {
        let factory = InMemoryTransportFactory::new();
        let (a_tx, _a_rx) = factory.create(NodeId::from("a"));
        let (b_tx, mut b_rx) = factory.create(NodeId::from("b"));
        let _ = b_tx; // keep b's tx alive so its receiver is not disconnected

        let res = block_on(a_tx.send(NodeId::from("b"), raft(b"hello")));
        assert_eq!(res, Ok(()));

        let got = block_on(b_rx.recv());
        assert_eq!(got, Some((NodeId::from("a"), raft(b"hello"))));
    }

    #[test]
    fn send_to_unknown_peer_is_an_error_not_a_panic() {
        let factory = InMemoryTransportFactory::new();
        let (a_tx, _a_rx) = factory.create(NodeId::from("a"));
        // "ghost" was never created.
        let res = block_on(a_tx.send(NodeId::from("ghost"), raft(b"x")));
        assert_eq!(res, Err(TransportError::UnknownPeer(NodeId::from("ghost"))));
    }

    #[test]
    fn send_to_disconnected_peer_is_an_error() {
        let factory = InMemoryTransportFactory::new();
        let (a_tx, _a_rx) = factory.create(NodeId::from("a"));
        // Create b, then drop its Rx so its channel is closed.
        let (_b_tx, b_rx) = factory.create(NodeId::from("b"));
        drop(b_rx);

        let res = block_on(a_tx.send(NodeId::from("b"), raft(b"x")));
        assert_eq!(res, Err(TransportError::Disconnected(NodeId::from("b"))));
    }

    #[test]
    fn recv_yields_messages_in_fifo_order() {
        let factory = InMemoryTransportFactory::new();
        let (a_tx, _a_rx) = factory.create(NodeId::from("a"));
        let (_b_tx, mut b_rx) = factory.create(NodeId::from("b"));

        for i in 0..5u8 {
            let msg = raft(&[i]);
            let res = block_on(a_tx.send(NodeId::from("b"), msg));
            assert_eq!(res, Ok(()));
        }

        for i in 0..5u8 {
            let got = block_on(b_rx.recv());
            assert_eq!(got, Some((NodeId::from("a"), raft(&[i]))));
        }
        // After the queue is drained but the sender is still alive, the next
        // recv is Pending with no message — we do not poll here (block_on would
        // spin); instead we verify drain-then-close below.
    }

    #[test]
    fn recv_returns_none_when_all_senders_dropped_and_drained() {
        let factory = InMemoryTransportFactory::new();
        let (a_tx, a_rx) = factory.create(NodeId::from("a"));
        let (b_tx, mut b_rx) = factory.create(NodeId::from("b"));

        // Deliver one message to b.
        block_on(a_tx.send(NodeId::from("b"), raft(b"one"))).unwrap();

        // The delivery `Sender` for each node lives in the switch (owned by the
        // factory), not in the `InMemoryTx`. So to fully disconnect b's channel
        // we drop every sender half AND the factory itself. `b_rx` owns its
        // `Receiver` independently, so it outlives the factory and can read the
        // closed tail.
        drop(a_tx);
        drop(a_rx);
        drop(b_tx);
        drop(factory);

        // The queued message is still delivered first (FIFO, not lost).
        assert_eq!(block_on(b_rx.recv()), Some((NodeId::from("a"), raft(b"one"))));
        // Now the queue is empty and every sender is gone: closed => None.
        assert_eq!(block_on(b_rx.recv()), None);
    }

    #[test]
    fn try_recv_reports_queued_empty_and_closed() {
        let factory = InMemoryTransportFactory::new();
        let (a_tx, _a_rx) = factory.create(NodeId::from("a"));
        let (_b_tx, mut b_rx) = factory.create(NodeId::from("b"));

        // Empty queue with senders still alive -> Ok(None) (not an error).
        assert_eq!(b_rx.try_recv(), Ok(None));

        // A queued message is reported as Ok(Some(..)) in FIFO order.
        block_on(a_tx.send(NodeId::from("b"), raft(b"one"))).unwrap();
        block_on(a_tx.send(NodeId::from("b"), raft(b"two"))).unwrap();
        assert_eq!(b_rx.try_recv(), Ok(Some((NodeId::from("a"), raft(b"one")))));
        assert_eq!(b_rx.try_recv(), Ok(Some((NodeId::from("a"), raft(b"two")))));

        // Drained again but senders alive -> Ok(None).
        assert_eq!(b_rx.try_recv(), Ok(None));

        // Drop every sender (and the factory that owns the switch's senders)
        // -> the channel is closed and drained -> Err(InMemoryClosed).
        drop(a_tx);
        drop(_a_rx);
        drop(_b_tx);
        drop(factory);
        assert_eq!(b_rx.try_recv(), Err(InMemoryClosed));
    }

    #[test]
    fn multiple_senders_deliver_to_one_receiver_in_order() {
        let factory = InMemoryTransportFactory::new();
        let (a_tx, _a_rx) = factory.create(NodeId::from("a"));
        let (c_tx, _c_rx) = factory.create(NodeId::from("c"));
        let (_b_tx, mut b_rx) = factory.create(NodeId::from("b"));

        block_on(a_tx.send(NodeId::from("b"), raft(b"from-a"))).unwrap();
        block_on(c_tx.send(NodeId::from("b"), raft(b"from-c"))).unwrap();

        assert_eq!(
            block_on(b_rx.recv()),
            Some((NodeId::from("a"), raft(b"from-a")))
        );
        assert_eq!(
            block_on(b_rx.recv()),
            Some((NodeId::from("c"), raft(b"from-c")))
        );
    }
}
