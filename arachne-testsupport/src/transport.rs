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
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
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
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::UnknownPeer(id) => write!(f, "unknown peer: {id}"),
            TransportError::Disconnected(id) => write!(f, "peer is disconnected: {id}"),
            TransportError::SwitchPoisoned => write!(f, "transport switch lock is poisoned"),
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
type Switch = Arc<Mutex<HashMap<NodeId, Sender<Queued>>>>;

/// Test/simulator factory that wires up in-memory transport halves.
///
/// Create one factory, then call [`create`](TransportFactory::create) once per
/// node. All nodes created from the same factory are connected to each other
/// and can exchange messages.
#[derive(Debug, Default)]
pub struct InMemoryTransportFactory {
    switch: Switch,
}

impl InMemoryTransportFactory {
    /// Create an empty switch with no nodes yet.
    pub fn new() -> Self {
        Self {
            switch: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl TransportFactory for InMemoryTransportFactory {
    type Tx = InMemoryTx;
    type Rx = InMemoryRx;

    fn create(&self, me: NodeId) -> (Self::Tx, Self::Rx) {
        let (tx, rx) = mpsc::channel::<Queued>();
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
            },
            InMemoryRx { receiver: rx },
        )
    }
}

/// The outbound half for one node.
#[derive(Debug)]
pub struct InMemoryTx {
    switch: Switch,
    self_id: NodeId,
}

impl Transport for InMemoryTx {
    type Error = TransportError;

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
    receiver: Receiver<Queued>,
}

impl TransportRx for InMemoryRx {
    fn recv(&mut self) -> impl Future<Output = Option<(NodeId, TransportMessage)>> + Send {
        RecvFuture {
            receiver: &mut self.receiver,
            waker: None,
        }
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
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(InMemoryClosed),
        }
    }
}

/// A hand-rolled future that pulls the next queued message off an mpsc channel.
///
/// It reports `Pending` (registering the current waker) when the channel is
/// empty, so a real executor would wake it when a message arrives. In tests the
/// producer enqueues first, so it is `Ready` on the first poll.
struct RecvFuture<'a> {
    // `&mut` (not `&`): a mutable borrow is `Send` whenever `Receiver: Send`,
    // whereas a shared `&Receiver` would additionally require `Receiver: Sync`
    // (which it is not). `recv(&mut self)` hands us exactly this mutable borrow.
    receiver: &'a mut Receiver<Queued>,
    waker: Option<Waker>,
}

impl Future for RecvFuture<'_> {
    type Output = Option<Queued>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.receiver.try_recv() {
            Ok(item) => Poll::Ready(Some(item)),
            // The sender side was dropped and the queue is drained: closed.
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
            // No message yet: register our waker and wait to be polled again.
            Err(TryRecvError::Empty) => {
                self.waker = Some(cx.waker().clone());
                Poll::Pending
            }
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
