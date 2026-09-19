//! ClientOracle v1 (test-plan §6.4, §7, §9).
//!
//! A **clock-free, pure** oracle over a client's recorded operation history.
//! It evaluates a set of consistency invariants that a linearizable KV system
//! must satisfy and produces a typed report — it never panics.
//!
//! # Model
//!
//! The history is a sequence of [`Event`]s (invokes and completes) recorded by a
//! single sequencer per client in real-time order. All "time" is a
//! caller-supplied **logical** timestamp ([`u64`]) — there is no wall clock, no
//! I/O, and no threads anywhere in this module, so every check is a pure
//! function over recorded data and is fully deterministic.
//!
//! * [`ValueId`], [`ClientId`], [`SeqNo`], [`CallId`] are thin newtypes so every
//!   check is pure over recorded integers.
//! * A **logical op** is identified by `(client, seq)`. Repeated invokes of the
//!   same `(client, seq)` (client retries) are collapsed by
//!   [`History::merge_retries`] into a single logical op (the original calls are
//!   kept for debugging) before any check runs.
//! * A **failed** op ([`OpResult::Err`]) is **not** a linearization point: it is
//!   still recorded and reported, but it is excluded from the linearization /
//!   read invariants.
//!
//! # Invariants checked by [`History::check`]
//!
//! | id ([`Invariant`]) | invariant (test-plan §6.4) |
//! |---|---|
//! | [`Invariant::PhantomValue`] (①) | a successful `Get(k) -> Some(v)` must correspond to a `Put(k, v)` that MAY have applied and whose interval permits a put-before-read linearization (`put.invoke < get.complete`); only a Delete that *definitely applied and necessarily sits between* invalidates it |
//! | [`Invariant::OneLogIdPerSeq`] (②) | all records for one `(client, seq)` logged op (`Put`/`Delete`) agree on a single `log_id` |
//! | [`Invariant::ReadYourWrites`] (③) | after a client's **latest** same-key write (`Put`) completes, a later read by that client must not return an older value; a later own `Delete` removes the constraint |
//! | [`Invariant::MonotonicReads`] (④) | per `(client, key)`, successive successful `Get` results must not regress in version order |
//! | [`Invariant::WellFormed`] (structural) | the raw event log is well-formed (see [`History::validate`]) |
//!
//! The two **opt-in** checks (they require scenario-side input) are separate
//! methods so they are never silently assumed:
//!
//! * [`History::check_lost_writes`] — [`Invariant::LostWrite`] (⑤): every value
//!   in the scenario-provided per-key version chain was actually acked.
//! * [`History::check_durability`] — [`Invariant::Durability`] (⑥): every
//!   acked value is present in the post-recovery observed set (the `ValueId`s
//!   must be globally unique across keys for this flat-set check).
//!
//! # Documented assumptions
//!
//! * **`ValueId` is a per-key monotonically-increasing version.** For the
//!   [`Invariant::ReadYourWrites`] and [`Invariant::MonotonicReads`] checks the
//!   harness assigns `ValueId`s so that, for any key, the values written to it
//!   in real-time order have strictly increasing `ValueId`. "Older/newer" in
//!   those checks means smaller/larger `ValueId`. The linearizability checker
//!   ([`crate::linearizability`]) does *not* assume this: it treats `ValueId`
//!   as an opaque value.
//! * **Single-sequencer clients.** A client completes a `(client, seq)` op
//!   before it invokes the next one (no per-client overlap). This matches the
//!   L2 harness's one-sequencer-per-client model (test-plan §6.4) and is the
//!   condition under which the stateright cross-validation is well-defined.
//! * **Real-time order** is the happens-before relation: op `a` is real-time
//!   before op `b` iff `a` completes at or before `b` begins
//!   (`a.complete_ts <= b.invoke_ts`).
//! * **Unknown-outcome failures.** A write that failed with an *unknown* outcome
//!   ([`OracleErrorKind::Timeout`] / [`OracleErrorKind::SessionExpired`],
//!   propsol N2/N3) or that is still in flight **may** have applied; the oracle
//!   treats it permissively, and the linearizability checker treats it as an
//!   *optional* linearization point (placed or skipped). A failure that is
//!   definitely not applied (`NotLeader` / `QuorumUnavailable` / `Busy`) is
//!   never a linearization point.
//! * **Well-formed history.** [`History::validate`] **reports** a
//!   [`Event::Complete`] with no matching [`Event::Invoke`], a reused `CallId`,
//!   or one `(client, seq)` carrying differing ops — a malformed history is a
//!   harness bug, not something silently dropped.

use std::collections::{BTreeMap, BTreeSet};

// ---------------------------------------------------------------------------
// Newtypes
// ---------------------------------------------------------------------------

/// An opaque value identifier (a `ValueId`).
///
/// In the linearizability model a `ValueId` is an opaque value. For the
/// monotonic / read-your-writes oracle checks the harness assigns `ValueId`s
/// as a per-key monotonically-increasing version (see the module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueId(pub u64);

/// The identity of a client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientId(pub u64);

/// The per-client sequence number of an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeqNo(pub u64);

/// A unique identifier for one physical (invoke/complete) call.
///
/// A client may retry the same `(client, seq)` with several distinct `CallId`s;
/// [`History::merge_retries`] collapses them into one logical op while keeping
/// the full set of call ids for debugging.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CallId(pub u64);

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// A key-value operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// Set `key` to `value`.
    Put { key: Vec<u8>, value: ValueId },
    /// Remove `key`.
    Delete { key: Vec<u8> },
    /// Read `key`.
    Get { key: Vec<u8> },
}

impl Op {
    /// The key this operation targets (all variants carry a key).
    pub fn key(&self) -> &Vec<u8> {
        match self {
            Op::Put { key, .. } | Op::Delete { key } | Op::Get { key } => key,
        }
    }

    /// `true` for a logged operation: a write (`Put`) or a delete (`Delete`).
    ///
    /// Both enter the raft log under a log id, so both are subject to the
    /// "at most one log-id per `(client, seq)`" check (test-plan §6.4 ②).
    pub fn is_logged_op(&self) -> bool {
        matches!(self, Op::Put { .. } | Op::Delete { .. })
    }

    /// `true` for a `Put` (a value-carrying write).
    pub fn is_put(&self) -> bool {
        matches!(self, Op::Put { .. })
    }
}

/// The kind of a failed operation.
///
/// Mirrors the subset of the client error model ([`ArachneError`](the
/// production `arachne::client`) is *not* depended on by this crate) that
/// represents a non-linearization-point failure: the op did not take effect
/// (or its effect is unknown) and must not be treated as a linearization point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OracleErrorKind {
    /// The node is not the leader.
    NotLeader,
    /// The quorum is unavailable (leader stepped down / quorum round failed).
    QuorumUnavailable,
    /// A bounded wait timed out; the result is unknown (propsol N3).
    Timeout,
    /// Back-pressure: a queue is full; retry later.
    Busy,
    /// The session expired; the result is unknown (propsol N2).
    SessionExpired,
}

impl OracleErrorKind {
    /// The stable wire/report name of the error kind.
    pub fn as_str(&self) -> &'static str {
        match self {
            OracleErrorKind::NotLeader => "NotLeader",
            OracleErrorKind::QuorumUnavailable => "QuorumUnavailable",
            OracleErrorKind::Timeout => "Timeout",
            OracleErrorKind::Busy => "Busy",
            OracleErrorKind::SessionExpired => "SessionExpired",
        }
    }

    /// `true` when the failed op may still have taken effect (the result is
    /// **unknown**, propsol N2/N3): [`Self::Timeout`] and
    /// [`Self::SessionExpired`]. Such an op is not a known-not-applied failure,
    /// so a write that failed this way is an *optional* linearization point.
    pub fn is_unknown_outcome(&self) -> bool {
        matches!(self, OracleErrorKind::Timeout | OracleErrorKind::SessionExpired)
    }

    /// `true` when the failed op definitely did **not** take effect
    /// (the node was not the leader / quorum was unavailable / back-pressure
    /// rejected it before propose): it is never a linearization point.
    pub fn is_known_not_applied(&self) -> bool {
        !self.is_unknown_outcome()
    }
}

/// The result of an operation.
///
/// * `Ok(Some(v))` — a `Get` that found the value `v` (or, for a `Put`, the
///   stored value — the model returns `Ok(None)` for writes, so this is the
///   read case).
/// * `Ok(None)` — a successful `Put`/`Delete` (acknowledged, no value) or a
///   `Get` of an absent key.
/// * `Err(kind)` — a failed op; **not** a linearization point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpResult {
    Ok(Option<ValueId>),
    Err(OracleErrorKind),
}

impl OpResult {
    /// `true` for a successful op (an `Ok`).
    pub fn is_ok(&self) -> bool {
        matches!(self, OpResult::Ok(_))
    }
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

/// A single recorded event, in real-time order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client invoked `op` (a physical call).
    Invoke {
        call: CallId,
        client: ClientId,
        seq: SeqNo,
        op: Op,
        /// Logical timestamp at which the op was invoked.
        ts: u64,
    },
    /// A physical call completed with `result`.
    Complete {
        call: CallId,
        /// Logical timestamp at which the op completed.
        ts: u64,
        result: OpResult,
        /// The log id the write entered under, if the record carries one.
        ///
        /// This field is what makes the "at most one log-id per seq" check
        /// (test-plan §6.4 ②) expressible; it is `None` for reads and for
        /// records that do not carry a log id.
        log_id: Option<u64>,
    },
}

/// A client's recorded operation history (the raw, per-call event log).
#[derive(Clone, Debug, Default)]
pub struct History {
    events: Vec<Event>,
}

impl History {
    /// An empty history.
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
        }
    }

    /// Record an invoke.
    pub fn invoke(
        &mut self,
        call: CallId,
        client: ClientId,
        seq: SeqNo,
        op: Op,
        ts: u64,
    ) -> &mut Self {
        self.events.push(Event::Invoke {
            call,
            client,
            seq,
            op,
            ts,
        });
        self
    }

    /// Record a completion (no log id).
    pub fn complete(&mut self, call: CallId, ts: u64, result: OpResult) -> &mut Self {
        self.events.push(Event::Complete {
            call,
            ts,
            result,
            log_id: None,
        });
        self
    }

    /// Record a completion carrying a log id (a write that entered the log).
    pub fn complete_logged(
        &mut self,
        call: CallId,
        ts: u64,
        result: OpResult,
        log_id: u64,
    ) -> &mut Self {
        self.events.push(Event::Complete {
            call,
            ts,
            result,
            log_id: Some(log_id),
        });
        self
    }

    /// The raw event log (for debugging / the record layer).
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// The number of recorded events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// `true` if the history has no events.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Collapse repeated `(client, seq)` retries into one logical op per
    /// `(client, seq)`, preserving real-time order.
    ///
    /// This is the reduction the checks and the linearizability checker run on
    /// (test-plan §6.4: "the retry-merge rule keeps the original multiple calls
    /// at the record layer, and merges when feeding the checker").
    pub fn merge_retries(&self) -> ReducedHistory {
        // Map each call id to its `(client, seq)` from the invokes.
        let call_key: BTreeMap<CallId, (ClientId, SeqNo)> = self
            .events
            .iter()
            .filter_map(|e| match e {
                Event::Invoke { call, client, seq, .. } => Some((*call, (*client, *seq))),
                Event::Complete { .. } => None,
            })
            .collect();

        // Group events by `(client, seq)`. A `BTreeMap` keeps iteration
        // deterministic.
        let mut groups: BTreeMap<(ClientId, SeqNo), Group> = BTreeMap::new();

        for event in &self.events {
            match event {
                Event::Invoke {
                    call,
                    client,
                    seq,
                    op,
                    ts,
                } => {
                    let key = (*client, *seq);
                    let group = groups.entry(key).or_default();
                    if group.op.is_none() {
                        group.op = Some(op.clone());
                    }
                    group.invoke_ts = Some(group.invoke_ts.unwrap_or(u64::MAX).min(*ts));
                    group.calls.push(*call);
                }
                Event::Complete { call, ts, result, log_id } => {
                    // A complete without a matching invoke cannot be attributed
                    // to a `(client, seq)`; drop it (well-formedness note).
                    let Some(&(client, seq)) = call_key.get(call) else {
                        continue;
                    };
                    let group = groups.entry((client, seq)).or_default();
                    group.completes.push((*ts, result.clone()));
                    if let Some(log_id) = log_id {
                        group.log_ids.push(*log_id);
                    }
                }
            }
        }

        // Produce one logical op per group, in real-time order. A group with no
        // recorded op can only come from a malformed history (a complete without
        // an invoke); such groups are dropped here and reported by
        // [`History::validate`].
        let mut ops: Vec<LogOp> = groups
            .into_iter()
            .filter_map(|((client, seq), group)| group.into_log_op(client, seq))
            .collect();
        ops.sort_by(|a, b| {
            a.invoke_ts
                .cmp(&b.invoke_ts)
                .then(a.client.cmp(&b.client))
                .then(a.seq.cmp(&b.seq))
        });

        ReducedHistory { ops }
    }

    /// Structural well-formedness of the raw event log (test-plan §6.4): a
    /// malformed history is a **harness bug** and must be reported loudly, not
    /// silently massaged away by [`Self::merge_retries`].
    ///
    /// Detects:
    /// * a `Complete` with no matching `Invoke`;
    /// * one `CallId` invoked more than once with differing `(client, seq, op)`;
    /// * one `(client, seq)` carrying differing `op`s across its retries.
    pub fn validate(&self) -> Vec<Failure> {
        let mut failures = Vec::new();

        // Invokes: detect a `CallId` reused for a different call.
        let mut invokes: BTreeMap<CallId, (ClientId, SeqNo, Op)> = BTreeMap::new();
        for event in &self.events {
            if let Event::Invoke {
                call,
                client,
                seq,
                op,
                ..
            } = event
            {
                match invokes.get(call) {
                    Some((c0, s0, o0)) if c0 != client || s0 != seq || o0 != op => {
                        failures.push(Failure {
                            invariant: Invariant::WellFormed,
                            detail: format!(
                                "call {} is invoked more than once with differing (client, seq, op)",
                                call.0
                            ),
                            calls: vec![*call],
                        });
                    }
                    Some(_) => {}
                    None => {
                        invokes.insert(*call, (*client, *seq, op.clone()));
                    }
                }
            }
        }

        // Completes must have a matching invoke.
        for event in &self.events {
            if let Event::Complete { call, .. } = event {
                if !invokes.contains_key(call) {
                    failures.push(Failure {
                        invariant: Invariant::WellFormed,
                        detail: format!("complete for call {} has no matching invoke", call.0),
                        calls: vec![*call],
                    });
                }
            }
        }

        // One `(client, seq)` must carry a single op across its retries.
        let mut group_op: BTreeMap<(ClientId, SeqNo), Op> = BTreeMap::new();
        for event in &self.events {
            if let Event::Invoke {
                client, seq, op, ..
            } = event
            {
                match group_op.get(&(*client, *seq)) {
                    Some(prev) if prev != op => failures.push(Failure {
                        invariant: Invariant::WellFormed,
                        detail: format!(
                            "(client {}, seq {}) carries differing ops across retries",
                            client.0, seq.0
                        ),
                        calls: Vec::new(),
                    }),
                    Some(_) => {}
                    None => {
                        group_op.insert((*client, *seq), op.clone());
                    }
                }
            }
        }

        failures
    }

    /// Run all always-on oracle checks (test-plan §6.4 ①–④, plus structural
    /// well-formedness) and return a typed report. The opt-in checks
    /// ([`Self::check_lost_writes`] / [`Self::check_durability`]) are separate so
    /// they are never silently assumed.
    pub fn check(&self) -> OracleReport {
        let reduced = self.merge_retries();
        let mut failures = self.validate();
        failures.extend(check_phantom(&reduced.ops));
        failures.extend(check_one_log_id(&reduced.ops));
        failures.extend(check_read_your_writes(&reduced.ops));
        failures.extend(check_monotonic_reads(&reduced.ops));
        // Deterministic report order: by invariant, then by the calls involved.
        failures.sort_by(|a, b| {
            a.invariant
                .as_str()
                .cmp(b.invariant.as_str())
                .then(a.calls.cmp(&b.calls))
        });
        OracleReport { failures }
    }

    /// Opt-in check ⑤ (predecessor chain / missing lower bound): every value in
    /// the scenario-provided per-key version chain must have been acked.
    ///
    /// `expected_chain[key]` is the expected ordered list of values for `key`
    /// (oldest → newest), as known by the scenario. This is **scenario-side
    /// input**: the oracle does not infer the chain — the harness supplies the
    /// ground truth it believes was committed. A value in the chain with no
    /// matching acked `Put` is reported as a lost write (a missing lower bound
    /// in the chain).
    pub fn check_lost_writes(&self, expected_chain: &BTreeMap<Vec<u8>, Vec<ValueId>>) -> OracleReport {
        let reduced = self.merge_retries();
        let failures = check_lost_writes(&reduced.ops, expected_chain);
        OracleReport { failures }
    }

    /// Opt-in check ⑥ (post-recovery durability scan): every value in `acked`
    /// (committed before a crash/recovery) must be present in `observed`
    /// (the values seen after recovery). An acked value missing from `observed`
    /// is a durability violation (a committed write was lost across recovery).
    pub fn check_durability(&self, acked: &[ValueId], observed: &[ValueId]) -> OracleReport {
        let failures = check_durability(acked, observed);
        OracleReport { failures }
    }

    /// Render the history as JSONL (test-plan §9): **one object per logical op**
    /// (the reduced/merged history), one per line, with the §9 fields
    /// `{client_id, seq, op, key, val, invoke_ts, complete_ts, result, log_id?}`.
    ///
    /// * `key` is hex-encoded (deterministic, no escaping).
    /// * `val` is the input value (a `Put`'s `ValueId`); `null` otherwise.
    /// * `result` is `ok:<v>` / `ok:null` / `err:<Kind>` / `null` (in-flight).
    /// * `log_id` is the first log id recorded for the op, or `null`.
    pub fn to_jsonl(&self) -> String {
        let reduced = self.merge_retries();
        let mut out = String::new();
        for op in &reduced.ops {
            out.push_str(&jsonl_line(op));
            out.push('\n');
        }
        out
    }
}

/// One logical operation after retry-merge, identified by `(client, seq)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogOp {
    pub client: ClientId,
    pub seq: SeqNo,
    pub op: Op,
    /// Real-time start of the op (the minimum invoke ts across its retries).
    pub invoke_ts: u64,
    /// Real-time completion (the ts at which the client observed a result).
    /// `None` if the op never completed (still in flight or dropped).
    pub complete_ts: Option<u64>,
    /// The result the client observed, if any.
    pub result: Option<OpResult>,
    /// Every log id recorded for this `(client, seq)` (for the one-log-id check).
    pub log_ids: Vec<u64>,
    /// Every physical call id merged into this logical op (for debugging).
    pub calls: Vec<CallId>,
}

impl LogOp {
    /// `true` if this op is a **required** linearization point: it completed
    /// successfully. For a write this means the value was committed; for a read
    /// the observed value must be reproduced by the reference model.
    pub fn is_linearization_point(&self) -> bool {
        self.complete_ts.is_some() && self.result.is_some_and(|r| r.is_ok())
    }

    /// `true` if this op is a logged write (`Put`/`Delete`).
    pub fn is_write_op(&self) -> bool {
        self.op.is_logged_op()
    }

    /// `true` if this op failed in a way that **definitely did not take effect**
    /// (`NotLeader` / `QuorumUnavailable` / `Busy`): it is never a
    /// linearization point.
    pub fn is_known_not_applied(&self) -> bool {
        matches!(self.result, Some(OpResult::Err(k)) if k.is_known_not_applied())
    }

    /// `true` if this op **may** have taken effect: it succeeded, failed with an
    /// *unknown* outcome (propsol N2/N3), or is still in flight.
    pub fn may_have_applied(&self) -> bool {
        !self.is_known_not_applied()
    }

    /// `true` if this op is an **optional** linearization point: a write whose
    /// outcome is unknown (failed with `Timeout`/`SessionExpired`) or that is
    /// still in flight. The checker may either place it (choose that it applied)
    /// or skip it (choose that it never applied).
    pub fn is_optional_write_point(&self) -> bool {
        self.is_write_op() && !self.is_known_not_applied() && !self.is_linearization_point()
    }
}

/// The reduced history: one logical op per `(client, seq)`, in real-time order.
#[derive(Clone, Debug, Default)]
pub struct ReducedHistory {
    pub ops: Vec<LogOp>,
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// The invariant a failure is tagged with (test-plan §6.4 ①–⑥).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Invariant {
    /// ① Phantom value: a successful read of a value that was never (durably)
    /// written.
    PhantomValue,
    /// ② At most one log-id per `(client, seq)` write.
    OneLogIdPerSeq,
    /// ③ Read-your-writes.
    ReadYourWrites,
    /// ④ Monotonic reads.
    MonotonicReads,
    /// ⑤ Lost write / predecessor-chain (missing lower bound).
    LostWrite,
    /// ⑥ Post-recovery durability.
    Durability,
    /// History well-formedness (structural): a `Complete` with no `Invoke`, a
    /// duplicate `CallId`, or one `(client, seq)` carrying differing ops. A
    /// malformed history is a **harness bug**, reported rather than silently
    /// dropped.
    WellFormed,
}

impl Invariant {
    /// A stable, human-readable identifier for report rendering (test-plan §9:
    /// the report must name the invariant so it can be turned into a regression
    /// case).
    pub fn as_str(&self) -> &'static str {
        match self {
            Invariant::PhantomValue => "phantom",
            Invariant::OneLogIdPerSeq => "one-log-id",
            Invariant::ReadYourWrites => "ryw",
            Invariant::MonotonicReads => "monotonic",
            Invariant::LostWrite => "lost-write",
            Invariant::Durability => "durability",
            Invariant::WellFormed => "well-formed",
        }
    }
}

/// A single invariant violation, with a minimal excerpt (test-plan §9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    /// The violated invariant.
    pub invariant: Invariant,
    /// A minimal, human-readable excerpt explaining the violation.
    pub detail: String,
    /// The call ids involved (a pointer back into the record layer for the
    /// minimal counterexample).
    pub calls: Vec<CallId>,
}

/// The result of an oracle run.
#[derive(Clone, Debug, Default)]
pub struct OracleReport {
    pub failures: Vec<Failure>,
}

impl OracleReport {
    /// `true` iff no invariant was violated.
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    /// The set of violated invariants (deduplicated, sorted).
    pub fn violated(&self) -> Vec<Invariant> {
        let set: BTreeSet<Invariant> = self.failures.iter().map(|f| f.invariant).collect();
        let mut v: Vec<Invariant> = set.iter().copied().collect();
        v.sort_by_key(|i| i.as_str());
        v
    }

    /// Render the report in the test-plan §9 shape:
    /// `[<invariant>] <detail>` per failure.
    pub fn render(&self) -> String {
        if self.passed() {
            return "oracle: PASS (no invariant violations)".to_string();
        }
        let mut out = String::new();
        for f in &self.failures {
            let calls: Vec<String> = f.calls.iter().map(|c| c.0.to_string()).collect();
            out.push_str(&format!(
                "[{}] {} (calls: {})\n",
                f.invariant.as_str(),
                f.detail,
                calls.join(", ")
            ));
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Check ① — Phantom value
// ---------------------------------------------------------------------------

fn check_phantom(ops: &[LogOp]) -> Vec<Failure> {
    let mut failures = Vec::new();

    for get in ops {
        // Only successful `Get(k) -> Some(v)`.
        let (key, value) = match (&get.op, &get.result) {
            (Op::Get { key }, Some(OpResult::Ok(Some(value)))) => (key.clone(), *value),
            _ => continue,
        };
        let Some(get_complete) = get.complete_ts else {
            continue;
        };

        // A candidate is a `Put(key, value)` that MAY have applied and whose
        // interval permits a put-before-read linearization: its invoke precedes
        // the read's completion (`p.invoke_ts < get.complete_ts`). Requiring the
        // put to be *fully completed* before the read begins (`rt_before`) would
        // be too strict — a read may legally observe a concurrent writer that
        // completed during the read's interval.
        let candidates: Vec<&LogOp> = ops
            .iter()
            .filter(|p| {
                matches!(&p.op, Op::Put { value: v, .. } if *v == value)
                    && p.op.key() == &key
                    && p.may_have_applied()
                    && p.invoke_ts < get_complete
            })
            .collect();

        if candidates.is_empty() {
            failures.push(Failure {
                invariant: Invariant::PhantomValue,
                detail: format!(
                    "client {} seq {} read Some({}) for key {:?}, but no Put of it could have preceded the read",
                    get.client.0,
                    get.seq.0,
                    value.0,
                    key
                ),
                calls: get.calls.clone(),
            });
            continue;
        }

        // A candidate explains the read unless a Delete of the key that
        // DEFINITELY applied *necessarily* sits between the put and the read:
        // `put.complete <= delete.invoke` and `delete.complete <= read.invoke`.
        // Only a definitely-applied (successful) delete invalidates; a delete
        // that merely *might* have applied is left to the linearizability
        // checker, keeping this oracle permissive (no false positives).
        let explained = candidates.iter().any(|p| {
            !ops.iter().any(|d| {
                matches!(&d.op, Op::Delete { .. })
                    && d.op.key() == &key
                    && d.is_linearization_point()
                    && p.complete_ts.is_some_and(|pc| pc <= d.invoke_ts)
                    && d.complete_ts.is_some_and(|dc| dc <= get.invoke_ts)
            })
        });

        if !explained {
            failures.push(Failure {
                invariant: Invariant::PhantomValue,
                detail: format!(
                    "client {} seq {} read Some({}) for key {:?}, but every earlier Put of it was deleted before the read",
                    get.client.0,
                    get.seq.0,
                    value.0,
                    key
                ),
                calls: get.calls.clone(),
            });
        }
    }

    failures
}

// ---------------------------------------------------------------------------
// Check ② — At most one log-id per seq
// ---------------------------------------------------------------------------

fn check_one_log_id(ops: &[LogOp]) -> Vec<Failure> {
    let mut failures = Vec::new();

    for op in ops {
        if !op.op.is_logged_op() {
            continue;
        }
        // Distinct log ids for this `(client, seq)` logged op (Put or Delete).
        let distinct: BTreeSet<u64> = op.log_ids.iter().copied().collect();
        if distinct.len() <= 1 {
            continue;
        }
        let ids: Vec<u64> = distinct.iter().copied().collect();
        failures.push(Failure {
            invariant: Invariant::OneLogIdPerSeq,
            detail: format!(
                "client {} seq {} logged op carries {} distinct log ids ({})",
                op.client.0,
                op.seq.0,
                ids.len(),
                ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ")
            ),
            calls: op.calls.clone(),
        });
    }

    failures
}

// ---------------------------------------------------------------------------
// Check ③ — Read-your-writes
// ---------------------------------------------------------------------------

fn check_read_your_writes(ops: &[LogOp]) -> Vec<Failure> {
    let mut failures = Vec::new();

    for get in ops {
        // Only a successful `Get(k) -> ret` (ret may be `None`, an absent key).
        let (key, ret) = match (&get.op, &get.result) {
            (Op::Get { key }, Some(OpResult::Ok(ret))) => (key.clone(), *ret),
            _ => continue,
        };

        // The client's LATEST same-key mutation that definitely applied and
        // completed before this read was invoked. Clients are single-sequencer,
        // so this is well-defined. Latest-mutation-wins: an older own Put is
        // superseded by a later own Put or Delete.
        let latest = ops
            .iter()
            .filter(|m| {
                m.client == get.client
                    && m.op.key() == &key
                    && m.op.is_logged_op()
                    && m.is_linearization_point()
                    && m.complete_ts.is_some_and(|mc| mc <= get.invoke_ts)
            })
            .max_by_key(|m| (m.complete_ts.unwrap_or(0), m.seq.0));

        let Some(mutation) = latest else {
            continue;
        };
        let value = match &mutation.op {
            Op::Put { value, .. } => *value,
            // The latest own mutation was a Delete: it constrains nothing. The
            // read may legitimately be `None`, or `Some(other)` from another
            // client's concurrent re-put.
            Op::Delete { .. } => continue,
            Op::Get { .. } => continue,
        };

        // The read must return the written value or a newer one.
        if ret.is_some_and(|w| w >= value) {
            continue;
        }
        let returned = match ret {
            Some(w) => format!("Some({})", w.0),
            None => "None".to_string(),
        };
        failures.push(Failure {
            invariant: Invariant::ReadYourWrites,
            detail: format!(
                "client {} seq {} read {} for key {:?} after its own latest Put of {} completed",
                get.client.0,
                get.seq.0,
                returned,
                key,
                value.0
            ),
            calls: [mutation.calls.clone(), get.calls.clone()].concat(),
        });
    }

    failures
}

// ---------------------------------------------------------------------------
// Check ④ — Monotonic reads
// ---------------------------------------------------------------------------

fn check_monotonic_reads(ops: &[LogOp]) -> Vec<Failure> {
    let mut failures = Vec::new();

    // Per `(client, key)`, the successful `Get -> Some(v)` reads.
    let mut by_client_key: BTreeMap<(ClientId, Vec<u8>), Vec<&LogOp>> = BTreeMap::new();
    for get in ops {
        let Some(OpResult::Ok(Some(_))) = &get.result else {
            continue;
        };
        let key = get.op.key().clone();
        by_client_key
            .entry((get.client, key))
            .or_default()
            .push(get);
    }

    for ((client, key), reads) in by_client_key.iter() {
        // Real-time order (deterministic tie-breaks).
        let mut ordered: Vec<&LogOp> = reads.clone();
        ordered.sort_by(|a, b| {
            a.complete_ts
                .cmp(&b.complete_ts)
                .then(a.invoke_ts.cmp(&b.invoke_ts))
                .then(a.seq.cmp(&b.seq))
        });
        // Successive values must not regress (the ValueId-as-version assumption).
        for pair in ordered.windows(2) {
            let (prev, cur) = (&pair[0], &pair[1]);
            let (Some(OpResult::Ok(Some(prev_v))), Some(OpResult::Ok(Some(cur_v)))) =
                (&prev.result, &cur.result)
            else {
                continue;
            };
            if cur_v < prev_v {
                failures.push(Failure {
                    invariant: Invariant::MonotonicReads,
                    detail: format!(
                        "client {} read key {:?} as {} and later as {} (regression)",
                        client.0,
                        key,
                        prev_v.0,
                        cur_v.0
                    ),
                    calls: [prev.calls.clone(), cur.calls.clone()].concat(),
                });
            }
        }
    }

    failures
}

// ---------------------------------------------------------------------------
// Check ⑤ — Lost write (opt-in, requires the scenario's version chain)
// ---------------------------------------------------------------------------

fn check_lost_writes(ops: &[LogOp], expected_chain: &BTreeMap<Vec<u8>, Vec<ValueId>>) -> Vec<Failure> {
    let mut failures = Vec::new();

    for (key, chain) in expected_chain.iter() {
        // The set of values actually acked for this key.
        let acked: BTreeSet<ValueId> = ops
            .iter()
            .filter(|o| {
                o.op.is_put()
                    && o.op.key() == key
                    && o.result.is_some_and(|r| r.is_ok())
            })
            .filter_map(|o| match &o.op {
                Op::Put { value, .. } => Some(*value),
                _ => None,
            })
            .collect();

        for value in chain.iter() {
            if acked.contains(value) {
                continue;
            }
            failures.push(Failure {
                invariant: Invariant::LostWrite,
                detail: format!(
                    "value {} for key {:?} is in the expected version chain but was never acked",
                    value.0, key
                ),
                calls: Vec::new(),
            });
        }
    }

    failures
}

// ---------------------------------------------------------------------------
// Check ⑥ — Durability (opt-in)
// ---------------------------------------------------------------------------

fn check_durability(acked: &[ValueId], observed: &[ValueId]) -> Vec<Failure> {
    let observed_set: BTreeSet<ValueId> = observed.iter().copied().collect();
    let missing: Vec<ValueId> = acked
        .iter()
        .filter(|a| !observed_set.contains(a))
        .copied()
        .collect();
    if missing.is_empty() {
        return Vec::new();
    }
    let ids: Vec<String> = missing.iter().map(|v| v.0.to_string()).collect();
    vec![Failure {
        invariant: Invariant::Durability,
        detail: format!(
            "{} acked value(s) not present in the observed set ({})",
            missing.len(),
            ids.join(", ")
        ),
        calls: Vec::new(),
    }]
}

// ---------------------------------------------------------------------------
// JSONL rendering
// ---------------------------------------------------------------------------

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX_DIGITS[(b >> 4) as usize] as char);
        s.push(HEX_DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

fn op_name(op: &Op) -> &'static str {
    match op {
        Op::Put { .. } => "put",
        Op::Delete { .. } => "delete",
        Op::Get { .. } => "get",
    }
}

fn val_field(op: &Op) -> String {
    match op {
        Op::Put { value, .. } => value.0.to_string(),
        _ => "null".to_string(),
    }
}

fn result_field(result: &Option<OpResult>) -> String {
    match result {
        Some(OpResult::Ok(Some(v))) => format!("ok:{}", v.0),
        Some(OpResult::Ok(None)) => "ok:null".to_string(),
        Some(OpResult::Err(kind)) => format!("err:{}", kind.as_str()),
        None => "null".to_string(),
    }
}

fn jsonl_line(op: &LogOp) -> String {
    let key = hex(op.op.key());
    let val = val_field(&op.op);
    let complete_ts = op.complete_ts.map_or_else(|| "null".to_string(), |t| t.to_string());
    let result = result_field(&op.result);
    let log_id = op
        .log_ids
        .first()
        .map_or_else(|| "null".to_string(), |id| id.to_string());
    format!(
        "{{\"client_id\":{},\"seq\":{},\"op\":\"{}\",\"key\":\"{}\",\"val\":{},\"invoke_ts\":{},\"complete_ts\":{},\"result\":\"{}\",\"log_id\":{}}}",
        op.client.0, op.seq.0, op_name(&op.op), key, val, op.invoke_ts, complete_ts, result, log_id
    )
}

/// A builder group accumulating the raw events of one `(client, seq)`.
#[derive(Default)]
struct Group {
    op: Option<Op>,
    invoke_ts: Option<u64>,
    calls: Vec<CallId>,
    completes: Vec<(u64, OpResult)>,
    log_ids: Vec<u64>,
}

impl Group {
    fn into_log_op(self, client: ClientId, seq: SeqNo) -> Option<LogOp> {
        // A well-formed group always has the op from its invoke; a group without
        // one is malformed and reported by `History::validate`.
        let op = self.op?;
        // The op the client observed: the first successful completion, else the
        // last completion (an error), else none (in-flight).
        let (result, complete_ts) = match self
            .completes
            .iter()
            .find(|(_, r)| r.is_ok())
            .or_else(|| self.completes.last())
        {
            Some((ts, result)) => (Some(result.clone()), Some(*ts)),
            None => (None, None),
        };

        Some(LogOp {
            client,
            seq,
            op,
            invoke_ts: self.invoke_ts.unwrap_or(0),
            complete_ts,
            result,
            log_ids: self.log_ids,
            calls: self.calls,
        })
    }
}

// ---------------------------------------------------------------------------
// Meta-tests (TDD): one good history passes; one bad history per check fails
// with the right invariant tag.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A single successful write+read round trip that satisfies every oracle
    /// invariant.
    fn good_history() -> History {
        let mut h = History::new();
        // client 0: Put(k, v=1) at [0, 10], then Get(k) -> Some(1) at [20, 30].
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete_logged(CallId(1), 10, OpResult::Ok(None), 100);
        h.invoke(CallId(2), ClientId(0), SeqNo(1), Op::Get { key: b"k".to_vec() }, 20)
            .complete(CallId(2), 30, OpResult::Ok(Some(ValueId(1))));
        h
    }

    #[test]
    fn good_history_passes_all_checks() {
        let h = good_history();
        let report = h.check();
        assert!(
            report.passed(),
            "expected a clean history to pass, got: {}",
            report.render()
        );
    }

    // -- ① phantom -----------------------------------------------------------

    #[test]
    fn phantom_read_of_never_written_value_fails() {
        let mut h = History::new();
        // A successful read of a value that was never Put.
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Get { key: b"k".to_vec() }, 0)
            .complete(CallId(1), 10, OpResult::Ok(Some(ValueId(7))));
        let report = h.check();
        assert!(
            report.violated().contains(&Invariant::PhantomValue),
            "expected phantom, got: {}",
            report.render()
        );
    }

    #[test]
    fn phantom_read_of_deleted_value_fails() {
        let mut h = History::new();
        // Put(k,1) completes, Delete(k) completes, then Get(k) -> Some(1): the
        // value was deleted before the read.
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(CallId(1), 5, OpResult::Ok(None));
        h.invoke(CallId(2), ClientId(1), SeqNo(0), Op::Delete { key: b"k".to_vec() }, 6)
            .complete(CallId(2), 8, OpResult::Ok(None));
        h.invoke(CallId(3), ClientId(2), SeqNo(0), Op::Get { key: b"k".to_vec() }, 9)
            .complete(CallId(3), 12, OpResult::Ok(Some(ValueId(1))));
        let report = h.check();
        assert!(
            report.violated().contains(&Invariant::PhantomValue),
            "expected phantom (deleted value), got: {}",
            report.render()
        );
    }

    #[test]
    fn re_put_after_delete_is_not_phantom() {
        let mut h = History::new();
        // Put(k,1) -> Delete(k) -> Put(k,1) -> Get(k) -> Some(1): the second
        // Put re-establishes the value, so the read is explained.
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(CallId(1), 2, OpResult::Ok(None));
        h.invoke(CallId(2), ClientId(0), SeqNo(1), Op::Delete { key: b"k".to_vec() }, 3)
            .complete(CallId(2), 4, OpResult::Ok(None));
        h.invoke(CallId(3), ClientId(0), SeqNo(2), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 5)
            .complete(CallId(3), 6, OpResult::Ok(None));
        h.invoke(CallId(4), ClientId(0), SeqNo(3), Op::Get { key: b"k".to_vec() }, 7)
            .complete(CallId(4), 8, OpResult::Ok(Some(ValueId(1))));
        let report = h.check();
        assert!(
            !report.violated().contains(&Invariant::PhantomValue),
            "expected no phantom, got: {}",
            report.render()
        );
    }

    // -- ② one log-id per seq ------------------------------------------------

    #[test]
    fn two_log_ids_for_one_seq_fails() {
        let mut h = History::new();
        // A retry storm: the same (client, seq) write is acked twice with two
        // different log ids.
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete_logged(CallId(1), 10, OpResult::Ok(None), 100);
        h.invoke(CallId(2), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 11)
            .complete_logged(CallId(2), 20, OpResult::Ok(None), 200);
        let report = h.check();
        assert!(
            report.violated().contains(&Invariant::OneLogIdPerSeq),
            "expected one-log-id violation, got: {}",
            report.render()
        );
    }

    #[test]
    fn same_log_id_for_one_seq_is_fine() {
        let mut h = History::new();
        // A retry storm where both acks carry the same log id (dedup worked).
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete_logged(CallId(1), 10, OpResult::Ok(None), 100);
        h.invoke(CallId(2), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 11)
            .complete_logged(CallId(2), 20, OpResult::Ok(None), 100);
        let report = h.check();
        assert!(
            !report.violated().contains(&Invariant::OneLogIdPerSeq),
            "expected no one-log-id violation, got: {}",
            report.render()
        );
    }

    // -- ③ read-your-writes --------------------------------------------------

    #[test]
    fn ryw_read_of_older_value_after_own_write_fails() {
        let mut h = History::new();
        // client 0 writes k=5, then (same client) reads k and gets an older
        // value 3.
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(5) }, 0)
            .complete(CallId(1), 10, OpResult::Ok(None));
        h.invoke(CallId(2), ClientId(0), SeqNo(1), Op::Get { key: b"k".to_vec() }, 20)
            .complete(CallId(2), 30, OpResult::Ok(Some(ValueId(3))));
        let report = h.check();
        assert!(
            report.violated().contains(&Invariant::ReadYourWrites),
            "expected RYW violation, got: {}",
            report.render()
        );
    }

    #[test]
    fn ryw_sees_own_write_is_fine() {
        let mut h = History::new();
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(5) }, 0)
            .complete(CallId(1), 10, OpResult::Ok(None));
        h.invoke(CallId(2), ClientId(0), SeqNo(1), Op::Get { key: b"k".to_vec() }, 20)
            .complete(CallId(2), 30, OpResult::Ok(Some(ValueId(5))));
        let report = h.check();
        assert!(
            !report.violated().contains(&Invariant::ReadYourWrites),
            "expected no RYW violation, got: {}",
            report.render()
        );
    }

    // -- ④ monotonic reads ---------------------------------------------------

    #[test]
    fn non_monotonic_read_fails() {
        let mut h = History::new();
        // client 0 reads k=5 then later k=3 (a regression).
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Get { key: b"k".to_vec() }, 0)
            .complete(CallId(1), 10, OpResult::Ok(Some(ValueId(5))));
        h.invoke(CallId(2), ClientId(0), SeqNo(1), Op::Get { key: b"k".to_vec() }, 20)
            .complete(CallId(2), 30, OpResult::Ok(Some(ValueId(3))));
        let report = h.check();
        assert!(
            report.violated().contains(&Invariant::MonotonicReads),
            "expected monotonic violation, got: {}",
            report.render()
        );
    }

    #[test]
    fn monotonic_reads_are_fine() {
        let mut h = History::new();
        // client 0 reads k=3 then k=5 (non-regressing).
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Get { key: b"k".to_vec() }, 0)
            .complete(CallId(1), 10, OpResult::Ok(Some(ValueId(3))));
        h.invoke(CallId(2), ClientId(0), SeqNo(1), Op::Get { key: b"k".to_vec() }, 20)
            .complete(CallId(2), 30, OpResult::Ok(Some(ValueId(5))));
        let report = h.check();
        assert!(
            !report.violated().contains(&Invariant::MonotonicReads),
            "expected no monotonic violation, got: {}",
            report.render()
        );
    }

    // -- ⑤ lost write (opt-in) -----------------------------------------------

    #[test]
    fn missing_acked_value_in_chain_fails() {
        let mut h = History::new();
        // The scenario expects k to have values 1 and 2, but only 1 was acked.
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(CallId(1), 10, OpResult::Ok(None));
        let mut expected = BTreeMap::new();
        expected.insert(b"k".to_vec(), vec![ValueId(1), ValueId(2)]);
        let report = h.check_lost_writes(&expected);
        assert!(
            report.violated().contains(&Invariant::LostWrite),
            "expected lost-write violation, got: {}",
            report.render()
        );
    }

    #[test]
    fn full_chain_passes_lost_write() {
        let mut h = History::new();
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(CallId(1), 10, OpResult::Ok(None));
        h.invoke(CallId(2), ClientId(0), SeqNo(1), Op::Put { key: b"k".to_vec(), value: ValueId(2) }, 20)
            .complete(CallId(2), 30, OpResult::Ok(None));
        let mut expected = BTreeMap::new();
        expected.insert(b"k".to_vec(), vec![ValueId(1), ValueId(2)]);
        let report = h.check_lost_writes(&expected);
        assert!(
            report.passed(),
            "expected no lost-write violation, got: {}",
            report.render()
        );
    }

    // -- ⑥ durability (opt-in) -----------------------------------------------

    #[test]
    fn missing_acked_write_after_recovery_fails() {
        let h = History::new();
        // acked {1, 2, 3}, observed {1, 3}: value 2 was lost across recovery.
        let report = h.check_durability(
            &[ValueId(1), ValueId(2), ValueId(3)],
            &[ValueId(1), ValueId(3)],
        );
        assert!(
            report.violated().contains(&Invariant::Durability),
            "expected durability violation, got: {}",
            report.render()
        );
    }

    #[test]
    fn all_acked_writes_present_passes_durability() {
        let h = History::new();
        let report = h.check_durability(
            &[ValueId(1), ValueId(2)],
            &[ValueId(1), ValueId(2), ValueId(3)],
        );
        assert!(
            report.passed(),
            "expected durability pass, got: {}",
            report.render()
        );
    }

    // -- merge_retries -------------------------------------------------------

    #[test]
    fn merge_retries_collapses_repeated_seq() {
        let mut h = History::new();
        // A retry storm: the same (client, seq) written twice.
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(CallId(1), 10, OpResult::Err(OracleErrorKind::Timeout));
        h.invoke(CallId(2), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 11)
            .complete_logged(CallId(2), 20, OpResult::Ok(None), 100);
        let reduced = h.merge_retries();
        assert_eq!(reduced.ops.len(), 1, "retries should collapse to one logical op");
        let op = &reduced.ops[0];
        assert_eq!(op.calls, vec![CallId(1), CallId(2)]);
        assert_eq!(op.invoke_ts, 0, "the logical op starts at the first retry");
        assert_eq!(op.complete_ts, Some(20), "the logical op ends at the successful ack");
        assert_eq!(op.result, Some(OpResult::Ok(None)));
        assert_eq!(op.log_ids, vec![100]);
    }

    // -- to_jsonl ------------------------------------------------------------

    #[test]
    fn to_jsonl_emits_one_object_per_logical_op() {
        let h = good_history();
        let jsonl = h.to_jsonl();
        let lines: Vec<&str> = jsonl.lines().collect();
        assert_eq!(lines.len(), 2, "one line per logical op");
        // The Put line carries its value and log id.
        assert!(
            lines[0].contains("\"op\":\"put\"") && lines[0].contains("\"val\":1") && lines[0].contains("\"log_id\":100"),
            "unexpected put line: {}",
            lines[0]
        );
        // The Get line carries the read result.
        assert!(
            lines[1].contains("\"op\":\"get\"") && lines[1].contains("\"result\":\"ok:1\""),
            "unexpected get line: {}",
            lines[1]
        );
    }

    // -- report shape --------------------------------------------------------

    #[test]
    fn report_render_names_the_invariant() {
        let mut h = History::new();
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Get { key: b"k".to_vec() }, 0)
            .complete(CallId(1), 10, OpResult::Ok(Some(ValueId(7))));
        let report = h.check();
        let rendered = report.render();
        assert!(
            rendered.contains("[phantom]"),
            "the report must name the invariant: {}",
            rendered
        );
    }

    // -- B1/B2 regressions (legal histories must NOT be flagged) -------------

    #[test]
    fn phantom_allows_concurrent_put_and_read() {
        let mut h = History::new();
        // Put(k,5) cA [0,10]; Get(k)->Some(5) cB [5,15] — overlapping intervals.
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(5) }, 0)
            .complete(CallId(1), 10, OpResult::Ok(None));
        h.invoke(CallId(2), ClientId(1), SeqNo(0), Op::Get { key: b"k".to_vec() }, 5)
            .complete(CallId(2), 15, OpResult::Ok(Some(ValueId(5))));
        let report = h.check();
        assert!(
            !report.violated().contains(&Invariant::PhantomValue),
            "a concurrent put/read is legal: {}",
            report.render()
        );
    }

    #[test]
    fn ryw_after_own_delete_is_fine() {
        let mut h = History::new();
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(5) }, 0)
            .complete(CallId(1), 5, OpResult::Ok(None));
        h.invoke(CallId(2), ClientId(0), SeqNo(1), Op::Delete { key: b"k".to_vec() }, 6)
            .complete(CallId(2), 8, OpResult::Ok(None));
        h.invoke(CallId(3), ClientId(0), SeqNo(2), Op::Get { key: b"k".to_vec() }, 9)
            .complete(CallId(3), 12, OpResult::Ok(None));
        let report = h.check();
        assert!(
            !report.violated().contains(&Invariant::ReadYourWrites),
            "the client's own delete removes the RYW constraint: {}",
            report.render()
        );
    }

    #[test]
    fn ryw_after_own_overwrite_is_fine() {
        let mut h = History::new();
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(3) }, 0)
            .complete(CallId(1), 5, OpResult::Ok(None));
        h.invoke(CallId(2), ClientId(0), SeqNo(1), Op::Put { key: b"k".to_vec(), value: ValueId(5) }, 6)
            .complete(CallId(2), 8, OpResult::Ok(None));
        h.invoke(CallId(3), ClientId(0), SeqNo(2), Op::Get { key: b"k".to_vec() }, 9)
            .complete(CallId(3), 12, OpResult::Ok(Some(ValueId(5))));
        let report = h.check();
        assert!(
            !report.violated().contains(&Invariant::ReadYourWrites),
            "the latest own write wins: {}",
            report.render()
        );
    }

    // -- S1 regression: Delete is a logged op (one-log-id applies) -----------

    #[test]
    fn delete_with_two_log_ids_for_one_seq_fails() {
        let mut h = History::new();
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Delete { key: b"k".to_vec() }, 0)
            .complete_logged(CallId(1), 10, OpResult::Ok(None), 100);
        h.invoke(CallId(2), ClientId(0), SeqNo(0), Op::Delete { key: b"k".to_vec() }, 11)
            .complete_logged(CallId(2), 20, OpResult::Ok(None), 200);
        let report = h.check();
        assert!(
            report.violated().contains(&Invariant::OneLogIdPerSeq),
            "a delete retried into two log ids violates one-log-id: {}",
            report.render()
        );
    }

    // -- S5 regressions: malformed history is loud ---------------------------

    #[test]
    fn validate_reports_complete_without_invoke() {
        let mut h = History::new();
        h.complete(CallId(9), 1, OpResult::Ok(None));
        let report = h.check();
        assert!(
            report.violated().contains(&Invariant::WellFormed),
            "a complete without an invoke is malformed: {}",
            report.render()
        );
    }

    #[test]
    fn validate_reports_differing_ops_for_one_seq() {
        let mut h = History::new();
        h.invoke(CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(CallId(1), 5, OpResult::Ok(None));
        h.invoke(CallId(2), ClientId(0), SeqNo(0), Op::Get { key: b"k".to_vec() }, 6)
            .complete(CallId(2), 8, OpResult::Ok(Some(ValueId(1))));
        let report = h.check();
        assert!(
            report.violated().contains(&Invariant::WellFormed),
            "one (client, seq) with differing ops is malformed: {}",
            report.render()
        );
    }
}
