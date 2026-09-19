//! A self-built **complete** linearizability checker (test-plan §4 T2).
//!
//! Given a reduced client history ([`crate::oracle::ReducedHistory`]) and a KV
//! reference model (an initial state plus `Put`/`Delete`/`Get` semantics over
//! [`ValueId`]), this module decides whether the history is **linearizable**:
//! is there a total order of the successful operations that (a) respects the
//! real-time (happens-before) order and (b) is a valid sequential execution of
//! the reference model?
//!
//! # Approach (a clearly-correct equivalent of Wing–Gong)
//!
//! Wing–Gong ("Verifying Linearizability with Constraint Lattices") searches
//! for a total order extending the real-time partial order that is consistent
//! with the reference semantics, memoizing over the constraint lattice. This
//! module implements the **same search problem** as a backtracking depth-first
//! search over the **linear extensions** of the real-time poset, with **semantic
//! pruning**: an operation is only placed when the reference model, applied at
//! that point, yields exactly the operation's observed result.
//!
//! * **Correctness.** The search enumerates *every* linear extension of the
//!   real-time poset that survives semantic pruning. It returns
//!   [`CheckOutcome::Linearizable`] iff at least one such extension is a valid
//!   sequential execution; otherwise [`CheckOutcome::Violation`]. Because a
//!   history is linearizable exactly when some linear extension is a valid
//!   execution, the verdict is complete (decides correctly), not a heuristic.
//! * **Complexity.** Worst case `O(n!)` in the number of linearization points
//!   `n` (the number of linear extensions). In practice the pruning is strong:
//!   a `Get` may only be placed where the model holds the value it returned,
//!   which eliminates most orderings. To bound the worst case, histories with
//!   more than [`MAX_CHECK_POINTS`] points return [`CheckOutcome::Inconclusive`]
//!   rather than risking a hang; the checker is meant to run on **reduced
//!   histories** (test-plan §6.4). Constraint-lattice memoization (Wing–Gong's
//!   optimization) is a documented future improvement.
//! * **Determinism.** Operations are iterated in a fixed order (the reduced
//!   history's `(invoke_ts, client, seq)` order), so the same input always
//!   yields the same verdict and the same [`CheckOutcome::Violation`] witness.
//!   There is no `HashMap` iteration anywhere in the check path (`BTreeMap` /
//!   `Vec` only).
//!
//! # Scope
//!
//! Successful operations are **required** linearization points. A **write**
//! whose outcome is *unknown* (`Timeout`/`SessionExpired`, propsol N2/N3) or that
//! is still in flight is an **optional** point: the search may place it (choose
//! that it applied) or skip it. A write that definitely did not apply
//! (`NotLeader`/`QuorumUnavailable`/`Busy`) and any non-successful read are
//! excluded. The `ValueId` is treated as an **opaque value** here (no
//! version-order assumption — that belongs to the oracle's monotonic /
//! read-your-writes checks).
//!
//! # Cross-validation
//!
//! The `#[cfg(test)]` module cross-validates this checker against
//! [`stateright`](https://crates.io/crates/stateright)'s
//! `LinearizabilityTester` (a dev-dependency, decision D-T2) on a battery of
//! small histories. If the two ever disagree, a checker bug is suspected.

use std::collections::BTreeMap;

use crate::oracle::{CallId, History, LogOp, Op, OpResult, ValueId};

// ---------------------------------------------------------------------------
// Reference KV model
// ---------------------------------------------------------------------------

/// The KV reference state: a map from key to current [`ValueId`].
///
/// This is the "initial state + semantics" the checker runs against. [`ValueId`]
/// is opaque: the model does not assume any ordering on it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvState {
    map: BTreeMap<Vec<u8>, ValueId>,
}

impl KvState {
    /// An empty store.
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    /// A store seeded with the given `(key, value)` entries (the initial state).
    pub fn with_entries(entries: &[(Vec<u8>, ValueId)]) -> Self {
        let mut map = BTreeMap::new();
        for (key, value) in entries {
            map.insert(key.clone(), *value);
        }
        Self { map }
    }

    /// The current value of `key`, if present.
    pub fn get(&self, key: &[u8]) -> Option<ValueId> {
        self.map.get(key).copied()
    }

    /// Apply `op` to a *copy* of this state, returning the new state and the
    /// result the reference model would produce. Pure (no mutation of `self`).
    fn apply(&self, op: &Op) -> (Self, OpResult) {
        let mut next = self.clone();
        let result = match op {
            Op::Put { key, value } => {
                next.map.insert(key.clone(), *value);
                OpResult::Ok(None)
            }
            Op::Delete { key } => {
                next.map.remove(key);
                OpResult::Ok(None)
            }
            Op::Get { key } => OpResult::Ok(next.map.get(key).copied()),
        };
        (next, result)
    }
}

// ---------------------------------------------------------------------------
// Outcome
// ---------------------------------------------------------------------------

/// The verdict of a linearizability check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckOutcome {
    /// The history is linearizable.
    Linearizable,
    /// The history is not linearizable. `witness` is a **greedily minimized**
    /// counterexample: the call ids of a subset of the operations whose
    /// sub-history is still non-linearizable (test-plan §9 "minimal counter-
    /// example excerpt").
    Violation { witness: Vec<CallId> },
    /// The history has more linearization points than the exhaustive search will
    /// run on (see [`MAX_CHECK_POINTS`]); the verdict is unknown. This bounds the
    /// worst-case factorial search so a large history fails fast instead of
    /// hanging.
    Inconclusive { points: usize },
}

/// The maximum number of linearization points the exhaustive search will run on.
/// Histories with more points return [`CheckOutcome::Inconclusive`] rather than
/// risking the worst-case `O(n!)`. (Wing–Gong constraint-lattice memoization,
/// which removes this limit, is a documented future improvement; M1 runs the
/// checker on small reduced histories.)
pub const MAX_CHECK_POINTS: usize = 12;

/// Whether a linearization point is required or optional.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PointKind {
    /// A successful op: it must be placed, and the reference model must
    /// reproduce its observed result.
    Required,
    /// A write with an unknown outcome (`Timeout`/`SessionExpired`) or still in
    /// flight: it may be placed (choose that it applied — result-match waived) or
    /// skipped (choose that it never applied).
    Optional,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Check a raw [`History`] for linearizability against `initial`.
///
/// The history is first reduced (retries merged) and then checked. This is the
/// convenience entry point; [`check_reduced`] is the lower-level form that
/// operates on an already-reduced set of logical ops.
pub fn check_linearizable(history: &History, initial: &KvState) -> CheckOutcome {
    let reduced = history.merge_retries();
    check_reduced(&reduced.ops, initial)
}

/// Check a set of logical ops (a reduced history) for linearizability against
/// `initial`.
pub fn check_reduced(ops: &[LogOp], initial: &KvState) -> CheckOutcome {
    // Linearization points: required (successful) ops, plus optional writes
    // whose outcome is unknown or in flight (propsol N2/N3). A read that did not
    // succeed is dropped; a write that definitely did not apply is dropped.
    let points: Vec<usize> = (0..ops.len())
        .filter(|&i| ops[i].is_linearization_point() || ops[i].is_optional_write_point())
        .collect();
    if points.is_empty() {
        return CheckOutcome::Linearizable;
    }
    if points.len() > MAX_CHECK_POINTS {
        return CheckOutcome::Inconclusive {
            points: points.len(),
        };
    }
    let kinds: Vec<PointKind> = points
        .iter()
        .map(|&i| {
            if ops[i].is_linearization_point() {
                PointKind::Required
            } else {
                PointKind::Optional
            }
        })
        .collect();

    if search(ops, &points, &kinds, initial) {
        CheckOutcome::Linearizable
    } else {
        let witness = shrink_witness(ops, initial, &points);
        CheckOutcome::Violation { witness }
    }
}

// ---------------------------------------------------------------------------
// The search
// ---------------------------------------------------------------------------

/// Decide whether some linear extension of the real-time poset over `points` is
/// a valid sequential execution of the reference model starting from `initial`.
///
/// `kinds[i]` says whether `points[i]` is a required point (placed and
/// result-checked) or an optional write point (placed with the result-match
/// waived, or skipped entirely).
fn search(ops: &[LogOp], points: &[usize], kinds: &[PointKind], initial: &KvState) -> bool {
    let n = points.len();

    // Real-time predecessor/successor relations over the linearization points.
    // `ops[points[a]]` is real-time before `ops[points[b]]` iff
    // `ops[points[a]].complete_ts <= ops[points[b]].invoke_ts`.
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut remaining_pred: Vec<usize> = vec![0; n];
    for a in 0..n {
        for b in (a + 1)..n {
            let a_before_b = ops[points[a]]
                .complete_ts
                .is_some_and(|ca| ca <= ops[points[b]].invoke_ts);
            let b_before_a = ops[points[b]]
                .complete_ts
                .is_some_and(|cb| cb <= ops[points[a]].invoke_ts);
            if a_before_b && !b_before_a {
                successors[a].push(b);
                remaining_pred[b] += 1;
            } else if b_before_a && !a_before_b {
                successors[b].push(a);
                remaining_pred[a] += 1;
            }
            // If neither direction holds the pair is concurrent and imposes no
            // ordering.
        }
    }

    let mut resolved = vec![false; n];
    dfs(
        ops,
        points,
        kinds,
        &mut resolved,
        &mut remaining_pred,
        initial,
        0,
        &successors,
    )
}

/// Backtracking DFS over linear extensions, with semantic pruning and optional
/// points. Returns `true` iff a valid resolution (every point placed or, for an
/// optional point, skipped) exists. `count` is the number of resolved points.
fn dfs(
    ops: &[LogOp],
    points: &[usize],
    kinds: &[PointKind],
    resolved: &mut [bool],
    remaining_pred: &mut [usize],
    state: &KvState,
    count: usize,
    successors: &[Vec<usize>],
) -> bool {
    let n = points.len();
    // Base case: every point has been resolved (placed or skipped).
    if count == n {
        return true;
    }

    for i in 0..n {
        // Skip already-resolved points and points whose real-time predecessors
        // are not all resolved yet.
        if resolved[i] || remaining_pred[i] != 0 {
            continue;
        }
        let point = &ops[points[i]];
        let required = kinds[i] == PointKind::Required;

        // Branch 1: PLACE the op. For a required op the reference model must
        // reproduce its observed result; an optional op is placed with the
        // result-match waived (we choose that it applied).
        let (next_state, expected) = state.apply(&point.op);
        if !required || observed_matches(&point.result, &expected) {
            resolved[i] = true;
            for &s in &successors[i] {
                remaining_pred[s] -= 1;
            }
            if dfs(
                ops,
                points,
                kinds,
                resolved,
                remaining_pred,
                &next_state,
                count + 1,
                successors,
            ) {
                return true;
            }
            for &s in &successors[i] {
                remaining_pred[s] += 1;
            }
            resolved[i] = false;
        }

        // Branch 2: SKIP an optional write (choose that it never applied). The
        // state is unchanged; its successors are still released.
        if !required {
            resolved[i] = true;
            for &s in &successors[i] {
                remaining_pred[s] -= 1;
            }
            if dfs(
                ops,
                points,
                kinds,
                resolved,
                remaining_pred,
                state,
                count + 1,
                successors,
            ) {
                return true;
            }
            for &s in &successors[i] {
                remaining_pred[s] += 1;
            }
            resolved[i] = false;
        }
    }

    false
}

/// `true` iff the operation's observed result equals the reference model's.
fn observed_matches(observed: &Option<OpResult>, expected: &OpResult) -> bool {
    observed.as_ref() == Some(expected)
}

/// Greedily minimize a violation witness: drop a point if the remaining
/// sub-history is still non-linearizable. Deterministic (ascending point order).
/// The returned call ids name a counterexample that a regression test can replay.
fn shrink_witness(ops: &[LogOp], initial: &KvState, points: &[usize]) -> Vec<CallId> {
    let mut keep: Vec<usize> = points.to_vec();
    let mut i = 0;
    while i < keep.len() {
        let mut trial = keep.clone();
        trial.remove(i);
        let trial_kinds: Vec<PointKind> = trial
            .iter()
            .map(|&p| {
                if ops[p].is_linearization_point() {
                    PointKind::Required
                } else {
                    PointKind::Optional
                }
            })
            .collect();
        if search(ops, &trial, &trial_kinds, initial) {
            // Removing this op made the history linearizable: it is needed.
            i += 1;
        } else {
            // Still non-linearizable without it: drop it from the witness.
            keep = trial;
        }
    }
    keep.iter()
        .flat_map(|&p| ops[p].calls.iter().copied())
        .collect()
}

// ---------------------------------------------------------------------------
// Meta-tests (TDD): linearizable → Linearizable; known-bad → Violation with a
// valid witness; determinism (twice → identical); cross-validation vs stateright.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oracle::{ClientId, History, OpResult, OracleErrorKind, SeqNo, ValueId};
    use std::collections::BTreeMap;

    // -- stateright reference (dev-dependency, cross-validation only) -------

    use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};

    /// The reference KV spec for stateright: identical semantics to [`KvState`].
    #[derive(Clone)]
    struct KvRef {
        map: BTreeMap<Vec<u8>, ValueId>,
    }

    impl KvRef {
        fn with_entries(entries: &[(Vec<u8>, ValueId)]) -> Self {
            let mut map = BTreeMap::new();
            for (key, value) in entries {
                map.insert(key.clone(), *value);
            }
            Self { map }
        }
    }

    impl SequentialSpec for KvRef {
        type Op = Op;
        type Ret = OpResult;
        fn invoke(&mut self, op: &Op) -> OpResult {
            match op {
                Op::Put { key, value } => {
                    self.map.insert(key.clone(), *value);
                    OpResult::Ok(None)
                }
                Op::Delete { key } => {
                    self.map.remove(key);
                    OpResult::Ok(None)
                }
                Op::Get { key } => OpResult::Ok(self.map.get(key).copied()),
            }
        }
    }

    /// A feed event kind for the deterministic stateright feed.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum FeedKind {
        Invoke,
        Return,
    }

    /// A deterministic topological order of the invoke/return event DAG.
    ///
    /// Edges: `invoke_i -> return_i`, and `return_i -> invoke_j` whenever op `i`
    /// is real-time before op `j` (`i.complete_ts <= j.invoke_ts`). This order
    /// interleaves concurrent ops so stateright's in-flight tracking captures the
    /// true real-time partial order (not an arbitrary total order).
    fn feed_order(ops: &[LogOp]) -> Vec<(usize, FeedKind)> {
        let n = ops.len();
        let total = 2 * n;
        let mut in_deg = vec![0usize; total];
        let mut successors: Vec<Vec<usize>> = vec![Vec::new(); total];

        for i in 0..n {
            let inv = 2 * i;
            let ret = 2 * i + 1;
            successors[inv].push(ret);
            in_deg[ret] += 1;
            for j in 0..n {
                if i == j {
                    continue;
                }
                if ops[i]
                    .complete_ts
                    .is_some_and(|ci| ci <= ops[j].invoke_ts)
                {
                    successors[ret].push(2 * j);
                    in_deg[2 * j] += 1;
                }
            }
        }

        // Kahn's algorithm; among available nodes pick the one with the smallest
        // (timestamp, kind_rank, op_index) key for determinism.
        let mut available: Vec<usize> = (0..total).filter(|&node| in_deg[node] == 0).collect();
        let mut order: Vec<(usize, FeedKind)> = Vec::new();
        while !available.is_empty() {
            available.sort_by_key(|&node| node_key(ops, node));
            let node = available.remove(0);
            let (i, kind) = if node % 2 == 0 {
                (node / 2, FeedKind::Invoke)
            } else {
                (node / 2, FeedKind::Return)
            };
            order.push((i, kind));
            for &succ in &successors[node] {
                in_deg[succ] -= 1;
                if in_deg[succ] == 0 {
                    available.push(succ);
                }
            }
        }
        order
    }

    fn node_key(ops: &[LogOp], node: usize) -> (u64, u8, usize) {
        let i = node / 2;
        if node % 2 == 0 {
            (ops[i].invoke_ts, 1, i) // Invoke
        } else {
            (ops[i].complete_ts.unwrap_or(0), 0, i) // Return
        }
    }

    /// stateright's verdict on a reduced history: `true` iff linearizable.
    fn stateright_verdict(ops: &[LogOp], initial: &[(Vec<u8>, ValueId)]) -> bool {
        let points: Vec<LogOp> = ops
            .iter()
            .filter(|o| o.is_linearization_point())
            .cloned()
            .collect();
        let mut tester = LinearizabilityTester::new(KvRef::with_entries(initial));
        let order = feed_order(&points);
        for (i, kind) in order {
            let op = &points[i];
            match kind {
                FeedKind::Invoke => {
                    tester.on_invoke(op.client.0, op.op.clone()).expect("valid invoke");
                }
                FeedKind::Return => {
                    tester
                        .on_return(op.client.0, op.result.clone().expect("completed op"))
                        .expect("valid return");
                }
            }
        }
        tester.is_consistent()
    }

    // -- history builders ---------------------------------------------------

    /// `client 0: Put(k,1) [0,10]; client 1: Get(k)->Some(1) [20,30]`.
    fn seq_put_then_get() -> History {
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete_logged(crate::oracle::CallId(1), 10, OpResult::Ok(None), 100);
        h.invoke(crate::oracle::CallId(2), ClientId(1), SeqNo(0), Op::Get { key: b"k".to_vec() }, 20)
            .complete(crate::oracle::CallId(2), 30, OpResult::Ok(Some(ValueId(1))));
        h
    }

    /// Two concurrent writes, then a read of the later value.
    fn concurrent_writes_then_read() -> History {
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Ok(None));
        h.invoke(crate::oracle::CallId(2), ClientId(1), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(2) }, 5)
            .complete(crate::oracle::CallId(2), 15, OpResult::Ok(None));
        h.invoke(crate::oracle::CallId(3), ClientId(2), SeqNo(0), Op::Get { key: b"k".to_vec() }, 20)
            .complete(crate::oracle::CallId(3), 30, OpResult::Ok(Some(ValueId(2))));
        h
    }

    /// One client does two sequential writes; another reads the final value.
    fn one_client_two_writes_then_read() -> History {
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(crate::oracle::CallId(1), 5, OpResult::Ok(None));
        h.invoke(crate::oracle::CallId(2), ClientId(0), SeqNo(1), Op::Put { key: b"k".to_vec(), value: ValueId(2) }, 10)
            .complete(crate::oracle::CallId(2), 15, OpResult::Ok(None));
        h.invoke(crate::oracle::CallId(3), ClientId(1), SeqNo(0), Op::Get { key: b"k".to_vec() }, 20)
            .complete(crate::oracle::CallId(3), 30, OpResult::Ok(Some(ValueId(2))));
        h
    }

    /// A read of a value that was never written (phantom).
    fn phantom_read() -> History {
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Get { key: b"k".to_vec() }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Ok(Some(ValueId(9))));
        h
    }

    /// A read of a value that was never the current value (wrong value).
    fn wrong_value_read() -> History {
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Ok(None));
        h.invoke(crate::oracle::CallId(2), ClientId(1), SeqNo(0), Op::Get { key: b"k".to_vec() }, 20)
            .complete(crate::oracle::CallId(2), 30, OpResult::Ok(Some(ValueId(2))));
        h
    }

    /// T1 writes k=1; T2 (after T1) reads k=1; T3 (after T2) reads k=0.
    /// T3 must see T1's write (transitive real-time), so this is not
    /// linearizable.
    fn stale_read_after_newer() -> History {
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Ok(None));
        h.invoke(crate::oracle::CallId(2), ClientId(1), SeqNo(0), Op::Get { key: b"k".to_vec() }, 15)
            .complete(crate::oracle::CallId(2), 25, OpResult::Ok(Some(ValueId(1))));
        h.invoke(crate::oracle::CallId(3), ClientId(2), SeqNo(0), Op::Get { key: b"k".to_vec() }, 30)
            .complete(crate::oracle::CallId(3), 40, OpResult::Ok(Some(ValueId(0))));
        h
    }

    // -- linearizable -------------------------------------------------------

    #[test]
    fn linearizable_histories_are_linearizable() {
        let initial = KvState::new();
        for h in [
            seq_put_then_get(),
            concurrent_writes_then_read(),
            one_client_two_writes_then_read(),
        ] {
            let outcome = check_linearizable(&h, &initial);
            assert!(
                matches!(outcome, CheckOutcome::Linearizable),
                "expected linearizable, got {outcome:?}"
            );
        }
    }

    #[test]
    fn empty_history_is_linearizable() {
        let initial = KvState::new();
        let h = History::new();
        assert!(matches!(
            check_linearizable(&h, &initial),
            CheckOutcome::Linearizable
        ));
    }

    #[test]
    fn all_failed_ops_are_linearizable() {
        let initial = KvState::new();
        let mut h = History::new();
        // Two failed ops: not linearization points, so vacuously linearizable.
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Err(OracleErrorKind::QuorumUnavailable));
        h.invoke(crate::oracle::CallId(2), ClientId(1), SeqNo(0), Op::Get { key: b"k".to_vec() }, 20)
            .complete(crate::oracle::CallId(2), 30, OpResult::Err(OracleErrorKind::Timeout));
        assert!(matches!(
            check_linearizable(&h, &initial),
            CheckOutcome::Linearizable
        ));
    }

    // -- non-linearizable ---------------------------------------------------

    #[test]
    fn phantom_read_is_a_violation_with_witness() {
        let initial = KvState::new();
        let outcome = check_linearizable(&phantom_read(), &initial);
        match outcome {
            CheckOutcome::Violation { witness } => {
                assert_eq!(witness, vec![crate::oracle::CallId(1)], "witness must name the offending op");
            }
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    #[test]
    fn wrong_value_read_is_a_violation() {
        let initial = KvState::new();
        let outcome = check_linearizable(&wrong_value_read(), &initial);
        assert!(
            matches!(outcome, CheckOutcome::Violation { .. }),
            "expected a violation, got {outcome:?}"
        );
    }

    #[test]
    fn stale_read_after_newer_is_a_violation() {
        let initial = KvState::new();
        let outcome = check_linearizable(&stale_read_after_newer(), &initial);
        assert!(
            matches!(outcome, CheckOutcome::Violation { .. }),
            "expected a violation, got {outcome:?}"
        );
    }

    // -- determinism --------------------------------------------------------

    #[test]
    fn checker_is_deterministic() {
        let initial = KvState::new();
        // A non-linearizable history: the verdict AND the witness must be
        // identical across runs.
        let histories = [
            seq_put_then_get(),
            concurrent_writes_then_read(),
            phantom_read(),
            wrong_value_read(),
            stale_read_after_newer(),
            one_client_two_writes_then_read(),
        ];
        for h in histories {
            let first = check_linearizable(&h, &initial);
            let second = check_linearizable(&h, &initial);
            assert_eq!(first, second, "the checker must be deterministic");
        }
    }

    // -- cross-validation vs stateright (mandatory, D-T2) -------------------

    #[test]
    fn cross_validation_agrees_with_stateright() {
        let empty = Vec::<(Vec<u8>, ValueId)>::new();
        // A seeded initial state: key "k" starts at ValueId(50).
        let seeded = vec![(b"k".to_vec(), ValueId(50))];

        // `(history, initial_state)` pairs. The empty and seeded initial states
        // both exercise the reference semantics.
        let cases: Vec<(History, Vec<(Vec<u8>, ValueId)>)> = vec![
            (seq_put_then_get(), empty.clone()),
            (concurrent_writes_then_read(), empty.clone()),
            (one_client_two_writes_then_read(), empty.clone()),
            (phantom_read(), empty.clone()),
            (wrong_value_read(), empty.clone()),
            (stale_read_after_newer(), empty.clone()),
            (History::new(), empty.clone()),
            // Read of the seeded value (linearizable): the read sees the initial 50.
            (read_seeded_value(), seeded.clone()),
            // Read of a value that is not the seeded value and was never written
            // (non-linearizable).
            (read_wrong_seeded_value(), seeded.clone()),
        ];

        for (h, initial) in cases {
            let mine = matches!(
                check_linearizable(&h, &KvState::with_entries(&initial)),
                CheckOutcome::Linearizable
            );
            let reduced = h.merge_retries();
            let theirs = stateright_verdict(&reduced.ops, &initial);
            assert_eq!(
                mine, theirs,
                "cross-validation disagreement: mine={mine}, stateright={theirs}\nhistory={:?}",
                reduced
            );
        }
    }

    /// `client 0: Get(k) -> Some(50) [0, 10]` with `k` seeded to 50.
    fn read_seeded_value() -> History {
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Get { key: b"k".to_vec() }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Ok(Some(ValueId(50))));
        h
    }

    /// `client 0: Get(k) -> Some(7) [0, 10]` with `k` seeded to 50 (wrong value).
    fn read_wrong_seeded_value() -> History {
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Get { key: b"k".to_vec() }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Ok(Some(ValueId(7))));
        h
    }

    // -- S2a: a positive case that needs backtracking -----------------------

    #[test]
    fn greedy_adversarial_positive_needs_backtracking() {
        // put1(k=1) and put2(k=2) are concurrent; R1 reads 2, R2 (after both)
        // reads 1. Linearizable ONLY as put2, R1, put1, R2 — a greedy
        // first-available placement dead-ends, so this exercises backtracking.
        let initial = KvState::new();
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Ok(None));
        h.invoke(crate::oracle::CallId(2), ClientId(1), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(2) }, 0)
            .complete(crate::oracle::CallId(2), 10, OpResult::Ok(None));
        h.invoke(crate::oracle::CallId(3), ClientId(2), SeqNo(0), Op::Get { key: b"k".to_vec() }, 2)
            .complete(crate::oracle::CallId(3), 4, OpResult::Ok(Some(ValueId(2))));
        h.invoke(crate::oracle::CallId(4), ClientId(3), SeqNo(0), Op::Get { key: b"k".to_vec() }, 20)
            .complete(crate::oracle::CallId(4), 30, OpResult::Ok(Some(ValueId(1))));
        assert!(
            matches!(check_linearizable(&h, &initial), CheckOutcome::Linearizable),
            "the backtracking-only history is linearizable"
        );
    }

    // -- B3: unknown-outcome writes are OPTIONAL points ---------------------

    #[test]
    fn timeout_write_race_is_linearizable() {
        // Put(k,5) failed with Timeout (result unknown: may have applied), then a
        // read observes 5. Legal because the write is an optional point.
        let initial = KvState::new();
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(5) }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Err(OracleErrorKind::Timeout));
        h.invoke(crate::oracle::CallId(2), ClientId(1), SeqNo(0), Op::Get { key: b"k".to_vec() }, 20)
            .complete(crate::oracle::CallId(2), 30, OpResult::Ok(Some(ValueId(5))));
        assert!(
            matches!(check_linearizable(&h, &initial), CheckOutcome::Linearizable),
            "an unknown-outcome write may be placed"
        );
        // The oracle agrees (the put may have applied, so the read is no phantom).
        assert!(
            !h.check().violated().contains(&crate::oracle::Invariant::PhantomValue),
            "oracle must not flag the timeout-committed race as phantom"
        );
    }

    #[test]
    fn known_not_applied_write_is_dropped() {
        // Put(k,5) definitely did NOT apply (QuorumUnavailable): a later read of
        // Some(5) is then non-linearizable.
        let initial = KvState::new();
        let mut h = History::new();
        h.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(5) }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Err(OracleErrorKind::QuorumUnavailable));
        h.invoke(crate::oracle::CallId(2), ClientId(1), SeqNo(0), Op::Get { key: b"k".to_vec() }, 20)
            .complete(crate::oracle::CallId(2), 30, OpResult::Ok(Some(ValueId(5))));
        assert!(
            matches!(check_linearizable(&h, &initial), CheckOutcome::Violation { .. }),
            "a known-not-applied write is not an optional point"
        );
    }

    // -- S3: the witness is minimized ---------------------------------------

    #[test]
    fn violation_witness_is_minimized() {
        let initial = KvState::new();
        let h = stale_read_after_newer(); // 3 points, non-linearizable
        match check_linearizable(&h, &initial) {
            CheckOutcome::Violation { witness } => assert!(
                witness.len() < 3,
                "the witness must be a minimal-ish counterexample, got {witness:?}"
            ),
            other => panic!("expected a violation, got {other:?}"),
        }
    }

    // -- S4: the search is bounded (no factorial hang) ----------------------

    #[test]
    fn too_many_points_is_inconclusive() {
        let initial = KvState::new();
        let mut h = History::new();
        let n = MAX_CHECK_POINTS as u64 + 1;
        for i in 0..n {
            let key = format!("k{i}").into_bytes();
            h.invoke(crate::oracle::CallId(i + 1), ClientId(0), SeqNo(i), Op::Get { key }, i * 2)
                .complete(crate::oracle::CallId(i + 1), i * 2 + 1, OpResult::Ok(None));
        }
        assert!(
            matches!(
                check_linearizable(&h, &initial),
                CheckOutcome::Inconclusive { points } if points == MAX_CHECK_POINTS + 1
            ),
            "a history above the point cap must be Inconclusive"
        );
    }

    // -- S2b: seeded differential agreement with stateright -----------------

    /// Build a small deterministic history (3 single-sequencer clients) for a
    /// differential test. All ops succeed; a `Get`'s result is drawn from the
    /// values written so far (or `None`), so many cases are linearizable and
    /// some are not — either way the verdict must match stateright.
    fn differential_history(seed: u64) -> History {
        let mut s = seed | 1;
        let mut next = || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let mut h = History::new();
        let mut call = 0u64;
        let mut written: Vec<ValueId> = Vec::new();
        let mut vcounter = 0u64;
        for j in 0..6u64 {
            let client = ClientId(j % 3);
            let seq = SeqNo(j / 3);
            let inv = j * 4;
            let comp = inv + 1 + (next() % 3);
            call += 1;
            match next() % 3 {
                0 => {
                    let v = ValueId(100 + vcounter);
                    vcounter += 1;
                    written.push(v);
                    h.invoke(crate::oracle::CallId(call), client, seq, Op::Put { key: b"k".to_vec(), value: v }, inv)
                        .complete(crate::oracle::CallId(call), comp, OpResult::Ok(None));
                }
                1 => {
                    h.invoke(crate::oracle::CallId(call), client, seq, Op::Delete { key: b"k".to_vec() }, inv)
                        .complete(crate::oracle::CallId(call), comp, OpResult::Ok(None));
                }
                _ => {
                    let r = next() % (written.len() as u64 + 1);
                    let res = if r == 0 {
                        None
                    } else {
                        Some(written[(r - 1) as usize])
                    };
                    h.invoke(crate::oracle::CallId(call), client, seq, Op::Get { key: b"k".to_vec() }, inv)
                        .complete(crate::oracle::CallId(call), comp, OpResult::Ok(res));
                }
            }
        }
        h
    }

    #[test]
    fn seeded_differential_agrees_with_stateright() {
        let empty = Vec::<(Vec<u8>, ValueId)>::new();
        for seed in 1..=32u64 {
            let h = differential_history(seed);
            let mine = matches!(
                check_linearizable(&h, &KvState::new()),
                CheckOutcome::Linearizable
            );
            let reduced = h.merge_retries();
            let theirs = stateright_verdict(&reduced.ops, &empty);
            assert_eq!(
                mine, theirs,
                "seeded differential disagreement (seed {seed}): mine={mine}, stateright={theirs}\nhistory={:?}",
                reduced
            );
        }
    }

    // -- Additional local self-evidence -------------------------------------

    /// A multi-key differential generator: `clients` single-sequencer clients
    /// issuing `ops` successful ops over `keys` keys. Read results are drawn from
    /// the values written to that key so far (or `None`), so both linearizable
    /// and non-linearizable histories arise.
    fn differential_history_multi(seed: u64, ops: u64, clients: u64, keys: u64) -> History {
        let mut s = seed | 1;
        let mut next = || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let mut h = History::new();
        let mut call = 0u64;
        let mut written: BTreeMap<u64, Vec<ValueId>> = BTreeMap::new();
        let mut vcounter = 0u64;
        for j in 0..ops {
            let client = ClientId(j % clients);
            let seq = SeqNo(j / clients);
            let inv = j * 4;
            let comp = inv + 1 + (next() % clients.max(1));
            call += 1;
            let key_idx = next() % keys;
            let key = format!("k{key_idx}").into_bytes();
            match next() % 3 {
                0 => {
                    let v = ValueId(100 + vcounter);
                    vcounter += 1;
                    written.entry(key_idx).or_default().push(v);
                    h.invoke(crate::oracle::CallId(call), client, seq, Op::Put { key, value: v }, inv)
                        .complete(crate::oracle::CallId(call), comp, OpResult::Ok(None));
                }
                1 => {
                    h.invoke(crate::oracle::CallId(call), client, seq, Op::Delete { key }, inv)
                        .complete(crate::oracle::CallId(call), comp, OpResult::Ok(None));
                }
                _ => {
                    let res = {
                        let list = written.entry(key_idx).or_default();
                        let r = next() % (list.len() as u64 + 1);
                        if r == 0 {
                            None
                        } else {
                            Some(list[(r - 1) as usize])
                        }
                    };
                    h.invoke(crate::oracle::CallId(call), client, seq, Op::Get { key }, inv)
                        .complete(crate::oracle::CallId(call), comp, OpResult::Ok(res));
                }
            }
        }
        h
    }

    #[test]
    fn seeded_differential_multi_key_agrees_with_stateright() {
        let empty = Vec::<(Vec<u8>, ValueId)>::new();
        for seed in 1..=1000u64 {
            let h = differential_history_multi(seed, 8, 4, 2);
            let mine = matches!(
                check_linearizable(&h, &KvState::new()),
                CheckOutcome::Linearizable
            );
            let reduced = h.merge_retries();
            let theirs = stateright_verdict(&reduced.ops, &empty);
            assert_eq!(
                mine, theirs,
                "multi-key differential disagreement (seed {seed}): mine={mine}, stateright={theirs}\nhistory={:?}",
                reduced
            );
        }
    }

    /// Generate a history from a legal, strictly sequential KV execution (one
    /// client, non-overlapping intervals, per-key increasing values), so the
    /// oracle must not flag it and the checker must accept it.
    fn sequential_history(seed: u64, ops: u64, keys: u64) -> History {
        let mut s = seed | 1;
        let mut next = || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let mut h = History::new();
        let mut model: BTreeMap<u64, ValueId> = BTreeMap::new();
        let mut vcounter = 0u64;
        let mut ts = 0u64;
        for j in 0..ops {
            let key_idx = next() % keys;
            let key = format!("k{key_idx}").into_bytes();
            let call = crate::oracle::CallId(j + 1);
            let seq = SeqNo(j);
            match next() % 3 {
                0 => {
                    let v = ValueId(100 + vcounter);
                    vcounter += 1;
                    model.insert(key_idx, v);
                    h.invoke(call, ClientId(0), seq, Op::Put { key, value: v }, ts)
                        .complete(call, ts + 1, OpResult::Ok(None));
                }
                1 => {
                    model.remove(&key_idx);
                    h.invoke(call, ClientId(0), seq, Op::Delete { key }, ts)
                        .complete(call, ts + 1, OpResult::Ok(None));
                }
                _ => {
                    let res = model.get(&key_idx).copied();
                    h.invoke(call, ClientId(0), seq, Op::Get { key }, ts)
                        .complete(call, ts + 1, OpResult::Ok(res));
                }
            }
            ts += 2;
        }
        h
    }

    #[test]
    fn sequential_histories_pass_oracle_and_checker() {
        let initial = KvState::new();
        for seed in 1..=500u64 {
            let h = sequential_history(seed, 8, 2);
            let report = h.check();
            assert!(
                report.passed(),
                "oracle flagged a legal sequential history (seed {seed}): {}",
                report.render()
            );
            assert!(
                matches!(check_linearizable(&h, &initial), CheckOutcome::Linearizable),
                "checker rejected a legal sequential history (seed {seed})"
            );
        }
    }

    #[test]
    fn witness_replays_as_violation() {
        let initial = KvState::new();
        // Deterministic known violation: its witness must itself replay as one.
        let reduced = stale_read_after_newer().merge_retries();
        match check_reduced(&reduced.ops, &initial) {
            CheckOutcome::Violation { witness } => {
                let sub: Vec<LogOp> = reduced
                    .ops
                    .iter()
                    .filter(|o| o.calls.iter().any(|c| witness.contains(c)))
                    .cloned()
                    .collect();
                assert!(
                    matches!(check_reduced(&sub, &initial), CheckOutcome::Violation { .. }),
                    "the witness must replay as a violation"
                );
            }
            other => panic!("expected a violation, got {other:?}"),
        }
        // Fuzz breadth: whenever a violation is found, its witness must replay.
        for seed in 1..=500u64 {
            let reduced = differential_history_multi(seed, 8, 4, 2).merge_retries();
            if let CheckOutcome::Violation { witness } = check_reduced(&reduced.ops, &initial) {
                let sub: Vec<LogOp> = reduced
                    .ops
                    .iter()
                    .filter(|o| o.calls.iter().any(|c| witness.contains(c)))
                    .cloned()
                    .collect();
                assert!(
                    matches!(check_reduced(&sub, &initial), CheckOutcome::Violation { .. }),
                    "the witness must replay as a violation (seed {seed})"
                );
            }
        }
    }

    #[test]
    fn optional_write_only_adds_flexibility() {
        let initial = KvState::new();
        let mut base = History::new();
        base.invoke(crate::oracle::CallId(1), ClientId(0), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(1) }, 0)
            .complete(crate::oracle::CallId(1), 10, OpResult::Ok(None));
        base.invoke(crate::oracle::CallId(2), ClientId(1), SeqNo(0), Op::Get { key: b"k".to_vec() }, 20)
            .complete(crate::oracle::CallId(2), 30, OpResult::Ok(Some(ValueId(1))));
        assert!(matches!(
            check_linearizable(&base, &initial),
            CheckOutcome::Linearizable
        ));

        // Adding an unknown-outcome (optional) write cannot make a linearizable
        // history non-linearizable.
        let mut with_opt = base.clone();
        with_opt
            .invoke(crate::oracle::CallId(3), ClientId(2), SeqNo(0), Op::Put { key: b"k".to_vec(), value: ValueId(9) }, 15)
            .complete(crate::oracle::CallId(3), 18, OpResult::Err(OracleErrorKind::Timeout));
        assert!(
            matches!(check_linearizable(&with_opt, &initial), CheckOutcome::Linearizable),
            "an optional write cannot turn a linearizable history into a violation"
        );
    }
}
