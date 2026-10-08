# Upstream request to `arachne-kv`: expose a commit index (or an index-bearing stale read)

> 这份文件是给上游的请求正文（英文，可直接粘贴成 issue / PR 描述）。
> 背景与实测来自本仓 ADR-0001 的"观察"节：`dev-docs/aegis/plans/2026-10-05-arachne-control-plane.md`。
> 我们侧的决定是 **乙：先带着这个 hazard 运行**（写路径在诚实情形下会大声回 `503`，
> 静默形态只在承压机器上出现，本地空闲 0/5、有负载约 1/3，且失败的门会变）；
> 这份请求是 **甲：向上游要一个原语**——有了它，我们能给出一个既正确又不扰动的修法。

---

## Title

Expose a commit/index value on `put` and/or a stale read that reports the index of the value it returned

## Summary

We maintain `hydra`, a gateway whose cluster config lives in `arachne-kv`: a head key names a tree, the
tree's entities are separate keys, and **every node materializes the tree into local state**. Applying a
tree is a *replace*, so a client that cannot tell "is this value newer than the one I already applied?"
will eventually apply an older one and destroy newer state. Your API has, by design, no cheap way to
answer that question:

* `get` is the linearizable read (ReadIndex, quorum-confirmed) — correct, but it fails while a node is
  in a minority or during elections, which is exactly when a client must keep serving what it has;
* `get_stale` is documented as "arbitrary stale read allowed; direct local state-machine read; **not
  monotone** (propsol N1)" — cheap and always available, but a client that needs an ORDER cannot use a
  non-monotone read;
* `put` returns `Result<(), ArachneError>` — the write does not tell the caller where it landed.

We are not asking for a stronger consistency level. **We are asking for an order attached to the
value**, so that a plain stale read can drive ordering decisions without a quorum round.

## The ask (any one of these is enough; we would use the first that appears)

```rust
/// Option A — the write tells the caller where it landed.
pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<Index, ArachneError>;

/// Option B — the (stale) read also reports how old the value it returned is.
/// Enough on its own: we read a specific key every second and would refuse any value whose index is
/// lower than the index of what we have already applied.
pub async fn get_stale_with_index(&self, key: &[u8])
    -> Result<Option<(Vec<u8>, Index)>, ArachneError>;

/// `Index` = u64 is fine: the state machine applies entries in log order, so an index is comparable
/// across keys and across nodes.
```

Option B is what we would reach for: it costs nothing (same local read), works with no quorum, and
turns `get_stale` into something a caller can order on. Option A is useful when a client wants to record
"the value I just wrote" without a follow-up read.

## Why we cannot work around it today (all four were tried or ruled out)

| Workaround | Result |
|---|---|
| Use `get` (linearizable) instead of `get_stale` | **Measured failure.** On our materializer path (one read per second per node): 4/5 acceptance failures vs 0/5 in an idle A/B (5 runs per arm), because a node that needs a quorum to read the head stops following it while in a minority. On our publish path (one read per management write): 2 cluster tests fail (`a_write_on_a_non_leader_node_reaches_every_node`, `a_management_write_on_any_node_is_accepted_and_the_cluster_converges`); 4/4 pass after reverting. |
| Derive an order from a **stale** head (e.g. "revision = the head I can see, + 1") | Reproduces the defect one hop over: reading revision 6 while 8 is committed writes 7 and moves the head **backwards**. An order that is not authoritative is not an order. |
| Remember "values we have seen" and refuse to go back to one | Breaks a legitimate operation: rolling back a config means re-publishing an older value (in our case a new tree with old content), and the head really does return to a previously seen value. |
| Read twice and require the two reads to agree | A stale replica can be **self-consistently** stale; two identical old answers are still old. |

Ordering information has to come from the consensus layer; there is nowhere else to get it.

## What it costs us today (the observed harm, for context)

Our config is a tree of entities under a head key. Each node materializes it into SQLite + an in-memory
snapshot; the apply **replaces** both. In two CI samples we measured, end to end:

```
12 seed writes, all answered 201
12 publishes, all counted {result="ok"}
both nodes' materialization loops: {outcome="succeeded"}
no publish failure anywhere
→ one written row was absent from BOTH nodes' databases (tp-t1), i.e. from the tree the head named
```

The shape we can explain with the non-monotone read: a node applies an older head, rolls its database
back past writes it had already published, and every later publish is built from the rolled-back
database — so the loss becomes permanent in the final head, and every node agrees on the missing row
(which is why it looks like a config that was never written). It reproduces on loaded machines at
roughly 1 in 3 runs and never on an idle one, which is consistent with "a lagging local replica answers
the head read".

With an index we would materialize only when the incoming value's index is **greater than or equal to**
the one we applied, using the read we already do, with no extra round trip and no quorum requirement.
That is the whole change on our side (a comparison), and it would also let the publish path order
without the linearizable read that measurably breaks bring-up.

## Non-goals

* We are not asking for `get` to be relaxed, or for leases, or for a new consistency mode. A
  monotone-per-key index is sufficient and we are fine with `get_stale` remaining stale — we only need
  to know *how stale*.
* We are not asking for the index to be exposed on every API surface. One of the two options above is
  enough.

## How we would verify it

Our acceptance drill starts three real nodes, kills the leader, checks that exactly one node claims
leadership, then reduces to a minority and asserts the data plane still serves. Acceptance for us is
that drill on an idle machine (5 runs per arm) plus the three cluster drills in CI, with the index
comparison in place. We are happy to run that and report the numbers here.

## Environment

* `arachne-kv` 0.1.2, used through `Handle::{put,get,get_stale}` and a 3-node in-process/real cluster.
* The defect's evidence, including the two A/B experiments above and the raw CI logs of the missing row,
  is in our repository under `dev-docs/aegis/plans/2026-10-05-arachne-control-plane.md` ("观察" section).
