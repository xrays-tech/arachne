# etcd Concurrent-Read Architecture — Findings for Arachne (etcd v3.5.x)

Researched 2026-10-07 against `etcd-io/etcd` `release-3.5` (v3.5.16-era) plus the vendored `etcd-io/raft` v3.5, official docs, and the grpc-go source.

**Purpose:** explain why a minimal single-leader ReadIndex raft KV (Arachne) wins single-connection linearizable reads (~3000 vs ~1531 ops/s) but loses throughput scaling under 4+ concurrent client connections where etcd scales better, and to identify architectural headroom.

---

## 1. Read consistency modes and defaults

### 1.1 Two consistency levels on the wire

etcd v3 KV API exposes two read consistencies, selected per request via the `serializable` flag on `RangeRequest` (and derived for `TxnRequest`):

- **Linearizable (default):** the response must reflect the latest committed state. Achieved with a *ReadIndex* quorum round trip *before* the local store is read (Section 4).
- **Serializable (opt-in):** served **directly from the local node’s bbolt store with no raft interaction at all** (no consensus, no ReadIndex), and may return stale data (Section 3).

Client default — `clientv3.WithSerializable()` doc (client/v3/op.go):

```go
// WithSerializable makes `Get` and `MemberList` requests serializable.
// By default, they are linearizable. Serializable requests are better
// for lower latency requirement, but users should be aware that they
// could get stale data with serializable requests.
func WithSerializable() OpOption {
	return func(op *Op) { op.serializable = true }
}
```

Server side — `EtcdServer.Range` (server/etcdserver/v3_server.go):

```go
func (s *EtcdServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	// ...
	if !r.Serializable {
		err = s.linearizableReadNotify(ctx) // ReadIndex quorum barrier
		trace.Step("agreement among raft nodes before linearized reading")
		if err != nil { return nil, err }
	}
	chk := func(ai *auth.AuthInfo) error { return s.authStore.IsRangePermitted(ai, r.Key, r.RangeEnd) }
	get := func() { resp, err = s.applyV3Base.Range(ctx, nil, r) }
	if serr := s.doSerialize(ctx, chk, get); serr != nil { err = serr; return nil, err }
	return resp, err
}
```

**Key point: both modes read the same local bbolt store through the same `applierV3backend.Range`. The ONLY difference for linearizable is the pre-read barrier.** That single fact is why serializable reads are ~2x faster at low concurrency and are completely free of consensus / leader involvement.

Txn handling — `isTxnReadonly` / `isTxnSerializable` (v3_server.go): a read-only txn behaves like Range (linearizable unless every inner Range op is serializable); a read-write txn always goes through propose/apply:

```go
func isTxnSerializable(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil || !r.Serializable { return false }
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil || !r.Serializable { return false }
	}
	return true
}
```

### 1.2 Official description of the model

- Performance docs (v3.5 = v3.8, identical): “Linearizable read requests go through a quorum of cluster members for consensus to fetch the most recent data. Serializable read requests are cheaper than linearizable reads since they are served by any single etcd member, instead of a quorum of members, in exchange for possibly serving stale data.”
- tests/robustness/README.md glossary: linearizable = strongest single-object model (ops appear instant, in order, matching real-time order); etcd provides strict serializability for KV ops and eventual consistency for Watch.

---

## 2. Client / connection concurrency model (gRPC, HTTP/2, goroutines)

### 2.1 gRPC server setup (server/etcdserver/api/v3rpc/grpc.go)

- etcd serves the v3 API over gRPC (HTTP/2). Key `grpc.ServerOption`s set: `grpc.MaxRecvMsgSize(MaxRequestBytes+grpcOverheadBytes)`, `grpc.MaxSendMsgSize(MaxInt32)`, `grpc.MaxConcurrentStreams(Cfg.MaxConcurrentStreams)`, plus unary/stream interceptors (logging, prometheus metrics, auth).
- `DefaultMaxConcurrentStreams = math.MaxUint32` (server/embed/config.go) — effectively **no limit on concurrent streams per connection**.
- grpc-go semantics (grpc-go/server.go): `MaxConcurrentStreams(0)` → `math.MaxUint32`; `NumStreamWorkers(0)` (default) → “disable workers and spawn a new goroutine for each stream”. **Each in-flight unary RPC on each stream runs on its own goroutine**; HTTP/2 multiplexes many concurrent streams (one per in-flight request) over a single TCP connection. N concurrent clients ⇒ N goroutines (plus N bbolt read txns), with no user-space serialization before the store.
- Keep-alive defaults: `GRPCKeepAliveMinTime 5s`, `GRPCKeepAliveInterval 2h`, `GRPCKeepAliveTimeout 20s` — long-lived connections; there is no per-request connection setup cost.
- Read-side interceptors are cheap (tracing, auth, prometheus); none take a global store lock.

### 2.2 No shared lock in the RPC layer

Range/Txn handlers call straight into `EtcdServer`. Serializable reads never take the raft state machine lock; the only store-level synchronization is a brief `store.mu.RLock()` + `revMu.RLock()` to snapshot the compaction/revision (Section 3.3). The raft *apply* loop owns writes; reads are fully off-loop. This is the root reason etcd read parallelism grows linearly with request concurrency.

---

## 3. Backend concurrency: bbolt MVCC, in-memory buffer, single writer

### 3.1 bbolt transaction model (go.etcd.io/bbolt)

- `db.rwlock` — comment: “Allows only one writer at a time” (db.go). Writable txns (`Begin(true)` → `beginRWTx`) take `rwlock.Lock`; read txns (`Begin(false)` → `beginTx`) take only `mmaplock.RLock`. **One write txn at a time; unbounded concurrent read txns**, all over the same mmapped page snapshot.
- etcd sets `InitialMmapSize = 10 GiB` (backend.go): “Setting this larger than the potential max db size can prevent writer from blocking reader” — a big pre-mmap avoids frequent remapping (which would take `mmaplock.Lock` and block all readers).
- Docs: the boltdb-backed MVCC engine takes “tens of microseconds” per serialized request (performance docs).

### 3.2 etcd backend structure (server/mvcc/backend/backend.go)

```go
defaultBatchLimit    = 10000
defaultBatchInterval = 100 * time.Millisecond

type backend struct {
	// ...
	db    *bolt.DB
	batchLimit    int
	batchTx       *batchTxBuffered   // single shared WRITE transaction (batched)
	readTx *readTx                   // single long-lived bbolt READ txn + in-memory buffer
	// txReadBufferCache mirrors readTx.baseReadTx.buf so concurrentReadTx
	// can reuse the buffer copy when it is not stale.
	txReadBufferCache txReadBufferCache
}
```

- `batchTx *batchTxBuffered` — the **single writer**: accumulates up to `defaultBatchLimit = 10000` ops or `100ms`, whichever comes first, then commits + fsyncs. Writes are batched into raft proposals on the apply side and into bbolt on the backend side.
- `readTx *readTx` — one long-lived bbolt read transaction shared by all default-mode readers, plus the **in-memory `txReadBuffer`** carrying the latest applied writes that are not yet committed to disk.

Every read path is a `baseReadTx` (backend/read_tx.go):

```go
func (baseReadTx *baseReadTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	// ...
	keys, vals := baseReadTx.buf.Range(bucketType, key, endKey, limit) // in-memory buffer first
	if int64(len(keys)) == limit { return keys, vals }
	// ... only then open the bbolt bucket cursor under txMu
}
```

- `UnsafeRange` first consults the in-memory buffer (`buf.Range`); only when the key is not found does it fall through to the bbolt bucket cursor, protected by `txMu` (an RWMutex: `RLock` for existing buckets, `Lock` to open/cache a bucket). **Hot keys are served from memory and never touch bbolt.** The buffer is filled by the single `batchTxBuffered` as it applies writes.

### 3.3 Two read modes: shared readTx vs per-request concurrentReadTx

`store.Read(mode, ...)` (server/mvcc/kvstore_txn.go):

```go
func (s *store) Read(mode ReadTxMode, trace *traceutil.Trace) TxnRead {
	s.mu.RLock()
	s.revMu.RLock()
	// For read-only workloads, we use shared buffer by copying transaction
	// read buffer for higher concurrency with ongoing blocking writes.
	var tx backend.ReadTx
	if mode == ConcurrentReadTxMode {
		tx = s.b.ConcurrentReadTx()
	} else {
		tx = s.b.ReadTx()
	}
	tx.RLock() // RLock is no-op for concurrentReadTx
	firstRev, rev := s.compactMainRev, s.currentRev
	s.revMu.RUnlock()
	return newMetricsTxnRead(&storeTxnRead{s, tx, firstRev, rev, trace})
}
```

- `SharedBufReadTxMode` (default, `s.b.ReadTx()`): every reader goes through the ONE shared bbolt read txn. Concurrent readers only contend on `txMu.RLock` while touching bbolt cursors; buffer reads never contend on the store.
- `ConcurrentReadTxMode` (`s.b.ConcurrentReadTx()`): each request gets **its own bbolt read txn referencing the same snapshot + a private copy of the read buffer**. After creation its locks are no-ops (`Lock/Unlock/RLock` empty; `RUnlock` = `txWg.Done()`), so reads execute fully in parallel with zero shared locking:

```go
func (rt *concurrentReadTx) Lock()   {}
func (rt *concurrentReadTx) Unlock() {}
// RLock is no-op. concurrentReadTx does not need to be locked after it is created.
func (rt *concurrentReadTx) RLock() {}
// RUnlock signals the end of concurrentReadTx.
func (rt *concurrentReadTx) RUnlock() { rt.txWg.Done() }
```

`ConcurrentReadTx()` (backend.go) does: take `readTx.RLock()`, `txWg.Add(1)`, copy the buffer (via `txReadBufferCache` if fresh, else `unsafeCopy`), and share `readTx.tx`/`buckets`/`txMu`/`txWg`. `txWg` guarantees the old bbolt read txn is not rolled back (at batch commit) while any concurrent read txn still uses it.

**In v3.5, plain Range reads use ConcurrentReadTxMode** (server/etcdserver/apply.go):

```go
func (a *applierV3backend) Range(ctx, txn mvcc.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	// ...
	if txn == nil {
		txn = a.s.kv.Read(mvcc.ConcurrentReadTxMode, trace)
		defer txn.End()
	}
	// ...
}
```

Txn: read-only txns also use `ConcurrentReadTxMode` unless `ExperimentalTxnModeWriteWithSharedBuffer` is set (then `SharedBufReadTxMode`).

### 3.4 The batch-commit / snapshot-switch cycle (server/mvcc/backend/batch_tx.go)

At each batch trigger, `batchTxBuffered.commit`:

```
1. wait for txWg (all in-flight concurrentReadTx drain)
2. rollback the old bbolt read tx (readTx.tx)
3. readTx.reset()
4. batchTx.commit(stop)                  // persist writes + fsync
5. if !stop: readTx.tx = begin(false)    // fresh snapshot for the next batch
```

All readers are frozen on one consistent snapshot until the writer finishes; etcd amortizes fsync over 10k ops / 100ms, which is exactly why bulk writes reach 44k QPS even though a single-connection write is 583 QPS (Section 5).

**Concurrency summary for reads:** per-node read parallelism = number of request goroutines, each with its own bbolt read txn + buffer snapshot; the only writer proceeds in 100ms / 10k batches, so reads essentially never block behind writes.

---

## 4. Linearizable read path and ReadIndex concurrency

### 4.1 The per-node read barrier

`linearizableReadNotify` (v3_server.go):

```go
func (s *EtcdServer) linearizableReadNotify(ctx context.Context) error {
	s.readMu.RLock()
	nc := s.readNotifier
	s.readMu.RUnlock()

	// signal linearizable loop for current notify if it hasn't been already
	select {
	case s.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-nc.c:
		return nc.err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrStopped
	}
}
```

`readwaitc` is buffered size 1 (`s.readwaitc = make(chan struct{}, 1)`, server.go). Many concurrent readers can block on the **same** notifier channel `nc.c`.

### 4.2 The single linearizableReadLoop that coalesces ALL readers

`linearizableReadLoop()` (v3_server.go) is ONE goroutine per node (extracted to server/etcdserver/read/read.go in later v3.5):

```go
func (s *EtcdServer) linearizableReadLoop() {
	for {
		leaderChangedNotifier := s.LeaderChangedNotify()
		select {
		case <-leaderChangedNotifier:
			continue
		case <-s.readwaitc:
		case <-s.stopping:
			return
		}

		// "as a single loop is can unlock multiple reads, it is not very useful
		// to propagate the trace from Txn or Range."
		nextnr := newNotifier()
		s.readMu.Lock()
		nr := s.readNotifier
		s.readNotifier = nextnr // readers that arrive now wait for the NEXT round
		s.readMu.Unlock()

		confirmedIndex, err := s.requestCurrentIndex(leaderChangedNotifier)
		if err != nil { nr.notify(err); continue }

		appliedIndex := s.getAppliedIndex()
		if appliedIndex < confirmedIndex {
			select {
			case <-s.applyWait.Wait(confirmedIndex): // safety: wait until local applied index catches up
			case <-s.stopping:
				return
			}
		}
		// all l-reads requested at indices before confirmedIndex are unblocked
		nr.notify(nil)
	}
}
```

**This is the key concurrency mechanism for linearizable reads:** while one ReadIndex round trip is in flight, every arriving linearizable read queues on the same notifier; a single quorum round trip releases ALL of them. Per-round fan-out is unbounded, so throughput under concurrency ≈ (readers released per round) / RTT. The upstream comment “as a single loop can unlock multiple reads” is the canonical statement of this batching. Everything “agreement among raft nodes” is amortized over the whole cohort of concurrent readers.

### 4.3 The raft ReadIndex round trip (etcd-io/raft release-3.5)

`requestCurrentIndex` → `sendReadIndex(requestID)` → `s.r.ReadIndex(cctx, 8-byte big-endian request ID)`:

```go
func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}
```

On the leader (`stepLeader`, raft/raft.go):
- singleton cluster → respond immediately (`responseToReadIndexReq`);
- leader that has not yet committed in its term → MsgReadIndex parked in `pendingReadIndexMessages`, released by `releasePendingReadIndexMessages` on the first commit of the term;
- otherwise `sendMsgReadIndexResponse` dispatches on the read-only option:

```go
func sendMsgReadIndexResponse(r *raft, m pb.Message) {
	switch r.readOnly.option {
	case ReadOnlySafe:
		r.readOnly.addRequest(r.raftLog.committed, m)
		r.readOnly.recvAck(r.id, m.Entries[0].Data)
		r.bcastHeartbeatWithCtx(m.Entries[0].Data)
	case ReadOnlyLeaseBased:
		if resp := r.responseToReadIndexReq(m, r.raftLog.committed); resp.To != None {
			r.send(resp)
		}
	}
}
```

- **`ReadOnlySafe` (the default; etcd uses it by leaving `raft.Config.ReadOnlyOption` unset — server/etcdserver/raft.go builds Config with only ID/ElectionTick/HeartbeatTick/Storage/MaxSizePerMsg/MaxInflightMsgs/CheckQuorum/PreVote):** the leader records the read in a FIFO `readOnly` queue tagged with the current commit index, acks itself, and **piggybacks the read context on the next heartbeat broadcast** to followers. Followers reply with `MsgHeartbeatResp`; when a quorum of acks is in, `readOnly.advance` emits a `ReadState{Index, RequestCtx}` into the Ready struct. This is the quorum round trip — one broadcast + a quorum of heartbeats (no log append, no disk).
- **`ReadOnlyLeaseBased`** (raft/raft.go): responds immediately with the local committed index relying on the leader lease; requires CheckQuorum; sensitive to clock drift. Not used by etcd v3.5.

Follower path (`stepFollower`): a `MsgReadIndex` is forwarded to the leader (`m.To = r.lead`), and the confirmed `ReadState` returns as `MsgReadIndexResp`. **Linearizable reads issued to any member are still resolved through the leader**; only serializable reads are truly local (Section 1). This is why per-replica scaling applies to serializable-but-not-linearizable reads.

ReadState is delivered to the server through the raft Ready; `raftNode.start` pushes it on `readStateC chan raft.ReadState` (buffered size 1, raft.go), and `requestCurrentIndex` matches it by request ID:

```go
responseID := uint64(0)
if len(rs.RequestCtx) == 8 { responseID = binary.BigEndian.Uint64(rs.RequestCtx) }
if _, ok := requestIDs[responseID]; !ok {
	// a previous request might time out ... continue waiting
	lg.Warn("ignored out-of-date read index response; local node read indexes queueing up ...")
	slowReadIndex.Inc()
	continue
}
return rs.Index, nil
```

### 4.4 Read-index queue batching inside raft (second coalescing layer)

`readOnly` (raft/read_only.go) keeps `pendingReadIndex map[string]*readIndexStatus` and a FIFO `readIndexQueue`. `advance` dequeues and completes **every** request up to and including the acked context on a single quorum confirmation — i.e. even multiple distinct ReadIndex requests share one heartbeat round. Upstream (etcd-io/raft#392): removing the queue would “lose the current batching behavior, where multiple ReadIndex requests can be completed with a single confirmation”. In etcd the effective fan-out is dominated by the 4.2 loop coalescing, because the loop injects one unique 8-byte ctx per round; but the raft layer also batches when many independent ReadIndex makers demand progress.

### 4.5 Retries, timeouts, metrics

- `readIndexRetryTime = 500ms` — re-send ReadIndex with a fresh request ID after each retry interval (v3_server.go).
- Hard timeout = `ReqTimeout` → `slowReadIndex.Inc()`, `ErrTimeout`.
- Leadership change → `readIndexFailed.Inc()`, retryable `ErrLeaderChanged` on both the loop and `requestCurrentIndex`.
- Admin/metrics knobs: `etcd_server_read_indexes_failed_total`, `etcd_server_slow_read_indexes_total`, plus the “took too long” trace for expensive Range (`warnOfExpensiveReadOnlyRangeRequest`).
- After the ReadState is confirmed, the loop waits for the local **applied** index (`ApplyWait`) so the snapshot it reads contains the confirmed commit — this safety wait is shared by the whole cohort (another bulk unblock).

---

## 5. Known performance characteristics / numbers

### 5.1 Official etcd performance table (v3.5 docs; v3.8 table is identical)

Range reads, 8B keys / 256B values, 3-node cluster (each 8 vCPU + 50GB SSD, GCE):

| reqs | conns | clients | consistency   | QPS     | avg latency |
|------|-------|---------|---------------|---------|-------------|
| 10k  | 1     | 1       | Linearizable  | **1,353**  | 0.7ms |
| 10k  | 1     | 1       | Serializable  | **2,909**  | 0.3ms |
| 100k | 100   | 1000    | Linearizable  | **141,578** | 5.5ms |
| 100k | 100   | 1000    | Serializable  | **185,758** | 2.2ms |

Puts (same env): 1 conn / 1 client / leader-only **583 QPS @ 1.6ms**; 100 conns / 1000 clients / leader-only **44,341 QPS @ 22ms**; all-members 50,104 QPS.

Direct relevance to the Arachne benchmark:
- Arachne’s ~3000 ops/s single-connection *linearizable* beats the etcd doc number (1353 QPS). etcd has a fixed ~0.7ms/op floor at one client: gRPC + ReadIndex quorum round trip + applied-index wait; with nothing to overlap, a single linearizable read takes ≈ one quorum RTT.
- At 100 conns × 1000 clients etcd linearizable jumps to **141,578 QPS (~104×)** and serializable to 185,758 QPS — read throughput scales ~linearly with concurrency because every request is an independent goroutine + independent bbolt read txn, and every ReadIndex round releases a large batch. The documented mechanism (not a 4-connection-specific figure) is that scaling is smooth from 1 connection onward; there are no doc numbers specifically for 4 connections.
- Writes are the slow path — “writes are even more slow” than reads: single-conn write 583 QPS vs reads 2909/1353 QPS (2.3–5× gap) because every committed write pays fdatasync + quorum replication (see 5.3).

### 5.2 Other official claims (performance / hardware / tuning docs)

- “a three member etcd cluster finishes a request in less than one millisecond under light load, and can complete more than 30,000 requests per second under heavy load” (standard cloud n-4-class machines).
- “To increase throughput, etcd batches multiple requests together and submits them to Raft. This batching policy lets etcd attain high throughput despite heavy load.”
- MVCC engine adds “tens of microseconds” per serialized request; snapshot merge can spike latency (~2× on HDD); gRPC “introduces additional latency, especially for local reads”.
- Disk sensitivity: WAL fsync + backend commit dominate; slow members raise latency for any request they serve, and slow disks can stall heartbeats → elections (hardware + tuning docs).
- Monitoring: `etcd_disk_wal_fsync_duration_seconds`, `etcd_disk_backend_commit_duration_seconds`; etcd also ships `etcdctl check perf`.

### 5.3 Write-path latency model (etcd issue #7156, xiang90)

Single-client put throughput ≈ `1 / (client→leader RTT + leader→follower RTT + fdatasync)` ≈ `1 / (300 + 300 + 600)µs ≈ 833 req/s` theoretical; etcd measured ~580–1000 req/s on 3 nodes. This quantifies why the *same* engine that does 141k reads/s only does ~44k writes/s at high concurrency and ~583/s at 1 client — and why Arachne’s stale/linearizable read wins are about the read path only.

---

## 6. Architectural differences vs a minimal single-leader ReadIndex raft KV (Arachne)

What etcd has that a minimal single-actor, single-leader, ReadIndex-only linearizable-read KV typically lacks:

1. **Per-request goroutines end-to-end** (gRPC stream goroutine → handler → bbolt txn), with NO global actor loop on the read path. A cork: Arachne funnels reads through one actor loop, so concurrent clients serialize inside the loop and cannot overlap.
2. **Client-side read coalescing** — one `readwaitc` wakeup + one `notifier` release an unbounded number of concurrently-waiting readers per ReadIndex round. A design that does one ReadIndex per read (or one per actor tick) is bound to ≈ 1/RTT regardless of client count. **Highest-value headroom: a single-flight read-index waiter with a broadcast notifier.**
3. **Raft-level read-index queue batching** (`readOnly.advance` completes all pending ReadIndex requests on one quorum ack) — even many distinct ReadIndex requests share one heartbeat round.
4. **No consensus for serializable reads** — local bbolt MVCC read, zero raft interaction; cluster read parallelism grows with member count (each node reads locally, including followers). A minimal raft KV that routes even weak reads through the leader/actor loses this. Corollary: the “parallelism scales with replicas” property is strictly a serializable-read property; linearizable reads still funnel through the leader’s ReadIndex.
5. **Concurrent bbolt read transactions** — each request opens its own read txn over the same mmap snapshot (no shared cursor lock, no reader serialization). A single shared store behind one mutex serializes readers.
6. **In-memory read buffer** — hot keys are served from a per-reader buffer copy and never touch bbolt.
7. **Shared applied-index wait** — one `ApplyWait(confirmedIndex)` releases the whole cohort, not per-read waits.
8. **HTTP/2 stream multiplexing + unlimited concurrent streams** — many clients per connection are first-class; connection count is not a per-node bottleneck (MaxConcurrentStreams default = MaxUint32).

Recommended Arachne headroom, in priority order:
- (a) coalescing single-flight linearizable-read barrier (notifier + readwaitc pattern);
- (b) parallel read execution (worker pool / per-request tasks) instead of serial read handling in the actor loop;
- (c) keep serializable/stale reads fully off the consensus path (already present) — ensure they never take the raft lock;
- (d) if the storage layer can expose multiple concurrent readers on a frozen snapshot, mirror etcd’s per-request read txn model;
- (e) batch/single-flight the applied-index wait shared by all pending readers.

---

## 7. Sources

### etcd source (release-3.5)
- server/etcdserver/v3_server.go — Range, Txn, isTxnReadonly/isTxnSerializable, linearizableReadNotify, linearizableReadLoop, requestCurrentIndex, sendReadIndex, readIndexRetryTime. https://github.com/etcd-io/etcd/blob/release-3.5/server/etcdserver/v3_server.go
- server/etcdserver/raft.go — raftNode Config (no ReadOnlyOption ⇒ ReadOnlySafe), readStateC (cap 1). https://github.com/etcd-io/etcd/blob/release-3.5/server/etcdserver/raft.go
- server/etcdserver/apply.go — ApplierV3Backend.Range uses ConcurrentReadTxMode. https://github.com/etcd-io/etcd/blob/release-3.5/server/etcdserver/apply.go
- server/etcdserver/api/v3rpc/grpc.go — grpc.ServerOption set: MaxConcurrentStreams, MaxRecv/MaxSendMsgSize, interceptors. https://github.com/etcd-io/etcd/blob/release-3.5/server/etcdserver/api/v3rpc/grpc.go
- server/etcdserver/server.go — readwaitc (cap 1), readNotifier. https://github.com/etcd-io/etcd/blob/release-3.5/server/etcdserver/server.go
- server/embed/config.go — DefaultMaxConcurrentStreams = MaxUint32, MaxRequestBytes, keep-alive defaults. https://github.com/etcd-io/etcd/blob/release-3.5/server/embed/config.go
- server/mvcc/backend/backend.go — backend struct, ConcurrentReadTx, InitialMmapSize 10GiB, batch limits. https://github.com/etcd-io/etcd/blob/release-3.5/server/mvcc/backend/backend.go
- server/mvcc/backend/read_tx.go — baseReadTx, concurrentReadTx (no-op locks, txWg). https://github.com/etcd-io/etcd/blob/release-3.5/server/mvcc/backend/read_tx.go
- server/mvcc/backend/batch_tx.go — batchTxBuffered.commit cycle (rollback readTx, reset, begin(false)). https://github.com/etcd-io/etcd/blob/release-3.5/server/mvcc/backend/batch_tx.go
- server/mvcc/kvstore_txn.go — store.Read(mode), ConcurrentReadTxMode vs shared. https://github.com/etcd-io/etcd/blob/release-3.5/server/mvcc/kvstore_txn.go
- client/v3/op.go — WithSerializable (“by default they are linearizable”). https://github.com/etcd-io/etcd/blob/release-3.5/client/v3/op.go
- raft/raft.go — stepLeader/stepFollower MsgReadIndex, sendMsgReadIndexResponse, pendingReadIndexMessages, ReadOnlyOption constants. https://github.com/etcd-io/etcd/blob/release-3.5/raft/raft.go
- raft/read_only.go — readOnly queue, addRequest/advance. https://github.com/etcd-io/etcd/blob/release-3.5/raft/read_only.go
- raft/node.go — Node.ReadIndex. https://github.com/etcd-io/etcd/blob/release-3.5/raft/node.go

### bbolt
- go.etcd.io/bbolt db.go — single writer lock, beginTx (mmaplock.RLock) vs beginRWTx (rwlock.Lock). https://github.com/etcd-io/bbolt/blob/main/db.go

### grpc-go
- grpc-go/server.go — MaxConcurrentStreams(0)→MaxUint32; NumStreamWorkers(0)→goroutine-per-stream. https://github.com/grpc/grpc-go/blob/master/server.go

### Official docs
- Performance (numbers table, incl. 1353 / 2909 / 141578 / 185758; 583 put). https://etcd.io/docs/v3.5/op-guide/performance/ and https://etcd.io/docs/v3.8/op-guide/performance/
- Hardware recommendations. https://etcd.io/docs/v3.3/op-guide/hardware/
- Tuning. https://etcd.io/docs/v3.3/tuning/
- Metrics (slow_read_indexes_total etc.). https://etcd.io/docs/v3.4/metrics/

### Design analysis / discussions
- Pierre Zemb, “Diving into ETCD linearizable reads” (walks ReadIndex, ReadOnlySafe vs LeaseBased, follower forwarding). https://pierrezemb.fr/posts/diving-into-etcd-linearizable/
- etcd-io/etcd#12335 — traces on linearized reading (read index received vs applied-index wait). https://github.com/etcd-io/etcd/pull/12335
- etcd-io/etcd#12762 — postpone MsgReadIndex until first commit in term. https://github.com/etcd-io/etcd/pull/12762
- etcd-io/raft#392 — read-index queue batching behavior; removing it loses batching. https://github.com/etcd-io/raft/issues/392
- etcd-io/etcd#7156 — xiang90 single-client write latency model (~833 req/s ceiling). https://github.com/etcd-io/etcd/issues/7156
- etcd-io/etcd discussion #18521 — concurrent linearizable reads share one notifier round. https://github.com/etcd-io/etcd/discussions/18521
