# Arachne-KV 批量读 + 写入/应用索引可见性——设计文档

> 响应：`dev-docs/arachne-kv-batch-read-applied-index-request.md`（Hydra 24k 实体配置树）。
> 状态：**已实现（Phase 1–4，0.5.0；Gate-5 已审）**。
> 关联既有决议：
> - `dev-docs/arachne-kv-commit-index-design.md`（`get_stale_with_index` 的 value 溯源 index，v0.3.0 已交付）
> - `dev-docs/plan-vs-etcd.md` §3（M1 outcome 回传管线；§8 曾以「下游不需要」关闭 `put_with_index`）
> - `dev-docs/plan-commit-index-followups.md` #9（`put_with_index` 关闭记录）
>
> 本设计**重新打开** `put_with_index`，原因是下游请求已从「可选优化」升级为「验收必需」；重启理由与新增范围必须由 Gate-1 记录。

---

## 1. 目标与非目标

### 目标

- **G1 `multi_get`（最高优先级）**：任意精确 key 集合的批量本地读，整批结果共享一个 `applied_index`，无撕裂。
  - `Stale` 走本地状态机，无 quorum，任意节点（含少数派 follower）可用。
  - `Linearizable` 复用既有 ReadIndex 语义（仅为完整性，Hydra 不依赖）。
- **G2 写入返回 index**：`put_with_index` / `multi_put_with_index` / `delete_with_index` 返回写入所在 log entry 的 index；重放同 `(client_id, seq_no)` 返回原始 index；跨 key、跨节点可比；不额外增加 quorum 往返。
- **G3 `wait_applied(index, timeout)`**：本地、无 quorum 的应用水位等待；已追平立即返回，否则在 apply 推进时唤醒，超时返回 `Timeout`；取消安全。
- **G4 纯 additive**：`get` / `get_stale` / `get_stale_with_index` / `put` / `multi_put` / `delete` / `watch` / `get_stale_range` 的签名与语义不变。
- **G5 线协议与数据格式可控**：新增能力有明确的失败门控（旧对端 → 大声 `Unsupported`，不静默降级）；快照格式变更走破坏性升级 runbook。

### 非目标

- 不做 MVCC / 任意历史读 / 时间旅行。
- 不提高 `max_value_bytes` 或 `multi_put` 的 4 MiB 总上限。
- 不让 Arachne 理解 Hydra 的 TOC / manifest / SQLite。
- 不改 `watch` 的既有契约（snapshot + 严格 `>` 事件 + 有界断连 + 超限拒绝）。
- 不把 `multi_get` 做成第二套一致性模型；`Stale`/`Linearizable` 复用既有定义。
- `wait_applied` 不是 quorum barrier，不替代线性读。

---

## 2. 现状与代码锚点（已核对 main @ 8976994）

| 事实 | 落点 |
|---|---|
| `Handle` 已有 `get_stale_with_index`、`get_stale_prefix/range`、`multi_put`、`watch` | `arachne/src/client/handle.rs:406,432,457,501,535`；`arachne/src/server/mod.rs:193-285` |
| `Command` 枚举含 `GetStale` / `GetStaleWithIndex` / `GetStaleRange` / `Watch` / `Read` | `arachne/src/runtime/mod.rs:208-336` |
| 弱读在 worker 上执行，不占 actor；线性读走 `PendingRead` + ReadIndex 闸门 | `runtime/mod.rs:1931-1955`（弱读）、`2196-2268`（resolve_reads）、`2280-2350`（spawn worker） |
| C3 读合并：同一 cycle 的线性读共享一个 ReadIndex round/token | `runtime/mod.rs:1264-1290`、`1312-1326` |
| apply-task 已把 **per-entry** `(LogIndex, ApplyOutcome)` 回传 actor；`Pending.index` 已是 entry index | `runtime/mod.rs:370-381`、`550-600`、`744-...`；`reply_pendings` `2030-2120` |
| apply 返回 `(ApplyOutcome, Vec<WatchEvent>)`；session 表缓存 `ApplyOutcome` | `state_machine/kv.rs:820-991`、`243` |
| KV 快照版本 2；storage `FORMAT_VERSION` 3 | `state_machine/kv.rs:102`、`storage/meta.rs:55` |
| `ForwardCommand` 只有 Propose/Read/GetStale；`ForwardOutcome` 无 index/多值 | `arachne-seam/src/seam/forward.rs:45-105` |
| proto `ForwardReply` 无 index/多值字段；`kind` 0/1/2 | `arachne-transport-tonic/proto/raft.proto`；`transport.rs:238-320`；`server.rs:262-370` |
| 握手拒绝**更新**的 minor（只允许 peer_minor ≤ self_minor） | `arachne-transport-tonic/src/handshake.rs:47-60` |
| 现有上限：`MAX_MULTI_PUT_ENTRIES=4096`、`MAX_MULTI_PUT_TOTAL_BYTES=4 MiB`、`MAX_STALE_RANGE_ENTRIES=10000` | `state_machine/kv.rs:74-86` |

---

## 3. 公共 API

全部为 `Handle` 新增方法；facade `Arachne` 提供同名静态委托。

```rust
/// 与 `LogIndex` 同义；公开 API 使用请求文档中的名字。
pub type Index = LogIndex; // = u64

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    /// 本地状态机读；无 quorum；可能陈旧。
    Stale,
    /// 既有 ReadIndex 语义；leader 服务，需 quorum。
    Linearizable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiGetResult {
    /// 与输入 keys 一一对应、同序；`None` = 缺键。重复 key 产生重复结果。
    pub values: Vec<Option<Vec<u8>>>,
    /// 所有 values 被观察时的状态机 applied index（单一版本）。
    pub applied_index: Index,
}

impl Handle {
    pub async fn multi_get(
        &self,
        keys: &[&[u8]],
        consistency: ReadConsistency,
    ) -> Result<MultiGetResult, ArachneError>;

    pub async fn put_with_index(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Index, ArachneError>;

    pub async fn multi_put_with_index(
        &self,
        entries: &[(&[u8], &[u8])],
    ) -> Result<Index, ArachneError>;

    pub async fn delete_with_index(&self, key: &[u8]) -> Result<Index, ArachneError>;

    pub async fn wait_applied(
        &self,
        index: Index,
        timeout: std::time::Duration,
    ) -> Result<Index, ArachneError>;
}
```

- `Index` 不在 `arachne-seam` 重复定义；在 `arachne` 根导出 `pub type Index = LogIndex`（或直接 `pub use ... LogIndex as Index`）。
- `ReadConsistency` / `MultiGetResult` 放在 `arachne/src/client/mod.rs`（公开类型层），`lib.rs` 根导出。
- facade：`Arachne::multi_get` / `put_with_index` / `multi_put_with_index` / `delete_with_index` / `wait_applied`，委托到本地 `Handle`（`server/mod.rs` 现有一致性）。
- `index == 0` 对 `wait_applied` 是 no-op：返回当前 applied index。

---

## 4. 语义与不变量

### 4.1 `multi_get`

| # | 不变量 |
|---|---|
| M1 | `values.len() == keys.len()`，顺序严格对应；重复 key 允许重复结果，绝不重排。 |
| M2 | 所有 values 与 `applied_index` 在**同一次读锁**下取得；`applied` 先读、map 后读（D-Arc/D-Ord）。 |
| M3 | 缺键 = `None`，不是错误；空 `keys` 合法（返回空 values + 当前 applied）。 |
| M4 | `Stale` 不要求 leader、不发 ReadIndex、少数派可用。 |
| M5 | `Linearizable` 复用 ReadIndex：leader + quorum 确认 + `applied ≥ read_index` 后才读。**值的保证**绑定到 read index（读到 ≥ read_index 的已提交状态）；返回的 `applied_index` 是读锁观察到的 SM 水位，在 ConfChange 窗口可能等于 `read_index − 1`（该缺口是 KV no-op，不影响值）。 |
| M6 | 超限**不截断**：条目数/总字节超限返回 `InvalidArgument`，不返回部分结果。 |
| M7 | 不改变 `get` / `get_stale` / `get_stale_range` 的任何语义或成本。 |

### 4.2 写入返回 index

| # | 不变量 |
|---|---|
| W1 | 返回值 = 使本次写入**实际生效**的 log entry index（首次 apply 时即本次 entry index）。 |
| W2 | 同 `(client_id, seq_no)` 重放返回**原始** index（session 缓存携带 index），不返回重放 entry 的 index。该保证**仅在 session TTL+grace 内成立**；GC 后的重试是新 session，返回新 index（标准 exactly-once 语义，必须写入下游文档）。 |
| W3 | follower 转发的写入返回 leader 的 committed/applied index，不是本地猜测。 |
| W4 | index 与 `get_stale_with_index` 的 value 溯源 index 同源、跨 key/节点可比（同一 raft group）。 |
| W5 | 不额外增加 quorum 往返；index 由既有 commit→apply→outcome 回传管线携带。 |
| W6 | `multi_put_with_index` 的整批 entry 共享同一 index（一次 apply）。 |
| W7 | `delete_with_index` 即使 key 不存在也返回该 delete entry 的 index（write-set 语义，与 `delete` 一致）。 |
| W8 | 旧服务端/旧 peer 不支持时返回 `Unsupported`（新增错误），绝不伪造 index 或退化为逐 key 读。 |

### 4.3 `wait_applied`

| # | 不变量 |
|---|---|
| A1 | 若本地状态机 `applied >= index`，立即返回当前 applied（可 ≥ index）。 |
| A2 | 否则在 apply 推进到 `index` 时唤醒；**无忙轮询**（由 apply→actor 的 `Progress` 事件驱动）。 |
| A3 | 纯本地：不重定向、不要求 quorum，少数派 follower 可用。 |
| A4 | 超时返回 `Timeout`，不静默成功；Handle 侧 timeout 必须同时包住 channel send 与 await（send 阻塞在满队列时也返回 `Timeout`，不挂起）。 |
| A5 | 取消安全：drop future 后 waiter 不永久泄漏（`ack.is_closed()` 剪枝 + deadline 兜底）。 |
| A6 | `index == 0` 为 no-op。 |
| A7 | 不在 await 期间持有状态机锁；actor 侧同步检查后立即释放。 |
| A8 | 等待的是**状态机 applied index**（含 ConfChange 的 no-op 占位，索引无洞），不是 raft 的 `committed`，也不是 quorum barrier。 |

### 4.4 组合用法（Hydra 竞态闭环）

```text
(token, head_index) = get_stale_with_index(head_key)
wait_applied(head_index, timeout)          // 状态机已 apply 到 head
multi_get(entity_keys, Stale)              // 更早的 entity entry 必已 apply
```

前提（必须写入下游对接说明）：entity/head 的写入顺序保证 entity index < head index（Hydra 当前 publish 顺序）；Arachne 只保证「同一状态机、按 log 顺序 apply」，不解释树结构。

---

## 5. 内部机制

### 5.1 `multi_get`：状态机

在 `KvStateMachine` 增加**只读**方法：

```rust
pub fn multi_get(&self, keys: &[&[u8]]) -> Result<(Vec<Option<Vec<u8>>>, LogIndex), KvError>
```

- 单次 `store.read()`；先 `let applied = guard.applied;`，再逐 key `guard.kv.get(k).map(|e| e.val.clone())`。
- 结果与 `applied` 同源；不在循环中重新读 `applied`。
- 字节预算：在克隆过程中累计 value 字节；超过 `MAX_MULTI_GET_TOTAL_BYTES` 立即返回 `KvError::TooLarge`（新增内部错误）→ runtime 映射为 `InvalidArgument`。**不返回部分结果**。
- 空 keys：返回 `(vec![], applied)`。
- 不改 `get` / `get_with_index` / `get_range_with_index`。

### 5.2 `multi_get`：runtime

新增命令：

```rust
Command::MultiGet {
    keys: Vec<Vec<u8>>,
    consistency: ReadConsistency,
    ack: oneshot::Sender<Result<(Vec<Option<Vec<u8>>>, LogIndex), ArachneError>>,
}
```

- `Stale`：`self.spawn_multi_get(keys, ack)`，在 worker 上取读锁 + oneshot 回复；不经过 actor 热路径（与 `GetStale` 同型）。
- `Linearizable`：复用 `PendingRead`，用**单一枚举**把请求与回复绑定（两个平行枚举无法保证 One/One 配对）：

```rust
enum PendingReadKind {
    One { key: Vec<u8>, ack: oneshot::Sender<Result<Option<Vec<u8>>, ArachneError>> },
    Many { keys: Vec<Vec<u8>>, ack: oneshot::Sender<Result<(Vec<Option<Vec<u8>>>, LogIndex), ArachneError>> },
}
struct PendingRead { token, kind: PendingReadKind, deadline, read_index, attempts }
```

  - `Command::Read` → `One`；`Command::MultiGet{Linearizable}` → `Many`。
  - C3 的 token stamping / retry / `fail_all_pending` 逻辑只依赖 `token`/`deadline`/`attempts`，泛化后语义不变。
  - `resolve_reads` 在闸门通过后按 `kind` 分派到 `spawn_read_reply` 或 `spawn_multi_get`。
  - **不接受**在 Handle 层做「逐 key `get`」或「barrier 后二次命令」的简化，因为那会破坏「一个 applied_index 覆盖整批」的原子性论证。
  - **字节预算（Gate-1 MF2）**：`MAX_PENDING_READS` 是条目数，不能约束 `Many` 的 payload。引入共享的 `MultiGetBudget`（`Arc<AtomicU64>` + `MAX_INFLIGHT_MULTI_GET_BYTES = 16 MiB`）：
    - Handle 在 `tx.send` 前按 `sum(key.len())` 预留（RAII guard，send/await 取消时 Drop 释放），覆盖 **commands channel** 窗口；
    - actor 在把 `Many` 放入 `pending_reads` 时再预留（覆盖 **pending ReadIndex** 窗口），解析/超时/失败时释放；
    - `ForwardCommandSink` 转发到 actor 前同样预留；
    - 预留失败 → `Busy`（可重试；与 `InvalidArgument` 的「拆分批次」语义区分）。
    - **保守重复预留**：Handle/sink 的 guard 持有到 reply，actor 又为 pending 窗口预留，所以同一 payload 在 in-process/forwarded 路径会被计两次；16 MiB 预算下唯一 payload 上限约 8 MiB。这是有意的安全侧，不改变语义。
  - `Linearizable` 跨进程线协议按 Gate-1 结论保留（§6.3）；本地/in-process 路径同样走 `Many`。

- 上限（Gate-1 Q4 定稿；三类 `InvalidArgument`，消息分别点名原因与补救）：
  - **dispatch 前可知**：`keys.len() > MAX_MULTI_GET_ENTRIES (4096)`；任一 key > `max_key_bytes`；`sum(key.len()) > MAX_MULTI_GET_REQUEST_BYTES (4 MiB)`。
  - **dispatch 后、返回前**：返回 value 总字节 > `MAX_MULTI_GET_RESULT_BYTES (4 MiB)`。值大小在 dispatch 前不可知，因此这是**唯一**能在读锁内早退的检查；超限返回 `InvalidArgument`，**不截断**，消息建议「split the batch」。
  - 三类消息必须可区分：entry count / request key bytes / result value bytes。
  - 单 key value 仍受写侧 `max_value_bytes` 约束（写路径已强制；读路径不重复放宽）。
  - 不用 `Busy` 表达超限（`Busy` 意味着「原样重试」，而超限的补救是拆批）。

### 5.3 写入返回 index：apply 管线

核心约束（既有 design §9.1 的并发修正）：**不能在 apply 后回查 store**（同批并发写会读错）。必须由 apply 返回值携带「本次实际生效的 index」。

1. **session 记录携带 index**
   - `Store.sessions: BTreeMap<SessionKey, ApplyOutcome>` → `BTreeMap<SessionKey, SessionRecord>`，
     `SessionRecord { outcome: ApplyOutcome, index: LogIndex }`。
   - 首次 apply 写入 `(outcome, entry_index)`；重放命中缓存，返回缓存的 `index`。
   - 该记录必须进入快照（见 §7），否则重启后重放会丢失原始 index。

2. **apply 返回值**
   - 保留 `StateMachine::apply`（seam）签名不变。
   - 新增 inherent 方法：
     ```rust
     pub fn apply_with_events_and_index(
         &self, index: LogIndex, command: &[u8],
     ) -> Result<(ApplyOutcome, LogIndex, Vec<WatchEvent>), KvError>
     ```
     第三个返回值是 **effective index**：首次 apply = `index`；dedup = 缓存记录的 index；no-op/session-gc = `index`（不被写 API 使用）。
   - 现有 `apply_with_events` 保留为薄包装（丢弃 effective index），避免测试/外部调用破坏。

3. **apply → actor 回传**
   - `outcomes` 通道元素从 `(entry_index, ApplyOutcome)` 扩展为
     `(entry_index, ApplyOutcome, effective_index)`。
   - actor 的 `received_outcomes: BTreeMap<LogIndex, (ApplyOutcome, LogIndex)>`。
   - `Pending` 增加回复形状：
     ```rust
     enum PendingReply {
         Outcome(oneshot::Sender<Result<ApplyOutcome, ArachneError>>),
         Index(oneshot::Sender<Result<Index, ArachneError>>),
     }
     ```
     `Command::Propose` 保持 `Outcome`；新增 `Command::ProposeWithIndex` 使用 `Index`。
   - `reply_pendings` 在 `applied >= p.index` 且 outcome 到达后，按形状回复：
     - `Outcome` → 现有行为；
     - `Index` → `Ok(effective_index)`，outcome 仅用于内部配对。
   - `p.index` 仍是「本次 entry index」；`effective_index` 来自 apply 回传，专门解决 dedup/并发。

4. **Handle**
   - `put_with_index` / `multi_put_with_index` / `delete_with_index` 与现有写方法共用 `validate_*`、`encode_*`、`SeqCst` seq；把 `Command::Propose` 换成 `Command::ProposeWithIndex`。
   - 写 redirect 循环增加回复形状枚举，避免泛化整个写路径：
     ```rust
     enum WriteReply { Outcome(ApplyOutcome), Index(Index) }
     ```
     `request_with_redirect` / `reach_target` / `forward_request` 返回 `WriteReply`；现有调用者取 `Outcome`，indexed 方法取 `Index`。
   - `multi_get` **不**复用写循环：新增 `multi_get_with_redirect`，镜像 `get_with_redirect`（自/peer → `send_multi_get`，远程 → `forward_multi_get`），并处理 `NotLeader`/不可达/重定向上限。
   - `multi_get` 的 Handle 侧 timeout 同时包住 `tx.send` 与 `await_oneshot`（Gate-1 should-fix）。

5. **CAS 不返回 index**（本次范围外）；但 session 记录格式统一，CAS 的 effective index 也被存储，避免类型分叉。

### 5.4 `wait_applied`

- 新增命令：
  ```rust
  Command::WaitApplied {
      index: Index,
      deadline: Instant,
      ack: oneshot::Sender<Result<Index, ArachneError>>,
  }
  ```
- actor 结构：`pending_applied: Vec<PendingApplied>`，元素 `{ index, deadline, ack: Option<...> }`。
- `handle_command`：
  - `index == 0` 或 `self.sm.applied_index() >= index` → 立即 `Ok(self.sm.applied_index())`。
  - 否则 push；若队列达到 `MAX_PENDING_APPLIED_WAITS`（建议 4096）→ `Busy`。
- `drive_cycle` 末尾调用 `resolve_applied_waits()`：
  - 先取一次 `let applied = self.sm.applied_index();`（短读锁，随后释放）。
  - 对每个 waiter：`applied >= index` → `Ok(applied)`；`ack.is_closed()` → 丢弃；`now >= deadline` → `Timeout`；否则保留。
- 唤醒：apply-task 的 `ApplyProgress` 变化已经通过 `progress.changed()` 唤醒 actor；`Outcome::Progress` 后必走 `drive_cycle`。**无轮询**。
- 超时双保险：Handle 侧 `await_oneshot` 用调用者给的 `timeout` 做 tokio timeout；actor 侧 deadline 处理是安全网（actor 至少每个 heartbeat tick 被唤醒一次）。
- 取消安全：drop future → oneshot receiver 关闭 → `ack.is_closed()` 剪枝；deadline 兜底。
- 无状态机锁跨 await：actor 路径全同步；worker 路径不参与等待。
- `fail_all_pending` 同样清空 `pending_applied`（返回 `Unrecoverable`）。

### 5.5 为什么不用 `watch::Receiver<ApplyProgress>` 直接给 Handle

- 会暴露内部进度类型、绕过 actor 的 `applied_index` 权威视图，并让 Handle 依赖 runtime 内部 channel 生命周期。
- `Command::WaitApplied` 与既有 Command 面一致，便于取消/失败统一处理；代价仅一次 actor 消息。

---

## 6. 线协议

### 6.1 版本门控原则

- **不 bump `PROTOCOL_MINOR`**：`validate_hello` 拒绝 `peer_minor > self_minor`（`handshake.rs:55`），bump 会让新节点无法与旧节点通信，破坏滚动升级。
- 用 **feature flags + 字段 presence + `unknown_kind`** 做门控：
  - 新客户端在 `Hello.feature_flags` 声明 `multi_get_v1` / `write_index_v1`（信息性）。
  - 旧服务端忽略未知 flag；对 `kind=3` 返回 `unknown_kind`；对 `ForwardReply.commit_index` 缺省。
  - 新客户端收到 `unknown_kind` / 缺省 index → `ArachneError::Unsupported`（**大声失败**，不逐 key 回退）。
  - `Handle::code_to_error` 必须显式把 `"unknown_kind"` 映射到 `Unsupported`（否则会落入 `Unrecoverable`）。
- 旧客户端不会发送新 kind，也不会读新字段 → 天然兼容。

### 6.2 proto 增量

```proto
message ForwardReply {
  // ... existing fields 1-6 ...
  // 写入实际生效的 log index（presence 区分“旧节点/无 index”）。
  optional uint64 commit_index = 7;
  // 批量读的回复（仅 kind=3）。
  MultiGetReply multi_get = 8;
}

message MultiGetReply {
  // 与请求 keys 同序；message presence = Some，缺省 = None。
  repeated BytesValue values = 1;
  uint64 applied_index = 2;
}
```

- `ForwardRequest` 增加 `repeated bytes keys = 7;` 与 `bool linearizable = 8;`：
  - `kind=3` = `MultiGet`（`key` 字段保持单 key 语义；`linearizable` 仅对该 kind 有意义）；
  - `kind=4` = `ProposeWithIndex`（Gate-1 MF1；**不得复用 kind=0**）。旧 leader 对未知 kind 在**执行前**返回 `unknown_kind`，因此 `Unsupported` 诚实地表示「未执行任何写入」。
- 现有 `BytesValue` 已用于 CAS 的 `current_value`，复用即可。

### 6.3 seam / transport 增量

- `ForwardCommand::MultiGet { keys: Vec<Vec<u8>>, linearizable: bool }`；`ForwardCommand::ProposeWithIndex { cmd, client_id, seq_no }`。
- **一致性必须随 wire 传递**（Phase 1 复核修复）：`ForwardRequest.linearizable = 8` -> `ForwardCommand::MultiGet { linearizable }` -> leader sink 映射为 `Command::MultiGet { consistency: Linearizable|Stale }`。**禁止**把 forwarded `Linearizable` 硬编码成 `Stale`（那会绕过 ReadIndex，使线性读退化为陈旧读）。
- `ForwardOutcome`：
  - `index: Option<LogIndex>`（写 index；absent = 旧节点/不可用）。
  - `multi: Option<(Vec<Option<Vec<u8>>>, LogIndex)>`（批量读；absent = 非批量读/旧节点）。
- `transport.rs::do_forward`：kind 3 + keys 编码；成功时解码 `multi_get` 与 `commit_index`。
- `server.rs::forward`：kind 3 → `ForwardCommand::MultiGet`；成功时编码 `multi_get` 与 `commit_index`。
- `Handle::forward_request`：`Request::ProposeWithIndex` → `ForwardCommand::ProposeWithIndex`（kind=4）；`Request::MultiGet{Linearizable}` → `ForwardCommand::MultiGet`（kind=3）。
  - **不允许**把 `ProposeWithIndex` 映射到 kind=0：旧 leader 会先执行写入、再因缺 `commit_index` 报 `Unsupported`，形成「报错但已提交」。kind=4 让旧 leader 在执行前拒绝（MF1）。
  - `commit_index` presence 检查保留为 defense-in-depth：新服务端回 ok 但缺 index 视为协议 bug。
- `Request` 枚举增加 `ProposeWithIndex` 与 `MultiGet`（内部）。

---

## 7. 快照与数据格式

### 7.1 变更

- `SessionRecord` 的 index 必须持久化：
  - `KV_SNAPSHOT_VERSION` **2 → 3**。
  - session 编码：在现有 outcome tag 之后追加 8 字节 LE `effective_index`（或先 index 后 outcome，二选一，实现时固定）。
- 旧 v2 snapshot → `MalformedSnapshot` **fail-stop**；绝不把缺失 index 迁移成 0（0 会流入公开 API 并被新快照永久传播）。

### 7.2 storage `FORMAT_VERSION` 是否 bump（待 Gate-1 裁决，推荐 bump）

- **推荐**：`FORMAT_VERSION` **3 → 4**。
  - 理由：与 v0.3.0 的先例一致（格式变更 = 数据目录破坏性升级），旧目录在 `WalStorage::open` 即 fail-stop，operator 得到的是清晰的「格式不匹配」而不是启动后期的 restore 失败。
  - 代价：需要新增/修订 runbook（`runbook-format-v4-upgrade.md`），并更新 `plan-vs-etcd.md` 的格式账。
  - **必须写清**：本次**没有新 opcode**，WAL command 流对旧二进制仍可解析；破坏性来自 **session 快照格式 + 升级策略**，不是 opcode 不兼容。避免格式账失真（Gate-1 Q2）。
- **备选**：只 bump `KV_SNAPSHOT_VERSION`，不 bump storage 版本；旧目录在 restore 时 fail-stop。省一次数据目录版本，但 operator 诊断路径更晚、更隐晦。
- 无论选哪个，都必须更新：
  - `dev-docs/runbook-format-v3-upgrade.md`（或新增 v4）
  - `storage/meta.rs` 的版本注释
  - `docs/` 的升级说明

### 7.3 为什么不把 index 放进 `ApplyOutcome`

- `ApplyOutcome` 是 seam 类型，直接进入 `ForwardResult`（proto）。扩展它会把「写 index」耦合进所有 outcome 消费者，并扩大 wire 变更面。
- 用 `SessionRecord` 承载 index、用 apply 的额外返回值承载 effective index，边界更窄。

---

## 8. 兼容性与版本

- **crate**：`0.4.0 → 0.5.0`（新增 API + 新错误变体 + 数据格式变更；0.x minor 允许）。
- **API**：所有既有方法签名/语义不变；新增方法为 additive。
- **错误**：新增 `ArachneError::Unsupported(String)`（旧 peer 不支持新能力）。若 Gate-1 否决新增变体，备选映射为 `Unrecoverable("peer does not support ...")`，但语义较弱。
- **wire**：不 bump minor；新增 proto 字段/kind；presence/`unknown_kind` 门控。
- **数据目录**：按 §7 结论执行破坏性升级；滚动升级期间禁止新 leader 向旧节点发新格式快照（既有 runbook 规则延续）。

---

## 9. 风险清单

| # | 风险 | 缓解 |
|---|---|---|
| R1 | `multi_get` 读锁内克隆 4 MiB 造成读尾延迟 | 上限硬约束；只在锁内做 `BTreeMap` 查找 + clone；超限早退；bench 观测 p99 |
| R2 | `Linearizable` 泛化 `PendingRead` 触碰读热路径 | 保持 C3/retry/gate 语义不变；INV14 + `read_latency` + `read_concurrency_off_actor` 回归门 |
| R3 | session index 格式变更导致旧目录静默污染 | fail-stop；不迁移为 0；runbook + 版本 bump |
| R4 | wire 字段缺省被误读成合法 index 0 | `Option<Index>` + `optional` presence；absent → `Unsupported`；测试构造旧 reply |
| R5 | actor 等待队列泄漏 | `ack.is_closed()` 剪枝 + deadline；上限 `Busy` |
| R6 | `wait_applied` 使用 raft applied 而非 SM applied 导致语义漂移 | 明确使用 `sm.applied_index()`；ConfChange 已以 no-op 占位推进 SM index |
| R7 | `multi_get` 返回的 applied_index 与 values 撕裂 | 单读锁 + version-first；并发写测试断言 value 内嵌 index |
| R8 | 超限语义不一致（条目/字节/单 key） | 统一 `InvalidArgument`；不截断；文档 + 测试 |
| R9 | `ProposeWithIndex` 复用 kind=0 导致「报错但已提交」 | 独立 kind=4；旧 leader 执行前 `unknown_kind`；测试旧服务端路径（MF1） |
| R10 | in-flight MultiGet payload 无字节背压（4096×4 MiB） | 共享 `MultiGetBudget`（16 MiB），Handle/sink/actor 三段预留；超限 `Busy`（MF2） |

---

## 10. 验证计划（概要；详见开发计划）

- **单元**：`multi_get` 顺序/重复/缺键/空集/原子 index/超限；`SessionRecord` snapshot v3 往返；dedup 返回原始 index。
- **运行时/集成**：单节点 `multi_get(Stale)` 与 `get_stale_with_index` 对齐；并发写无撕裂；`put_with_index`/`multi_put_with_index`/`delete_with_index` 的 index 断言；`wait_applied` 立即/唤醒/超时/index 0/取消；`multi_get(Linearizable)` leader/follower 重定向。
- **3 节点**：follower 写 → leader 读 index → 另一 follower `wait_applied`；杀 leader 重选；少数派 `multi_get(Stale)` + `wait_applied` 超时。
- **wire**：`commit_index` / `multi_get` 字段往返；旧 reply（无字段）→ `Unsupported`；`unknown_kind`。
- **负载**：24k keys、100–500B、batch 256–512；读命令数 ≤ ~100。**必须用独立计数器** `arachne_multi_get_commands_total`（Gate-1 Q7），不能用「测试自己循环次数」的自证断言。
- **回归**：`watch`、`get_stale_range`、`multi_put_range`、`stale_read_with_index`、`read_*`、`quorum_loss`、`sessions` 全绿；`cargo test --workspace`；`scripts/check-*`。

---

## 11. Gate-1 裁决结果（@oracle attempt 1/3，2026-11-06）

**Verdict: APPROVE WITH CHANGES**。以下 Q1–Q7 已裁决；MF1/MF2 已并入 §5.2/§6.3，其余 should-fix 已并入 §5.2/§5.3/§6.1/§7.2/§9/§10。

| # | 裁决 | 落地 |
|---|---|---|
| Q1 | **是**：`SessionRecord{outcome,index}` + KV snapshot v3；apply 返回值携带 effective index（无 post-apply 回查） | §5.3、§7.1 |
| Q2 | **是**：`FORMAT_VERSION` 3→4；runbook 必须声明「无新 opcode，破坏来自快照格式+策略」 | §7.2 |
| Q3 | **是**：本轮实现 wire `Linearizable` multi_get（kind=3）；热路径风险由 `PendingReadKind` 单枚举 + 回归门控制 | §5.2、§6.2 |
| Q4 | **`InvalidArgument`**（三类可区分消息）；结果字节上限只能 post-read 检查；不截断、不用 `Busy` | §5.2 |
| Q5 | **是**：新增 `ArachneError::Unsupported`；文档标明 terminal-not-retryable 且「未执行」 | §8、§6.1 |
| Q6 | **SM applied index**；已验证 ConfChange 以 empty no-op 推进 SM index，无洞 | §4.3、§5.4 |
| Q7 | **需要**：`arachne_multi_get_commands_total` | §10 |

**Must-fix（已并入）**：
- MF1：index 写使用独立 wire kind=4 + `ForwardCommand::ProposeWithIndex`；禁止 kind=0。
- MF2：`MultiGetBudget` 16 MiB，Handle/sink/actor 预留；超限 `Busy`。

**Should-fix（已并入）**：`PendingReadKind` 单枚举；`unknown_kind→Unsupported`；M5 措辞；Phase 1 门禁加 `read_batch.rs`/`read_index.rs`；Phase 2 加同批同 key 并发 `put_with_index` 测试 + v3 fuzz + nightly target 名；Q7 计数器；`multi_get` 独立 redirect 循环 + send+await 超时；runbook「无新 opcode」；下游文档补「原始 index 仅 TTL+grace 内保证」。

---

## 12. 交付顺序（与请求一致）

1. `multi_get` + `MultiGetResult.applied_index`（Stale 主交付；Linearizable 复用 ReadIndex）。
2. `put_with_index` / `multi_put_with_index` / `delete_with_index`（含 session index + wire）。
3. `wait_applied`。
4. 可选：Hydra 侧改用 `watch` 做缓存物化（不在本仓范围）。

每个阶段完成后进入 @oracle 阶段门禁，再进入下一阶段。
