# 开发计划：批量精确读 + 写入/应用索引可见性

> 关联设计：`dev-docs/arachne-kv-batch-read-applied-index-design.md`
> 需求：`dev-docs/arachne-kv-batch-read-applied-index-request.md`
> 基线：`main @ 8976994`（v0.4.0；`get_stale_with_index` / `multi_put` / `watch` 已交付）
> 流程：deepwork；Gate-1（本计划+设计）→ 逐阶段实现 → 每阶段 @oracle 门禁 → 合入。
>
> **Gate-1 结果（@oracle attempt 1/3）**：`APPROVE WITH CHANGES`。MF1（index 写用独立 wire kind=4，禁止 kind=0）与 MF2（`MultiGetBudget` 16 MiB 三段预留）已并入设计 §5.2/§6.3；本计划的 Phase 1/2 验收已同步加入对应门禁。
> **实现状态**：Phase 1–4 **已完成**（Gate 2/3/4/5 均已满足；0.5.0；工作树待用户请求提交）。

---

## 0. 门禁与执行原则

- **Gate-1（现在）**：@oracle 审核设计/计划，裁决 §11 的 Q1–Q7。未通过不得进入开发。
- **每阶段门禁**：完成后先跑验证，更新 `.slim/deepwork/batch-read-applied-index.md`，再请 @oracle 评审；阶段门禁前必须提供：目标、改动路径、验证证据、待评审决策/风险。
- **回归门禁**：每个阶段结束至少跑
  - `cargo test -p arachne-kv`（含集成）
  - `cargo build -p arachne-kv-transport-tonic -p arachne-kv-node`
  - `cargo test -p arachne-kv --no-default-features`（lean core）
  - 触及读路径的阶段额外跑 `read_latency` / `read_concurrency_off_actor` / `inv14_deposed_leader`
  - 触及格式的阶段额外跑 `scripts/check-release-features.sh`（发布前）与格式 runbook 自检
- **提交**：每个阶段是独立可交付边界，阶段门禁通过后创建一个聚焦 commit。
- **并行**：同一文件/模块只允许一个写 owner；跨层工作按 phase 串行，phase 内可拆 bounded lanes。

---

## Phase 1 — `multi_get`（最高优先级）

### 目标
- `Handle::multi_get(keys, ReadConsistency)` + `MultiGetResult` + `Index` 类型。
- `Stale` 本地批量读：单 read lock、单 `applied_index`、保序、缺键 `None`、超限拒绝。
- `Linearizable` 复用 ReadIndex（泛化 `PendingRead`）。
- facade 同名静态方法。

### 改动面
| 文件 | 改动 |
|---|---|
| `arachne/src/state_machine/kv.rs` | `multi_get(&[&[u8]]) -> (Vec<Option<Vec<u8>>>, LogIndex)`；字节预算；`KvError::TooLarge`（内部） |
| `arachne/src/runtime/mod.rs` | `Command::MultiGet`；`spawn_multi_get`；`PendingRead` 泛化为**单枚举** `PendingReadKind{One{key,ack},Many{keys,ack}}`；`resolve_reads` 分派；`fail_all_pending` 兼容；共享 `MultiGetBudget`（16 MiB，Handle/sink/actor 三段预留） |
| `arachne/src/metrics.rs` | `arachne_multi_get_commands_total`（Gate-1 Q7） |
| `arachne/src/client/handle.rs` | `multi_get`；校验（entries/key bytes/总字节）；`Request::MultiGet`；`forward_request`/`reach_*` 分支 |
| `arachne/src/client/mod.rs` | `ReadConsistency`、`MultiGetResult` |
| `arachne/src/lib.rs` | 根导出 `Index` / `ReadConsistency` / `MultiGetResult` |
| `arachne/src/server/mod.rs` | `Arachne::multi_get` 委托 |
| `arachne-seam/src/seam/forward.rs` | `ForwardCommand::MultiGet`；`ForwardOutcome.multi` |
| `arachne-transport-tonic/proto/raft.proto`、`transport.rs`、`server.rs` | kind=3 + `MultiGetReply` encode/decode |
| `arachne/tests/` | 新 `multi_get.rs`（单节点/并发/上限/Linearizable） |

### 验收
- [x] `multi_get(Stale)` 在单节点与 follower 返回保序结果，`applied_index` 与同锁读一致。
- [x] 空 keys、重复 keys、缺键、单 key、4096 边界行为符合设计；三类超限（entry count / request key bytes / result value bytes）返回**可区分**的 `InvalidArgument`，不截断。
- [x] `MultiGetBudget` 生效：并发大批 `multi_get` 超过 16 MiB in-flight 时返回 `Busy`，不 OOM（MF2）。
- [x] 并发写 + `multi_get` 无撕裂：构造 value 内嵌写入 index，断言每个 value ≤ `applied_index` 且对应。
- [x] `multi_get(Linearizable)` 在 leader 成功；follower → `NotLeader` 重定向；少数派 → `QuorumUnavailable`。
- [x] 旧 wire reply（无 `multi_get`）→ `Unsupported`；`unknown_kind` 路径有测试；`code_to_error("unknown_kind") == Unsupported`。
- [x] `arachne_multi_get_commands_total` 在每次 `multi_get` 命令处理时递增（Q7）。
- [x] Handle 的 timeout 包住 send+await：满 commands 队列时 `multi_get` 返回 `Timeout`，不挂起。
- [x] 既有弱读/范围读/watch 测试零改动通过。
- [x] **Gate-1 增补回归**：`read_batch.rs`（C3 单轮共享）与 `read_index.rs`。
- [x] `read_latency` / `read_concurrency_off_actor` / `inv14_deposed_leader` 绿。

### Gate-1 后立即决策
- 若 Q3 选择「远程 `Linearizable` 暂不支持」，则 Phase 1 的 wire 部分缩减为 `Unsupported` 分支 + 测试，`MultiGetReply` 留到后续。

---

## Phase 2 — 写入返回 index

### 目标
- `put_with_index` / `multi_put_with_index` / `delete_with_index`。
- session 记录携带 effective index；dedup 返回原始 index。
- follower 转发返回 leader index；旧 peer → `Unsupported`。
- 快照格式升级（按 Gate-1 Q1/Q2）。

### 改动面
| 文件 | 改动 |
|---|---|
| `arachne/src/state_machine/kv.rs` | `SessionRecord{outcome,index}`；`apply_with_events_and_index`；snapshot/decode v3；`KV_SNAPSHOT_VERSION 2→3` |
| `arachne/src/runtime/mod.rs` | `outcomes` 携带 effective index；`received_outcomes` 二元组；`Command::ProposeWithIndex`；`PendingReply` 枚举；`reply_pendings` 分派 |
| `arachne/src/client/handle.rs` | 三个新方法；`Request::ProposeWithIndex`；`forward_request` 读取 `ForwardOutcome.index`；缺省 → `Unsupported` |
| `arachne/src/client/mod.rs` | `ArachneError::Unsupported`（按 Q5） |
| `arachne/src/server/mod.rs` | facade 三个委托 |
| `arachne-seam/src/seam/forward.rs` | `ForwardOutcome.index` |
| `arachne-transport-tonic/proto/raft.proto`、`transport.rs`、`server.rs` | `optional uint64 commit_index = 7` 编解码 |
| `arachne/src/storage/meta.rs` | `FORMAT_VERSION 3→4`（按 Q2） |
| `dev-docs/runbook-format-v4-upgrade.md` | 新 runbook（或修订 v3） |
| `arachne/tests/` | 新 `write_index.rs`；`sessions.rs`/`write_outcome.rs` 增补 |

### 验收
- [x] `put_with_index` 返回值 == 同 key 立即 `get_stale_with_index().1`。
- [x] `multi_put_with_index` 整批共享同一 index；每个 key 的 origin index 相同。
- [x] `delete_with_index` 返回 delete entry index；再 put 的 index 更大。
- [x] 同 `(client_id, seq_no)` 重放（`propose_raw` 或新增 hidden seam）返回**原始** index。
- [x] follower 转发写入返回 leader 的 index（与 leader 本地 `get_stale_with_index` 对照），且使用 **kind=4**；旧 leader 在执行前返回 `unknown_kind` → `Unsupported`（MF1）。
- [x] **同批同 key 并发**：两次 `put_with_index` 落在同一 apply batch 时，各自返回**不同的 entry index**（专门捕获 §9.1 的 post-apply 回查陷阱）。
- [x] 旧 reply（无 `commit_index`）→ `Unsupported`，不伪造 0；新服务端 ok 但缺 index → 协议 bug 断言。
- [x] snapshot v3 round-trip 保 session index；v2 旧 payload → fail-stop。
- [x] v3 session 编码纳入 `fuzz_targets/kv_snapshot_restore.rs`，并更新 nightly CI 的 target 名（followups #6 的硬编码陷阱）。
- [x] 旧目录按 Q2 结论 fail-stop；runbook 可执行，且写明「本次无新 opcode」。
- [x] 既有 `put`/`multi_put`/`delete`/CAS/session 测试零改动通过。

---

## Phase 3 — `wait_applied`

### 目标
- `Handle::wait_applied(index, timeout)`；facade 同名。
- 本地、无 quorum、少数派可用；立即/唤醒/超时/取消安全。

### 改动面
| 文件 | 改动 |
|---|---|
| `arachne/src/runtime/mod.rs` | `Command::WaitApplied`；`pending_applied`；`handle_command` 立即路径；`resolve_applied_waits`；`fail_all_pending`；`MAX_PENDING_APPLIED_WAITS` |
| `arachne/src/client/handle.rs` | `wait_applied`（本地 tx，不 redirect；`await_oneshot` 用调用者 timeout） |
| `arachne/src/server/mod.rs` | facade 委托 |
| `arachne/tests/` | 新 `wait_applied.rs` |

### 验收
- [x] 已追平 → 立即返回当前 applied（≥ index）。
- [x] 落后 → 在 apply 推进到 index 后返回；无 1 Hz 轮询（用 `Progress` 事件驱动，测试用短 heartbeat）。
- [x] 少数派 follower：`wait_applied` 不被 redirect/quorum 阻塞；超时返回 `Timeout`。
- [x] `index == 0` no-op。
- [x] 取消：drop future 后无永久 waiter；后续 `wait_applied` 正常。
- [x] 不持有 SM 锁跨 await（代码审查 + 测试）。
- [x] 3 节点：A 写、B `wait_applied`、C `wait_applied` 均收敛；杀 leader 重选后可重复。

---

## Phase 4 — 集群/负载/回归/文档

> 状态：**已完成**（Gate 5 已审）。文档/版本 lane 与集群/负载 lane 均已收口。

### 目标
- 3 节点故障矩阵 + 24k 负载 + 文档/版本收口。
- 与 Hydra 的对接说明（组合用法与前提）。

### 改动面
| 文件 | 改动 |
|---|---|
| `arachne/tests/` | 3 节点覆盖落在 `wait_applied.rs`（leader kill/重选）与 `three_node_client.rs`（wire kind 3/4）；`batch_read_load.rs`（含 `#[ignore]` 24k）；`write_index_restart.rs` |
| `docs/api-reference.html`、`tutorial.html`、`README.md` | 新 API 与错误矩阵 |
| `dev-docs/` | 设计/计划状态、runbook、格式账、下游对接说明 |
| `Cargo.toml`/members | 版本 `0.5.0`；`PROTOCOL_MINOR` 不变（§6.1） |

### 验收
- [x] 3 节点：follower 写 → leader 读 index → 另一 follower `wait_applied`；杀 leader 重选后 index 可比（`wait_applied.rs::three_node_follower_waits_for_head_index` 的 kill/re-elect/repeat 循环；wire 由 `three_node_client.rs` 覆盖）。
- [x] 少数派：`multi_get(Stale)` 与 `wait_applied` 工作；`wait_applied` 超时而非挂起（`wait_applied.rs::is_local_on_a_minority_follower`）。
- [x] 24k keys / 256–512 batch：materialization 读命令数 ≤ ~100（`batch_read_load.rs`，观测 47）。
- [x] `watch` / `get_stale_range` / 全部既有测试绿（`cargo test --workspace` EXIT 0）。
- [x] `scripts/check-*` 全绿；`check-release-features.sh` 通过；未执行发布。
- [x] 文档/错误矩阵/runbook 与实现一致（文档/版本 lane）。

---

## 3. 角色与并行（deepwork）

| Phase | 主实现 | 并行/评审 |
|---|---|---|
| 1 | @fixer（SM+runtime，bounded） | @oracle Gate-1 后；wire 部分可拆第二个 @fixer（transport/proto） |
| 2 | @fixer（SM+apply 管线） | @fixer（transport/proto）、@oracle 阶段门禁 |
| 3 | @fixer（runtime+handle） | @oracle 阶段门禁 |
| 4 | @fixer（测试/文档） | @explorer（结构扫描）、@oracle 阶段门禁 |

> 实现阶段优先用 @fixer 做 bounded 机械实现；跨层设计/裁决留在 orchestrator + @oracle。读路径/格式变更的测试先写（TDD）再实现。

---

## 4. 预计工作量

| Phase | 估量 | 关键不确定性 |
|---|---|---|
| 1 | M–L | `PendingRead` 泛化 + wire kind=3；读热路径回归 |
| 2 | L | session index 格式 + dedup 语义 + 滚动静默失败门控 |
| 3 | S–M | actor 等待队列/超时/取消 |
| 4 | M | 集群矩阵与 24k 负载 |

---

## 5. Gate-1 裁决结果（见设计 §11；均已落地）

> Q1–Q7 已由 @oracle Gate-1 裁决；MF1/MF2 与 should-fix 已并入设计 §11 并实现。Phase 1–4 的 Gate 2/3/4/5 均已满足。
