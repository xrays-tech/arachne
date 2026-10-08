# Arachne-KV 暴露提交序号（commit index）—— 设计文档

> 响应：`dev-docs/arachne-kv-commit-index-request.md`（hydra / ADR-0001「观察」节）。
> 状态：**定稿**（经 @oracle 架构评审，评审意见已并入 §4/§5/§9）。
> 关联决议：propsol v0.2.1 **Q5**（「首个用户反馈」触发重审）——本设计是其落地：以「附着在 value 上的溯源序号」原语替代 Q5 备选的 per-handle 单调水位。

---

## 1. 背景与触发

- 下游缺陷链：`get_stale` 非单调（propsol N1）→ 节点物化「旧树」回滚已发布写入 → 后续 publish 基于回滚后的库 → 丢行成为最终头的一致形态（负载机约 1/3，空闲 0/5）。
- 下游四个 workaround 全部失败；结论：**顺序信息只能来自共识层**。
- 请求原语：`get_stale_with_index`（B，首选）或 `put` 返回 Index（A）；`Index = u64`，跨 key、跨节点可比。

## 2. 目标 / 非目标

### 目标

- **G1** 新增 stale 读原语（下游 B，主交付）：
  ```rust
  pub async fn get_stale_with_index(&self, key: &[u8])
      -> Result<Option<(Vec<u8>, u64)>, ArachneError>;
  ```
  返回的 index 描述「这个 value 是哪条 log entry 写入的」，供下游在不做仲裁的前提下跨调用、跨节点排序。
- **G2** index 语义：**per-key 最后一次写入的 log index（value 溯源）**；同一 raft group 内跨 key、跨节点可比；per-key 单调（value 演化 → index 严格递增）。
- **G3** 保持 `get_stale` 成本/可用性不变：本地读、无仲裁、无额外 RTT、任意节点可用。
- **G4** 纯 additive：不改 `put`/`get`/`get_stale` 签名与语义。
- **G5** 线协议零改动：v0.3.0 的 B 不触碰 proto / ForwardOutcome / Hello（见 §7）。

### 非目标

- 不改 `get`（线性读）语义；不加 lease；不加新一致性级别。
- 不做 per-handle「已见水位」单调读变体（Q5 备选，被本原语替代）。
- **`put_with_index`（下游 A）不在 v0.3.0 交付**，缓后；其设计要点见 §9.1（含并发 ack 修正）。
- `delete` 不改签名、不给序号（缺键 = `None`，无 index）。删除语义的隐藏洞见 §3.3。
- 不给 `get` 附 index（下游不要求；`get` 的线性保证不传递给随后的 stale 读——文档明示）。
- 不收紧 tonic Hello 握手。

## 3. Index 语义（正确性裁决）

### 3.1 定义

`Index(key) := 使 key 当前 value 生效的那条 log entry 的 index`。由 raft 全局分配：

- 同一 raft group 内，同一 log index 唯一对应一条 entry、一个 key、一个值 → **`index 相等 ⟹ 值相同`**（除非 delete，见 3.3）；这使下游 `>=` 判据对「只写不删」的 key 天然幂等安全。
- per-key 单调：key 每被 `Put` 改写，index 严格递增。
- **跨集群 / 跨 raft group 不可比**（文档明示）。

### 3.2 为什么不是「节点 applied 水位」

`store.applied` 单调，但「值可以旧」：stale 节点 catch-up 到 12 而 key 的值是 index 5 写的，回 `(旧值, 12)` 会让下游误判新鲜 → 不拒绝 → 覆盖新状态。与下游否掉的 `revision=head+1` 同因：**非权威的序不是序**。只有锚定 value 出处的 index 是权威序。

### 3.3 删除语义（隐藏洞，必须书面约束）

`>=` 判据仅在 **key 的历史是纯 Put（两次写之间无 Delete）** 时成立。若 key 在某 index D 被删，客户端的溯源水位不会被推进，而一个未 apply 到 D 的 stale 节点仍会回 `(旧值, i)`，`i ≥ 水位` 时下游会**复活已删值**。

- v0.3.0 处理：`get_stale_with_index` 对缺键回 `None`；在公开文档中明示「**有序判据只适用于只写不删的 key**」（hydra 的整树替换模型恰好满足：替换是 re-put，从不 delete 树内 key）。
- 后续选项（下游若出现硬依赖再立项）：delete 附 index / tombstone 推进水位。

### 3.4 多 key 非原子（世代用 head 承担）

per-key stale 读**非原子**（每 key 独立取读锁），head 与实体各自的 index 间可被新 publish 插入；**不能把实体 index 与 head index 混比作「整树版本」**。正确用法（与 hydra 现有模型一致）：**以 head key 的 index 作为树世代**，实体 index 仅作信息/单 key 判据。此条必须写进下游对接说明。

## 4. API 设计（v0.3.0 = 仅 B）

```rust
/// Stale 读 + value 的溯源序号。
/// Ok(None)      —— 本节点状态机中该 key 不存在（任意陈旧语义不变）。
/// Ok(Some((v, i))) —— v 是本地已 applied 的值；i = 写入 v 的 log index（>= 1）。
/// 与 get_stale 同级：本地直接读数、无仲裁、任意节点可用、不保证单调（N1）。
pub async fn get_stale_with_index(&self, key: &[u8])
    -> Result<Option<(Vec<u8>, u64)>, ArachneError>;
```

- 保留 `get_stale` 原样；本方法为纯新增，**零破坏**。
- facade `Arachne` 同步提供同名方法（委托到本地 Handle，embedding 一致性）。
- **公开 API 永不出现 index 0**：缺键 = `None`；present 值的 index ≥ 1（apply 从 1 起）。不做 sentinel（见 §5.4 裁决）。
- 不新增 `Command` / 不改 ack 结构：`get_stale` 的本地读路径（`Command::GetStale` → 状态机 `get`）加一个读变体即可，actor 主循环无需改动共识/写路径。

## 5. 内部机制

### 5.1 状态机（`arachne/src/state_machine/kv.rs`）

- `Store.kv`: `BTreeMap<Vec<u8>, Vec<u8>>` → `BTreeMap<Vec<u8>, Entry { val, index: LogIndex }>`（单条目承载，快照单一数据源）。
- `apply(index, cmd)`：`Put` 分支写锁内 `entry = Entry { val, index }`（沿用 D-Ord：先改 map 后 bump `applied`）。
- 新增读取变体 `get_with_index(key) -> (Option<Vec<u8>>, Option<LogIndex>)`；`get` 保持不变。
- 会话去重不变：重放命中缓存、不回写 → index 不变（对 B 无副作用：读的 index 始终与 store 里的 value 同源）。

### 5.2 运行时（`arachne/src/runtime/mod.rs`）

- 仅新增 `Command::GetStaleWithIndex { key, ack: oneshot<Result<Option<(Vec<u8>, u64)>, ArachneError>> }`，在 `GetStale` 分支旁用同一本地读路径应答。
- **不触碰** `Command::Propose` / `ReplyPendings` / `Pending.index`（写路径 v0.3.0 零改动）。

### 5.3 线协议

- **零改动**：本机 `get_stale_with_index` 由 Handle 直接发往本地 actor，不经 Forward RPC；服务端 gRPC `Forward` 无新 kind、`ForwardReply` 无新字段、`Hello` 不动。
- 多进程 `arachne-node` 的 HTTP 表层如需暴露，直接调 facade/Handle 本地方法即可（无 wire 往返）。

### 5.4 快照 / 持久化（关键裁决）

- kv 快照 payload（`KvStateMachine::snapshot()`）编码：
  - **payload 首字段加入版本字节**（本状态机私有格式版本，独立于 storage 层 `FORMAT_VERSION`），其后每条 kv 在 `[klen][k][vlen][v]` 后追加 8 字节 `index`（LE）。
  - 目的：让状态机格式可以独立演进/可检测，不冒充 storage 版本。
- **storage `FORMAT_VERSION` bump**（storage/snapshot.rs:9-19 + wal.rs:68 既有共享版本）：
  - 仓库惯例 = protocol major 内冻结、同 major 可回滚 → 本变更 bump 即**数据目录破坏性升级**：旧目录在 `WalStorage::open` 时对旧 `META` fail-stop（wal.rs:605），不会进入「静默恢复旧快照」路径。
  - 因此 **不做「旧快照 → index 0」迁移**（评审否决：0 会说成合法 index 流入公开 API、且会被新快照持久化并跨节点传播成永久污染）。被压缩进旧快照的写无法从 WAL 重算 → 数据目录层面强制重播种/迁移，方向安全。
- **升级操作约束**：滚动升级期间**禁止新 leader 把新格式快照发给旧节点**（旧节点无法解析、无能力协商）。配合 FORMAT_VERSION bump，实际以「整集群停机 → 重播种/迁移数据 → 起新版本」为推荐路径；若做滚动，需快照能力门控 + 先追上再做快照。此条写入运维文档。

## 6. 语义/不变量清单

| # | 不变量 | 层 |
|---|---|---|
| I1 | 同节点连续 `get_stale_with_index(k)`（无改写）→ index 非降；改写后严格递增 | KV SM 单测 + 集成 |
| I2 | 追平后各节点 `get_stale_with_index(k)` 报同一 index（跨节点可比） | 3-node 集成 |
| I3 | 重放（同 client_id/seq_no）不改变 store 中 value 的 index | KV SM 单测 |
| I4 | 新格式快照 → restore 往返保 index | KV SM 单测 |
| I5 | 旧 payload / 旧 FORMAT_VERSION → **fail-stop**；公开 API 永不出现 index 0（present ⇒ ≥1） | 单测（构造旧字节断言拒绝） |
| I6 | 缺键 / delete 后 → `None`；delete 后重新 put → index 高于历史（空行程） | KV SM + 集成 |
| I7 | 多 key 非原子：头/实体 index 混比无「整树版本」语义（§3.4，文档断言） | 文档 + 下游对接说明 |
| I8 | `get`/`get_stale`/`put` 语义、成本、签名不变（回归门禁：现存测试零改动通过） | 全仓测试套件 |
| I9 | 同 raft group 内可比；跨集群不承诺（文档） | 文档 |

## 7. 版本与兼容

- **API**：additive → `arachne/Cargo.toml` 0.2.0 → **0.3.0**（workspace 同步，跟随既有 release 惯例）。
- **语义版本号与数据格式版本是两回事**：crate 0.3.0 ≠ 数据目录可原地升级。数据目录按 **storage FORMAT_VERSION bump** 破坏性升级（重播种/迁移）。
- **线协议**：本轮零改动 → 无滚动升级 wire 风险（滚动的唯一风险在快照格式，见 §5.4）。

## 8. 验证计划

- 单测（`state_machine/kv.rs`）：I1、I3、I4、I5（构造旧 payload 断言拒绝、present index ≥1）。
- 集成（`arachne/tests/`）：I2（3 节点写 → 各节点同 index）；I6（delete→None→再 put 递增）；既有 `get_stale` 相关测试（`client_runtime.rs:86`、`quorum_loss.rs:265-272`、`three_node_client.rs:177,216`、`server_facade.rs`）零改动通过（I8 门禁）。
- `test-plan-v0.1.md` 对齐：S02/S16 弱读场景补「附 index」断言 + 新增 INV（编号实现期定）。
- 多进程 `arachne-node`：HTTP 表层 `get_stale_with_index` 冒烟。
- **下游三方抽验**：hydra 以「head index ≥ 已应用值 index 才物化」接入，按其验收流程回执（含仅有 put、不 delete 有序键的用法约束核对）。

## 9. 后续（不在 v0.3.0）

### 9.1 `put_with_index`（下游 A）—— 已设计、缓后

- 语义：返回「本次写入的 value 的溯源 index」（= 追平后 `get_stale_with_index` 同 key 报告值），覆盖「写下即记录、免回读」。
- **并发修正（评审要点）**：ack **不能**「apply 后读 store 的 entry.index」——同一批 apply 中两个并发 put 时，先者的 reply 唤醒会读到后者的 index。正确做法：`apply()` 返回值携带「本次实际生效后的 per-key index」（dedup 命中回缓存/原始 index），经 apply-task→actor 进度回传，而不是 ack 时回查 store。
- 需要改动：新 `Command::PutWithIndex` + ack 类型带 u64 + `ForwardReply` 加 optional `commit_index` 字段（proto3 add、默认 0 = 缺省，与「缺失」显式区分）+ `ForwardOutcome.index`。滚动期旧 leader 回缺省的场景需与新节点的「真 index 0（不可能出现）」语义分离。
- 交付价值：写路径免一次回读；但功能上可被「put 后跟一次 `get_stale_with_index`」覆盖 → 优先级低。

### 9.2 delete 附 index / tombstone

下游若对「被删的有序键」产生硬依赖再立项（§3.3）。

## 10. 相关参考

- 请求正文：`dev-docs/arachne-kv-commit-index-request.md`
- 缺陷证据：`dev-docs/aegis/plans/2026-10-05-arachne-control-plane.md`（ADR-0001「观察」节）
- propsol v0.2.0：N1（§2.2, L707）、读路径（§5.4, L909）、Q5（§D, L51/L1222）
- 代码落点：`state_machine/kv.rs`｜`runtime/mod.rs`（GetStale≈L236、读回复≈L1864）｜`client/handle.rs`（get_stale≈L292）｜`server/mod.rs`（facade≈L209）｜`storage/{snapshot.rs:9-19, wal.rs:605, meta.rs}`｜`seam/forward.rs`(ForwardOutcome≈L76-82)｜`proto/raft.proto`(ForwardReply≈L129-139)
- 评审记录：本文件 §3/§5.4/§9.1 已并入 @oracle 逐条裁决（exp/oracle 会话归档于本次工作记录）
