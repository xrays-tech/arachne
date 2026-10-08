# Runbook：存储格式 v1 → v2 破坏性升级（`FORMAT_VERSION` 1→2）

> 适用版本：升级到 **arachne-kv v0.3.0**（含`get_stale_with_index`；快照存储 `FORMAT_VERSION` 1→2）。
> 关联：`dev-docs/arachne-kv-commit-index-design.md`（§5.4/§7 升级约束）；`dev-docs/propsol-v0.2.md`（rev AC）。
> 一句话：**v0.3.0 的数据目录不能被 v0.2.x 打开，v0.2.x 的数据目录也不能被 v0.3.0 无缝打开——这是有意的破坏性升级，必须重播种/迁移。**

---

## 1. 这是什么升级

| 项 | 说明 |
|---|---|
| 变更 | 存储 `FORMAT_VERSION` 1 → 2（`arachne/src/storage/meta.rs`）；kv 快照 payload 增加版本字节 + per-key 8B 溯源 index（value 的 log index） |
| 为什么破坏 | `WalStorage::open` 在打开数据目录时校验 `META.format_version`，与 `FORMAT_VERSION` 不一致即 **fail-stop**（`arachne/src/storage/wal.rs` 约 L605）；不提供静默迁移 |
| 何时生效 | 任何 v0.3.0 二进制打开（或从中读取快照）→ 旧格式目录/快照一律拒绝 |

## 2. 硬约束（必须先理解，再操作）

1. **旧数据目录 = 打开即 fail-stop**：`WalStorage` 对 v1 META 直接报错；不存在「v0.2.x 目录原地升 v0.3.0」。
2. **禁止新 leader 在滚动期间把 v2 快照发给旧节点**：旧节点（v0.2.x）无法解析 v2 快照（快照文件 `format_version` 校验拒绝），且新旧格式**无能力协商**——混合版本拓扑中一次跨版本快照传输会让旧节点 fail-stop。
3. **线协议不受影响**：`get_stale_with_index` 是纯 additive API，`Hello` 握手与 `Forward` RPC 未变；**本次破坏仅限数据目录/快照格式，不是 RPC 协议**。

## 3. 推荐路径（整集群停机重播种）

> 适用于：尚无真实存量用户、或可接受一次性迁移成本的部署（当前 arachne-kv 0.3.0 首个发布，推荐此路径）。

1. **停机**：停止全部 `arachne-kv` 进程（写出/读入全部停止）。
2. **保留旧数据**（回滚/审计用）：原 `data_dir` 整体备份（含 `META`、`segments`、`snapshot-*.snap`）。**不要删除**直到新集群验证通过。
3. **重播种**：
   a. 为新 v0.3.0 节点准备全新 `data_dir`（空目录）。
   b. 如需保留业务数据：在旧 v0.2.x 集群/环境中**导出**数据（例如经 `get`/`get_stale` 逐 key 读出），再在 v0.3.0 集群**重新写入**（`put`）。**不要**尝试直接复制 v1 目录或快照文件。
   c. 数据写入后，新格式快照会在 `snapshot_threshold` 触发时自然生成；建议重播种后手动验证一次快照生成（见 §5）。
4. **校验**（新集群）：见 §5。
5. **回滚预案**：若 v0.3.0 校验不通过，恢复第 2 步备份的旧目录 + 旧二进制（v0.2.x）即可回到上一版本（旧目录从未被 v0.3.0 打开过则完整无损）。

## 4. 滚动替代路径（有条件）

> 仅当你能保证「任何时刻都不会产生跨版本快照传输」时考虑；否则走 §3。

前置门控（缺一不可）：
1. 所有节点先在同一版本运行并**全部追平**（无落后 follower）；
2. 升级期间禁止触发快照（或由 supervisor 保证「旧节点收到 v2 快照」不可能发生——无法真正做到，故谨慎）；
3. 新 leader 只在其能确认所有 follower 都是 v0.3.0 后才允许写新格式快照。

风险声明：由于新旧格式无法协商，滚动窗口内一次意外快照传输即导致旧节点 fail-stop。**不推荐**；仅在单节点/全控场景使用。

## 5. 升级后校验（用新原语验证顺序语义）

在新 v0.3.0 集群上：

1. **基础读写**：`put` 后 `get` / `get_stale` 正常。
2. **溯源序号**：`handle.get_stale_with_index(k)` 返回 `Ok(Some((v, i)))` 且 **`i >= 1`**；写两次同一 key 后 index 严格递增。
3. **跨节点一致**：3 节点写一条后，各节点 `get_stale_with_index(k)` 在追平后报**同一 index**。
4. **缺键语义**：未写过的 key 返回 `Ok(None)`；删除后返回 `Ok(None)`（公开 API 永不出现 index 0）。
5. **快照生成**：确认 `snapshot-<index>-<term>.snap` 以 v2 格式产出（`format_version == 2`），且从快照恢复（新节点加入/重启）后 2、3 两项仍成立。

## 6. 运维要点

- **不要**把 v0.3.0 数据目录交给 v0.2.x 二进制打开（`FORMAT_VERSION` 拒绝）——回滚必须整目录回退（§3.5），不能「新目录反降」。
- 混合版本拓扑（v0.2.x 与 v0.3.0 共存）**仅允许在「不触发跨版本快照传输」的前提下**；一旦出现互指 leader 的滚动升级，先按 §4 门控，否则直接停机走 §3。
- 快照体积：v2 格式每条 key 增加 8B，`snapshot_threshold` 触发点会略前移——属预期内权衡，告警阈值无需调整。

## 7. 相关文档

- 设计：`dev-docs/arachne-kv-commit-index-design.md`（§3 index 语义 / §5.4 快照版本化 / §7 版本与兼容）
- 决议：`dev-docs/propsol-v0.2.md`（rev AC：Q5 重审兑现）
- 后续：`dev-docs/plan-commit-index-followups.md`
