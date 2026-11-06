# Runbook：存储格式 v3 → v4 破坏性升级（`FORMAT_VERSION` 3→4）

> 适用版本：升级到 **arachne-kv 0.5.x（Phase 2：写入返回 index）**（含 `KV_SNAPSHOT_VERSION` 3）。
> 关联：`dev-docs/arachne-kv-batch-read-applied-index-design.md`（§7/§11 Gate-1 Q1/Q2）；`dev-docs/plan-batch-read-applied-index.md`（Phase 2）；`dev-docs/runbook-format-v3-upgrade.md`（上一版，重播种路径同款）。
> 一句话：**本次没有新 opcode；破坏性来自 KV session 快照格式 + 升级策略。旧数据目录在新二进制启动打开时即 fail-stop，推荐整集群停机 + 重播种/迁移；任何情况下新 leader 都不得把 v3 快照发给旧节点。**

---

## 1. 这是什么升级

| 项 | 说明 |
|---|---|
| 变更 | 存储 `FORMAT_VERSION` 3 → 4（`arachne/src/storage/meta.rs`）；KV 快照 `KV_SNAPSHOT_VERSION` 2 → 3（每条 session 记录在原有 outcome 编码后追加 8 字节 LE **effective index**，`arachne/src/state_machine/kv.rs`） |
| 为什么破坏 | `WalStorage::open` 校验 `META.format_version`，不一致即 **fail-stop**（`arachne/src/storage/wal.rs`）；session 快照缺少 index 时无法安全恢复（绝不能把缺失 index 迁移成 0） |
| **不**含什么 | **没有新增命令 opcode**。WAL 中的 `Put`/`Delete`/`MultiPut`/`Cas`/`SessionGc` 命令流对 v3 二进制仍可解析；v4 新写的 session 快照才需要新二进制 |
| 何时生效 | 新 v4 二进制打开 v3 数据目录 → 启动即拒绝；旧 v3 节点收到 v4（`KV_SNAPSHOT_VERSION=3`）快照 → 恢复失败 / fail-stop |

### 1.1 与 wire 的关系

`ProposeWithIndex` 使用独立的转发 kind（不复用普通 `Propose`），旧 leader 在执行前返回 `unknown_kind`，新客户端映射为 `Unsupported`（“未执行”）。这是 wire 兼容门控，不是存储格式的一部分；但滚动期间仍应把「新 leader 不得给旧节点发 v3 快照」作为硬约束（见 §2）。

---

## 2. 硬约束（必须先理解，再操作）

1. **旧数据目录 = 打开即 fail-stop**：不存在「v3 目录原地升 v4」。新二进制在 `WalStorage::open` 读到 `format_version=3` 会直接报格式不匹配。
2. **session index 不可伪造**：v2（`KV_SNAPSHOT_VERSION=2`）快照没有 per-session effective index。恢复时拒绝（`MalformedSnapshot`），绝不迁移成 `index 0`——否则 `0` 会作为合法值流入 `put_with_index`/`delete_with_index` 的公开返回并被新快照永久传播。
3. **禁止新 leader 向旧节点发送 v3 快照**：新旧快照格式无能力协商。滚动窗口中一次跨版本快照传输就会让旧节点 fail-stop。**默认按 §3 整集群停机。**
4. **回滚 = 整目录回退**：一旦 v4 打开并写过数据目录，不能把该目录交回 v3 二进制（`FORMAT_VERSION` 拒绝）。回滚必须恢复升级前的整目录备份 + 旧二进制。

---

## 3. 推荐路径（整集群停机重播种/迁移）

> 当前 0.5.x 发布推荐此路径；适用于可接受一次性迁移成本的部署。

1. **停机**：停止全部 `arachne-kv` 进程（写入/读取全部停止）。
2. **保留旧数据**（回滚/审计用）：原 `data_dir` 整体备份（含 `META`、`segments`、`snapshot-*.snap`）。**不要删除**直到新集群验证通过。
3. **重播种或迁移**：
   a. 为新 v4 节点准备全新 `data_dir`（空目录）。
   b. 如需保留业务数据：在旧集群/环境中**导出**数据（经 `get`/`get_stale`/`get_stale_with_index` 逐 key 读出），再在 v4 集群**重新写入**（`put`/`multi_put`/`put_with_index`）。**不要**直接复制 v3 目录或快照文件。
   c. 数据写入后，新格式快照会在 `snapshot_threshold` 触发时自然生成；建议重播种后手动验证一次快照生成（见 §5）。
4. **校验**（新集群）：见 §5。
5. **回滚预案**：若 v4 校验不通过，恢复第 2 步备份的旧目录 + 旧二进制（v3）即可回到上一版本（旧目录从未被 v4 打开过则完整无损）。

---

## 4. 滚动替代路径（不支持；仅单节点/全控场景）

v4 与前版**无快照能力协商**（session 快照格式变更）。滚动升级窗口内任何一次新 leader 生成/传输 v3 快照，都会让旧节点恢复失败。**不推荐**；仅当单节点、且能保证旧二进制永远不会读取 v4 产物时才考虑，且必须视为一次性迁移而非长驻混合拓扑。

若必须短暂共存：

1. 所有节点先运行旧版本并**全部追平**；
2. 升级期间禁止触发/传输快照（无法真正保证，风险自负）；
3. 新 leader 只在其能确认所有 follower 都是 v4 后才允许写新格式快照；
4. 一旦出现跨版本快照传输，立即按 §3 停机。

---

## 5. 升级后校验（用新原语验证语义）

在新 v4 集群上：

1. **基础读写**：`put` 后 `get` / `get_stale` 正常；`get_stale_with_index` 返回 `Ok(Some((v, i)))` 且 `i >= 1`。
2. **index-returning 写**：
   - `put_with_index(k, v)` 返回的 index == 随后 `get_stale_with_index(k)` 的 origin index；
   - `multi_put_with_index` 整批共享同一 index，且每个 key 的 origin 都等于它；
   - `delete_with_index` 返回 delete entry 的 index；之后的 put 返回更大的 index。
3. **session dedup**：同一 `(client_id, seq_no)` 重放返回**原始** index（TTL+grace 内），且不重复应用新 payload。
4. **快照**：确认新快照以 `format_version == 4` 产出，`KV_SNAPSHOT_VERSION == 3`；从快照恢复后第 2、3 项仍成立（session 的原始 index 跨重启保留）。
5. **旧快照拒绝**：构造/保留的 v2 快照恢复应 fail-stop（`MalformedSnapshot`），公开 API 永不出现 index 0。
6. **multi-put 原子**：一次 `multi_put` 后各 key 的 `get_stale_with_index` 报同一 index；3 节点追平后各节点 index 一致。

---

## 6. 运维要点

- **不要**把 v4 数据目录交给旧二进制打开（`FORMAT_VERSION` 拒绝）——回滚必须整目录回退（§3.5），不能「新目录反降」。
- **不要**尝试只回退 `KV_SNAPSHOT_VERSION` 或手改 `META` 版本：session index 缺失无法重建。
- 快照体积：每条 session 记录增加 8 字节（effective index），`snapshot_threshold` 触发点基本不变——属预期内，告警阈值无需调整。
- 会话 TTL：session 记录同时承载 outcome 与 index；GC 语义不变（GC 后的重试是新 session，返回新 index，属标准 exactly-once 边界）。
- wire：`ProposeWithIndex` 的 `unknown_kind → Unsupported` 只表示「旧 peer 未执行」；不要把它当作「写入已成功」或伪造 index。

---

## 7. 相关文档

- 设计：`dev-docs/arachne-kv-batch-read-applied-index-design.md`（§4.2/§5.3/§7/§11）；`dev-docs/arachne-kv-commit-index-design.md`（§9.1：禁止 post-apply 回查）
- 计划：`dev-docs/plan-batch-read-applied-index.md`（Phase 2）
- 上一版：`dev-docs/runbook-format-v3-upgrade.md`（整集群停机重播种路径同款）
- 格式账：`dev-docs/plan-vs-etcd.md`（§2.2 格式打包策略）
- 测试：`arachne/tests/write_index.rs`、`arachne/tests/sessions.rs`、`arachne/tests/write_outcome.rs`
