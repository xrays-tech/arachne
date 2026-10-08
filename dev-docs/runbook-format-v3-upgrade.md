# Runbook：存储格式 v2 → v3 破坏性升级（`FORMAT_VERSION` 2→3）

> 适用版本：升级到 **arachne-kv 0.3.x（P0–P1 打包发布）**（含 `OP_MULTI_PUT`/`OP_CAS` 命令与 `KV_SNAPSHOT_VERSION` 2）。
> 关联：`dev-docs/plan-vs-etcd.md`（§2.2 格式打包策略、§8 触发式/不做清单）；`dev-docs/runbook-format-v2-upgrade.md`（上一版，重播种路径同款）。
> 一句话：**v3 数据目录不能被旧节点打开，旧数据目录也不能被 v3 无缝打开——这是有意的破坏性升级，必须重播种/迁移；且 v3 不支持与旧节点滚动混跑。**

---

## 1. 这是什么升级

| 项 | 说明 |
|---|---|
| 变更 | 存储 `FORMAT_VERSION` 2 → 3（`arachne/src/storage/meta.rs`）；kv 快照 `KV_SNAPSHOT_VERSION` 1 → 2（session 表结果新增 `CasFailed` tag=2，`arachne/src/state_machine/kv.rs`）；新命令 opcode `OP_MULTI_PUT`/`OP_CAS` |
| 为什么破坏 | `WalStorage::open` 校验 `META.format_version`，不一致即 **fail-stop**（`arachne/src/storage/wal.rs`）；旧节点不识别新 opcode → `MalformedCommand` → fail-stop（混合版本无法复制 multi-put/CAS） |
| 何时生效 | 任何 v3 二进制打开旧目录 → 拒绝；任何旧二进制收到 v3 快照/new-opcode 条目 → fail-stop |

## 2. 硬约束（必须先理解，再操作）

1. **旧数据目录 = 打开即 fail-stop**：不存在「v2 目录原地升 v3」。
2. **禁止新旧节点混跑**：v3 的 `OP_MULTI_PUT`/`OP_CAS` 条目对新 log 是合法、对旧节点是 `MalformedCommand`；v3 快照（`format_version=3`、`KV_SNAPSHOT_VERSION=2`）旧节点无法解析。混合版本拓扑中一次跨版本复制/快照传输即让旧节点 fail-stop。**因此 v3 不支持滚动升级，必须整集群停机升级。**
3. **线协议**：`result` 信封（M1 引入）扩展为 oneof 承载 `CasFailed`——新节点之间完整；与「只运行 v2 节点」的拓扑中，v2 节点回缺省=无结果（滚动兼容语义对纯读取路径成立），但任何写路径（含 multi-put/CAS）不可跨版本。

## 3. 推荐路径（整集群停机重播种）

> 适用于：尚无真实存量用户、或可接受一次性迁移成本的部署（当前 0.3.x 发布，推荐此路径）。

1. **停机**：停止全部 `arachne-kv` 进程（写出/读入全部停止）。
2. **保留旧数据**（回滚/审计用）：原 `data_dir` 整体备份（含 `META`、`segments`、`snapshot-*.snap`）。**不要删除**直到新集群验证通过。
3. **重播种**：
   a. 为新 v3 节点准备全新 `data_dir`（空目录）。
   b. 如需保留业务数据：在旧集群/环境中**导出**数据（经 `get`/`get_stale`/`get_stale_with_index` 逐 key 读出），再在 v3 集群**重新写入**（`put`/`multi_put`）。**不要**尝试直接复制 v2 目录或快照文件。
   c. 数据写入后，新格式快照会在 `snapshot_threshold` 触发时自然生成；建议重播种后手动验证一次快照生成（见 §5）。
4. **校验**（新集群）：见 §5。
5. **回滚预案**：若 v3 校验不通过，恢复第 2 步备份的旧目录 + 旧二进制（v2）即可回到上一版本（旧目录从未被 v3 打开过则完整无损）。

## 4. 滚动替代路径（不支持；仅单节点/全控场景）

v3 与前版**无兼容协商**（新 opcode + 快照格式双变），滚动升级窗口内任何写/快照复制都可能 fail-stop 旧节点。**不推荐**；仅当单节点、且能保证旧二进制永远不会读取 v3 产物时才考虑，且必须视为一次性迁移而非长驻混合拓扑。

## 5. 升级后校验（用新原语验证语义）

在新 v3 集群上：

1. **基础读写**：`put` 后 `get` / `get_stale` 正常；`get_stale_with_index` 返回 `Ok(Some((v, i)))` 且 `i >= 1`。
2. **multi-put 原子**：一次 `multi_put` 后各 key 的 `get_stale_with_index` 报**同一 index**（原子性观测断言）；3 节点追平后各节点 index 一致。
3. **前缀一致读**：hydra 形状（head + `head\x00…` entity）下 `get_stale_prefix` 返回整段 + 统一 applied index。
4. **CAS**：`stale 读 → cas(IndexEquals(i))` 成功；陈旧 compare → `NotApplied{current}` 且状态不变。
5. **快照生成**：确认 `snapshot-<index>-<term>.snap` 以 v3 格式产出（`format_version == 3`），且从快照恢复后 1–4 项仍成立（含 `CasFailed` 失败结果在 snapshot→restore 后仍可重放返回——恰一次不变量）。

## 6. 运维要点

- **不要**把 v3 数据目录交给旧二进制打开（`FORMAT_VERSION` 拒绝）——回滚必须整目录回退（§3.5），不能「新目录反降」。
- 混合版本（v2 与 v3 共存）**仅允许在纯读取、无写/无快照进行的前提下**；一旦写入 `OP_MULTI_PUT`/`OP_CAS` 或传出 v3 快照即 fail-stop。**默认按 §3 全线停机。**
- 快照体积：session 表 `CasFailed` tag 增加少量字节，`snapshot_threshold` 触发点基本不变——属预期内，告警阈值无需调整。
- 会话 TTL：CAS 失败也占 session（`SessionTableFull` 风险 §6.3）——热热点 CAS 场景监控 `session_count`，必要时客户端以有界重试 + `seq` 复用缓解。

## 7. 相关文档

- 设计：`dev-docs/plan-vs-etcd.md`（§2.2 一次打包 / §6 CAS / §8 不做清单）；`dev-docs/arachne-kv-commit-index-design.md`（index 语义、快照版本化）
- 决议：`dev-docs/propsol-v0.2.md`（会话/背压/读路径/存储）
- 测试：`dev-docs/test-plan-v0.1.md`（INV16–18 延续；本计划新增 J1–J4）
