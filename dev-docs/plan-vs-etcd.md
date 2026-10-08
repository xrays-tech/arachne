# 开发计划：对拍 etcd 后的功能增量（P0–P2）

> 状态：**M1–M4（P0–P2）已全部实现并合入主干**（2026-10-08；M1 `4ad459d` / M2 `6281557` / M3 `2356b46` / M4 `c41ed57`；每里程碑过 Oracle 门禁，必修项落地并有聚焦测试）。
> 用途：**供新 session 在本仓独立执行**——本文件自包含，含：目标、路线与依赖、每项功能的 API/内部机制/代码锚点/不变量/测试/验收/坑、格式升级打包策略、执行编排建议。
> 前置事实（新 session 直接信任，如需复核见 §12）：当前版本 v0.3.1；仓库 crates.io 已发布 5 crate；`get_stale_with_index` 已交付（value 溯源序号）；`put_with_index` 已关闭（下游确认不需要，#9）；快照存储 `FORMAT_VERSION = 2`（`arachne/src/storage/meta.rs:48`）。
> 完成情况锚点：P0 管线见 runtime `WatchEvents`/`outcomes` 通道；P1-A `get_stale_prefix/range`；P1-B `multi_put`；P1-C `cas` + `KV_SNAPSHOT_VERSION` 2 / `FORMAT_VERSION` 3（runbook `runbook-format-v3-upgrade.md`）；P2 `watch`（快照 + 写入集事件）。

---

## 0. 目标与定位

对拍 etcd v3.5 功能面后，为 arachne-kv **有选择地补齐**功能。三条原则（评审锁定）：
1. **不照抄 etcd 清单**——只补「补短 + 强化差异化」的交汇点；已排除 auth/RBAC、MVCC 全量历史、完整多键 txn、DeleteRange、key Lease/TTL、Election/Lock、defrag/quota、downgrade、etcd wire 兼容层（§8）。
2. **以唯一已知下游 hydra 的真实模型为准**：整树 put 替换（每次 N 个单键写）、每节点本地每秒 `get_stale_with_index(ctl/head)`、head index ≥ 已应用 index 才物化、且**多键 stale 读非原子**（design §3.4）。
3. **一次格式升级打包全部 P0–P1**（§2.2）：避免逐个新 opcode/outcome 连续触发破坏性 `FORMAT_VERSION` bump。

本计划只交付 P0–P2；触发式与不做清单（§8）明确写死，执行时不得扩scope。

---

## 1. 背景：对 etcd 的对比结论（摘要）

| 能力 | etcd v3.5 | arachne-kv v0.3.1 | 本计划处理 |
|---|---|---|---|
| 写 | put/delete；`Txn`（CAS/多键/嵌套）；`ignore_value` | put/delete（单键） | **P0 管线 + multi-put + CAS** |
| 读 | Range（range_end/limit/sort/count_only/…）；serializable 读 | get / get_stale(+index)（单键） | **P1-A 一致 Prefix/Range 读** |
| 历史 | revision/MVCC/compact | 无（WAL tail 即轻量历史） | 触发式（watch 不需要写依赖） |
| Watch | 流式 key/prefix/fragment/bookmark | 无 | **P2 Watch** |
| 差异化（etcd 没有） | — | 会话幂等（写恰好一次）；`get_stale_with_index`（值-序同源弱读） | 保留，不为其堆功能 |

评审三处关键修正（相对初版提案）：
- **CAS 不便宜**：需写 outcome 回传管线（`ApplyOutcome` 第三态 + ack 通道 + proto）+ 失败入会话表；且对 hydra 单写者无用、多写者也只在 head 上 CAS 挡不住 entity 交错 → 降为通用 RMW 能力（§6），价值低于 multi-put 与一致 prefix 读。
- **Watch 不依赖 MVCC/历史**：= from-now + prefix-snapshot-at-index + WAL-tail 回放（§7）。
- **漏项已补**：有界原子 multi-put（§5，直击 hydra 写放大）与一致 prefix 读（§4，补多键非原子洞，兼作 watch 快照原语）。

---

## 2. 总路线与依赖

### 2.1 里程碑

```
M1  P0 写 outcome 回传管线（一次性基建；§3）        ← 无前置，第一优先
M2  P1-A 一致 Prefix/Range 读（§4）                 ← 依赖 M1（读返回统一 index 无关，但共用读路径改动纪律）
M2  P1-B 有界原子 multi-put（§5）                   ← 依赖 M1
M3  P1-C CAS（§6）                                  ← 依赖 M1（+可选 M2 的 NotExists/读语义）
M4  P2 Watch（§7）                                  ← 依赖 M2（同一 prefix 快照原语）+ P1 完成的格式打包
```

- **P0 是唯一硬前置**；M2 内 P1-A/P1-B 可并行（不同文件面，见 §11）；CAS 放 P1-C 因其独立性最弱、复用 P0 最多。
- 每个里程碑独立可测、可发布（`cargo test -p arachne-kv --no-default-features` 全绿为门禁）。

### 2.2 格式升级打包策略（重要）

- 本次 P0–P1（M1–M3）涉及：`ApplyOutcome` 新增 Third 态（CAS）、multi-put 新 opcode、可能新增 per-key 数据、session 表结果编码扩展、无格式变更的纯读改（范围读）不算。
- **约定**：M1–M3 完成后，把 `KV_SNAPSHOT_VERSION`（`arachne/src/state_machine/kv.rs` 顶部常量，当前=1）与 storage `FORMAT_VERSION`（`meta.rs:48`，当前=2）**仅 bump 一次**，纳入 M3 的提交；在此之前的新 opcode/outcome 落进同一版本不单独 bump。
- 升级runbook 沿用 `dev-docs/runbook-format-v2-upgrade.md`（重播种路径；把 v2 改成 v3 复制一份或作修订）。
- **M4（watch）若引入持久事件流，才需再评估**：首选模型（§7）不持久化事件（WAL tail 即回放窗口），故一般不再 bump；若实现时选择持久化 watch 事件，**必须**先独立评审再决定格式版本。

### 2.3 通用纪律

- 新命令必须纳入背压与上限：`proposal_queue_bytes`（字节背压，触顶 `Busy`）与 key/value 大小上限（`Handle::validate_*`）；multi-put 定义**总字节上限**（见 §5）。
- 会话表交互：所有新写型命令的 outcome（含**失败结果**）都写入 session 表（恰好一次）；评估失败结果是否应更短 TTL（见 §10 风险 2）。
- 所有新 API 纯 additive；`get`/`get_stale`/`put`/`delete`/`get_stale_with_index` 语义与签名**不变**（回归门禁）。

---

## 3. M1（P0）：写 outcome 回传管线

### 3.1 为什么（评审锁定）
今天 `Propose` 的 ack 是 `Result<(), ArachneError>`（`runtime/mod.rs` 约 L233 的 `Command::Propose` ack 类型；`reply_pendings` 约 L1668 只回 `Ok(())`）；apply 只通过聚合 `ApplyProgress` 回传（`runtime/mod.rs` apply 任务）。CAS 的「成功/失败/旧值」与 multi-put 的「结果」都需要**每条命令的 outcome 从 apply-task 回到 actor、再回客户端**。**不能在 apply 后回查 store**（同批并发写会读错——design §9.1 已记录此坑，`put_with_index` 的并发修正同源）。

### 3.2 改动面（锚点）
- `arachne-kv-seam`：`ApplyOutcome`（`seam/state_machine.rs:12`）当前 `Value|None` → 规划扩展（CAS 第三态在 §6；**M1 先只把管线铺好，不引入语义**——建议 M1 把 `ApplyOutcome` 扩展为可携带「附带结果」的载体，或新增 apply→actor 的 per-entry outcome 通道）。
- `arachne/src/runtime/mod.rs`：`Command::Propose` ack 由 `Result<(), _>` 演化为可携带 outcome（`Option<Vec<u8>>` 或结构化结果）；`Pending`/`reply_pendings`（约 L1219 stamp index、L1668 回送）把 apply-task 回传的 per-entry outcome 对到等待者。
- `arachne/src/state_machine/kv.rs`：apply 返回值打通（本次仍是 `Value|None`，形态留给下两节消费）。
- 线协议（若需跨进程带结果）：`ForwardOutcome`（`seam/forward.rs:76-82`）+ proto `ForwardReply`（`proto/raft.proto` 约 L129-139）加可选结果字段；**滚动兼容**：老节点回缺省=无结果（new 客户端本地路径不受影响）。B 已零线协议改动，本项是首次动线协议，需走 §2.3 纪律。
- 快照：session 表结果编码（`kv.rs` snapshot() 内的 tag 序列化，约 L432-439）与 `KV_SNAPSHOT_VERSION` 在 §2.2 时一并处理。

### 3.3 不变量
- 每个 `(client_id, seq_no)`：至多一条 outcome；重放返回缓存结果（不重算）；失败也缓存（供 CAS，见 §6）。
- apply 顺序 = log 顺序；outcome 回传不改变 apply 的时序（actor 侧仍是「applied ≥ index 才回」）。

### 3.4 测试与验收
- 单测：apply 各种命令的 outcome 往返；重放返回缓存；`snapshot→restore` 往返保 outcome（新格式）。
- 集成：propose→ack 带结果（用现有 put 验证回归：ack `Ok(())` 行为不变）；跨进程 Forward 带结果（tonic 路径，feature 打包构建）。
- 验收门：`cargo test -p arachne-kv --no-default-features` 全绿 + 既有套件零改动；tonic 构建 `cargo build -p arachne-kv-transport-tonic -p arachne-kv-node`。

---

## 4. M2-P1A：一致 Prefix/Range 读

### 4.1 目标
- 单次读在一把读锁下返回：`[key, range_end)` 匹配的 `(key, value)` 无序或有序列表 + **统一 applied index**（快照一致性）。
- 补 design §3.4 / I7 的「多键 stale 读非原子」洞：调用方拿到的整段结果属于同一个 applied 水位，可安全地把该 index 用作批量判断。
- 语义边界（评审锁定）：`[a, b)` 半开区间；prefix = byte 前缀（客户端以 `key + 0x00 前缀扩展`或「下一个字节续一」构造 range_end，本端文档给明确构造法）；返回带 `applied_index`；本地读（同级 `get_stale`，不仲裁）。
- `get`/线性路径不变；本功能是**弱读**的区间版（命名建议 `get_stale_prefix` / `get_stale_range`，由实现评审定名，保持 additive）。

### 4.2 内部机制
- `KvStateMachine` 新增：`get_range_with_index(&self, start: &[u8], end: &[u8]) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, LogIndex), KvError>`（单读锁内 `BTreeMap::range` + `guard.applied`，D-Arc 同款原子性）；prefix 由 Handle 层把 key 换算成 `(start, end)`。
- 运行时：新增 `Command::GetStaleRange { start, end, ack }`，走 `spawn_read_reply` 同类 off-actor 回复（P4 纪律）。
- 大小上限（§2.3）：返回条目数/总字节需有界（防一次拖垮），超限可 `Busy` 或分页（首版规定「返回上限条目数 + `truncated` 标志」）。

### 4.3 测试
- 单测：空区间、prefix、`[a,b)` 边界（含 a=b=空=全量语义、a 存在 b 不存在）、有序性、统一 index（写后半段后，结果要么全旧要么全新，无撕裂）。
- 集成：hydra 形状（head=前缀首键 + entity 段）——一致读返回统一 index；与 `get_stale_with_index` 单键值对齐。
- 回归：既有 `get_stale` 相关测试零改动。

### 4.4 验收
`get_stale*` 区间读在 no-quorum/分区下可用（对齐 S02/S16 弱读语义）；返回统一 applied index 的断言成立。

---

## 5. M2-P1B：有界原子 multi-put

### 5.1 目标
- 单命令写入一组 key（整树替换一次落盘）：`multi_put(entries: &[(&[u8], &[u8])]) -> Result<(), _>`，**一个 log entry、一个 session**，原子（全有或全无，一次性 apply）。
- 直击 hydra「整树替换 = N 次单键写」的写放大（N 次 propose→fsync→round → 1 次）。
- **不做** compare（那是 §6 的事）；不做 read（§4）；有界（§2.3：总字节上限，超限 `InvalidArgument`）。

### 5.2 内部机制
- `KvStateMachine`：新 opcode（如 `OP_MULTI_PUT`）+ `encode_multi_put(client_id, seq_no, &[(k,v)...])` + apply 分支：校验所有 key 各自限长、总长上限 → 写入锁内全部 `Entry{val, index}`（**同一 apply index**，原子）→ `ApplyOutcome::None`（结果由 M1 管线回传）。
- 幂等：同 `(client_id, seq_no)` 重放命中缓存、不回写（与单键 put 一致；validate 在 Handle 侧先做）。
- 背压：总字节计入 `proposal_queue_bytes`。
- 快照：无新字段（Entry 结构不变），但 opcode 变更随 §2.2 打包。

### 5.3 测试
- 单测：多组原子写入（全落/单键重复在组内时后者覆盖——**需定语义：组内重复 key 后写覆盖**，文档写明）；重放幂等；长度/总长超限拒绝（不写入）。
- 集成：hydra 整树替换（head+entities 一次 multi_put，各节点一致追平、index 单调）；`get_stale_with_index` 对组内每键返回**同一 index**（原子性的观测断言）。
- 回归：`stale_read_with_index.rs`、`quorum_loss.rs`、`m2_*` 零改动。

### 5.4 验收
3 节点：`multi_put` 后各节点相同 index；失败（超限/not-leader 重定向）语义与 `put` 一致。

---

## 6. M3-P1C：CAS（compare-and-swap）

### 6.1 目标与定位
- `compare(key, pred) → op(success|failure)`：pred 支持 **三类**（评审锁定：单靠 index 无法表达缺键）：
  1. `IndexEquals(index)`——该 key 溯源 index == X（**推荐/主用**：单调、无 ABA，与 `get_stale_with_index` 对齐）；缺键时 index 不存在 → 与 `NotExists` 配合。
  2. `ValueEquals(bytes)`——值比较（兼容直觉，内部次选）。
  3. `NotExists`——create-if-absent（缺键无法用 index 表达）。
- success 分支：`put` 或 `delete`；failure 分支：`noop`（首版只做单键，**不做**多分支/嵌套——那是触发式完整 txn 的地盘）。
- 定位：**通用 RMW 能力**，服务于「stale 读到 `(v, i)` → CAS(i) → re-put」的乐观并发；**不承诺**修复 hydra 的多写者 entity 交错（评审：单写者无需 CAS，多写者只在 head 上 CAS 也挡不住 entity 交错——这属于后续/触发式完整 txn 域）。

### 6.2 内部机制（评审锁定的三个硬点）
1. **`ApplyOutcome` 第三态**：`CasFailed`（可携带当前 index/value 可选）。`seam/state_machine.rs:12` 扩展；对应快照 session 表 tag 编码（`kv.rs` ≈L432-439）与 `KV_SNAPSHOT_VERSION`（§2.2 打包）。
2. **失败也写 session 表**：否则同 `(client_id, seq_no)` 重试会**重算 compare 并可能成功** → 破坏恰好一次。失败不改 kv，只占 session。
3. **比较结果回客户端**：依赖 M1 管线；`Propose` ack 携带 `CasFailed` → Handle 返回如 `ArachneError::CasFailed { current_index }`（新变体，additive）或 `Result<bool, _>` 由 API 评审定。
- **重放语义**：命中 session 直接返回缓存结果、**不重比较**（现有 `apply` 命中缓存即 `cached.clone()`，正是所需）。
- 新增 opcode（`OP_CAS`），command 编码含 pred + success op。

### 6.3 风险（评审锁定，必须写进实现与测试）
- **SessionTableFull**：CAS 失败会占会话；热点 CAS 下可能先撞 `max_sessions`（`runtime/mod.rs` 约 L1492）→ 表现为 `SessionTableFull`。评估：会话 GC（`maybe_collect_sessions`，TTL 扫描）是否需对「失败结果」给更短 TTL；首版若不做，文档明示该风险。
- 乐观重试会以失败 outcome 灌 session → 与上面的 GC 策略联动。

### 6.4 测试
- 单测：IndexEquals 命中/未命中/缺键（NotExists 组合）、ValueEquals、失败入缓存（重放返回 CasFailed）、delete→re-put 后 IndexEquals 语义（I6 方式）、快照往返保 CasFailed outcome。
- 集成：stale 读到 `(v,i)` → CAS(i) 成功；构造 `i-1` 的陈旧 compare → CasFailed 且不改变状态；两个写者竞争同一 key 的 CAS 只有一方成功。
- fuzz：CAS command 编解码 + apply 确定性（与 `kv_snapshot_restore` 同 fence：无 panic、结果确定性）。
- 回归：全量零改动。

---

## 7. M4（P2）：Watch

### 7.1 模型（评审修正：不依赖 MVCC/历史）
- **语义**：`watch(key 或 prefix, from_index)` → 流式 `(index, key, value|delete)` 事件；**不保证线性化**（etcd 同口径，事件带 index 供消费端自行对齐去重）。
- **首版范围**：`watch_from_now + prefix-snapshot-at-index`：
  1. 订阅者**先向节点注册**（actor 内维护 watcher 列表）；
  2. 注册后立刻在同一读锁下取 `prefix` 的**一致快照 + 统一 applied index**（= §4 的读取原语，评审点：这就是为什么 watch 依赖 M2）；
  3. 之后 apply-task 产生的逐条事件（**需在 apply 处新增 per-command 事件广播**——当前 apply 只发聚合 watch 状态，无 per-command 事件）按 index 推给 watcher；
  4. 消费端按 index 去重/排序（快照与事件之间可能有重叠窗口：**先注册、再取快照、再按 index≥快照index 过滤事件**，否则快照与首事件之间丢事件）。
- **回放窗口 = WAL tail** `[snapshot_index+1, applied]`：请求 `from_index` 在该窗口内 → 从 WAL 重建事件；`from_index < snapshot_index` → 返回「已压缩」（响应带 `compacted`/快照引导标志），客户端改用 snapshot-at-index 重抓。**不持久化事件**（一般无需再 bump 格式，§2.2）。
- 节点侧实现无需 MVCC：事件来自 apply（内存 + WAL tail）。

### 7.2 改动面
- runtime：watcher 表（订阅注册/取消、按 index 过滤、背压——**慢消费者如何办**：首版可「满则丢弃 + 计数」或「断开」，明确并测试；etcd 是窗口语义，首版不必完全对齐）。
- `KvStateMachine`/apply：事件广播（不改变 apply 返回值路径；事件 = 副作用发送）。
- Handle：`watch`（流式 future/`mpsc` 收事件，由实现评审定 async 形态）；facade 镜像。
- 线协议：watch 若跨进程需要，另行评审（etcd 是 gRPC 流；本仓 embedding 为主，首版可**仅内存/单进程**，跨进程 watch 触发式）。

### 7.3 测试
- 单测/集成：prefix watch 收到 put/delete 事件且 index 单调；先注册再写不丢事件；快照+事件窗口去重正确；`from_index` 在 WAL tail 内回放；`< snapshot_index` 触发 compact 引导（fallback 重抓）；慢消费者背压行为。
- 回归：全量零改动。

---

## 8. 触发式 / 不做清单（评审扩充，执行时不得擅自扩scope）

**触发式（等真实下游信号后单独立项）**：
- 完整多键 txn（任意 read-set + 多分支 compare + 嵌套）——本轮 CAS（§6.1）与 multi-put（§5）是它的两个子集，届时复用 P0 管线；
- key Lease / TTL（需墙钟 + checkpoint + lease 驱动删除进 watch；**不复刻**，除非出现 TTL 需求）；
- Election / Lock 服务（依赖 txn/CAS 就绪后薄层）；
- 轻量历史 / compact API——WAL tail 已是轻量历史（`[snapshot_index+1, applied]`），仅在「watch 回放旧历史 / 审计需求」出现时开；
- delete tombstone（design §3.3/§9.2；hydra put-only，无信号）；
- 跨进程 watch（wire 流）。

**不做（YAGNI 纪律）**：
- MVCC 全量历史 / 时间旅行（与既有 log-index / per-key origin 序重复，制造第二套序）；
- DeleteRange（hydra put-only 替换模型，从不删树内 key）；
- auth/RBAC、defrag/quota、Downgrade；
- etcd v3 wire / API 兼容层（embedding 是使用模式；追兼容拖垮 YAGNI）；
- **不复活 `put_with_index`**（plan #9 已关闭；CAS/multi-put 如需返回 index，复用 M1 同一管线，不以旧 API 名义开新口）。

---

## 9. 测试与验证总体策略

- **门禁**：每里程碑 `cargo test -p arachne-kv --no-default-features` 全绿（含全部集成）+ tonic 构建绿（`arachne-kv-transport-tonic`/`arachne-kv-node`）+ 发布门禁 `scripts/check-release-features.sh`（发布时）。
- **确定性**：状态机保持「只读 index+command、无时钟/IO」；新 opcode 的 apply 进 fuzz（扩 `fuzz_targets/kv_snapshot_restore.rs`：任意 command 字节 apply 不 panic、确定性；任意 snapshot payload restore 不 panic）。
- **不变量编号延续**：现有 I1–I9（design §6）/ INV16–18（test-plan）。新增建议：J1 一致范围读（统一 applied index、无撕裂——对应 §4）、J2 multi-put 原子（同 index、重放幂等—对应 §5）、J3 CAS 恰好一次（失败入缓存、重放不改判—对应 §6）、J4 watch 事件 index 单调且窗口去重正确（对应 §7）。编号由实现评审按 test-plan 惯例接续。
- **回归**：每个里程碑后跑 `stale_read_with_index.rs`、`quorum_loss.rs`、`client_runtime.rs`、`client_redirect.rs`、`m2_*`、`sessions.rs`，断言零改动通过。

---

## 10. 风险清单（评审锁定，实现时逐条核对）

1. **写 ack 管线是 CAS/multi-put 公共前置**——M1 没做好，后续全部返工；**不得**在 apply 后回查 store（同批并发读错）。
2. **CAS 失败 × session 表**：失败入表 + 热点下 `SessionTableFull`（`max_sessions` L1492）+ TTL GC（`maybe_collect_sessions`）——评估失败结果更短 TTL；不做则文档明示。
3. **格式升级连续迁移**：P0–P1 必须打包进**一次** `KV_SNAPSHOT_VERSION` + `FORMAT_VERSION` bump（§2.2）；未来 watch 若持久化事件另立评审。
4. **新命令背压**：multi-put 总字节、CAS 等必须进 `proposal_queue_bytes` 与大小上限（§2.3）；否则批量写绕过背压。
5. **Range 边界语义**：`[a,b)` 半开 + prefix 构造法必须文档化；返回带统一 applied index 才有意义（§4）。
6. **watch 慢消费者/背压**：首版明确「满则丢+计数 或 断开」并测试（§7.2）；勿假装 etcd 窗口。
7. **线协议首次改动**（M1 ForwardOutcome/ForwardReply）：滚动兼容（老节点回缺省）；Hello 不收紧。
8. **session 结果编码**（snapshot tag）：CAS 新态必须与 `KV_SNAPSHOT_VERSION` 同一次升级一致，旧 payload fail-stop（沿用 v2 runbook 纪律）。

---

## 11. 执行编排建议（给新 session）

1. **顺序**：严格 M1 → M2 → M3 → M4（§2.1 依赖）。不要在 M1 管线未绿前开 M2/M3。
2. **可并行**：M2 内 P1-A（状态机读 + Command + Handle）与 P1-B（multi-put 写路径）文件面不同，可在 M1 后拆两个分支并行实现，再合并验证（读、写分别独立测试，合并跑回归）。
3. **提交粒度**：每项功能一个提交（feat(arachne-kv): …），P0 metadata/管线先行提交；格式升级（KV_SNAPSHOT_VERSION + FORMAT_VERSION）随 M3 一次提交；文档（本计划更新「已完成」标记）随动。
4. **验证节奏**：每提交前 `cargo test -p arachne-kv --no-default-features` + 目标文件单测；合并入主干仅在全部门禁绿后（本仓工作流：全量 lean 套件 + tonic 构建 + 发布门禁）。
5. **发布**：P0–P2 全部完成后走 release（mirror v0.3.0/0.3.1 惯例：chore(release) bump + 附注 tag + crates.io `cargo publish` 依赖序发布 5 crate + 独立锁 resync——见 `dev-docs/plan-commit-index-followups.md` 与本次发布经验；届时可顺手把 kv publish 恢复 testsupport dev-dep 不再裁剪）。
6. **新 API 命名**：`get_stale_prefix/range`、`multi_put`、`cas`、`watch` 均为**建议名**；正式命名由实现评审定，但保持 additive 与「弱读/线性写」命名规则一致。
7. **不确定即查**：凡涉及既有语义（会话、快照、背压、线协议）的改动，先读 `dev-docs/propsol-v0.2.md`（§2.2 会话/§2.3、§4.1 背压、§5.4 读路径、§5.5 存储）与 `arachne-kv-commit-index-design.md`（§3/§5/§6）再动手。

---

## 12. 参考

- 对比事实来源：etcd v3.5 API（rpc.proto / kv.proto / auth.proto；API guarantees 文档）——本计划 §1 摘要即落地依据，细节如需复核走 `@librarian`。
- 评审：`ref:ora-3`（本轮路线图评审，存档于会话记录）＋ `ref:ora-1`（commit-index 设计评审）。
- 本仓：`dev-docs/propsol-v0.2.md`、`dev-docs/arachne-kv-commit-index-design.md`、`dev-docs/plan-commit-index-followups.md`、`dev-docs/test-plan-v0.1.md`、`dev-docs/recipe-stale-read-partition.md`、`dev-docs/runbook-format-v2-upgrade.md`。
