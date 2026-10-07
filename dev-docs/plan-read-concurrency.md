---
status: in-progress
phase: 2
updated: 2026-10-07
---

# Implementation Plan: 多客户端并发读性能提升（对齐 etcd 并发读模型）

## Goal
在不牺牲线性一致性（保留 ReadIndex Safe 模式、禁止 lease 读）的前提下，通过 (1) 单飞 **cohort** ReadIndex 屏障与 (2) 快照化并行读服务，把多客户端并发线性读的"每-RTT 放行 cohort"保持在 ≥ C3 基线并扩大、把读服务并发度从 1 提到 N、1w 无回归，并将 4w 门槛以稳健方式固化进门禁。

> 目标措辞说明：不再以"追平 14%"为目标 —— 该数字是 §Z 记录的 cluster-state 方差内单次观测、方向可翻转（`propsol-v0.2.md:592-600`）。目标改为可证伪的机制性指标（每轮 cohort 释放数、rounds/sec、1w 无回归）。

## Context & Decisions
| Decision | Rationale | Source |
|----------|-----------|--------|
| A 屏障语义重设计：**轮中到达者排队到"下一轮"**；每轮完成时放行**自己的 cohort**、以**自己的确认索引**为下界；完成的同一 drive_cycle 内立即发射下一轮 | 字面"in-flight-attach"违反 INV14/deposed-leader：轮中到达读会以早于其到达的轮索引为下界，废黜 leader 可放行陈旧读。etcd `linearizableReadLoop` 在 `requestCurrentIndex` **之前**切换 notifier，等待者总是落在自己到达**之后**的轮上 | `ref:ora-1` (F1), `research/findings-etcd.md` §4.2 |
| **实现顺序 B 先行、A 次之**（读并行化 → 屏障） | B 机械、共识安全、且是 P4 线程迁移的前置；A 是最高风险/最低期望值阶段 —— C4 验尸测得 4w 稳态读到达时 in-flight≈0、聚合读间隔(1–2ms) < quorum 往返(0.1–0.5ms)，修复后 A 仅在已重叠读上赢，收益可能≈0；A 需预置证伪标准 | `ref:ora-1` (F2), `propsol-v0.2.md:549-562`, `research/report-read-concurrency.md` §6-A |
| B 采用**单一机制 `Arc<RwLock<BTreeMap>>`**，不做"每批不可变快照克隆" | 每批克隆 = 每批 O(store) 复制，写路径不可接受；`Arc<RwLock>` 可无锁读 + 写锁互斥，方向对齐 etcd bbolt 快照读模型 | `ref:ora-1` (F3.3), `research/findings-etcd.md` §3.3 |
| B 发布顺序：**写锁内先改 map 后提升版本号；读者先读版本后读 map**；保留 actor 门禁 `applied ≥ read_index` + 版本断言（P4 移走 actor 后版本检查升级为关键路径） | 防止读者看到 `version ≥ C` 但 map 缺条目 C；etcd 等价 `appliedIndex < confirmedIndex → ApplyWait` | `ref:ora-1` (F3.1/3.2), `findings-etcd.md` §4.5 |
| P4.1 限定为"读服务移出 actor 线程"（受 B 的 `Arc<RwLock>` 支持、sim 兼容）；如必须移 apply，加**构建接缝**（构造参数/feature）保持 l2/turmoil 确定性 | 裸移 apply 出 sim runtime 会逃离 turmoil、破坏确定性门控（vendored-raft 单线程 RNG 约束） | `ref:ora-1` (F4) |
| A 前置 TDD 地基：**INV14 分区注入测试**（deposed leader + 延迟心跳 + 轮中途读到达），在任何读路径改动前落库 | P2 的 l2/model-check/fuzz 现有门槛捕获不了 F1（`read_index.rs` 区分不了健康集群 ReadIndex 与旧 leader 本地读）；A 的重写语义以此测试为绿门 | `ref:ora-1` (F1/F3 风险门控), `handoff-m1.md:100` |
| A 必须保留 C3 空闲单读同 cycle 发射特性 | step() 之后发射曾记录 +10ms 回归（`mod.rs:976-984`），空闲线性读 p99≤5ms 是 read_latency 绊网 | `ref:ora-1` (P2 空闲性), `handoff-m1.md:273` |
| 删除"收集窗口"旋钮（原 P5.1） | C4 已对同款时间窗口实测并回滚为无操作；修复后的 A 本身就是收集机制，时间窗口只加 p50 | `ref:ora-1` (YAGNI), `propsol-v0.2.md:549-556` |
| 4w 门槛健壮化：脚本内 n=2–4 次运行取中位数、以"不劣于改动前 4w 底线"为阈值、1w 保持主信号 | `check-perf-baseline.sh` 当前单次运行 + 宽松阈值；4w 有 ~2× 方差（21 次 1360–2346）与跨集群方向翻转，硬阈值会 CI 不稳定 | `ref:ora-1` (F5), `propsol-v0.2.md:559,592-600` |
| 排外：不实现 lease/clock read；follower/redirect 路径不在本计划范围内（仅 leader 服务线性读） | v1 硬约束 `ReadOnlyOption=Safe`；读路径范围封闭 | `ref:ora-1`, `handoff-m1.md:94`, `runtime/mod.rs:1652-1660` |

---

## Phase 1: 基线与可观测性 [COMPLETE]
- [x] 1.1 重跑 docker/bench 基线（1w/4w linear、stale、put，多轮取中位数、同集群 A/B，n=400）→ `dev-docs/bench-baseline-2026-10-07.md`（commit 0ade947）
- [x] 1.2 扩可观测性：确认 `arachne_read_index_rounds_total` / `pending` 基线，规划 **cohort 指标**（released-per-round、attach rate）作 A 阶段评估单位
- [x] 1.3 录入对照表：本机 4w Arachne 2739 vs etcd 1979 ops/s（+38%，方向与 §Y"慢 14%"相反，符合 §Z 方差）；1w 4157 ≥ 4000 无回归；4w/8w `reads/round < 1`、pending 峰值 3–4 → 无 cohort 批合并收益（P2-A 证伪基线）

## Phase 2: P3-B 状态机读并行化（`Arc<RwLock>` 快照 + 直接读） [IN PROGRESS]
- [ ] **2.0 (TDD) 读路径正确性地基：INV14 分区注入测试**（旧 leader 已废黜未察觉 + `set_link_latency`/`hold` 延迟心跳 + 读在轮中途到达）→ 落库为所有后续读路径改动的绿门 ← CURRENT
- [ ] 2.1 (TDD) `KvStateMachine` 改 `Arc<RwLock<BTreeMap>>`：发布顺序 = 写锁内先改 map 后升版本、读者先读版本后读 map → `state_machine/kv.rs:66-71,301-311`
- [ ] 2.2 (TDD) `resolve_reads` 直接内存读（去 `reads` mpsc hop）；**保留 actor 门禁 `applied ≥ read_index` + 版本断言**（P4 前以断言/告警形式存在）→ `runtime/mod.rs:1824-1833,1816-1819`
- [ ] 2.3 (TDD) `get_stale` 读**实时最新发布态**、脱离 apply 任务并发服务；HTTP `Busy→503` 映射不变 → `runtime/mod.rs:1637-1651`
- [ ] 2.4 `restore()`（快照安装）持写锁时长设定上限，规避大恢复期间的读 p99 尾部 → `runtime/mod.rs:515-522`
- [ ] 2.5 回归：写风暴下 stale/线性读语义、fsync ledger、`l2`/`model-check`/`fuzz` + `cargo build --release -p arachne-kv-node`
- [ ] 2.6 bench A/B（同集群、多轮中位数）：并发读吞吐提升、rounds/sec、1w 无回归、写路径开销不反弹

## Phase 3: P2-A 单飞 cohort ReadIndex 屏障 + E 簿记瘦身 [PENDING]
- [ ] **3.0 前置绿门：2.0 INV14 sim 在 A 改动前再次全绿**
- [ ] 3.1 (TDD) 重设计实现：轮中到达者排队等**下一轮**；轮完成时放行**自己的 cohort**、以**自己的确认索引**为下界；同 drive_cycle 内立即发射下一轮；保留 C3 空闲单读同 cycle 发射（+10ms 回归警戒、空闲 p99≤5ms）→ `runtime/mod.rs:976-996,1800-1860`
- [ ] 3.2 (TDD) 重试/超时（S12）：重试读加入下一 cohort、**per-read 截止时间**（不放行引入 cohort 级期限）→ `runtime/mod.rs:1838-1855`
- [ ] 3.3 (TDD) E 簿记：token 匹配改 `HashMap`、`Option<NonZeroU64>` token（消除 `token:0` 占位脆弱性）、去 `pending_reads` 每轮 drain/重分配 → `runtime/mod.rs:850,1027-1032,1671,1804-1859`
- [ ] 3.4 回归（含 2.0 INV14、l2/model-check/fuzz）+ 编译
- [ ] 3.5 bench A/B + **证伪标准**：若 `rounds/sec ≥ 0.8 × reads/sec` 且 `released-per-round ≤ 1.3×`，判定 A 无操作、聚焦并行读上限（记录并转向，不返工）
- [ ] 3.6 cohort 指标（released-per-round、attach rate）入 metrics，更新 `read_index_rounds_total` 语义

## Phase 4: P4 读服务线程解耦 + 出站不内联 await [PENDING]
- [ ] 4.0 (构建接缝前置) 若需将 apply 移出 sim runtime，加构建接缝（构造参数/feature）保 l2/model-check 确定性；否则按"读服务移出 actor 线程"执行（B 的 `Arc<RwLock>` 支持、sim 兼容）
- [ ] 4.1 (TDD) 读服务（SM 读侧）移出 actor 的 current-thread runtime → `runtime/mod.rs:885-886,2213-2227`
- [ ] 4.2 (TDD) peer 出站 `Message` 改 per-peer 有界队列 + 独立发送任务：**满=丢弃+计数**、per-peer FIFO 保持（`deliver_grouped` 契约）；审计快照传输（`HeldSnapshot`/`finish_snapshot_fetch`）**不经过**该队列 → `consensus/node.rs:1124-1171,1199-1202`
- [ ] 4.3 回归 + bench：尾延迟、并发写风暴下线性读 p99、慢 peer、快照安装场景

## Phase 5: 4w 门槛固化 + 文档 + 合入 [PENDING]
- [ ] 5.1 `scripts/check-perf-baseline.sh` 增 4w 线性读门槛：脚本内 n=2–4 运行取中位数、阈值=**不劣于改动前 4w 底线**（回归式）、1w 保持主信号
- [ ] 5.2 更新 `README.md` 测量章节与 `dev-docs/propsol-v0.2.md` 记录（含 A 证伪结论、cohort 语义迁移）
- [ ] 5.3 完整门禁 `scripts/check-*.sh` 全绿 + 合入 main（PR，含 2.0 INV14 测试与基准对照）

## Notes
- 2026-10-07: 计划源自已合入调研报告（PR #1, `research/report-read-concurrency.md`）`ref:exp-1`, `ref:res-1`
- 2026-10-07: @oracle 评审（`ref:ora-1`）：需重构 —— F1 语义钉死为"每轮放行自己的 cohort"；F2 调整顺序 B→A；F4 保 sim 确定性；删除原 P5.1 收集窗口
- 2026-10-07: 4w 高方差警告 —— 每阶段"同集群 A/B + 多轮中位数"，不得以单次 4w 判胜；1w 为稳定主信号
- 2026-10-07: 目标以机制性指标可证伪（cohort 释放、rounds/sec、1w 无回归），不追方差内单次 14% 数字
- 已删除的原 P5.1（收集窗口旋钮）：C4 已实测回滚同款（`propsol-v0.2.md:549-562`）；修复后 A 本身即收集机制；如未来重提，先回看 §Y 验尸
