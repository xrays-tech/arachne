---
status: phase-3-closed
phase: 4
updated: 2026-10-07
---

# Implementation Plan: 多客户端并发读性能提升（对齐 etcd 并发读模型）

## Goal
在不牺牲线性一致性（保留 ReadIndex Safe 模式、禁止 lease 读）的前提下，通过快照化并行读服务（B，已落地）与读簿记瘦身（E），把读服务并发度从 1 提到 N、1w 无回归，并将 4w 门槛以稳健方式固化进门禁。ReadIndex **cohort 批量屏障（A）已按 §3.5 证伪标准判定为预证伪（见 Phase-3 证伪结论），不再实现**。

> 目标措辞说明：不再以"追平 14%"为目标 —— 该数字是 §Z 记录的 cluster-state 方差内单次观测、方向可翻转（`propsol-v0.2.md:592-600`）。亦不再以"扩大每-RTT cohort"为目标 —— A 已证伪。目标以可证伪的机制性指标（读服务并行度、rounds/sec、1w 无回归）为准。

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

## Phase 2: P3-B 状态机读并行化（`Arc<RwLock>` 快照 + 直接读） [COMPLETE]
- [x] **2.0 (TDD) 读路径正确性地基：INV14 分区注入测试**（旧 leader 已废黜未察觉 + `set_link_latency`/`hold` 延迟心跳 + 读在轮中途到达）→ 落库为所有后续读路径改动的绿门（`test(read): INV14 deposed-leader partition gate`，commit `c23430b`）
- [x] 2.1 (TDD) `KvStateMachine` 改 `Arc<RwLock<Store>>`：发布顺序 = 写锁内先改 map 后升版本、读者先读版本后读 map；`StateMachine` seam → `&self`/`Send+Sync`（commit `f9f45cb`）
- [x] 2.2 (TDD) `resolve_reads` 直接内存读（去 `reads` mpsc hop）；**保留 actor 门禁 `applied ≥ read_index` + 版本断言**（commit `da13a1f`）
- [x] 2.3 (TDD) `get_stale` 读**实时最新发布态**、脱离 apply 任务并发服务；HTTP `Busy→503` 映射不变；删除 `reads` 通道基础设施（commit `0c2af66`）
- [x] 2.4 `restore()`（快照安装）持写锁时长设定上限，规避大恢复期间的读 p99 尾部：`decode_store()` 解码移到写锁外，`restore()` 仅 swap（commit `b8c4ae7`）
- [x] 2.5 回归全绿：写风暴下 stale/线性读语义、fsync ledger（`cargo test --workspace` 435+ 通过）、`l2`（确定性双跑）、`model-check`（无 counterexample）、`fuzz`（wal_recovery 541628 run 无 crash）、`cargo build --release -p arachne-kv-node`
- [x] 2.6 bench A/B（同集群、3 轮中位数，n=400, keep-alive, `--process`）：**并发读吞吐提升、1w 无回归、写路径不反弹**（见 §Phase-2 结果）

### Phase-2 结果（bench A/B，同 3-node 集群，主口径 `--process --keep-alive`，3 轮中位数）

| benchmark | Phase-1 rps | **Phase-2 rps (med)** | Δ% | Phase-1 p50 | **Phase-2 p50** | verdict |
|-----------|-----------:|----------------------:|:---:|:----------:|:---------------:|:-------:|
| put (seq, 1w) | 563 | **583** | +3.6 | 1.290 | **1.287** | OK |
| put (con4, 4w) | 381 | **399** | +4.7 | 2.790 | **2.705** | OK |
| get (linear 1w) | 4157 | **4256** | +2.4 | 0.240 | **0.237** | OK (≥4157 无回归) |
| get (linear 2w) | 3338 | **3532** | +5.8 | 0.266 | **0.262** | OK |
| get (linear 4w) | 2739 | **2892** | +5.6 | 0.365 | **0.295** | OK (不劣于 4w 底线) |
| get (linear 8w) | 1809 | **2073** | +14.6 | 0.475 | **0.434** | OK |
| get (stale 4w) | 7593 | **7822** | +3.0 | 0.120 | **0.116** | OK |

**结论：**
- **1w 无回归**：4256 ≥ 4157 ✓（主信号通过）。
- **并发读吞吐提升**：4w +5.6%、8w +14.6%、2w +5.8%，p50 全线改善；4w 不劣于 2739 底线。
- **写路径不反弹**：put 1w +3.6%、4w +4.7%（无回归，实际微升）。
- **弱读**：stale 4w +3.0%，p99 改善（0.217 vs 0.251）。

**ReadIndex 机制指标（8w linear，n=8000 稳态采样，15ms 间隔，leader 8003）：**

| 指标 | Phase-1 (baseline §7) | **Phase-2** | 说明 |
|------|:---------------------:|:-----------:|------|
| reads/s | 2156 | **2292** | 与 ops/s 一致，提升 |
| rounds/s | 3154 | **4322** | 上升（见下） |
| reads/round | 0.68 | **0.53** | 下降 |
| peak read_index_pending | 4 | **8** | 上升 |

- **机制归因**：C3 单飞合并（`mod.rs:955-962`）在 Phase-2 **未改动**；吞吐提升来自**读服务侧去 mpsc 串行**（2.2/2.3 直接服务），而非 ReadIndex 批合并。去串行后读在 `pending_reads` 中更分散地到达，每轮 C3 轮服务更少读（reads/round 0.68→0.53），故 rounds/s 上升、每轮读变稀。
- **代价/收益**：Phase-2 以更多 ReadIndex 轮（3154→4322/s）换取更高读服务并行吞吐（2156→2292 ops/s）；1w 主信号无回归，4w 底线未破。**ReadIndex 批合并的进一步收益归 Phase 3 (P2-A)**（证伪标准 §3.5：`rounds/s ≥ 0.8× reads/s` 且 `released-per-round ≤ 1.3×` 判无操作、转向读并行上限）。
- **原始产物**：`docker/bench/bench-artifacts-phase2/round{1,2,3}.json|.txt`、`ss-metrics.txt`、`metrics-round.txt`（未入库）。

## Phase 3: P2-A 单飞 cohort ReadIndex 屏障 + E 簿记瘦身 [CLOSED — A 预证伪，E 落地]
- [x] 3.0 前置绿门：2.0 INV14 sim 在改动前全绿（已随 Phase-2 落地，重复确认通过）
- [ ] 3.1 (TDD) 重设计实现 cohort 屏障 → **NOT DONE — A 按 §3.5 证伪标准判定预证伪，见下方证伪结论**；不实现，避免高风险 INV14 敏感读路径重写验证已知先验
- [ ] 3.2 (TDD) 重试/超时 cohort 语义 → **NOT DONE — 依赖 A，随 A 一并证伪关闭**
- [x] 3.3 (TDD) E 簿记：`Option<NonZeroU64>` token（消除 `token:0` 占位脆弱性 + 删冗余 `read_index_issued`）、`next_read_token` 起于 1、read_states 直接按 `Option<NonZeroU64>` 匹配 → commit `bca9412`，**声明范围 = token 脆弱性修复 + 高 pending 尾部风险（O(pending²)），非基准性能**；`cargo test -p arachne-kv` 全绿（含 INV14）
- [x] 3.4 回归：INV14 绿 + workspace/l2/model-check/fuzz + 编译（随 E 执行）
- [ ] 3.5 bench A/B 证伪 → **以预注册标准 + 已有证据判定 A 无操作（不跑 A/B——A 未实现，判据由基线 §7 + Phase-2 机制表 + 上限核算闭合）**
- [ ] 3.6 cohort 指标（released-per-round、attach rate）→ **NOT DONE — YAGNI，A 证伪后无需新增指标；`read_index_rounds_total` 语义不变（E 为内部重构）**

### Phase-3 证伪结论（A — cohort ReadIndex 屏障）

**结论：A 预证伪，不再实现。** 四链证据，其中两条于实现后不可再得：

1. **C4 验尸**（`propsol-v0.2.md:553-556`）：读到达时 in-flight≈0；时间窗口合并实测无操作并回滚。
2. **Phase-1 基线 §7 + Phase-2 机制表**：reads/round 0.71→0.53（轮次 > 读次），pending 峰值仅 3–8 → 无排队形成，特征在基线即可观测。
3. **Phase-2 自然实验（决定性）**：B 落地使 rounds/s **+37%**（3154→4322）的同时吞吐 +5.6~14.6%、p50 全线下降 → ReadIndex 轮次发射**不是**吞吐约束；而 A 的唯一杠杆恰是减少轮次。
4. **到达流上限核算**：A 的 cohort 上限 ≈ 1+λ·RTT ≈ 1.6×（λ=2892/s，RTT≈0.2ms），轮次减少上限 ≈ 35%，且轮中到达者多付一轮 RTT 延迟；收益上限低于/紧贴阈值。

**替代说明（预注册修订）**：§3.5 字面为"实现后 A/B 判定"（`released-per-round` 仅 A 实现后才存在）；本结论以基线 `reads/round` + pending 峰值 + Phase-2 自然实验 + 上限核算**替代**实现后判据（亦为普通 1.3 早已标注"P2-A 证伪基线"的务实解读），因自然实验与上限共同闭合了因果缺口。证伪结论注册在 `ref:ora-1` 决策。

**重开触发条件**（基于现有指标，无需新工具）：若 `read_index_pending` 持续峰值 ≥ ~2× 基线（新负载形态下），或 RTT 膨胀后重算上限，则重开 A。

- 佐证：fix-2 独立推演（稳态下 A 与 C3 每轮相等、无增量收益）与 @oracle 决策一致（不统计为实现，避免沉没成本推理）。

## Phase 4: P4 读服务线程解耦 + 出站不内联 await [COMPLETE]
- [x] 4.0 构建接缝前置：F4 决策落库（仅读服务移出 actor 线程，apply 留在 actor 保 l2/turmoil 确定性；Phase-2 `Arc<RwLock<Store>>` 为接缝边界）（commit `4be38b8`）
- [x] 4.1 (TDD) 读服务（SM 读侧）移出 actor 的 current-thread runtime；线性/弱读的 `sm.get`+oneshot 交予 spawn 的 worker，ReadIndex 门/版本告警留 actor；新 TDD 门 `tests/read_concurrency_off_actor.rs`（16 并发线性读 + 写风暴，deadline 内全解析）（commit `4be38b8`）
- [x] 4.2 (TDD) peer 出站 per-peer 有界队列 + 后台 sender 任务（`OUTBOUND_QUEUE_DEPTH=32`，满=丢弃+计数、follower 重传覆盖；仅 `start_outbound_sender` 带 `+Clone` 约束，非 Clone 测试传输可编译；快照路径已按本任务要求审计不经过该队列）；TDD `tests/outbound_queue.rs`（fast 复制 + 慢传输溢出计数 + 收敛；慢传输下 `Timeout`=文档超载语义）→ commit `160d446`（fix-3 中途取消，orchestrator 收尾完成）
- [x] 4.3 回归 + bench：workspace 测试 全绿 + l2 确定性双跑 ✓ + model-check 无 counterexample ✓ + release 构建 ✓；A/B（`dev-docs/bench-baseline-phase4-2026-10-07.md`）：**1w 4267 ≥ 4157 无回归 ✓、4w 2917 ≥ 2892 底线、2w/8w/stale/put 均在方差带**；4w rounds/s ≈ 3182；pending 0–4（4.1 去 actor 后预期）；INV14 门保持绿（commit `6fea0d1`）

## Phase 5: 4w 门槛固化 + 文档 + 合入 [IN PROGRESS]
- [ ] 5.1 `scripts/check-perf-baseline.sh` 增 4w 线性读门槛：脚本内 n=2–4 运行取中位数、阈值=**不劣于改动前 4w 底线**（回归式）、1w 保持主信号
- [ ] 5.2 更新 `README.md` 测量章节与 `dev-docs/propsol-v0.2.md` 记录（含 A 证伪结论、cohort 语义迁移）
- [ ] 5.3 完整门禁 `scripts/check-*.sh` 全绿 + 合入 main（PR，含 2.0 INV14 测试与基准对照）

## Notes
- 2026-10-07: 计划源自已合入调研报告（PR #1, `research/report-read-concurrency.md`）`ref:exp-1`, `ref:res-1`
- 2026-10-07: @oracle 评审（`ref:ora-1`）：需重构 —— F1 语义钉死为"每轮放行自己的 cohort"；F2 调整顺序 B→A；F4 保 sim 确定性；删除原 P5.1 收集窗口
- 2026-10-07: 4w 高方差警告 —— 每阶段"同集群 A/B + 多轮中位数"，不得以单次 4w 判胜；1w 为稳定主信号
- 2026-10-07: 目标以机制性指标可证伪（cohort 释放、rounds/sec、1w 无回归），不追方差内单次 14% 数字
- 已删除的原 P5.1（收集窗口旋钮）：C4 已实测回滚同款（`propsol-v0.2.md:549-562`）；修复后 A 本身即收集机制；如未来重提，先回看 §Y 验尸


- 2026-10-07: **Phase 2 完成**（commit 链 `c23430b`→`f9f45cb`→`da13a1f`→`0c2af66`→`b8c4ae7`）。读并行化 B 全部落地：去 mpsc 串行、直接 SM 服务、weak 读脱离 apply 线程、`restore()` 解码移锁外。A/B（同集群、3 轮中位数）：1w 4256≥4157 无回归；4w 2892≥2739 底线、8w +14.6%；put 不反弹；stale +3.0%。ReadIndex rounds/s 3154→4322（reads/round 0.68→0.53）为去串行副作用，机制归因见 §Phase-2 结果。Phase 3 (P2-A cohort 屏障) 可启动：2.0 INV14 门已绿.
- 2026-10-07: **Phase 3 关闭 —— A 预证伪（`ref:ora-1` 决策），E 落地**。完整证伪结论见 §Phase-3 证伪结论块（C4 验尸 + 基线 §7 reads/round<1/pending 3–8 + Phase-2 自然实验 rounds/s+37% 而吞吐升 + 上限核算 ≤1.6×）。3.1/3.2/3.6 记为 NOT DONE（随 A 关闭，YAGNI）；3.3 (E) 落地为 Token/簿记瘦身，scope = 脆弱性修复 + 高 pending 尾部风险，非基准性能。Phase 4 启动（读服务线程解耦 + 出站不内联 await），前置 = E（已落地）+ INV14 门重挂 4.3。
