# 研究报告：多客户端并发读场景下 Arachne 性能落后于 etcd 的架构分析

- 工作区/分支：`.slim/worktrees/research-read-concurrency` / `omos/research-read-concurrency`
- 基线：`main` @ `3d7e1e2`
- 日期：2026-10-07
- 范围：本报告只回答两个问题 —— (1) 为什么在多客户端并发读（4 连接线性读）场景下 Arachne 落后于 etcd；(2) 架构上是否还有提升并发读性能的空间。**不包含代码改动**，属于纯研究交付。
- 证据来源：Arachne 源码走读（见 `research/findings-arachne.md` 关键结论摘要）、etcd release-3.5 源码与官方文档（`research/findings-etcd.md`）、仓库既有基准（`docker/bench`、`docker/bench-etcd`）与设计文档（`dev-docs/propsol-v0.2.md` §X–§Z、`dev-docs/handoff-m1.md`、`dev-docs/upgrade-http-wal.md`）。

---

## 1. 结论摘要（TL;DR）

1. **瓶颈在服务器端线性读（ReadIndex）的串行段**，不在客户端，也不在网络。仓库此前 GIL 对照实验已证明：剥离客户端 GIL 解救了 stale 读（415→6890 ops/s），却**没有**解救线性读（1495→1361 ops/s），即"瓶颈在 server 端线性读（ReadIndex）串行段"（`propsol-v0.2.md:530-532`）。
2. 根本差异：etcd 用 **每请求一个 goroutine + 并发 bbolt 只读事务 + 单飞（single-flight）ReadIndex 广播式放行**，读并发度随客户端数量线性增长；Arachne 把**所有读、写、tick、读确认、状态机读取都压在一个 actor 线程 + 一个 apply 任务**上，天然按客户端数量线性退化为串行。
3. **结论：架构上存在明确、可量化、按优先级排列的提升并发读性能的空间。** 具体见 §6。最高价值的两项：
   - **A. 单飞合并式 ReadIndex 屏障（broadcast notifier）** —— 把"每轮只放行当轮到达的读"改为"一轮 quorum 往返放行所有等待中的线性读"（etcd `linearizableReadLoop` 模式）。这是与 etcd 差距的最大来源。
   - **B. 状态机读并行化** —— 目前 `BTreeMap` 无锁但由唯一 apply 任务独占读取，且与 actor 同线程；改为快照/`Arc<RwLock>` + 直接读，去掉每读一次 mpsc 往返，并让读服务脱离 actor 线程。
4. 注意：v1 明确禁止 lease/clock read（`ReadOnlyOption`=Safe，`handle.rs:275-283`、`handoff-m1.md:94`），所以"线性读免 quorum 往返"不可行；空间在**合并/重叠/缩短 quorum 往返**与**并行化读服务**上，而不是消灭往返。
5. 观测量级参考（同一环境）：4w 线性读 arachne ≈ 2043 ops/s / p50 0.43ms vs etcd ≈ 2365 / 0.40ms，Arachne 落后约 14%（`propsol-v0.2.md:561`）。两者都随连接数"反缩放"（arachne 4w/1w≈72%，etcd≈65%），但 etcd 出场并发基数高。Go/Python 客户端开销≈1.12 恒定因子，14% 差距**不是** Arachne 特有客户端惩罚（`propsol-v0.2.md:577-600`）。

---

## 2. 问题定义与基准事实

### 2.1 场景

基准为 3 节点集群、同硬件、同客户端形态（`docker/bench` 与 `docker/bench-etcd`），线性读 = `GET /kv/<key>`（etcd 侧 `POST /v3/kv/range` + `linearizable:true`），每 worker 一个 keep-alive 长连接，全部打向 leader。

### 2.2 基准结论（来自仓库文档，均为已记录事实）

| 指标 | Arachne | etcd | 出处 |
|---|---|---|---|
| 线性读 1 连接 | ~2900–3500 ops/s / p50 0.28–0.36ms | ~1531 / 0.58ms | README:133-141 |
| 线性读 4 连接（同环境） | ≈ 2043 / p50 0.43ms | ≈ 2365 / 0.40ms | propsol-v0.2.md:561 |
| stale 读 4 连接 | ~2200 ops/s | ~1388 | README:137 |
| put | 500–700 ops/s | ~821 | README:136 |

要点：**单连接线性读 Arachne 明显领先；4 连接线性读 Arachne 落后约 14%**。README 与 propsol-v0.2 §Y/§Z 均指出 4w 是~2× 高方差区间，方向会在不同集群翻转，因此这不是一个极稳健的服务器侧结论；但机制归属是明确的：**残余差距 = 每个 drive_cycle 一轮 read-index quorum 往返，etcd 调度更优**（`propsol-v0.2.md:542`）。本报告研究的就是这一机制差距的架构根源与对策。

---

## 3. Arachne 并发读路径架构分析

### 3.1 读路径端到端（Linearizable `get`）

```
HTTP GET /kv/<key> (arachne-node http.rs, hyper, 每连接一个 tokio task, Semaphore(256))
  → Handle::get_with_redirect (client/handle.rs:522-579)
  → Command::Read 压入单一 mpsc(1024) 命令通道 (handle.rs:669-689)
  → actor 主循环 (runtime/mod.rs:888-955, 单线程 current_thread runtime, 专用 OS 线程)
      ├─ C3 合并: 每 drive_cycle 给本轮全部未签发读盖一个单调 token, read_index() (runtime/mod.rs:985-996)
      ├─ step(): 内联 await 向 peer 广播 heartbeat(+ctx) (commit: raft.rs:2176-2182; Arachne 侧 runtime/mod.rs:998)
      ├─ quorum ack → Ready.read_states (raft.rs:1906-1919, 2451, 2930)
      ├─ token 匹配 + resolve_reads (runtime/mod.rs:1016-1033, 1800-1860)
      └─ try_send 到 reads mpsc(4096) (runtime/mod.rs:1824-1833)
  → 唯一 apply 任务 serve_read: sm.get(&key) → BTreeMap::get().cloned() (runtime/mod.rs:504-509; state_machine/kv.rs:305-307)
```

`get_stale`（本地弱读）路径：`Command::GetStale` 走**同一个** `reads` mpsc 通道，由**同一个** apply 任务 `serve_read` 服务（`runtime/mod.rs:1637-1651`）。不经过 raft，但**读服务本身仍单线程**。

### 3.2 读热路径上的全部串行点（file:line）

| # | 串行点 | 位置 |
|---|---|---|
| S1 | **单一 actor 事件循环**：所有读/写/tick/入站/resolve 共用一个 biased `select!` | `runtime/mod.rs:888-955` |
| S2 | **专用 current-thread runtime，一个 OS 线程**（actor + apply 任务 + spawn 的子任务都在这一个线程上） | `runtime/mod.rs:2213-2227`；apply 于 `885-886` spawn |
| S3 | **单一命令通道** `mpsc(1024)` 承载所有客户端 | `runtime/mod.rs:799`、`handle.rs:829` |
| S4 | **round 发射串行化**：每 drive_cycle 最多一次 `read_index`；窗口外到达的读各自新开一轮 | `runtime/mod.rs:985-996` |
| S5 | **quorum 往返**：每个线性读门槛 = leader→2 peer 一次 heartbeat 交换（残余主导项） | `raft.rs:2176-2182,1906-1919` |
| S6 | **`step()` 内联 await peer I/O**：出站发送在 actor 循环内 `await`，慢 peer 会停摆整个循环，拖慢所有轮次 | `consensus/node.rs:1131-1137,1149-1171,1199-1202`；`runtime/mod.rs:998` |
| S7 | **有界入站 mpsc(1024)**（`try_send`，满则丢弃）—— 丢弃的 heartbeat 响应延迟确认 | `transport-tonic/src/server.rs:238-246`、`factory.rs:58,208` |
| S8 | **唯一 apply 任务读服务**：所有已确认线性读 + 所有 stale 读汇入一个 `reads` 通道（cap 4096）→ 一个线程 | `runtime/mod.rs:599,741,1824-1833,1637-1651,504-509` |
| S9 | 读上限 `MAX_PENDING_READS=4096` → `Busy` | `runtime/mod.rs:313,1662-1665` |
| S10 | `resolve_reads` 每轮线性扫描 + drain/重分配 `Vec`；token 逐读匹配 | `runtime/mod.rs:1804-1859,1027-1032` |
| S11 | tick 与读同循环争用（Lan tick=100ms，读负载下影响小） | `runtime/mod.rs:892`、`node.rs:507-509` |
| S12 | 读超时重试（deadline=2×election，至多重试一次） | `runtime/mod.rs:1838-1855` |

不在读热路径上（仅写路径）：apply 积压 `Busy`（Q7）、session 带宽/SessionExpired/SessionTableFull、`note_session`（`runtime/mod.rs:1479-1504,1920-1938`）。

### 3.3 并发模型关键事实

- 状态机 `KvStateMachine` 是**裸 `BTreeMap`，无锁**（`state_machine/kv.rs:66-71`），因为**只有一个 apply 任务独占它**。这带来的不是安全而是**吞吐上限**：读服务零并行，且线性读还要多一次 mpsc hop。
- C3 读合并（`propsol-v0.2.md` §X，commit `0d99cb3`）已把 4w/1w 从 32% 提到 60–65%、p50 0.66→0.48–0.51ms；C4 跨周期窗口合并（`5906210`）被实测**回滚**：quorum 往返 0.1–0.5ms < 读到达间隔 1–2ms，窗口宽 0 等价于无合并（`propsol-v0.2.md:549-562`）。
- 读确认是事件驱动（入站 heartbeat 响应），不依赖 tick；idle 线性读 ≈0.3ms（`handoff-m1.md:271`）。

---

## 4. etcd 并发读架构对比（详见 `research/findings-etcd.md`）

etcd 在并发读上具备 Arachne 所没有的八项机制：

1. **每请求 goroutine，端到端无 actor 循环**：gRPC（HTTP/2）每流一个 goroutine（`NumStreamWorkers(0)`），`MaxConcurrentStreams` 默认 `MaxUint32`，`readwaitc` cap=1。N 个客户端 = N 个 goroutine + N 个独立 bbolt 只读事务，读路径上没有任何用户态全局锁。
2. **客户端侧读合并（关键）**：每个节点**一个** `linearizableReadLoop` goroutine，一轮 ReadIndex quorum 往返后用 **broadcast notifier** 放行**无界**的等待读者队列（`v3_server.go`）。吞吐 ≈ (每轮放行的读者数) / RTT，与连接数解耦。上游注释原文："as a single loop can unlock multiple reads"。
3. **raft 层 read-index 队列批处理**：`readOnly.advance` 一次 quorum 确认完成所有排队的 ReadIndex 请求（`raft/read_only.go`），即使多个独立请求也共享一轮 heartbeat。
4. **serializable 读零共识**：直接从本地 bbolt 读，完全无 raft 交互 → 集群读并行度随成员数增长（follower 也服务本地弱读）。
5. **并发 bbolt 只读事务**：v3.5 Range 默认 `ConcurrentReadTxMode`，每请求独立只读事务 + 私有 buffer 拷贝，`Lock/Unlock/RLock` 均为空实现、`RUnlock` 仅 `txWg.Done()` → 零共享锁。
6. **内存读缓冲**：热 key 由 `txReadBuffer` 内存命中，不触 bbolt；写入端 10k ops / 100ms 批量提交+fsync，读几乎不被写阻塞。
7. **共享 applied-index 等待**：`ApplyWait(confirmedIndex)` 一次放行整个 cohort。
8. **HTTP/2 多路复用 + 无限并发流**：连接数不是 per-node 瓶颈。

官方性能数字（3 节点、8 vCPU）：线性读 1 连接 1,353 QPS（0.7ms）；100 连接 × 1000 客户端 → **141,578 QPS（~104×）**；serializable 同载 → 185,758 QPS。即 etcd 读吞吐随并发**近似线性**增长。

---

## 5. 根因分析：为什么 etcd 并发读更优

把两条发现对到一张表上：

| 维度 | etcd | Arachne | 差距定性 |
|---|---|---|---|
| 读执行粒度 | 每请求独立 goroutine（+独立 bbolt 只读事务） | 全部经过唯一 actor 循环 + 唯一 apply 任务 | **负载分散 vs 单一漏斗** |
| 线性读合并 | `linearizableReadLoop` 一轮放行无界队列 | C3 只合并 drive_cycle 事件窗口内的读；窗口外读各开一轮 | **etcd: 按 RTT 放行全队；Arachne: 按到达批次放行** |
| 状态机读 | 每请求独立快照读，零锁 | 单线程 BTreeMap + 每读一次 mpsc 往返 | **并行读 vs 串行读** |
| 共识/存储线程 | raft、apply、backend 分处独立 goroutine/核 | 共识 + apply + 读服务挤在一个 OS 线程 | **多核 => 零核** |
| 弱读 | 本地零共识 | 本地（已零共识），但读服务仍单线程 | 部分对齐 |
| 入站方阻塞 | goroutine-per-stream，无共享队列 | 有界 mpsc，满则丢响应 | 二阶 |

**一句话根因：etcd 的读并发度 ≈ min(客户端数, 机器核数)，且一轮 quorum 往返放行所有等待者；Arachne 的读并发度被压到 1（单 actor/单 apply 线程），合并窗口又窄到不足以抵消 quorum 每往返成本，于是在多客户端下把"每客户端成本"线性加回。**

C3 之所以只把 4w/1w 从 32% 提到 60–65% 就触顶，正是因为它只合并"同一 drive_cycle 突发窗口"内的读；稳态 keep-alive 负载下读到达间隔（1–2ms）> quorum 往返（0.1–0.5ms），窗口合并不产生额外收益——这已在 C4 实验中直接证实。

---

## 6. 架构提升空间评估（本报告核心结论）

**结论：有，且空间明确。** 按"性价比 × 与 etcd 差距的对应度"排序：

### A.（最高价值）单飞合并式 ReadIndex 屏障 —— 对齐 etcd `linearizableReadLoop`
- 机制：新增读时不再立刻按轮次签，而是在"当前有一轮在途 quorum 往返"时，把新到达的读挂到一个**共享广播 notifier** 上；该轮确认后**一次性放行所有挂起读者**（读下界 = 该轮确认的 `read_index`）。新轮只在这轮未确认且新读到达时才开。
- 与 C4 的区别：C4 是"时间窗口合并"（实测 0 收益）；本方案是"在途往返附加"（`in-flight-attach`），把后到达的读计入**已发射**那轮的放行集合，架构上完全不同且未试过。
- 目标位置：`runtime/mod.rs:985-996`（C3 块）+ `PendingRead`（`290-309`）+ `resolve_reads`（`1800-1860`）。etcd 参考实现：`server/etcdserver/v3_server.go` 的 `readwaitc`(cap 1)+`readNotifier`+`linearizableReadLoop`。
- 预期效果：多客户端下线性读吞吐从 ≈1/RTT×batch 提升到 ≈N/RTT（受 `MAX_PENDING_READS` 与响应延迟联合约束），直接吃掉 §2.2 的 ~14% 差距的大头。

### B. 状态机读并行化 + 去掉 apply 漏斗
- `KvStateMachine` 改为发布**不可变快照 / `Arc<RwLock<BTreeMap>>`**（`state_machine/kv.rs:66-71,305-307`）：
  - `resolve_reads` 直接从内存读，**去掉每线性读一次 mpsc hop**（`runtime/mod.rs:1824-1833` → 直接 `sm.get`）；
  - `get_stale` 可在 apply 外部（甚至 HTTP 边缘）并发服务（`runtime/mod.rs:1637-1651`）；
  - 快照可让**多个读 worker 并行**读同一版本，写方换快照不阻塞读者（etcd 快照式读模型的最小复刻）。
- 注意并发正确性：需保证"已确认 read_index ≤ 快照版本"，即快照发布与应用顺序强一致（可在 apply 侧每批写后发布新快照，读侧持旧快照合法）。

### C.（与 A 配套）apply 服务移出 actor 线程 / 或 apply 自身多核
- 目前 actor + apply 共用**一个** `current_thread` OS 线程（`runtime/mod.rs:885-886,2213-2227`）。把 apply（或至少读服务）放到多线程 runtime/独立线程，读 CPU 成本与共识事件处理解耦，与 etcd"raft/apply/backend 分核"对齐。
- 连带收益：S6（内联 await peer I/O）的副作用面变小。

### D. 消除 S6：出站消息不内联 await
- 把对等人的出站 `Message` 投递改为有界 per-peer 队列 + 独立发送任务，慢 peer 不再停摆 actor（`consensus/node.rs:1131-1137,1149-1171,1199-1202`）。属于稳健性/尾延迟改造，直接提升并发下的 quorum 确认吞吐。

### E.（低成本）读簿记瘦身
- 用 `HashMap<token, PendingRead>`/索引替换 `read_states` 线性 token 匹配（`runtime/mod.rs:1027-1032`）与 `pending_reads` 每轮 drain/重分配（`1804-1859`）。单读成本小幅下降，乘上并发基数有可观收益。

### F.（可选权衡）受控"收集窗口"
- 在 round 发射与发送之间加一个**有界**短收集窗口（≤1 tick）以扩大 C3 批量，以 p50 换吞吐。仅作为可调旋钮，非必选（`propsol-v0.2.md:556` 已注明该权衡）。

### 明确排外的选项
- **lease/clock read 不可行**：v1 设计硬性要求 `ReadOnlyOption=Safe`，禁止 lease 读（`handle.rs:275-283`、`handoff-m1.md:94`），不可作为"免 quorum 线性读"的途径。
- **降低一致性**不在本问题范围内（用户问的是"并发性能"而非语义让步）。

---

## 7. 建议的验证闭环（后续若要动代码）

1. 用仓库既有基准 `docker/bench`/`docker/bench-etcd` + `scripts/check-perf-baseline.sh` 作为回归（注意 4w 高方差：按 `propsol-v0.2.md:577-600` 的结论，**核心信号取 1w；4w 需多轮取中位数并同集群 A/B**）。
2. 指标对齐：`arachne_read_index_rounds_total`（服务器侧成本单位，`metrics.rs:107-113`）与 `arachne_read_index_pending`（`metrics.rs:24-30`），按 README 续篇清单（`docker/bench/README.md:184-195`）在并发写负载下观察 rounds/sec 与 4w p50 的相关性，验证"每轮放行数"（读者/轮）提升。
3. 任何改动先写单元/确定性测试（`l2/`、`model-check/`），再跑 `scripts/check-*.sh` 全套门禁；编译验证：`cargo build --release -p arachne-kv-node`。
4. 若实施 A（单飞合并屏障），用 `docker/bench` 同参数 A/B：预期 4w/1w 提升且 rounds/sec 显著下降。

---

## 8. 附件与引用

- `research/findings-etcd.md` —— etcd v3.5.x 并发读架构完整取证（428 行，含源码行级引用）。
- Arachne 侧走读摘要见本文 §3（详尽的 file:line 注释已内联）。
- 关键设计文档：`dev-docs/propsol-v0.2.md` §X/§Y/§Z（C3、C4、GIL 对照、§Y 数字）、`dev-docs/handoff-m1.md`（lease 禁用的由来与饱和 flush 教训）、`dev-docs/upgrade-http-wal.md` §2（历史基准演进）。
- 仓库既有测量词汇：单连接线性读 ~2900–3500 ops/s；4 连接线性读 ≈2043 vs etcd ≈2365；stale 4 连接 ~2200 vs ~1388。
