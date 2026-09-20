# Arachne 交接：M0 完成，M1 进行中

> 面向**新会话**。先读本文，再读 `dev-docs/propsol-v0.2.8`（设计权威）与 `dev-docs/test-plan-v0.1.3`（测试方案）。会话态进度见 `.slim/deepwork/arachne-m1.md`（git-local，但 OpenCode 可读）。
>
> 仓库：`/Users/alex/Projects/workspace/Arachne`，git 已 init，工作树干净，**291 tests 0 failed**、0 告警、`scripts/check-deps.sh` 与 `scripts/check-entropy.sh` 全绿。

## 1. 现状总览

- **M0 已完成**（含终检门禁，COMPLETE）。六工件工作区 + 崩溃安全 WAL + raft 集成 + KV/会话状态机 + 全套测试基建 + `arachne-node` 单节点可运行 + examples + L4 runner 脚手架 + stateright/turmoil 骨架 + spike 关闭。
- **M1 进行中**。已完成 M1-1、M1-2（含整改）、M1-3a、**(A) arachne-node 多进程 tonic 接线（commit 5081417）**、**(B) M1-3b ReadIndex 线性一致读（commit 6616827）**、**(C) stage 1 ClientOracle + 线性化检查器（a915f6b；自证 395fc9a；门禁 GO）** 与 **(C) stage 2 D-S1 raft 可播种选举 RNG + 双跑金丝雀（commit f132e44；门禁 GO）**；**未完成 (C) stage 3（transport I/O 接缝 + turmoil SimNetwork + S01/S02/S16 + INV3/4/7/8/9 + 双跑门禁）+ M1-5/(D)**（(A) 已覆盖 M1-5 的 L3 前置与冒烟主体，(D) 的杀 leader / CLI 集成测试仍待做）。
- **✅ 已恢复**：oracle provider 故障（模型 id 无法解析 + 空结果）已解决；(C) stage 1 确认门禁已补跑并 **GO**，stage 2 门禁亦 **GO**。

### 提交线（新 → 旧）
```
272967a M1-4 (C) stage 3c inc.7: ReadIndex 路径的读                          ← HEAD
c478d7d docs: (C) stage 3c inc.6 门禁 GO + 打磨
f010304 M1-4 (C) stage 3c inc.6: 门禁 P4 打磨（检查器侧断言 + EOF）
32b8b7e M1-4 (C) stage 3c inc.6: oracle 幻值负路径（注入）
6815bef M1-4 (C) stage 3c inc.1: 内存传输确定性 L2 场景（S01/S02+INV3/4/7/8/9）
76df587 M1-4 (C) stage 3b spike: 真实 tonic over turmoil（部分；commit 路径卡住）
377f3ce fix(runtime): actor 截止时间改用模拟 tokio::time::Instant（确定性）
edd913d M1-4 (C) stage 3a: 可注入 transport I/O 接缝（生产默认不变）
f132e44 M1-4 (C) stage 2: D-S1 可播种 raft 选举 RNG [patch.crates-io] + 双跑金丝雀
9bf857f docs: (C) stage 1 确认门禁 GO
395fc9a M1-4 (C) stage 1 自证补强（1000 种子差分/witness 回放/optional 单调性）
a915f6b M1-4 (C) stage 1: ClientOracle v1 + 自建线性化检查器
6616827 M1-3b 真实 ReadIndex 线性一致读（Safe 读 + wait applied + 1 重试）
5081417 M1-5 (A) arachne-node 真实 tonic 3 进程集群 + start_with_bind + config addresses
8cbc765 M1-3a(3/3) follower 写重定向到 leader（真实 tonic 3 节点）
ea08dab M1-3a(2/3) arachne-node 接线 lib runtime + /kv HTTP 端点
2b8b046 M1-3a(1/3) lib 客户端 Handle + runtime actor
67358fa ci: 安装 pinned protoc 3.x
148e3a7 fix(build): find-protoc.sh 兜底 PyTorch 3.x protoc
ffef8ef M1-2 整改（超时/有界入站/消息尺寸/start 泄漏/shutdown）
60d61e2 M1-2 tonic 真实传输 + 3 节点网络证明
b3043b3 docs: propsol v0.2.8（E-rev K：修正 §7 约束与预设矛盾）
151066a M1-1 Profile/配置 + tick 接线 + crate 文档语义表
69d3038 M0 终检整改（关闭）
… (M0 的 P1–P5 提交见 git log)
```

## 2. 工件与关键路径

- `arachne-seam`（零依赖叶：接缝 trait + 核心类型）、`arachne`（产品核心：WAL/raft 集成/KV 状态机/客户端库）、`arachne-transport-tonic`（唯一含 tonic/prost 的 crate）、`arachne-node`（运维 bin）、`arachne-testsupport`（dev-only）、`arachne-sim`（L2 bin）。
- 非工作区独立项目：`fuzz/`、`model-check/`（stateright）、`l2/`（turmoil）、`l4/`（kill -9 脚本）。
- 客户端 API：`arachne::client::{ArachneError, Handle}`；运行时 actor：`arachne::runtime::{Runtime, RuntimeConfig, Command}`；指标：`arachne::Metrics`。
- `arachne-node` 的 HTTP：`/readyz`、`/metrics`、`PUT /kv/<key>/<value>`、`GET /kv/<key>[?stale=1]`、`DELETE /kv/<key>`（非法参数→400、quorum→503）。**多进程注意**：非 leader 的写当前返回 **503**——`Handle` 的 redirect 只认识**进程内** peer（`register_peer`），跨进程没有 client redirect，因此文档语义中的 `NotLeader→409+leader hint` 在多进程 HTTP 面**不可达**（进程内嵌入仍有 409）。跨进程 409+hint 留给 (D)。

## 3. M1 剩余工作（按推荐顺序）

### (A) arachne-node → tonic 多进程（M1-5/L3 前置）— **已完成（commit 5081417）**
实现与最初建议的差异（供 (D)/(B) 接手）：
1. `TonicTransportFactory::start_with_bind(me, bind)` 已加（`start()` 会绑定地址表**所有**条目——多进程会 EADDRINUSE）；公共 serve 逻辑抽到私有 `start_targets`，`start()` 行为不变。
2. 配置：`initial_cluster` 条目现支持 `"<id>"` 或 `"<id>=<ip:port>"`（**向后兼容**，l4/ 脚本的 `["n1"]` 不变）；`Config.addresses: HashMap<NodeId, SocketAddr>`，self 恒为 `listen`（若 self 内联地址与 `listen` 不一致 → fail-start），**非 self 成员必须给出内联地址**（缺失 → fail-start）。
3. `node.rs` 用 tonic 传输 + `RuntimeConfig{ peers, addresses }`（peers 由 `initial_cluster` 的 1-based 下标导出）；`shutdown` 先 `factory.shutdown().await` 再 abort actor。M0 占位 `transport.rs` 已删除。
4. 多进程 e2e：`arachne-node/tests/multi_node.rs`（3 个真实进程，`/readyz`→PUT→GET→`?stale=1` 复制，Gate C 合规）+ 传输级 `arachne-transport-tonic/tests/multi_node_bind.rs`（per-node bind + 跨 factory 投递 + 0 握手拒绝）。
顺带修复：`http.rs::dispatch_line` 原先只放行 GET，导致文档化的 `PUT`/`DELETE /kv/...` 永远 405——现放行 `GET|PUT|DELETE`，其余 405。

### (B) M1-3b：ReadIndex 线性一致读 — **已完成（commit 6616827）**
- `ReadOnlyOption` 由 `LeaseBased` 改为 **`Safe`**（propsol §149 明确 v1 禁 Lease Read，读一致性只靠 ReadIndex，不靠时钟）。
- `RaftNode`：新增 `read_index(ctx)`；`step()` 返回 `StepOutcome { committed, read_states }`（`read_states` 在 `advance` 之前从 `Ready` 捕获——只出现在 `Ready`，不在 `LightReady`）。
- actor：`Command::Get` → `Command::Read`；`PendingRead` 等待队列（上限 4096 → `Busy`）；读状态 ctx→token 关联后等待 `applied ≥ read_index`，超时 `read_index_timeout`(=2×election) 重试 1 次，term 变化/step-down → `NotLeader{hint}`，仍失败 → `Timeout`。follower 直接 `NotLeader`（§5.4 step 5，不做服务端转发）。
- 指标：新增 `arachne_read_index_timeout_total`（counter）、`arachne_read_index_pending`（gauge）。
- 测试：`arachne/tests/read_index.rs`（进程内 3 节点：leader ReadIndex 读、follower `get` 重定向、缺失键 `None`）+ node 单测（read_state index 语义）。
- **顺带修复**：`client/mod.rs` 首页语义表与 `arachne-node/main.rs` 的 GET 说明原为"临时 leader-local 读"，已更正为 ReadIndex；`ArachneError::Busy` 文案覆盖读等待队列。
- **已知缺口/延后**：`read_index_round_latency` 直方图留 M2（无直方图设施）；`redirects_total` 属客户端侧（`Metrics` 无法从 `Handle` 触达）；**"已被废黜但尚未察觉的旧 leader 不得服务陈旧读"这一决定性性质需要分区注入 → 属 (C)/L2（INV14、S02/S16）**。现有 `read_index.rs` 健康集群下无法区分 ReadIndex 与旧 leader-local 读（已记录）。

### (C) M1-4：L2 turmoil + 不变量 + 线性化（M1 验收 ④）— **分阶段执行中（用户选定：每阶段一个 oracle 门禁）**
**stage 1（已完成，commit a915f6b）：ClientOracle v1 + 自建线性化检查器**
- `arachne-testsupport::oracle`：无时钟纯逻辑历史（`ValueId`/`ClientId`/`SeqNo`/`CallId` + 逻辑 ts）、`merge_retries`（记录层保留原多次调用）、检查①幻值（区间语义：Put 可解释读当且仅当 `put.invoke < get.complete`）、②one-log-id-per-seq（含 Put|Delete）、③RYW（latest-mutation-wins）、④单调读，可选⑤丢写/⑥持久性；`History::validate` 对畸形历史（complete 无 invoke／CallId 复用／同 `(client,seq)` op 不一致）**报错而非静默丢弃**；§9 JSONL。
- `arachne-testsupport::linearizability`：实时偏序上的完备回溯搜索（Wing–Gong 等价）；**required**点（成功 op）+ **optional**点（结果未知/在途写，故 timeout-committed 竞态不再是假违规）；超 `MAX_CHECK_POINTS=12` → `Inconclusive`；witness 贪心最小化；确定性（BTree/Vec、无 HashMap 迭代）。
- 测试：testsupport 79 项（含每检查一条坏历史元测试、贪心对抗回溯正例、**32 种子与 stateright 差分一致**——stateright 仅 dev-dependency，结构化隔离经 Gate C 验证）。
- **门禁状态：GO（已通过）**。初次 oracle 门禁 NO-GO（B1 并发 put/读假阳性、B2 自删 RYW 假阳性、B3 timeout 写被当作未生效）已按门禁处方修复并加回归；期间 oracle provider 出过故障（模型 id `zhipuai-coding-plan/GLM-5.3-Flash` 无法解析 + 空结果），先补强本地自证（commit `395fc9a`：1000 种子多键差分对 stateright、witness 回放 500 种子模糊、optional 单调性、500 顺序历史 oracle+checker 双通过）。provider 恢复后**确认门禁经 ora-2 判 **GO****：三个 blocker 均被证明真实修复且回归测试在旧代码上会失败；S1–S5 解决；新增自证被判为实质性。**遗留 nit（L2 阶段跟进）**：(a) 差分生成器均为顺序历史，未对并发/排序搜索做 stateright 交叉验证（现靠手写并发/回溯用例覆盖）；(b) `MAX_CHECK_POINTS=12` 在对抗性全失败历史上仍有 ~12! 最坏情况——L2 接真实交错历史时需降 cap 或加 memoization。stateright 交叉验证暂限"全成功"历史（未知结果写不喂 stateright，避免 pending-invocation 建模偏差），门禁判定为**可接受的已记录缺口**。

**stage 2（已完成，commit f132e44）：D-S1 可播种 raft 选举 RNG**：`arithmetic raft` 0.7.0 唯一 RNG 点（`reset_randomized_election_timeout`）原用不可播种的 `thread_rng`。已 vendor `third_party/raft/`（与 registry 仅差一个最小可上游化 hook：thread-local 可播种选举 RNG + 每节点子流 `base ^ node_id`；未设种子时行为与上游逐字节一致），root `Cargo.toml` 加 `[patch.crates-io]`，`ARACHNE-PATCH.md` 记录 diff 与「需单线程」约束；`arachne/tests/determinism_canary.rs` = 同种子双跑轨迹逐字节一致 + 不同种子→不同选举超时。**门禁 GO**；已知 nit：Cargo.lock 附带把 windows-sys 0.61.2→0.52.0（semver 合法、Windows-only）；hook 需单线程（E8），已在 PATCH.md 记录。
**stage 3a（已完成，commit edd913d）：transport I/O 接缝**：`arachne-transport-tonic` 新增 `TransportIo` 接缝 + `TokioIoProvider`（真实 tokio）+ `TcpConnector`；`TonicTransportFactory`/`TonicTransport` 对 `Io = TokioIoProvider` 泛型，server 经 `io.bind`/`io.incoming`、client 经 `Endpoint::connect_with_connector`。既有 API 不变（默认类型参数保住 `arachne-node` 用法）；`hyper/hyper-util/tower(util)/http` 均经 tonic 已传递，无新包。**门禁 NO-GO→修 `TcpConnector` 丢 `TCP_NODELAY`（tonic 默认连接器会设、自定义连接器绕过）→GO**。
**stage 3b（部分完成 → 记为 spike，commit 76df587）：真实 tonic over turmoil + SimNetwork**。已落地并可编译：`l2/` 的 `TurmoilIo`（`turmoil::net` listener/stream + `Accepted` newtype 实现 tonic `Connected` + `TokioIo` connector）、`SimNetwork`（partition/partition_oneway/repair/hold/release/crash/bounce/set_fail_rate/set_link_latency）、3 节点 in-sim 真实 tonic 集群；两个 `in_sim` 测试标 `#[ignore]`（原因见下）以保 `l2` suite 绿。
- **已证明可用**：3 节点全部启动（WAL+bind+Runtime actor）；在 turmoil 承载的真实 tonic 上**成功选出 leader**；follower 学到 leader；消息双向健康（`MsgHeartbeat`/`MsgHeartbeatResponse` 双向、term 1）。
- **卡点（未解）**：客户端 `put` 永不提交。经临时探针（已回退）诊断：transport 所有 `send` 均成功；**即使关掉 `check_quorum` 让 leader 稳定，actor 也收不到 `Command::Propose`**——actor 循环卡在 commit 路径上某个永不返回的 `await`（疑为 turmoil 下某次 `send`/`step` await 悬挂或跨 host 死锁）。相同 Runtime/RaftNode/Handle 路径在进程内内存传输下已证明可用（m0/client_runtime）。
- **附带修复**：`Runtime` 的 propose/ReadIndex 截止时间原用 `std::time::Instant`（真实墙钟）→ 在模拟器下是确定性泄漏，已改 `tokio::time::Instant`（commit 377f3ce，root 仍 277 绿）。
- **下一步建议（待用户决定）**：(a) 继续攻 3b：给 `TonicTransport::send`/`step` 加临时超时定位悬挂点，或做最小 tonic 双向 echo-under-turmoil 实验对照；(b) 3b 暂缓，3c 先用**内存传输**跑 INV3/4/7/8/9 + S01/S02/S16（保留 stage 3a seam，real-tonic 作为已记录 spike）。

**stage 3c（增量 1 已完成，commit 6815bef + 46a8328；门禁 GO）：内存传输上的确定性 L2 场景**。新增 `arachne/tests/l2_scenarios.rs`：进程内 3 节点 harness（沿用 `m0_determinism` 的同步 `RaftNode` 模式：手工 `tick`+`block_on(step)`+`on_message`，harness 自控消息投递），用 harness 级 `Faults{isolated}` 丢弃隔离节点往返消息来注入分区/崩溃；选举 RNG 用 stage-2 的 `raft::set_election_rng_seed` 保证 leader 身份可复现。
- 场景/不变量：**S02**（2+1 分区，并断言隔离节点确实未收到多数侧写、多数两侧都提交）+ **INV7**（全轨迹每 term ≤1 leader）；**S01**（隔离=崩溃旧 leader → 幸存者选新主 → 复活收敛）+ **INV9**（新主含已提交条目）+ **INV8**（重叠 index 日志一致）+ **INV3**（收敛后状态快照逐字节一致）；**INV4**（M1 验收④：分区下客户端 put/get 历史经 stage-1 ClientOracle **与** 自建检查器判定为线性一致）；**双跑确定性**（同种子 → leader 轨迹与日志逐字节一致）。
- 门禁 NO-GO 风险已修：分区此前只"配置"未被断言，现已断言（P2）；四测 4/4、workspace 281 绿。
- **增量 2（已完成，commit d895d1c）**：`Faults` 增单向丢包；**S16**（丢所有 follower→leader 消息 → CheckQuorum 将 leader 降级；INV7 仍成立）+ **换主 INV4**（历史中隔离 leader、幸存者选新主、客户端改投新主，整段 put/get 历史经 oracle+checker 判定线性一致；断言恰有一次换主）。`l2_scenarios` 6/6。
- **增量 3（已完成，commit 2b3e3b0；门禁 GO，ora-9；P4 空白已修 777fd6f）**：**并发 INV4**（两客户端 put/get 实时区间重叠 → 真正驱动检查器的并发搜索路径；oracle+checker 双通过）+ **ReadIndex 局部性**（leader 的 `read_index` 经全量投递产生读状态；follower 在不投递时不自服务读，只转发）。harness 记录全量读状态 `(node,ctx,index)`；`l2_scenarios` 8/8。
- **增量 4（已完成，commit dfc92be；门禁 GO，ora-9；P4 修正 5c4d6e4）：多键 INV4 + 分区下并发**：多键（"a"/"b"）写读交替 + 读一个从未写过的键断言为 `None`——使 oracle 首次跑在真正**多键**历史上（跨键混淆会被 harness 直断言 + checker 逐键模型 + oracle 正向幻值检查共同捕获；**注意**：absent-key 的 `None` 读走 oracle 的跳过路径，其"负路径"由 testsupport 单测 `phantom_read_is_a_violation_with_witness` 覆盖）；两客户端实时重叠区间在隔离一个 follower 的 2+1 分区下由多数侧服务、历史仍线性一致（断言被隔离者确实饥饿）。`l2_scenarios` 10/10。
- **增量 5（已完成，commit 2f35da1；门禁 GO，ora-9；P4/保留加强 97f785a）：INV4 跨越换主**：某次 put/get 在换主前被调用、换主后在**新 leader** 上完成（其实时区间横跨 leadership change）；oracle+checker 仍判定线性一致，并断言轨迹出现非初始 leader；加强：对该换主轨迹断言 INV7，且**新 leader 必须保留换主前已提交的条目**（独立键 `"old"`）。`l2_scenarios` 11/11。
- **增量 6（已完成，commit 32b8b7e；门禁 GO，ora-9；P4 打磨 f010304）：oracle 幻值负路径（注入）**：两条注入式违规测试断言 oracle 对幻值读**判定失败**——读一个从未写过的值、以及跨键读（值只写给另一个键）；打磨后两条测试**同时**断言自建检查器亦判 `Violation`。在 L2 层闭合幻值检查的负路径（此前仅 testsupport 单测覆盖）。`l2_scenarios` 13/13。
- **增量 7（已完成，commit 272967a）：ReadIndex 路径的读**：客户端读改走 raft ReadIndex（leader `read_index` → 等 quorum 确认的读状态 → 等 `applied ≥ read_index` → 再读 SM），而非直接读 leader 状态机；所得 put/get 历史经 oracle+checker 判定线性一致，并以"每次读都产生读状态"作非空断言。闭合"读绕过 ReadIndex"缺口。`l2_scenarios` 14/14。
- **增量 8（未开始）候选**：真实 crash+WAL 重启（当前 crash 用隔离建模，volatile 丢失已在 M0 覆盖）；更大规模/随机种子历史；3b（real-tonic-on-turmoil）仍为已记录 spike（`l2/tests/in_sim.rs` `#[ignore]`）。

### (D) M1-5：L3 冒烟 + bin CLI 集成测试（M1 验收 ①）
- (A) 已交付 3 进程成形/写读/复制冒烟（`arachne-node/tests/multi_node.rs`）。**本项剩余**：杀 leader ≤2×election_timeout 出新主；期间写返回 `NotLeader`/`QuorumUnavailable` 而非挂死（②）。
- **已知契约缺口（(D) 需处理）**：多进程下非 leader 写当前是 **503（QuorumUnavailable）**，不是文档的 **409+hint**——`Handle` redirect 只认进程内 peer。修法建议：给 `Handle` 加**不重定向的单发**路径（如 `max_redirects=0` 开关或 `try_put`），让 `NodeHttp::map_error`（已能渲染 409+hint）透传原始 `NotLeader{hint}`；跨进程**自动跟随** hint 还需要 HTTP-port 映射（config 暂无），M1 只做「409+hint body」即可。
- **另一 (D) 注意**：失联旧 leader 的 propose 超时映射为 **500**（`ArachneError::Timeout`），不是 503——② 的断言要么容忍 500，要么调整 `map_error`。
- bin CLI 集成测试（§3.3）：配置校验/flock/META 不一致 fail-start/优雅关闭/`/readyz`/`/metrics`。

### M1 验收对照（propsol §10）
① 杀 leader ≤2×election_timeout 出新主（→ M1-5/(D)）② 期间写返回 `NotLeader`/`QuorumUnavailable` 不挂死（**客户端面已在 `8cbc765` 证明**；(A) 已证多进程非 leader 写返回 503 不挂死；杀主窗口面待 (D)）③ hint 失效经 seeds 轮询恢复（Handle 已实现 seeds 兜底）④ 含切主窗口的 put/get 线性一致（**读路径已由 (B) ReadIndex 落地**；端到端线性化验证仍需 (C)/L2 分区注入）⑤ crate 文档首页语义表/错误矩阵（**已完成** `151066a`，并在 (B) 校正 `get` 行为）。

## 4. 环境注意（踩过的坑）

- **protoc 3.x**：`raft-proto` 的 `protobuf-build` 只接受 `protoc` 3.x；本机系统 protoc 是 25.3。`.cargo/config.toml` 设 `PROTOC = { value = "scripts/find-protoc.sh", relative = true }`；该脚本搜 `PATH`/常见目录/`PROTOC_FALLBACK`/PyTorch 内置 3.x。CI 已加 pinned protoc 3.20.3 安装步骤。**若构建报 `raft-proto` panic `Option::unwrap() on None` + `find-protoc:` 提示，就是没找到 3.x protoc**。
- **子代理稳定性**：本会话 subagent provider 约半数失败（`all candidate providers failed`），重试可恢复；失败时可能留下**未接线的部分文件**，接手前先 `git status`。
- **`NodeError<TonicTransport>` 未实现 `Debug`**（`TonicTransport` 未派生）→ 用 `.expect()` 会编译失败，改用 `match`/Display。可考虑给 `TonicTransport` 派生 Debug。
- **`TonicTransportFactory::start()` 绑定地址表全部条目**（单进程测试可用）；多进程用 `start_with_bind(me, bind)`（已实现，只绑定自身）。
- **Gate 误报**：`check-entropy.sh` 用纯文本 grep，**注释里字面写** `tokio::select!`/`std::time` 会被误判——注释请改述（如"biased select 事件循环"）。测试内用 `core::time::Duration`。
- **默认 profile**：`lan`（heartbeat 100ms / election 1s）。测试里可逐字段覆盖以加速。

## 5. 验证命令
```
cargo build --workspace
cargo test --workspace
cargo build --examples
cargo build -p arachne --no-default-features
bash scripts/check-deps.sh
bash scripts/check-entropy.sh
# 3 节点真实网络（库级）：
cargo test -p arachne-transport-tonic --test three_node -- --nocapture
cargo test -p arachne-transport-tonic --test three_node_client -- --nocapture
# 3 进程真实集群（bin 级，L3 冒烟）：
cargo test -p arachne-node --test multi_node -- --nocapture
cargo test -p arachne-transport-tonic --test multi_node_bind -- --nocapture
# ReadIndex 读路径（进程内 3 节点）：
cargo test -p arachne --test read_index -- --nocapture
```

## 6. 协作约定（沿用）
- 生产代码禁 `unwrap`/`expect`/`panic!`/`unsafe`；TDD；改后必编译+测试。
- 设计变更须按 propsol 决策记录格式追加 rev 条目（当前最新 v0.2.8）。
- 测试基建/工件边界见 test-plan §3.2/§3.3；改动边界须过 `check-deps.sh`/`check-entropy.sh`。
- 每阶段一个 @oracle 门禁（除非用户另有指示）。
