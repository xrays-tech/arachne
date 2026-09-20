# Arachne 交接：M0 完成，M1 进行中

> 面向**新会话**。先读本文，再读 `dev-docs/propsol-v0.2.8`（设计权威）与 `dev-docs/test-plan-v0.1.3`（测试方案）。会话态进度见 `.slim/deepwork/arachne-m1.md`（git-local，但 OpenCode 可读）。
>
> 仓库：`/Users/alex/Projects/workspace/Arachne`，git 已 init，工作树干净，**294 tests 0 failed**、0 告警、`scripts/check-deps.sh` 与 `scripts/check-entropy.sh` 全绿。

## 1. 现状总览

- **M0 已完成**（含终检门禁，COMPLETE）。六工件工作区 + 崩溃安全 WAL + raft 集成 + KV/会话状态机 + 全套测试基建 + `arachne-node` 单节点可运行 + examples + L4 runner 脚手架 + stateright/turmoil 骨架 + spike 关闭。
- **M1 进行中**。已完成 M1-1、M1-2（含整改）、M1-3a、**(A) arachne-node 多进程 tonic 接线（commit 5081417）**、**(B) M1-3b ReadIndex 线性一致读（commit 6616827）**、**(C) stage 1 ClientOracle + 线性化检查器（a915f6b；自证 395fc9a；门禁 GO）**、**(C) stage 2 D-S1 raft 可播种选举 RNG + 双跑金丝雀（commit f132e44；门禁 GO）** 与 **(C) stage 3a/3c**（3a transport I/O 接缝 `edd913d`；3c inc.1–10 见 §3(C)，L2 场景 18 项全绿）；**`(D)/M1-5` 已收尾（commit `adb2bd7`，含 `6c79886` 的持久化 commit 修复，见 §1.5）**；**(C) stage 3b（real tonic over turmoil）已解决**（harness 显式链路延迟，见 §1.6）。
- **✅ 已恢复**：oracle provider 故障（模型 id 无法解析 + 空结果）已解决；(C) stage 1 确认门禁已补跑并 **GO**，stage 2 门禁亦 **GO**。

### 1.5 本轮收尾（**已提交**：`6c79886` 修复 + `adb2bd7` 收尾）

面向 `(D)/M1-5` 与 L4 义务的一轮收尾：

1. **跨进程 `409 + leader hint`（M1 验收③）**
   - `Handle::without_redirect()`（`arachne/src/client/handle.rs`）：`max_redirects` 改为 **handle 级**字段（移出共享 `Arc<HandleInner>`），单发克隆把 `NotLeader{hint}` 原样返回。
   - `arachne-node` HTTP 改用 `node.handle().without_redirect()`；`NodeHttp::map_error` 新增 `Timeout`/`Busy` → **503**（原先 `Timeout` 落 500，与验收②"不挂死 + 服务不可用类状态"不一致）。
   - 测试：`arachne/tests/client_redirect.rs`（**新**：redirect 路径 + 单发 hint 断言）、`arachne-node/tests/multi_node.rs`（非 leader 必须回 `409` 且 hint 指向**真实 leader**）。
   - **顺带发现（重要）**：`arachne/src/runtime/tests.rs` 从未被声明（`runtime/mod.rs` 无 `mod tests;`，且其中的 `Node::new(...)` 类型早已不存在）——**从未编译、从未运行**的死代码（自 `2b8b046` 起）。已**删除**；其覆盖由 `client_runtime.rs`/`read_index.rs`/新的 `client_redirect.rs` 承担。
2. **杀 leader（M1 验收①②）**
   - `multi_node.rs` 新增 `killing_the_leader_elects_a_new_one_and_writes_never_hang`：SIGKILL leader 进程 → 幸存者接管；窗口内写只能是 `409`/`503`，**绝不挂死**（HTTP 读超时返回 `None` 即判失败）；新 leader 仍服务杀前已提交值。
   - `l2_scenarios.rs` 新增 `new_leader_within_two_election_windows_after_leader_loss`：**确定性 tick 计数**断言换主 ≤ `2×election_tick`（固定种子实测 **16 轮 / 预算 20**）。Gate C 禁止 `tests/` 读真实时钟，故 L3 只能做有界轮询；严格的时序上界由 L2 的 tick 版承担。
3. **bin CLI 集成测试（test-plan §3.3）**：新增 `arachne-node/tests/cli.rs`（**11 项**）+ 共享 `arachne-node/tests/common/mod.rs`：`--help`/参数错误退出码、配置不可读/校验失败 fail-start、**flock 冲突**、**META cluster_id 不一致**、**SIGTERM 优雅关闭并释放锁**、`/readyz` + `/metrics` + 404/405，以及 4 项 `force-recovery`（见下）。
4. **D-ART-testobs**：`arachne-node` 新增 `test-observability` feature（**默认关**；启用时仅追加 stdout marker `{"event":"ready"|"shutdown",…}`，零行为改变，已实测）。新增 `scripts/check-release-features.sh`（Gate A：无默认 feature；Gate B：release 产物**不含** marker，而 feature 构建含之且二者 hash 不同），已接入 CI。
5. **L4 已 ack 写入**：`l4/kill_loop.sh` 每轮发起真实 `PUT /kv/...`（200 = 已提交 + 已 apply），`kill -9` 后重启用线性一致 `GET` 读回复验；循环结束再额外重启一次复验**最后一次**写入。**顺带修复**：`l4/node.toml` / `kill_loop.sh` / `verify_wal.sh` 生成的配置缺 `rpc_timeout_ms`，在 M1-1 引入 profile 校验后**必然 fail-start**（实测复现）——三者已补齐；`l4/README.md` 的"M0 空命题"声明已更新。
6. **`force-recovery` 子命令（propsol §6.1）——现已实现**
   - **存储层**：`WalStorage::force_recovery(dir, node_id, new_cluster_id, config, ts)` + `ForceRecoveryReport`（`arachne/src/storage/wal.rs`，已从 `storage::mod` 导出）。语义：读 META → 以**旧** cluster_id 打开（取得数据目录锁，拒绝并发进程）→ **丢弃未提交尾部**（index > commit；跨 segment 正确截断，丢弃 0 条时为 no-op）→ 写新 HardState（`term+1`、`vote=self`、`commit` 不变）→ 重写 META（默认换 cluster_id）。单测 5 项（丢弃尾部 + 轮换 cluster_id / 保留 cluster_id / 零 commit 丢弃全部 / 锁占用拒绝 / node_id 不一致拒绝）。
   - **M1 membership 边界（如实声明）**：membership 到 **M3** 才持久化 `ConfState`（`raft_storage.rs` 自述静态），故本方法**不**持久化 `ConfState=[self]`；由 CLI 生成/打印**单票 config**（`initial_cluster=[self]`）使该后置条件成立。M3 落地后可去掉这层 config 依赖。
   - **CLI**：`arachne-node force-recovery --config <path> --i-know-data-loss [--keep-cluster-id] [--out-config <path>]`（`arachne-node/src/force_recovery.rs`）。前置条件：必须显式 `--i-know-data-loss`（否则 exit 2）；旧成员可达性预检（3s TCP，可达则**告警**）；数据目录锁（占用 → exit 1）。`--out-config` 写出可直接启动的单票 config。退出码 0/1/2。单测 5 项 + CLI 集成 4 项（含端到端：acked 写入跨 force-recovery 存活 + cluster_id 轮换 + 单票 config 复起）。
7. **发现并修复：持久化的 `commit` 从未被写入（严重）**
   - **现象**：`force-recovery` 报 `commit=0`，把已 ack 的写入全部当作"未提交尾部"丢弃；端到端表现为恢复后 `GET` 返回 not found。
   - **根因**：raft-rs 把 commit 前进放在 **`LightReady::commit_index`** 上，**不是** `Ready::hs`（`hs` 只在 term/vote 变化时携带），且 `LightReady` 路径会同时推进 raft 内部的 `prev_hs.commit`。`RaftNode::step()` 只持久化了 `ready.hs()`，于是 **commit 一旦前进就再也不会被写盘**，`initial_state()` 报出的 commit 永久滞后。
   - **为何此前没暴露**：单节点/多数派重启后会把日志**重新 commit**，指标与数据看起来正常；L4 的"commit/applied 不回退"也因重新 commit 而通过。只有"按 commit 截断"的 force-recovery 会暴露它。**这也说明 L4 原来的 commit 不回退断言对 commit 持久性其实不敏感。**
   - **修复**：`step()` 在 `advance` 之后，若 `light.commit_index()` 为 `Some(c)`，以当前 in-memory term/vote + `c` 调 `set_hard_state`（I1：逐次 fsync），与 raft-rs 自带示例 `examples/single_mem_node` 的用法一致。**代价**：每次 commit 前进多一次 HardState fsync（正确性优先）。
   - **回归测试**：`arachne/tests/force_recovery.rs::runtime_persists_the_committed_index_for_force_recovery`（真实 `Runtime` actor：put 后停机，断言 `force_recovery` 报出 `commit >= 1` 且 `discarded == 0`）。
   - **建议**：值得在 propsol 补一条 rev 说明"durable HardState 必须包含最新 commit（I1 的一部分）"，但**未擅自改动** v0.2.8 设计文档。

**验证（`6c79886` + `adb2bd7`）**：`cargo test --workspace` = **321 passed / 0 failed**（294 → 321）；`check-deps.sh`、`check-entropy.sh`、`check-release-features.sh` 全绿；`cargo build --workspace --all-targets`、`--examples` 无告警；`l4/kill_loop.sh`（3 轮 acked 写入）本地 PASS；`force-recovery` 端到端手工复验通过（恢复后 acked 值仍在）。

### 提交线（新 → 旧）
```
adb2bd7 M1-5 (D): 跨进程 409+hint + 杀 leader 断言 + force-recovery CLI + bin CLI 测试 + test-observability  ← HEAD
6c79886 fix(consensus): 持久化 LightReady 的 commit 前进（durable HardState 的 commit）
635da14 docs: M1 handoff — (C) stage 3c inc.10 gate GO + replay/resync sharpening
cbf1a85 M1-4 (C) stage 3c inc.10: gate P3 sharpen（隔离 WAL replay 与 resync）
e4bedbe docs: M1 handoff — (C) stage 3c inc.10（真实 crash + WAL 重启）
6226ec2 M1-4 (C) stage 3c inc.10: 真实 crash + WAL 重启
bdfea02 docs: (C) stage 3c inc.9（换主后同 seq 重试）
2159fd9 M1-4 (C) stage 3c inc.9: 换主后同 seq 重试（at-most-once）
fba2aca docs: (C) stage 3c inc.8 门禁 GO
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
- **卡点（已解，见 §1.6）**：客户端 `put` 永不提交。**旧记录的两处结论均已被推翻**：(a) 不是"actor 收不到 `Command::Propose`"——命令会被收到并被答复；(b) 不是 actor 逻辑死锁。根因是 harness 未设显式链路延迟、用了 turmoil 的大抖动默认延迟，超过 election_timeout 导致 CheckQuorum 反复降级。
- **附带修复**：`Runtime` 的 propose/ReadIndex 截止时间原用 `std::time::Instant`（真实墙钟）→ 在模拟器下是确定性泄漏，已改 `tokio::time::Instant`（commit 377f3ce，root 仍 277 绿）。
- **现状**：3b **已解决**——`l2/tests/in_sim.rs` 两个测试已去 `#[ignore]` 并通过，新增 `l2/tests/transport_echo.rs` 传输门禁，CI 增 `l2` job；L2 不变量/场景另有 **20 项**跑在内存传输（`arachne/tests/l2_scenarios.rs`），两条轨道互补。

### 1.6 stage 3b 复诊 → **已解决**（`l2` · real tonic over turmoil）

**结论：不是产品缺陷，是 L2 harness 的配置问题。** harness 之前没有设置显式链路延迟，于是使用了 **turmoil 的默认链路延迟**——它大且抖动（单次 RPC 往返几十毫秒模拟时间、尾部过百毫秒），超过节点的 `election_timeout`，使 leader 被 CheckQuorum 反复降级 → 选举抖动（term 1→2→3…）→ 客户端 `put` 先 `Timeout` 后 `QuorumUnavailable`。**harness 现显式固定 1ms 链路延迟**，`l2` 全部测试通过且**不再 `#[ignore]`**（3 节点真实 tonic 集群、双跑确定性、传输 echo），并新增 CI `l2` job。

**定位过程（探针均已回退）**
1. **事件序列探针**：actor 停在 `drive_cycle` 内，而这里唯一的 `await` 是 `deliver → transport.send`；同一时刻其他 actor 继续 tick。
2. **传输探针**：`n2->n3 connected` 之后没有 `replied`（该方向此前成功 7 次）；对端 actor 仍 tick，accept 未报错。
3. **命令探针**：`begin cmd` 紧跟 `Propose rejected as non-leader` —— 命令**被收到并被答复**（推翻旧记录的"actor 收不到 `Command::Propose`"）。
4. **最小可复现对照** `l2/tests/transport_echo.rs`（纯传输、无 raft/WAL，带 `ECHO_*` 旋钮与延迟分布）：
   - 默认链路延迟：`p50≈40–50ms`、`p90≈73–97ms`、`p99≈100ms`，100ms 界内偶发超时；
   - **显式 1ms：`p50=p90=p99=2ms`（一个 RTT）、0 超时**；显式 10ms：`p50=p99=20ms`、0 超时；
   - 关 keep-alive、预热、改发送间隔、单向/双向**都不改变结论** → 排除 keep-alive / 连接建立 / 排队 / 方向，确定为**默认网络模型的延迟量级**。
5. 把显式 1ms 加进 `l2/src/harness.rs` 后：3 节点集群 `put_ok=true`，双跑确定性通过。

**留下的门禁**：`l2/tests/in_sim.rs`（2 项，已去 `#[ignore]`）+ `l2/tests/transport_echo.rs`（1 项：默认显式 1ms，`ECHO_LINK_LATENCY_US=0` 可复现默认延迟导致的停机）；CI 新增 `l2` job 跑 `cargo test`。**教训**：模拟器下必须显式设定网络模型参数——其默认值可能远超测试 profile 的超时。

**stage 3c（增量 1 已完成，commit 6815bef + 46a8328；门禁 GO）：内存传输上的确定性 L2 场景**。新增 `arachne/tests/l2_scenarios.rs`：进程内 3 节点 harness（沿用 `m0_determinism` 的同步 `RaftNode` 模式：手工 `tick`+`block_on(step)`+`on_message`，harness 自控消息投递），用 harness 级 `Faults{isolated}` 丢弃隔离节点往返消息来注入分区/崩溃；选举 RNG 用 stage-2 的 `raft::set_election_rng_seed` 保证 leader 身份可复现。
- 场景/不变量：**S02**（2+1 分区，并断言隔离节点确实未收到多数侧写、多数两侧都提交）+ **INV7**（全轨迹每 term ≤1 leader）；**S01**（隔离=崩溃旧 leader → 幸存者选新主 → 复活收敛）+ **INV9**（新主含已提交条目）+ **INV8**（重叠 index 日志一致）+ **INV3**（收敛后状态快照逐字节一致）；**INV4**（M1 验收④：分区下客户端 put/get 历史经 stage-1 ClientOracle **与** 自建检查器判定为线性一致）；**双跑确定性**（同种子 → leader 轨迹与日志逐字节一致）。
- 门禁 NO-GO 风险已修：分区此前只"配置"未被断言，现已断言（P2）；四测 4/4、workspace 281 绿。
- **增量 2（已完成，commit d895d1c）**：`Faults` 增单向丢包；**S16**（丢所有 follower→leader 消息 → CheckQuorum 将 leader 降级；INV7 仍成立）+ **换主 INV4**（历史中隔离 leader、幸存者选新主、客户端改投新主，整段 put/get 历史经 oracle+checker 判定线性一致；断言恰有一次换主）。`l2_scenarios` 6/6。
- **增量 3（已完成，commit 2b3e3b0；门禁 GO，ora-9；P4 空白已修 777fd6f）**：**并发 INV4**（两客户端 put/get 实时区间重叠 → 真正驱动检查器的并发搜索路径；oracle+checker 双通过）+ **ReadIndex 局部性**（leader 的 `read_index` 经全量投递产生读状态；follower 在不投递时不自服务读，只转发）。harness 记录全量读状态 `(node,ctx,index)`；`l2_scenarios` 8/8。
- **增量 4（已完成，commit dfc92be；门禁 GO，ora-9；P4 修正 5c4d6e4）：多键 INV4 + 分区下并发**：多键（"a"/"b"）写读交替 + 读一个从未写过的键断言为 `None`——使 oracle 首次跑在真正**多键**历史上（跨键混淆会被 harness 直断言 + checker 逐键模型 + oracle 正向幻值检查共同捕获；**注意**：absent-key 的 `None` 读走 oracle 的跳过路径，其"负路径"由 testsupport 单测 `phantom_read_is_a_violation_with_witness` 覆盖）；两客户端实时重叠区间在隔离一个 follower 的 2+1 分区下由多数侧服务、历史仍线性一致（断言被隔离者确实饥饿）。`l2_scenarios` 10/10。
- **增量 5（已完成，commit 2f35da1；门禁 GO，ora-9；P4/保留加强 97f785a）：INV4 跨越换主**：某次 put/get 在换主前被调用、换主后在**新 leader** 上完成（其实时区间横跨 leadership change）；oracle+checker 仍判定线性一致，并断言轨迹出现非初始 leader；加强：对该换主轨迹断言 INV7，且**新 leader 必须保留换主前已提交的条目**（独立键 `"old"`）。`l2_scenarios` 11/11。
- **增量 6（已完成，commit 32b8b7e；门禁 GO，ora-9；P4 打磨 f010304）：oracle 幻值负路径（注入）**：两条注入式违规测试断言 oracle 对幻值读**判定失败**——读一个从未写过的值、以及跨键读（值只写给另一个键）；打磨后两条测试**同时**断言自建检查器亦判 `Violation`。在 L2 层闭合幻值检查的负路径（此前仅 testsupport 单测覆盖）。`l2_scenarios` 13/13。
- **增量 7（已完成，commit 272967a；门禁 GO，ora-9；P3/P4 打磨 f4f5b72）：ReadIndex 路径的读**：客户端读改走 raft ReadIndex（leader `read_index` → 等 quorum 确认的读状态 → 等 `applied ≥ read_index` → 再读 SM），而非直接读 leader 状态机；所得 put/get 历史经 oracle+checker 判定线性一致，并以"每次读都产生读状态"作非空断言；打磨后加"applied 屏障确已到达"断言。闭合"读绕过 ReadIndex"缺口——**M1 ④ 的核心主张（客户端历史在真实共识读机制上线性一致）至此已演示**。`l2_scenarios` 14/14。
- **增量 8（已完成，commit e1b7f31）：故障下的 ReadIndex 读**：一个读在 leader0 上发起后 leader0 被隔离——断言其**不能完成**（无该 ctx 的读状态，绝不返回陈旧值），客户端记录为失败（`OpResult::Err(Timeout)`，非线性化点）；换主后由新 leader 服务一次新的 ReadIndex 读。整段历史（含失败读）仍线性一致。`l2_scenarios` 15/15。
- **增量 9（已完成，commit 2159fd9）：换主后同 seq 重试（at-most-once）**：客户端对 `(client 0, seq 1)` 的 `Put` 先在 leader0 上发起、随即隔离（已 propose 但**未提交**，断言旧 leader 已提交日志无该命令）记为失败；换主后**用同一 `(client, seq)` 在新 leader 重试**成功，再以 ReadIndex 读观察该值。断言 oracle+checker 通过且 `merge_retries().ops.len()==2`（两次尝试合并为一个逻辑 op）。另给 inc.8 的换主轨迹补 INV7 断言。`l2_scenarios` 16/16。
- **增量 10（已完成，commit 6226ec2；门禁 GO，ora-9；P3 打磨 cbf1a85）：真实 crash + WAL 重启**：harness 现支持 `crash(i)`（丢弃节点=任务死亡，保留 WAL 目录）与 `bounce(i)`（从同一目录重开 `WalStorage`、重建 `RaftNode(applied=0)`、**重置内存状态机并清空已记录日志**＝volatile 丢失）。场景：崩溃一个 follower → 多数侧提交 v2 → 重启该 follower → 断言其状态机恢复出 v2，且全节点日志一致（INV8）、状态快照一致（INV3）、INV7 成立；打磨：重启后**仅驱动该节点（不投递）**断言其已由 WAL 重放出 v1（区分 replay 与后续 resync）。**M1 ④ 的最后一项实质内容已闭合。** `l2_scenarios` 17/17。
- **M1 ④（L2）覆盖小结**：S01/S02/S16；INV3/4/7/8/9；双跑确定性；INV4 共 9 种形态（顺序、分区下、换主、并发、多键、分区下并发、ReadIndex 读、故障下 ReadIndex 读+恢复、换主后同 seq 重试 at-most-once）；oracle 负路径（幻值/跨键）；真实 crash+WAL 重启。
- **剩余（非阻塞）**：更大规模/随机种子历史与更多故障组合。~~follower 服务读的完整来回~~ 已由 `follower_read_index_round_trip_completes` 覆盖（运行时契约差异已记入 `client/mod.rs`）；~~oracle ② 空转~~ 已由 `oracle_check_two_uses_real_log_ids` 激活；~~3b（real-tonic-on-turmoil）~~ **已解决**（§1.6）。

### 1.7 首次 CI 暴露并修复的两个问题（`d788878` + 本次修复）

仓库首次 push 到 GitHub 后 CI 变红（`l2` job 通过），暴露出两个问题：

1. **测试竞态（`d788878`）**：`three_node_client.rs` 只等"存在一个 leader"，随后对 follower 的 handle 只 `put` 一次。重定向需要 leader hint，而新选出的 leader 未必已到达每个 follower；缺 hint 返回 `QuorumUnavailable`（handle 不重试）。本地 macOS 侥幸通过，2 核 CI runner 上失败。改为：先等该 follower 报告 leader hint，再对窗口期错误做**有界重试**（全部失败仍判失败，重定向契约仍被断言）。
2. **`WalStorage::append` 缺覆盖语义（严重，本次修复）**：raft 在选举后会把**冲突后缀**重新交给存储（follower 必须覆盖上任短命 leader 的条目）。`append` 原本假设纯尾部追加、只用 `debug_assert` 守连续性 → debug 下 panic（`append continuity violation: expected 2, got 1`，actor 任务死亡）→ 测试在后段 `get_stale` 收到 `ShuttingDown`；**release 下 `debug_assert` 被编译掉，会写入重复 index，静默损坏日志**。现在 `append` 在追加前先截断到首个入参 index（复用 force-recovery 的物理截断，改名 `truncate_log_to`）；覆盖**已提交**条目属 raft 安全违规，故 fail-stop。新增 3 个单测（冲突后缀重写 + 重开后的持久性、从 index 1 覆盖、拒绝覆盖已提交）。

### (D) M1-5：L3 冒烟 + bin CLI 集成测试（M1 验收 ①②③）— **已收尾（commit `adb2bd7`，见 §1.5）**
- ✅ 3 进程成形/写读/复制冒烟 + **杀 leader 换主 + 窗口内写不挂死**（`arachne-node/tests/multi_node.rs`，2 项）。
- ✅ **跨进程 `409`+hint**：`Handle::without_redirect()` + node HTTP 单发 handle。跨进程**自动跟随** hint 仍需 HTTP-port 映射（config 暂无），M1 只做「409+hint body」，与 propsol §3.3 一致。
- ✅ 失联旧 leader 的 propose 超时 `ArachneError::Timeout` 现映射 **503**（原 500）。
- ✅ bin CLI 集成测试（§3.3）：`arachne-node/tests/cli.rs`（11 项）+ `tests/common/mod.rs`。
- ✅ **`force-recovery` 子命令已实现**（存储层原语 + CLI + 端到端测试，见 §1.5.6）。**唯一 M1 边界**：membership 到 M3 才持久化 `ConfState`，故单票 membership 由命令生成的 `initial_cluster=[self]` config 提供（M3 后去掉该依赖）。

### M1 验收对照（propsol §10）
① 杀 leader 后 ≤2×election_timeout 出新主 —— **已证**：L2 确定性 tick 计数（≤2×election_tick，实测 16/20）+ L3 进程 SIGKILL 换主。
② 期间写返回 `NotLeader`/`QuorumUnavailable` 不挂死 —— **已证**：L3 窗口内写只接受 409/503，挂死即判失败；客户端面 `8cbc765`；`Timeout` 现映射 503。
③ hint 失效经 seeds 轮询恢复 —— **契约已打通**：跨进程 `409`+hint 可达（`without_redirect`）；`Handle` 的 seeds 兜底已实现（`client_runtime`/`read_index` 覆盖）。
④ 含切主窗口的 put/get 线性一致 —— **已在 L2（内存传输）证明**：stage 3c inc.4–10（分区/换主/并发/多键/ReadIndex 读/故障下 ReadIndex 读/换主后同 seq 重试 at-most-once 共 9 种形态）；**real-tonic-on-turmoil（stage 3b）亦已打通**（3 节点真实 tonic 集群选主/提交/复制 + 双跑确定性 + 传输延迟门禁，见 §1.6）。
⑤ crate 文档首页语义表/错误矩阵 —— **已完成**（`151066a`，并在 (B) 校正 `get` 行为）。

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
bash scripts/check-release-features.sh          # D-ART-testobs（release 产物排除 test-observability）
# 3 节点真实网络（库级）：
cargo test -p arachne-transport-tonic --test three_node -- --nocapture
cargo test -p arachne-transport-tonic --test three_node_client -- --nocapture
# 3 进程真实集群（bin 级，L3 冒烟 + 杀 leader）：
cargo test -p arachne-node --test multi_node -- --nocapture
cargo test -p arachne-node --test cli -- --nocapture
cargo test -p arachne-transport-tonic --test multi_node_bind -- --nocapture
# ReadIndex 读路径（进程内 3 节点）：
cargo test -p arachne --test read_index -- --nocapture
# 客户端重定向契约（普通 handle + 单发 handle）：
cargo test -p arachne --test client_redirect -- --nocapture
# 持久化 commit 回归（真实 Runtime actor + force-recovery）：
cargo test -p arachne --test force_recovery -- --nocapture
# L4 真机 kill -9（每轮一次 acked 写入 + 崩溃后复验；需真实磁盘）：
ARACHNE_L4_ITERATIONS=3 bash l4/kill_loop.sh
# L2 独立工程（turmoil；真实 tonic 集群 + 传输延迟门禁，CI 有独立 job）：
cd l2 && cargo test
# 复现「默认链路延迟导致停机」的对照（预期 FAIL）：
cd l2 && ECHO_LINK_LATENCY_US=0 cargo test --test transport_echo -- --nocapture
```
**本地注意**：若 `CARGO_TARGET_DIR` 在仓库外（本机默认 `~/.cargo/global-target`），沙箱可能拒绝写入；测试时用仓库内目录覆盖，如 `CARGO_TARGET_DIR=.dsh-target cargo test --workspace`。

## 6. 协作约定（沿用）
- 生产代码禁 `unwrap`/`expect`/`panic!`/`unsafe`；TDD；改后必编译+测试。
- 设计变更须按 propsol 决策记录格式追加 rev 条目（当前最新 v0.2.8）。
- 测试基建/工件边界见 test-plan §3.2/§3.3；改动边界须过 `check-deps.sh`/`check-entropy.sh`。
- 每阶段一个 @oracle 门禁（除非用户另有指示）。
