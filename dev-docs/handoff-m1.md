# Arachne 交接：M0 完成，M1 进行中

> 面向**新会话**。先读本文，再读 `dev-docs/propsol-v0.2.8`（设计权威）与 `dev-docs/test-plan-v0.1.3`（测试方案）。会话态进度见 `.slim/deepwork/arachne-m1.md`（git-local，但 OpenCode 可读）。
>
> 仓库：`/Users/alex/Projects/workspace/Arachne`，git 已 init，工作树干净，**234 tests 0 failed**、0 告警、`scripts/check-deps.sh` 与 `scripts/check-entropy.sh` 全绿。

## 1. 现状总览

- **M0 已完成**（含终检门禁，COMPLETE）。六工件工作区 + 崩溃安全 WAL + raft 集成 + KV/会话状态机 + 全套测试基建 + `arachne-node` 单节点可运行 + examples + L4 runner 脚手架 + stateright/turmoil 骨架 + spike 关闭。
- **M1 进行中**。已完成 M1-1、M1-2（含整改）、M1-3a、**(A) arachne-node 多进程 tonic 接线（commit 5081417）** 与 **(B) M1-3b ReadIndex 线性一致读（commit 6616827）**；**未完成 M1-4 / M1-5**（(A) 已覆盖 M1-5 的 L3 前置与冒烟主体，(D) 的杀 leader / CLI 集成测试仍待做）。

### 提交线（新 → 旧）
```
6616827 M1-3b 真实 ReadIndex 线性一致读（Safe 读 + wait applied + 1 重试）      ← HEAD
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

### (C) M1-4：L2 turmoil + 不变量 + 线性化（M1 验收 ④）
- `l2/` 铺满 SimNetwork 原语（partition/hold/release/crash/bounce）。
- `ClientOracle` v1（逐 seq、幻值、one-log-id-per-seq、read-your-writes、单调读、持久性扫描）+ 自建 Wing–Gong 检查器（缩减历史）；stateright 作交叉验证（`model-check/`）。
- INV3/4/7/8/9 + 场景 S01/S02/S16。
- **D-S1**：raft 0.7 选举 RNG 不可注入（`raft-0.7.0/src/raft.rs` 的 `reset_randomized_election_timeout` 用 `thread_rng`）→ 视需要打 workspace `[patch.crates-io]` 一行补丁，以上游化为目标。

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
