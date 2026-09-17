既然确认了，下面基于修订后的语义给出一份可直接落地的设计文档初稿（RFC v0.1），你可以拿去当项目的 `DESIGN.md` 继续迭代。

---

# Arachne 设计文档（v0.1）

## 1. 目标与非目标

**目标**

- 嵌入式 Rust 分布式 KV 库，**CP 语义**：任何时刻最多一个有效 leader，提交需多数派。
- 共识内核复用成熟实现（首选 `tikv/raft-rs`，备选 `openraft`），自研部分限定为：传输层、存储层（WAL/快照）、状态机、客户端 API。
- 面向注册中心类应用的演进路径（TTL/Watch 列入 v2）。

**非目标（v1 明确不做）**

- ❌ 失去多数派后自动单节点接管（改为人工 `force-recovery` 命令，见 §6）
- ❌ 基于 ACK 超时自动剔除节点（成员变更只走 Quorum 投票的 ConfChange）
- ❌ 分片 / 多 Raft Group
- ❌ 自研共识算法

## 2. 一致性语义（对外承诺）

| 操作 | 语义 | 实现 |
|---|---|---|
| `put` / `delete` | 线性一致写 | Raft 日志，多数派提交后 apply |
| `get` | 线性一致读（默认） | ReadIndex |
| `get_stale` | 允许旧读 | 本地状态机直接读 |
| 失去多数派 | 写入返回 `QuorumUnavailable`，不降级、不脑裂 | CheckQuorum + leader 自动 step down |

**幂等性**：客户端会话表 `(client_id, seq_no) -> {result, applied_index}`，状态机 apply 时去重；重试返回缓存结果；会话带 TTL，定期 GC。nonce 概念整体删除，全局序由 `(term, log_index)` 承担。

## 3. API

```rust
let node = Arachne::builder()
    .node_id("node1")                       // 持久化，重启不变
    .listen("0.0.0.0:7000")                 // 自身地址必须显式给出
    .seeds(["node1:7000", "node2:7000", "node3:7000"])
    .data_dir("/var/lib/arachne")           // 启动时加文件锁，防同目录双进程
    .profile(Profile::Lan)                  // Lan | Wan，字段可逐项覆盖
    .build().await?;

let kv = node.handle();                     // cheap Clone，多任务共享
kv.put(b"k", b"v").await?;                  // follower 自动转发 leader
kv.get(b"k").await?;                        // ReadIndex 读
kv.get_stale(b"k").await?;                  // 旧读
kv.delete(b"k").await?;
node.shutdown().await?;                     // 优雅关闭：停止接收、转让 leader（可选）
```

**错误模型**（核心项）：

```rust
enum ArachneError {
    NotLeader { leader_hint: Option<NodeId> }, // 客户端据此重定向/重试
    QuorumUnavailable,
    SessionExpired,                            // 会话过期，需重建后重放
    Timeout,
    Unrecoverable(String),                     // 存储损坏等，进程应退出
}
```

## 4. 架构分层

```
┌─────────────────────────────────────┐
│  API 层（handle, 重定向, 重试策略）    │
├─────────────────────────────────────┤
│  共信层  raft-rs RawNode 封装         │  ← 不自研算法
├──────────────┬──────────────────────┤
│  传输层       │  存储层               │
│  tonic/mTLS  │  WAL + Snapshot      │
│  + 消息版本号  │  + 状态机(会话表+KV)  │
└──────────────┴──────────────────────┘
```

- **任务模型**：tokio async 任务 + `CancellationToken`，不是裸 `std::thread`；所有后台任务可优雅关停。
- **主循环**：单 tick loop 驱动 raft-rs（`tick()` → `ready()` → 持久化 → 发送 → `advance()`），这是 raft-rs 的标准集成姿势。
- **落盘顺序**：先 WAL fsync（含 HardState：term/voted_for），再发消息——这是崩溃安全的根基，不可省略。

## 5. 核心机制

### 5.1 选举
- 随机化选举超时（`[1x, 2x) 均匀分布`），心跳 = 选举超时的 1/10。
- **PreVote**：重新上线/分区内恢复的节点先预投票，不抬 term，防止抖动 leader。
- **CheckQuorum**：leader 若超过选举超时未收到多数派心跳，主动 step down，防止分区内旧 leader 继续服务读。

### 5.2 复制
- **多数派 ACK 即提交**，不等全量；落后者异步追赶。
- 流控：每 follower 限制 inflight 字节数（类似 etcd `MaxInflightMsgs`），慢节点堆积时 leader 暂停对其发送、改发 snapshot。
- 追赶不上（落后超过 retention）：转 **Learner** 或由运维决定移除——不是自动的。

### 5.3 成员变更
- 单步 ConfChange（每次只增/删一个节点，规避 joint consensus 的复杂度；raft-rs 已支持）。
- 新节点一律先以 **Learner** 加入，日志追平（lag < 阈值）后才允许转 Voter。
- rejoin 依赖 PreVote + 稳定 node_id，不重新走"加入"流程。

### 5.4 读路径
- `get`：ReadIndex——leader 记录当前 commit index，向多数派发一轮心跳确认自己未失权，等待 applied ≥ 该 index 后读状态机。
- 后续可选 Lease Read（依赖时钟偏移上限配置）优化 LAN 延迟，v1 不做。

### 5.5 存储与恢复
- WAL：条目带 CRC；启动时从尾部校验，损坏则截断到最近合法条目（Raft 日志属性保证安全）。
- fsync 策略：HardState/日志条目默认每批 fsync（group commit 合并），提供 `FsyncPolicy::{Always, BatchMs(u64)}` 供吞吐换持久性。
- Snapshot：applied 后定期（按日志字节数阈值）生成；安装 snapshot 时状态机原子替换（写临时文件 + rename）。
- 日志压缩：snapshot 之后的日志可截断，但保留最近 N 条以避免频繁 snapshot 传输。

### 5.6 传输与安全
- tonic + mTLS；协议消息带 `protocol_version`，滚动升级期间拒绝不兼容版本。
- 节点间身份 = 证书 CN / node_id 映射，防未授权节点加入。

## 6. 运维与故障恢复

- **强制恢复**：`arachne force-recovery --data-dir ...`（类似 etcd `--force-new-cluster`），将本节点重置为单节点集群，**文档必须显著声明可能丢数据**，仅用于多数派永久丢失的灾难场景。
- 数据目录文件锁、启动时校验 cluster_id 与配置一致性。

## 7. 配置预设

| 参数 | Lan | Wan | 说明 |
|---|---|---|---|
| heartbeat_interval | 100ms | 500ms | |
| election_timeout | 1s | 2.5s | 实际随机化 [1x, 2x) |
| rpc_timeout | 500ms | 2s | |
| max_inflight_bytes | 4MB | 1MB | |
| snapshot_threshold | 64MB 日志 | 16MB 日志 | |
| read_mode | ReadIndex | ReadIndex | |

所有字段可在 `Profile` 基础上逐项覆盖。

## 8. 可观测性

Prometheus metrics：`term`、`leader_info`、`commit_index`、`applied_index`、`per-follower lag`、`wal_fsync_p99`、`session_count`、`proposal_dropped`。tracing span 覆盖一次写请求的全链路（client → forward → propose → commit → apply）。

## 9. 测试策略（"坚固"的主要来源）

1. **确定性模拟**（madsim 或自建 deterministic runtime）：随机注入分区、丢包、延迟、进程崩溃、磁盘慢/损坏，种子可复现。
2. **线性一致性验证**：Porcupine 风格历史检查，作为 CI 必过项。
3. **混沌场景清单**（每个写成回归用例）：选举中 old leader 复活、双分区、follower 追赶中 leader 切换、WAL 尾部损坏、客户端重试风暴、Learner 追平瞬间断电。
4. `loom` 测内部并发；`cargo-fuzz` 测网络消息解析。

## 10. 里程碑

| 阶段 | 交付 | 验收 |
|---|---|---|
| M0 | 单进程 raft-rs 集成 + 内存状态机 + WAL | 崩溃重启不丢已提交数据 |
| M1 | 3 节点网络、选主、复制、切主 | 杀 leader 自动恢复；NotLeader 重定向可用 |
| M2 | Snapshot/压缩/追赶 + ReadIndex 读 | 杀掉多数派后正确拒绝写；恢复后自动追赶 |
| M3 | ConfChange + Learner + 会话幂等 | 节点替换不中断服务；重试不重复生效 |
| M4 | mTLS、metrics、force-recovery | 混沌清单全绿 |

## 11. 待决策项（需要你拍板）

1. **状态机存储**：全内存 + WAL（简单、快，数据量受内存限制） vs 嵌入式引擎（`redb` 稳定但要处理其与 WAL 的事务边界）。注册中心场景通常数据小，**建议 v1 全内存 + WAL + snapshot**。
2. **传输**：tonic（快、生态好，依赖重） vs 手写帧协议（轻，但 TLS/版本管理都要自己做）。**建议 tonic**。
3. Watch/TTL 是否提前到 v1.5？如果注册中心是首个目标用户，建议至少 TTL（lease）提前。

---

两个建议：一是 M0 开始就引入确定性模拟测试，越晚加越难改；二是把 §2 的语义表放进 crate 的文档首页——对使用者而言，“什么情况会返回什么错”比实现细节重要得多。

需要的话，下一步我可以展开某一层的详细设计（比如 WAL 文件格式与恢复算法，或 raft-rs 主循环的完整代码骨架）。
