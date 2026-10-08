# Recipe：确定性制造「存活但隔离」的 stale 读节点（outcome 级 E2E）

> 用途（回应下游 hydra 的请求）：端到端证明——**被分区但存活的节点仍然服务其最后已应用的旧状态，且客户端不会据此回滚**。这是提交序号原语在真实拓扑下的 outcome 保证。
> 覆盖边界：本配方**不测「拒绝分支」**（`incoming index < 已应用 index`），那只能由「落后的副本应答 head 读」产生（见 §4）。

## 1. 拓扑

3 节点（n1/n2/n3）：

1. 写入 h1 并确保**全部**节点已 apply（用 `get_stale`/`get_stale_with_index` 有界轮询确认）。
2. 把 n3 **双向隔离**出多数派（n1/n2 仍能组成多数派、继续推进日志）。
3. 在多数派侧继续写 h2。
4. 断言 n3：仍**存活**（actor 继续跑、本地读正常应答），且只服务最后一次已应用的旧状态（h1 + 与 h1 匹配的溯源 index，≤ 多数派侧最新）。

## 2. 机制（in-memory / 真机两版）

- **in-memory（`arachne-kv-testsupport`，**crates.io 已发布 0.3.1，仅作 dev-dependency 使用**）**：`InMemoryTransportFactory::firewall(from, to)`（`arachne-testsupport/src/transport.rs:141-149`）。它在交换处**静默丢包、不关通道**——发送端/接收端通道都活着，被隔离节点的 actor 继续运行（只是收不到消息），正是「存活但分区」。完全隔离一对 = 双向 `firewall(a,b)` + `firewall(b,a)`。非空守卫：`firewall_drop_count() > 0`（有流量被防火墙丢弃才算真正隔离，防测试假绿）。
- **真机（tonic/TCP）**：等价物是挂起该节点的出入流量（iptables DROP / 连接挂起），**切勿关闭进程或监听**——关闭会暴露 closed-channel，远端会把该节点视作死亡，就不再是「存活分区」而变成「崩溃」语义。
- **可直接参考的既有测试拓扑**：`arachne/tests/inv14_deposed_leader.rs`（失权 leader 存活、继续服务旧值）、`arachne/tests/quorum_loss.rs`（多数派丢失后 `get_stale` 仍可用）、test-plan `S02`（双分区）/ `S16`（非对称分区）。

## 3. 断言（outcome 级）

写入 h1 → 隔离 n3 → 多数派侧写 h2，然后断言：

1. 在 n3 上 `get_stale_with_index(ctl/head)` 返回 `(h1, i1)` 且 `i1 ≤` 多数派侧同一键的最新 index（**n3 不前进**）。
2. 以「head index ≥ 已应用 index 才物化」运行的客户端**不**把 h1 覆盖掉它已应用的 h2（无回滚）。若 head 值未变，`NoChange` 快路径不进入 staleness 检查——这是**预期行为**，不是测试空洞（见 §4）。
3. 解除隔离后，n3 经正常复制追平到 h2，其在 n3 上的 index 上升至多数派侧。

## 4. 覆盖边界（为什么测不到「拒绝分支」）

被隔离节点读的是「**它自己已应用的 head**」：该节点读取时 `index == 该节点 applied`，值-序同源自洽。若客户端已应用相同/更新的 head，则要么 index 相等、要么因 head 值未变走 `NoChange` 快路径——**不会产生「更旧 hash 且更低 index」**的应答。

拒绝分支（`incoming index < 已应用 index`）只能由**落后的副本应答 head 读**产生：lagging 副本、快照追赶中、或客户端从比本节点更前进的源读到最新后、又读到落后节点的旧值。这正是该原语要防的缺陷形态（下游 ADR「lagging local replica answers the head read」）。

- 本仓保证：`put` 只会抬升一个 key 的溯源 index（严格单调）⇒ 真集群**无法自然产生**「更旧 hash、更低 index」的应答。
- 因此拒绝分支的 E2E 需要**可注入的 head-read seam**（脚本化 `(hash, index)` 序列）——hydra 已确认由他们自持（不在本仓范围）。

## 5. 验收口径

- §3 断言 1–3 全绿 = outcome 级验证完成；拒绝分支由下游 seam 覆盖（双方各自闭环）。
- 本仓侧回归（防回退）：`tests/stale_read_with_index.rs`（I2/I6/I7/I8）、`tests/quorum_loss.rs`、test-plan `INV16/17/18`、`S02/S16` 不回归。

## 6. 关联

- 设计：`dev-docs/arachne-kv-commit-index-design.md`（§3 index 语义、§6 I1–I8）
- 下游验收：hydra 回执（v0.3.1；`--lib` 303/303、three-node 5/5、store 6/6、5 idle runs、三 cluster drills PASS、config-loss 形态 0/10 vs 改动前 3 CI 次）
- 计划：`dev-docs/plan-commit-index-followups.md`（#9 put_with_index 已关闭）
