---
status: phase-1-complete
phase: 1
updated: 2026-10-07
---

# 基线：多客户端并发线性读（Arachne vs etcd v3.5.21, n=400）

> 本文件为 Phase 1（`dev-docs/plan-read-concurrency.md` §1）基线测量产物：在**同一台机器、同一 3-node 集群形态**下，把 Arachne 的并发线性读放到与 etcd 相同的口径（keep-alive、n=400、read-workers 梯度 1/2/4/8、多轮中位数），记录 **ReadIndex 轮次/轮次合并** 的可证伪机制指标，并给出 4w 与已记录基线（`propsol-v0.2.md:559-562,592-599` §Y/§Z）的对照。
>
> **一句话结论：本轮（2026-10-07）Arachne 4w 线性读 2739 ops/s 领先 etcd 4w 1979 ops/s 约 38%——方向与 §Y 记录的"Arachne 慢 14%（2043 vs 2365）"相反。** 1w 无回归（4157 ops/s ≥ 4000）。ReadIndex 每读 ~1 轮、无批量合并收益。方向翻转与 §Z 判定的"4w 被 cluster-state/system-load 方差（~2×）主导"一致，故本数值同样降格为"方差内单次观测"，不可作为硬门槛。

---

## 1. 测量口径（caliber）

| 项 | 值 |
|----|----|
| 机器 | macOS（Darwin, Apple Silicon）主机；Docker 内 **aarch64-unknown-linux-musl** 3-node Arachne + **etcd v3.5.21** 3-node |
| 集群 | 同固定子网 172.30.0.0/24；WAL fsync 每提交（双方默认）；同容器内网络驱动 |
| 读口径 | 线性化（ReadIndex Safe / linearizable）；`keep-alive` 长连接；n=400 |
| 并发梯度 | `--read-workers 1,2,4,8`（1 个持久连接/worker，与 Arachne 一致） |
| 统计 | **3 轮取中位数**；4w 为高方差 regime，另报 min/max 展宽 |
| 二进制 | Arachne 复用已构建 target（`bin/arachne-node`，SHA256 与镜像内二进制一致，无需重编） |
| 轮次采样 | 5ms 间隔拉取 leader `/metrics` 的 `arachne_read_index_rounds_total`（计数器）与 `arachne_read_index_pending`（表） |

**Arachne 两级口径**
- **主口径（公平）**：`--process --keep-alive` —— GIL 隔离子进程，消除 Python 客户端 GIL 成本，逼近服务端真实成本（与 Go 基准角色对应）。
- **参考口径**：同命令去掉 `--process`（线程，共享 GIL）——用于量化 GIL/客户端侧成本。

---

## 2. 原始数据（3 轮中位数）

### 2.1 Arachne（主口径，`--process --keep-alive`, n=400）

| benchmark | ops/s (med) | p50 | p99 | min | max | 展宽% |
|-----------|-----------:|-----:|-----:|----:|----:|------:|
| put (seq, 1w) | 563 | 1.29 ms | 3.65 ms | 541 | 673 | 23 |
| put (con4, 4w) | 381 | 2.79 ms | 4.89 ms | 294 | 415 | 32 |
| **get (linear 1w)** | **4157** | **0.240 ms** | **0.314 ms** | 3558 | 4247 | 17 |
| get (linear 2w) | 3338 | 0.266 ms | 0.538 ms | 3262 | 3448 | 6 |
| **get (linear 4w)** | **2739** | **0.365 ms** | **0.568 ms** | **2205** | **3025** | **30** |
| get (linear 8w) | 1809 | 0.475 ms | 1.880 ms | 1750 | 2049 | 16 |
| get (stale 4w) | 7593 | 0.120 ms | 0.251 ms | 7591 | 7746 | 2 |

### 2.2 Arachne（参考口径，线程/无 `--process`）

| benchmark | ops/s | p50 | p99 |
|-----------|------:|-----:|-----:|
| get (linear 1w) | 4293 | 0.240 | 0.282 |
| get (linear 2w) | 2505 | 0.398 | 0.486 |
| get (linear 4w) | 1886 | 0.526 | 0.747 |
| get (linear 8w) | 1336 | 0.726 | 1.204 |
| get (stale 4w) | 4204 | 0.183 | 0.842 |

### 2.3 etcd v3.5.21（线性化 keep-alive, n=400）

| benchmark | ops/s (med) | p50 | p99 | min | max | 展宽% |
|-----------|-----------:|-----:|-----:|----:|----:|------:|
| range (linearizable) keepalive 1w | 3280 | 0.29 | 0.44 | 3119 | 3404 | 8.7 |
| range (linearizable) keepalive 2w | 2993 | 0.33 | 0.54 | 2781 | 3054 | 9.1 |
| **range (linearizable) keepalive 4w** | **1979** | **0.48** | **1.19** | **1909** | **1985** | **3.8** |
| range (linearizable) keepalive 8w | 1162 | 0.76 | 1.95 | 1135 | 1232 | 8.3 |
| — 上下文: put keepalive 1w | 2187 | 0.42 | — | — | — | — |
| — 上下文: range (linearizable) fresh 1w | 2292 | 0.43 | — | — | — | — |
| — 上下文: range (serializable) fresh 1w | 2371 | 0.42 | — | — | — | — |

---

## 3. 对照表：Arachne vs etcd（线性化 keep-alive, n=400）

| 并发 | Arachne ops/s | p50 | etcd ops/s | p50 | Arachne 领先/落后% |
|------|--------------:|-----:|-----------:|-----:|:-------------------:|
| 1w | 4157 | 0.240 | 3280 | 0.29 | **+27%** |
| 2w | 3338 | 0.266 | 2993 | 0.33 | **+12%** |
| 4w | 2739 | 0.365 | 1979 | 0.48 | **+38%** |
| 8w | 1809 | 0.475 | 1162 | 0.76 | **+56%** |

- 本轮各档 Arachne 均领先 etcd，且**并发越高领先越大**（4w 反扩展 66%，etcd 60%）。
- 注意口径非完全对称：Arachne 主口径为 GIL 隔离（`--process`），etcd 驱动为线程（共享 GIL）。因此该对比同时含 GIL 客户端效应（§5）；公平服务端对比应视 GIL 剥离后的残差。

---

## 4. 与已记录基线（§Y/§Z）对照

| | 2026-10-02 记录（§Y, Go 客户端） | 本轮（2026-10-07, Python `--process`） |
|---|---:|---:|
| Arachne 4w | 2043 / p50 0.43 ms | 2739 / p50 0.365 ms |
| etcd 4w | 2365 / p50 0.40 ms | 1979 / p50 0.48 ms |
| 相对差 | Arachne **慢 ~14%** | Arachne **快 ~38%** |

- **方向翻转**：与 `propsol-v0.2.md:592-599`（§Z）判定一致——"4w 测量被 cluster-state/system-load 方差（~2×）主导，跨集群方向可翻转"。本轮 Arachne 偏快、etcd 偏慢。
- **判定**：同 §Z 处理法——将 "Arachne 落后 etcd ~14%" 保留为**机制归因**（C3 读合并生效、残留为每周期 ReadIndex quorum 轮、etcd 调度更优）而非数值结论；**本轮 +38% 同样只作方差内单点观测**，不入硬门槛。4w 门槛须回归式（"不劣于改动前 4w 底线"）而非绝对 14%（§5.1 / plan §25）。

---

## 5. GIL / 客户端效应（Arachne：`--process` 主口径 vs 线程参考）

| 并发 | 主口径 ops/s | 线程 ops/s | 线程相对% |
|------|------------:|-----------:|----------:|
| 1w | 4157 | 4293 | +3.3% |
| 2w | 3338 | 2505 | **-24.9%** |
| 4w | 2739 | 1886 | **-31.1%** |
| 8w | 1809 | 1336 | **-26.2%** |
| stale 4w | 7593 | 4204 | **-44.6%** |

- 1w 无 GIL 影响（单连接串行）；2w+ 线程显著更慢 → **Python 客户端 GIL 是并发读吞吐的真实成本项**，这也是主口径必须 `--process` 的原因。
- stale 4w 线程版被 GIL 压得最惨（-45%），进一步印证"线程口径会低估服务端、高估客户端"。

---

## 6. 1w 无回归检查（plan §1.3 / §68）

- 记录基线 Arachne 1w ≈ 4000+ ops/s（Go 客户端）。
- 本轮 Arachne 1w 主口径 = **4157 ops/s**（≥ 4000）✓ 无回归。
- 反扩展：Arachne 4w/1w = **66%**（§Y 记录 ~72%）；etcd 4w/1w = **60%**（§Y 记录 ~65%）。两者均"随并发下降"（反扩展），本值与记录在同一量级。

---

## 7. ReadIndex 可观测性（机制指标；5ms 采样，leader 8001）

| 阶段 | reads/s | ReadIndex 轮数 | rounds/s | reads/round | pending 峰值 |
|------|--------:|---------------:|--------:|-----------:|:-----------:|
| linear 1w | 4816 | 401 | 4241 | 1.14 | — |
| linear 2w | 3863 | 278 | 2636 | 1.47 | — |
| **linear 4w** | **2908** | **210** | **4097** | **0.71** | **3** |
| linear 8w | 2156 | 155 | 3154 | 0.68 | 4 |

**解读（本机制归因的基线证据）：**
- `rounds_total` 只在 `drive_cycle` 内检测到未发 ReadIndex 的 pending 读时 +1，并将**本轮所有未发读合并到同一轮**（`mod.rs:985-996`）。
- 4w/8w 下 `reads/round < 1`（rounds 多于 reads）且 **pending 峰值仅 3–4** → 读未排队、**每个读基本各成一轮 ReadIndex（批量合并收益 ≈ 0）**。`reads/round < 1` 的"多"为事件触发 + 采样粒度的噪声，方向上只反映"无合并"。
- 1w/2w 下 `reads/round > 1` 为低频下偶发合并。
- 结论：**当前 C3 基线在高并发下未获得 cohort 级批合并收益**；P2-A（§3.5）的证伪标准（`rounds/sec ≥ 0.8× reads/sec` 且 `released-per-round ≤ 1.3×`）正由该基线支撑——若改后仍满足即判定 A 无操作、转向读并行上限（§2）。

> 单轮观察、5ms 粒度；4w 高方差，该机制数仅作方向/量级证据，非绝对值。

---

## 8. 复现命令

```bash
# 从工作树根（已 checkout 本分支）
cd docker/bench
rm -f bin/arachne-node   # 若重编；否则复用已构建二进制（SHA256 与镜像内一致）
docker compose up -d
# 主口径（--process）3 轮
for i in 1 2 3; do docker compose run --rm driver \
  --read-workers 1,2,4,8 --n 400 --keep-alive --process --json \
  --hosts node1,node2,node3 > bench-artifacts/round${i}.json 2> bench-artifacts/round${i}.txt; done
# 参考口径（线程）
docker compose run --rm driver --read-workers 1,2,4,8 --n 400 \
  --keep-alive --json --hosts node1,node2,node3 > bench-artifacts/ref.json
docker compose down -v

cd docker/bench-etcd
docker compose up -d
for i in 1 2 3; do docker compose run --rm driver \
  --hosts etcd1,etcd2,etcd3 --ports 2379,2379,2379 --read-workers 1,2,4,8 \
  > bench-out/etcd_r${i}.raw.txt; done
docker compose down -v
```

---

## 9. 局限 / 注意

1. **cluster-state 方差**：4w 为 ~2× 高方差 regime；本轮 Arachne/etcd 方向与 §Y 相反，符合 §Z。勿将本轮 ±38% 读作绝对能力差。
2. **口径不对称**：Arachne 主口径 GIL 隔离，etcd 线程；§3 的相对差含 GIL 客户端效应（§5）。绝对服务端对比需同客户端（Go）。
3. **ReadIndex 指标为单轮观察**；`reads/round<1` 的精确值受采样粒度影响，只作方向证据。
4. 本任务为**只读测量**：未改任何源码（仅新增 `dev-docs/` 文档与未追踪的 `bench-artifacts/`、`bench-out/` 原始产物）。

---

*原始产物（未入库，工作树内）：*
- `docker/bench/bench-artifacts/`：`round{1,2,3}.json|.txt`（含 5ms metrics `round1.metrics`）、`ref.json`、`r1b.metrics`（ReadIndex 观察）、`tslog.py`、`metrics_sample.py`
- `docker/bench-etcd/bench-out/`：`etcd_r{1,2,3}.raw.txt`
