# 升级开发计划：对标 etcd —— HTTP 面重构 + WAL 持久化平台化与组提交

> 设计权威：`propsol-v0.2.md` rev U（v0.2.18）。基准/对照数据与归因：`handoff-m1.md` §1.35。
> 本文档是把 rev U 拆成**可分派批次**的执行计划；每个批次独立可验证，由 fixer 按批次实施。

## 1. 目标与基线（docker 3 节点同配置，§1.35）

| 操作（单连接） | arachne 现状 | etcd v3.5.21 | 差距归因 |
|---|---|---|---|
| put | 40 ops/s，p50 25.1ms | 821 ops/s，p50 1.13ms | HTTP 20ms 空转轮询 + 一连接一请求 + `sync_all` 整设备刷 |
| 线性读 | 44 ops/s，p50 22.3ms | 1531 ops/s，p50 0.58ms | 主要是 HTTP 轮询（核心 ReadIndex 并发下 0.96ms） |
| 弱读 4 并发 | **44 ops/s（平顶）** | 1388 ops/s | 外层 HTTP+`get_stale` 路径问题 |
| 线性读 4 并发 | 767 ops/s，p50 0.96ms | ~2500 ops/s | 核心差距已小（2–3×） |

## 2. 效率目标与验证矩阵

实测环境：docker（Linux/aarch64 overlayfs）为主 —— fdatasync 收益只在这可见；macOS 开发机保持
正确性 + 组提交带来的减少 flush 次数收益。**基准工具**：`docker/bench/`（arachne）+ `docker/bench-etcd/`（etcd 对照）
in-network 驱动；`arachne/tests/bench_runtime.rs`（进程内，`#[ignore]`）。

| # | 目标 | 现状 | B1+B2 后 | B3 后 | **B4 后** | 验收阈值（docker） | 判定 |
|---|---|---|---|---|---|---|---|
| T1 | 单连接线性读脱离 20ms 地板 | 44 / 22.3ms | 701–809 / p50 1.10–1.13ms | 1456–1662 / p50 0.57ms | **2859–3503 / p50 0.28–0.36ms** | **≥ 400 ops/s 且 p50 ≤ 5ms** | ✅ |
| T2 | 单连接 put 脱离轮询地板 | 40 / 25.1ms | 224–238 / p50 4.10–4.29ms | 426–462 / p50 1.99–2.06ms | **503–703 / p50 1.13–1.77ms** | **≥ 200 ops/s / p50 ≤5ms** | ✅ |
| T3 | 弱读 4 并发平顶消失 | 44（平顶） | 746–859 / p50 1.01–1.18ms | 1401–1615 / p50 0.58–0.61ms | **2202–2365 / p50 ~0.4ms** | **≥ 200 ops/s** | ✅ |
| T4 | 写 p99 降低（fdatasync+预分配） | ~25ms | put p50 4.1ms | put p50 ~2.0ms | **put p50 1.13ms / p99 3.5ms** | B1+B2 后 ≤5ms；B3/B4 递降 | ✅ |
| T5 | 正确性零回归 | — | 414 / fi 430 | 414 / fi 433 | workspace 417 / fi 436 / node 全绿 / 门禁 4/4 | 全部现有测试 + 门禁绿 | ✅ |

**✅ 观察项已缓解（B3 后复测，落档）**：**线性读 4 并发**从 B2 后下滑区（286–337）回升到 **690–734 ops/s / p50 1.28–1.35ms**，重回 M1 基线（767）量级。归因：B1+B2 后下滑源于 `enable_offloaded_durability()` 尚未生产接线（写路径仍堵塞 actor 线程，经 HTTP 并发读取被写 p99 拖累）；B3 接线后流水线生效，写不再堵读。**首因即 rev P 的"可用性而非延迟"设计——offloaded 是前端开关**。

**B4（fallocate 预分配）对拍结论（orchestrator，2026-09）**：Linux `fallocate(FALLOC_FL_KEEP_SIZE)` 段预分配落地后（`b933808`），在 B3 同口径（n=100）下 **put 单连接 703 ops/s / p50 1.13ms**，较 B3（~445 / ~2.0ms）**吞吐 +58%、p50 −44%**——收益**远超** ora-2 预期 10–15%。线性读单连接 ~2900–3500、弱读 4 并发 ~2200 同步改善。B4 采纳判定成立。

**总对照（docker 3 节点同配置，§1.35 基线）**：| 操作 | M1 | B4 后 | etcd | 差距 |
|---|---|---|---|---|---|
| put 单连接 | 40 ops/s / 25.1ms | **~600–700 ops/s / p50 ~1.1–1.8ms** | 821 / 1.13ms | **已达/反超 etcd** |
| 线性读单连接 | 44 / 22.3ms | **~2900–3500 / p50 ~0.3ms** | 1531 / 0.58ms | **2×+ 超越 etcd** |
| 线性读 4 并发 | 767 / 0.96ms | **~700 / p50 1.3ms** | ~2500 | 3.5× 内 |
| 弱读 4 并发 | 44（平顶） | **~2200 / p50 ~0.4ms** | 1388 | 1.6× 超越 |

**回归门（每个批次通过才算完成）**：
- `cargo build --workspace`；`cargo test -p <该批 crate>`；全 workspace `cargo test --workspace`（fault-injection 全 workspace 也要过：`cargo test --workspace --features fault-injection`）。
- 门禁：`bash scripts/check-entropy.sh`、`check-deps.sh`、`check-profile-knobs.sh`。
- B2/B3 额外：INV2 崩溃注入、FsyncLedger 目录 fsync 计数、faulty_storage、重启恢复全部绿。
- 效率：B1/B2 调和后由 orchestrator 复跑 `docker/bench` 驱动对拍 T1–T4。

## 3. 批次拆分与依赖

```
B1 (U1) HTTP 面重构 hyper —— arachne-node 	┐ 相互独立，可并行派发
B2 (U2) WAL sync 平台化 + 段预分配 —— arachne 核心 ┘
B3 (U3) 组提交（一次 sync 落一批并发写）—— arachne 核心   ← 依赖 B2（在 B2 的 sync 抽象/流水线上续）
```

- **B1、B2 无文件重叠**（B1 限 `arachne-node/`，B2 限 `arachne/src/storage/`），可并行。
- **B3 必须等 B2 调和**（同一批 `wal.rs` 文件）；B2 完成后派发（可续 B2 会话）。
- 每个批次 TDD：先补/改行为测试 → 实现 → 编译 + 测试 + 门禁全绿（仓库规约：禁止假设正确，改后必跑）。

## 4. 批次规格（fixer 执行依据）

### B1（U1）：arachne-node HTTP 面改用 hyper 1.x
- **范围**：`arachne-node/src/http.rs`、`arachne-node/src/main.rs`、`arachne-node/Cargo.toml`、
  `arachne-node/tests/*`（只读验证，不改语义）。
- **现状**：`http.rs` = 单线程阻塞 `serve()`（nonblocking accept + WouldBlock→`sleep(20ms)` 轮询）+
  `handle_connection` 一连接一请求（无 keep-alive）；`HttpHandler::handle(&self, method, path) -> HttpResponse`
  **同步**签名；`main.rs NodeHttp::handle` 内部 `self.rt.block_on(kv.get/put/…)`。HTTP 服务器跑在独立
  blocking 线程（main.rs:363-403）。
- **做法**：
  1. `Cargo.toml` 加直接依赖（**已在依赖树内**，版本对齐 lock）：`hyper = "1"`、`hyper-util = { version = "0.1", features = ["server", "http1", "tokio", "service-ext"] }`、`http-body-util = "0.1"`、`futures-util`（如需要 service stream）。
  2. 用 hyper 替换 `serve()`：`TcpListener::bind`（回归 tokio 的 `TcpListener`）→ `loop { accept().await → tokio::spawn(每连接) }`；`hyper_util::server::conn::auto::Builder::new(TokioExecutor)`
     `.http1_only().serve_connection(TokioIo::new(stream), service_fn(handler))`；默认 keep-alive；
     `Semaphore` 限并发连接；优雅关停（连接 `graceful_shutdown()`，配合现有 signal）。
  3. `HttpHandler` 改为 **async**：`fn handle(&self, method, path) -> impl Future<Output = HttpResponse>`；
     `NodeHttp` 直接 `kv.get/put/delete/get_stale().await`（**删掉 `rt.block_on` 桥**）；
     保留 `/readyz`、`/metrics`、`/kv/*`（put/get/stale/delete）、`/members/*` 语义与全部状态码
     （200/409+hint/503/400/405/431）。
  4. **改写 `http.rs` 既有 4 个单测**到 hyper 服务器上（临时端口 + 裸 TcpStream 请求，保持断言：
     readyz/metrics/错误；put/delete 路由与 405；慢客户端仍被响应；超长请求行 → 431）。
     `arachne-node/tests/*`（multi_node、cli、common）必须不改且全绿。
- **验收**：§2 回归门（T1–T3 阈值由 orchestrator 复测；本批次只要求编译 + 测试 + 门禁绿，HTTP 行为等价）。
- **红线**：不碰 `arachne/` 核心、不碰 `arachne-transport-tonic/`、不碰基准 harness；不 git commit。

### B2（U2）：WAL sync 平台化（std-only，零新增依赖）
- **范围**：`arachne/src/storage/`（新增 `sync.rs`；改 `wal.rs`/`segment.rs` 的 sync 调用点）；新增单测。
    B2 本批 std-only；后续 B4（U2-bis）例外：仅 Linux 下经 cfg-gated 引入零树增长的 `libc`（0.2，
    已在 tokio 树内）做段预分配，**Gate B 红线不变**（瘦核 `--no-default-features` 仍无传输依赖，
    `libc` 不触 Gate B）。
- **现状**：真实 sync 落在三处 `file.sync_all()`：`FlushHandle::flush`（wal.rs≈223，flusher 线程路径）、
  flusher 循环（≈1117）、`Segment::sync`（`sync_entries` 同步路径走这里）；`fsync_dir` 已在段创建/rename 后调用 ✓；
  rev P 单 flusher 线程 + FlushToken 已存在；`fsync_observer`（每真实 fsync 后通知）语义不能变。
- **做法（已落地，B2 完成）**：
  1. 新增 `storage/sync.rs`：`pub fn sync_durable(file: &File) -> io::Result<()>`：
     `#[cfg(target_os = "linux")]` 用 `file.sync_data()`（= `fdatasync(2)`），其余平台（含 macOS）用
     `file.sync_all()`（macOS std 即 `F_FULLFSYNC`，维持现状）。**Linux 恒定 `fdatasync`**：其语义覆盖
     "检索数据所需的全部元数据"——append 造成的 `i_size` 扩展也在其范围内，故非预分配下也是完备屏障。
   2. **段预分配（B2 时"推迟"，B4 / U2-bis 已落地）**：B2 的结论（std-only `set_len` 是
      `ftruncate(2)`，产生稀疏 holes 而非 reserved extents）在 B4 下被 `fallocate(2)` +
      **`FALLOC_FL_KEEP_SIZE`** 推翻：该 flag 仅 Linux 可用，reserve extents 而 **不增大 `i_size`**。
      新增 `storage/prealloc.rs`（libc 0.2，linux-gated，零树增长），在 `Segment::open_with_max_bytes`
      后调 `preallocate(&file, max_bytes)`。因 `i_size` 不变，五个读 `i_size` 的子系统
      （`recover_wal`/tear 检测/`truncate_log_to`/`size`·`should_rollover`/`log_bytes`）字节不变，
      **不重写恢复、tear 检测、rollover 均不受影响**——"rollover 恒触发"结论不再成立。
      失败（EOPNOTSUPP/ENOSYS/EINVAL/ENOSPC）→ skip（非致命）；`StorageStats::segment_prealloc_skips`
      计数。`sync_durable` 仍无需 preallocated 标志。
  3. 三处 `sync_all` 全部改走 `sync_durable`；`fsync_observer` 通知时机与次数不变；快照文件 `sync_all` 保持
     原样（非 WAL 段）。
  4. 测试：单测 `sync_durable_persists_written_bytes` + Linux-only `fdatasync` 行为证据（`#[cfg(target_os = "linux")]`，
     CI 覆盖）；**不改既有持久性测试语义**，全部必须绿（INV2/ledger/faulty_storage/恢复全绿）。
- **验收**：§2 回归门（build/全 workspace 测试/release-fi、fault-injection、门禁）；INV2 + ledger + 恢复全绿。
- **红线**：不碰 `arachne-node/`、`arachne-transport-tonic/`、`arachne-seam/`、基准 harness；不 git commit。

### B3（U3）：组提交 —— 一次 sync 落一批并发写（**已落地**）
- **范围**：`arachne/src/storage/wal.rs`（flusher 批排合并 + fsync 计数修正）；
  `arachne-node/src/node.rs`（生产接线）；新增单测；新增 `dev-docs/b3-group-commit-spec.md`。
  **不**动 seam 类型（`FlushToken`/`PersistSubmit`/`FlushHandle`）、**不**动 `FsyncPolicy`
  变体/默认（B2 已定）、**不**动 HardState 批处理（I1）。
- **做法**：
  1. flusher 循环改为**排空合并**（无计时窗）：`recv` 取本批锚 Job → `try_recv` 排空
     所有已到达 Job → 按 `segment_first_index` 分组（同段取最后 Job 的 fd）→ **每段
     一次 `sync_durable`**（fail-stop：任一段失败则整批 tokens 报 `Err`）。
  2. `FlushJobResult` 增 `is_segment_owner: bool`（段内最后 Job）。`poll_flush`
     仅在 `is_segment_owner && 段==当前段` 时触发**单次** `notify_fsynced` +
     `pending_entry_fsync=false`，把 sync 路径的"逐 Job fsync 事件"坍缩为"每段一次"。
  3. `Offloaded` 增 `flusher_syncs: Arc<AtomicU64>`（每批每段一次，失败计入尝试）；
     `stats().offloaded_fsyncs` = 同步路径 `note_flushed` + `flusher_syncs`（`stats()` 求和）。
     **核心修正**：不再用 token 数冒充设备 fsync 数。
  4. `node.rs`：`Runtime::new` 前 `wal.enable_offloaded_durability()?` —— 生产节点
     默认走 offloaded 流水线；raft 节点（`consensus/node.rs`）已有 token 轮询 + fail-stop。
- **验收**：§2 回归门（build/全 workspace 测试/release-fi、fault-injection、门禁）；
  三处 TDD（单段 5/1=1、跨段 6/2=2、失败段全 Err）全绿；`I2/I4` + INV2 + 恢复全绿。
  效率 T4（put p50 进一步下降）由 orchestrator 用 `docker/bench` 复测对拍。
- **红线**：不碰 `arachne-node/` 之外的 crate、`arachne-transport-tonic/`、`arachne-seam/`、
  基准 harness；不 git commit。

## 5. 工具与沙箱
- 编译/测试/门禁必须 `CARGO_TARGET_DIR=$PWD/.dsh-target`（沙箱拒绝仓库外 target）；fi 全套同。
- 基准复测：orchestrator 用 `docker/bench`（重建 musl 二进制 → `docker compose up`
  → `driver/bench.py`）与 `docker/bench-etcd` 复核 T1–T4；不做代码的批次结论以 §2
  回归门为准。
- 基准门禁（防回归复测面）：`scripts/check-perf-baseline.sh`（重建 musl 二进制 →
  起 3 节点集群 → 等待 readyz → `docker compose run --rm driver --json` → 解析 JSON
  阈值表 T1–T4 + B3 基线；`--fast` 只跑关键项（put 单连 + 线性读单连）控制时长、
  `--etcd` 附 etcd v3.5.21 对照；跑毕自动 `down -v`、清节点镜像，driver 镜像作为
  缓存依赖保留，细节见 `docker/bench/README`）。
