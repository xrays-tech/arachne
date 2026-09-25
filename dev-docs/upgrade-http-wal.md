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

| # | 目标 | 现状 | 实测（B1+B2 后，两次） | 验收阈值（docker） | 判定 |
|---|---|---|---|---|---|
| T1 | 单连接线性读脱离 20ms 地板 | 44 / 22.3ms | 701–809 ops/s / p50 1.10–1.13ms | **≥ 400 ops/s 且 p50 ≤ 5ms**（B1 后） | ✅ |
| T2 | 单连接 put 脱离轮询地板 | 40 / 25.1ms | 224–238 ops/s / p50 4.10–4.29ms | **≥ 200 ops/s / p50 ≤5ms**（B1+B2 后） | ✅ |
| T3 | 弱读 4 并发平顶消失 | 44（平顶） | 746–859 ops/s / p50 1.01–1.18ms（单连接 1239–1443） | **≥ 200 ops/s**（B1 后） | ✅ |
| T4 | 写 p99 降低（Linux fdatasync） | ~25ms | put p50 4.1ms（含 keep-alive 收益，fdatasync 已在 Linux 侧落地） | B1+B2 后 put p50 ≤ 5ms；B3 组提交后再降 | ✅ |
| T5 | 正确性零回归 | — | workspace 414 / fi 430 / 门禁 4/4 全绿 | 全部现有测试 + 门禁绿 | ✅ |

**⚠ 观察项（未归因，B3 前留意）**：**线性读 4 并发较 M1 基线下滑**——M1 767 ops/s / p50 0.96ms，B1+B2 后实测 286–337 ops/s / p50 2.83–3.30ms（两次稳定）。单连接线性读达标（T1）且并发差距主要在并发 driver 侧的线性一致读聚合。已排除 HTTP 轮询地板（单连接已脱困）；待确认是 driver 并发形态、hyper 连接复用 vs 旧一连接一请求、还是核心 ReadIndex 并发路径被 B1/B2 之外的改动影响。B3 调和后复测，若仍下滑再单独归因。

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
  **禁止**引入 `libc`/`rustix`（核心零外部 crate 红线，Gate B）——**只用 std**。
- **现状**：真实 sync 落在三处 `file.sync_all()`：`FlushHandle::flush`（wal.rs≈223，flusher 线程路径）、
  flusher 循环（≈1117）、`Segment::sync`（`sync_entries` 同步路径走这里）；`fsync_dir` 已在段创建/rename 后调用 ✓；
  rev P 单 flusher 线程 + FlushToken 已存在；`fsync_observer`（每真实 fsync 后通知）语义不能变。
- **做法（已落地，B2 完成）**：
  1. 新增 `storage/sync.rs`：`pub fn sync_durable(file: &File) -> io::Result<()>`：
     `#[cfg(target_os = "linux")]` 用 `file.sync_data()`（= `fdatasync(2)`），其余平台（含 macOS）用
     `file.sync_all()`（macOS std 即 `F_FULLFSYNC`，维持现状）。**Linux 恒定 `fdatasync`**：其语义覆盖
     "检索数据所需的全部元数据"——append 造成的 `i_size` 扩展也在其范围内，故非预分配下也是完备屏障。
  2. **段预分配已放弃（决策落档，dev 时为 oracle 裁决）**：std-only `File::set_len` 是 `ftruncate(2)`，
     在 ext4/xfs 上产生**稀疏文件（holes）而非 reserved extents**——首次写入 hole 仍触发延迟分配/extent
     journal 工作，`fdatasync` 并不会变为 `fallocate` 那种廉价 data-only 屏障（xfs 上 `fdatasync`≡`fsync`）。
     且预分配会让 WAL 恢复/截断（`recover_wal` 整文件读）变成每段固定 128 MiB、tear 检测整体失效、
     rollover 恒触发——是 WAL 恢复子系统重写，与"不改既有持久性测试语义 + INV2/ledger/faulty_storage/恢复"
     验收门直接冲突。**结论：收益在 std-only 下不存在 → 不引入 crate、不重写恢复，推迟**。
     `sync_durable` 无需 preallocated 标志。
  3. 三处 `sync_all` 全部改走 `sync_durable`；`fsync_observer` 通知时机与次数不变；快照文件 `sync_all` 保持
     原样（非 WAL 段）。
  4. 测试：单测 `sync_durable_persists_written_bytes` + Linux-only `fdatasync` 行为证据（`#[cfg(target_os = "linux")]`，
     CI 覆盖）；**不改既有持久性测试语义**，全部必须绿（INV2/ledger/faulty_storage/恢复全绿）。
- **验收**：§2 回归门（build/全 workspace 测试/release-fi、fault-injection、门禁）；INV2 + ledger + 恢复全绿。
- **红线**：不碰 `arachne-node/`、`arachne-transport-tonic/`、`arachne-seam/`、基准 harness；不 git commit。

### B3（U3）：组提交 —— 一次 sync 落一批并发写（**等 B2 调和后派发**）
- 范围与做法待 B2 落地后细化；核心：在 rev P flusher 的批量语义上把"每批一次 sync"落到"一个提交窗口
  （写缓冲满或短窗口到点）内的所有并发提案"，并在 `FsyncPolicy::BatchMs(ms)` 语义上给出默认参数。
- 验收门：`I2/I4` 持久性不变式 + INV2 故障注入 + 读延迟（不能在窗口期恶化 p99）+ §2 T4。

## 5. 工具与沙箱
- 编译/测试/门禁必须 `CARGO_TARGET_DIR=$PWD/.dsh-target`（沙箱拒绝仓库外 target）；fi 全套同。
- 基准复测：orchestrator 用 `docker/bench`（重建 musl 二进制 → `docker compose up` → `driver/bench.py`）
  与 `docker/bench-etcd` 复核 T1–T4；不做代码的批次结论以 §2 回归门为准。
