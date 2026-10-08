# EdgeRuntime

EdgeRuntime 是一个面向同机 Linux 进程的 **C++17 共享内存 IPC 运行库**，提供 **SPSC latest-value 状态通道、MPMC 有界任务队列、SharedBufferPool 固定块共享缓冲池**，以及进程监督、等待通知和诊断工具。连续状态、离散任务和大块数据分别使用对应的数据面，也可以通过缓冲池句柄组合成跨进程处理链。

## 核心模块与选型

| 模块 | 数据语义与主要接口 | 适用场景 |
|---|---|---|
| SPSC 状态通道 | `Producer<T>` / `Consumer<T>`；单生产者、单消费者，只读取最新值，允许跳过中间样本 | 连续控制目标、设备状态、周期快照 |
| MPMC 有界任务队列 | `MpmcQueue<T>`；多生产者、多消费者，固定容量 FIFO，支持 `try_push/try_pop` 和绝对 deadline 等待 | 离散任务、事件、需要逐条出队的固定大小消息 |
| SharedBufferPool 共享缓冲池 | `SharedBufferPool`、`WriteBuffer` / `ReadBuffer`；直接访问共享块，以 `BufferHandle` 定位并校验数据 | 大块 payload 的跨进程写入、读取与复用 |

缓冲池可与 `MpmcQueue<BufferHandle>` 组合：payload 留在共享内存中，队列只传递
88B descriptor，接收进程独立打开 pool、校验句柄、读取并释放共享块。
队列负责至多一次成功出队；任务执行结果、业务 ACK、持久化和端到端交付策略由应用层管理。

## 共享机制

- **固定跨进程 ABI**：共享布局以显式宽度整数描述状态、偏移和身份；32/64 位原子访问为 lock-free。MPMC 使用生产侧与消费侧各自的跨进程 gate 协调并发。
- **进程共享 futex**：用于 SPSC 通道和 MPMC 队列的等待/唤醒；所有权与发布协议保证数据正确性，通知负责降低等待延迟。
- **所有权与故障处理**：校验实例代次、nonce、schema 和进程身份；SPSC 通道支持受验证的恢复，MPMC/Pool 在 owner 崩溃或身份校验存在歧义时返回 `RecoveryBlocked`，保留原有所有权状态。

## SPSC latest-value 状态通道

- **三槽位所有权协议**：生产者持续发布，消费者按需读取最新样本；慢消费者通过 sequence gap 观察跳过的中间样本。
- **双 transport**：POSIX shm（默认）与 **memfd + SCM_RIGHTS**。memfd 使用匿名 fd，consumer 通过 producer 进程内的 per-channel broker socket（`SO_PEERCRED` 同 UID 门控）收到 fd。creator/broker 退出后停止向新 peer 分发 fd；现有 fd 和 mmap 由各持有者管理生命周期。
- **可选 heartbeat**：`Producer<T>::heartbeat()` 提供活性观测；`kDataStale` 按业务样本时间判断新鲜度，`kProducerStalled` 按心跳判断生产者活性，owner 回收遵循独立的身份与代次校验。
- **loaned encoded buffers**：`WriteLoan` 直接编码到共享槽位，`ReadLoan` 原地解析校验后的共享字节，实现库内 payload 零拷贝；move-only RAII 负责 abort/release，借用者崩溃复用既有槽位回收。应用层负责对象编解码和上下游数据传递。
- **受验证的崩溃恢复**：通道重建、槽位回收与重连均经过身份和代次校验；存在安全性歧义时保持 fail-closed。

## MPMC 有界任务队列

- **多生产者/多消费者**：固定容量、固定大小 trivially-copyable payload，按 FIFO 位置至多成功交付一次；`try_push`/`try_pop` 提供即时操作，队列满时通过 `QueueFull` 向调用方传递背压。
- **绝对 deadline 等待**：`wait_push_until`/`wait_pop_until` 使用绝对 `steady_clock` deadline 和进程共享 futex；空/满、超时、损坏、计数耗尽和 `RecoveryBlocked` 有独立错误分类，EINTR/伪唤醒沿用原始等待预算。
- **ABI 与故障处理**：ABI major 2 使用 `EDGMPMC2`、320B header 和 64B slot；读取新增字段前校验 magic/major。使用 POSIX 共享内存，生产/消费两侧各自跨进程门控，owner/槽位崩溃时保留现场并返回 `RecoveryBlocked`。
- **真实跨进程压测**：`edge_mpmc_bench` 使用 `fork/exec`，支持 1P1C/2P2C、64B/1KiB/4KiB payload、可配置容量和消息数，统计 published/delivered、队列状态、`wait4` CPU 时间与逐 run 延迟分位数。

## SharedBufferPool 固定块共享缓冲池

- **共享块借用**：支持 4KiB/16KiB 固定块；`WriteBuffer` / `ReadBuffer` 直接访问 mmap 视图，RAII 管理 abort/release；`BufferHandle` 可经 MPMC 队列跨进程传递。
- **句柄与 ABI 校验**：Pool ABI major 2 使用 192B header、64B block header，block `owner_epoch` 位于 offset 48；`BufferHandle` 固定为 88B。校验 pool generation、instance nonce、schema、owner identity 和 block generation，打开时要求匹配的 magic/major。
- **安全复用与数据传递**：共享块按借用、发布、读取、释放的状态流转；活动 reader/writer 持有期间保留块所有权，崩溃时保持 fail-closed。payload 留在共享块中，队列复制并传递 88B descriptor。
- **跨进程压测**：`edge_buffer_pool_bench` 提供 1P1C/2P2C、4/16KiB 矩阵、逐 run 延迟摘要和原始 CSV，结果标注 `VM_ONLY`。

## 进程监督与诊断

- **ProducerSupervisor**：使用长驻 pidfd 监督自己 spawn 的 SPSC producer 子进程，提供有界 backoff 重启；崩溃重启后 generation+1，心跳卡死经 SIGTERM→grace→SIGKILL 处理；clean exit 正常结束监督，crash loop 封顶后进入 GAVE_UP。替换由子进程自己的 `Producer::create` 执行受验证的恢复流程。
- **诊断工具**：`edge_shm_ctl` 展示 SPSC 通道元数据和所有权状态（含 fd-pass 通道经只读 fd），并按实例身份执行受验证的删除。

## 构建与测试

环境要求：Ubuntu 22.04 / Linux ≥ 5.15、GCC 11（C++17）、CMake 3.22。

所有 CMake preset 都使用 out-of-source binary directory：构建输出位于源码树的
`build/<preset>/`，源码和构建产物分开管理；`src/CMakeLists.txt` 是库源码的唯一
归属入口。

```bash
cmake --preset dev-debug && cmake --build --preset dev-debug
ctest --preset dev-debug

# MPMC bounded queue：真实 fork/exec 集成测试
ctest --test-dir build/dev-debug -R mpmc_queue_er16 --output-on-failure

# MPMC 快速压测 smoke
ctest --test-dir build/dev-debug -R '^mpmc_bench_smoke$' --output-on-failure

# SharedBufferPool 基线压测 smoke（4/16KiB，1P1C/2P2C）
ctest --test-dir build/dev-debug -R '^buffer_pool_bench_smoke$' --output-on-failure

# ASan + UBSan
cmake --preset asan-ubsan && cmake --build --preset asan-ubsan && ctest --preset asan-ubsan

# Ordered Debug, ASan/UBSan, Release install, and external consumer gate
scripts/ci.sh
```

## 安装与使用

```bash
cmake --preset release && cmake --build --preset release
cmake --install build/release --prefix /your/prefix
```

下游项目通过 `find_package` 使用安装后的静态库与头文件：

```cmake
find_package(EdgeRuntime CONFIG REQUIRED)
target_link_libraries(app PRIVATE EdgeRuntime::edge_runtime)
```

完整示例见 [examples/consume_demo/](examples/consume_demo/)（通过安装包构建的独立 CMake 项目）。示例覆盖 channel、MPMC queue+wait，以及 `BufferHandle` 经队列传递后由独立进程重新打开 pool 并读取/释放。SPSC 数据编解码由调用方以 `PayloadCodec<T>` 特化提供，使用固定 schema。

## 目录结构

- `include/edge_runtime/<module>/` — canonical 公共 API：`common`、`channel`、`queue`、`buffer`、`process`；作为安装包公共头文件
- `include/edge_runtime/*.hpp` — 兼容旧版本 include 的 forwarding headers；项目内部代码和文档统一使用 module include
- `src/edge_runtime/<module>/` — 与公共功能域对应的私有头和实现：`channel`、`queue`、`buffer`、`process`，以及仅源码可见的 `transport`、`sync`、`utility`
- `src/CMakeLists.txt` — 库源码的统一构建入口，声明 `edge_runtime` target 及其按功能域组织的源码；功能目录由该入口集中管理
- `tools/src/` — 诊断、崩溃矩阵与 benchmark 工具源文件
- `tools/include/` — 工具私有头文件，由工具 target 使用
- `tools/` — 工具 CMake 入口；源码位于 `tools/src/` 与 `tools/include/`，构建产物位于 `build/<preset>/tools/`：`edge_shm_producer` / `edge_shm_consumer` / `edge_shm_ctl`、`edge_crash_matrix`、`edge_shm_bench`、`edge_mpmc_bench`、`edge_buffer_pool_bench`
- `test/` — 单元测试（gtest）与跨进程集成测试
- `examples/` — 下游消费示例（独立 CMake 项目，消费已安装的 target）
- `scripts/` — 本地 CI gate 脚本；`.github/workflows/` 提供相同 gate 的 GitHub Actions 版

## 性能测试

2026-10-07 在 Linux 虚拟机上使用 fresh Release 构建采集，结果标记为 `VM_ONLY`。
数据面源码对应发布提交 `f7d1d9e`。MPMC 与缓冲池使用独立 `fork/exec` 进程，
每组配置运行 3 轮，每轮每个 producer 发送 10,000 条消息，队列容量为 64；
MPMC 使用 busy/yield，缓冲池使用 2 个共享块。累计完成 **30 轮、450,000 条消息交付**，
每轮 `expected = published = delivered`，`errors = recovery = 0`，缓冲池 `invalid_payload = 0`。
原始 CSV 逐轮核对消息数，MPMC 同时核对 producer/sequence 唯一性。

下表展示三轮各自指标的最小值至最大值。吞吐按每轮 `delivered / wall_time` 计算，
单位为千条/秒；p99 取每轮独立分位数。吞吐包含工具的进程启动、处理、报告和退出开销。
MPMC 延迟从成功入队那次尝试的 payload 构造开始，计至出队完成；
缓冲池延迟从写入块内容开始，经过发布、descriptor 排队，计至接收端取得读借用。

| 模块 | payload / block | 进程配置 | 吞吐（千条/秒） | 每轮 p99 范围（µs） |
|---|---|---|---:|---:|
| MPMC | 64B | 1P1C | 1110.5–1391.6 | 30.967–32.577 |
| MPMC | 64B | 2P2C | 984.5–1070.7 | 7.470–12.933 |
| MPMC | 1KiB | 1P1C | 461.7–481.1 | 28.805–85.962 |
| MPMC | 1KiB | 2P2C | 644.8–702.7 | 15.052–106.888 |
| MPMC | 4KiB | 1P1C | 171.2–175.9 | 410.045–437.970 |
| MPMC | 4KiB | 2P2C | 287.6–332.3 | 211.736–303.691 |
| 缓冲池 + descriptor 队列 | 4KiB | 1P1C | 108.9–151.8 | 13.816–30.439 |
| 缓冲池 + descriptor 队列 | 4KiB | 2P2C | 120.2–124.6 | 13.203–15.681 |
| 缓冲池 + descriptor 队列 | 16KiB | 1P1C | 46.0–47.0 | 34.695–37.244 |
| 缓冲池 + descriptor 队列 | 16KiB | 2P2C | 44.0–44.7 | 33.933–35.281 |

SPSC 另有一组与 Unix Domain Socket 的同条件对照：64B payload、同核、100Hz、
总计 1,000 条，预热 100 条后统计 900 条，双方样本 gap、torn read 和 queue overlap 均为 0。

| 传输方式 | p50（µs） | p99（µs） |
|---|---:|---:|
| SPSC SHM + futex | 15.925 | 37.598 |
| Unix Domain Socket | 18.915 | 41.788 |

这些数值描述上述虚拟机、构建与负载配置。部署时可在目标硬件上使用相同参数复测，
按连续状态读取、任务交付或共享块处理链分别评估吞吐、尾延迟和 CPU 开销。

### 复现 Release 压测

在项目根目录执行，结果保存到新建的临时目录，每轮输出摘要与原始 CSV：

```bash
cmake --preset release && cmake --build --preset release
bench_out=$(mktemp -d /tmp/edgeruntime-perf.XXXXXX)

for bytes in 64 1024 4096; do
  for peers in 1 2; do
    build/release/tools/edge_mpmc_bench \
      --payload-size "$bytes" --producers "$peers" --consumers "$peers" \
      --messages 10000 --capacity 64 --runs 3 --timeout-ms 10000 \
      --retry-policy yield --wait-policy busy \
      --out-dir "$bench_out/mpmc-${bytes}-${peers}p${peers}c"
  done
done

for bytes in 4096 16384; do
  for peers in 1 2; do
    build/release/tools/edge_buffer_pool_bench \
      --block-size "$bytes" --block-count 2 \
      --producers "$peers" --consumers "$peers" \
      --messages 10000 --capacity 64 --runs 3 --timeout-ms 10000 \
      --out-dir "$bench_out/pool-${bytes}-${peers}p${peers}c"
  done
done

build/release/tools/edge_shm_bench --evidence \
  --producer build/release/tools/edge_shm_bench_producer \
  --consumer build/release/tools/edge_shm_bench_consumer \
  --sock build/release/tools/edge_shm_bench_sock \
  --ctl build/release/tools/edge_shm_ctl \
  --only shm-futex-64B-same-100-controlled,socket-64B-same-100-controlled \
  --out-dir "$bench_out/shm-socket"
```

快速矩阵由 `scripts/run_mpmc_bench_matrix.sh` 和
`scripts/run_buffer_pool_bench_matrix.sh` 提供，适合检查参数组合、CSV 输出和消息交付。
等待策略 A/B 可使用
`scripts/run_mpmc_bench_ab.sh --preset release --wait-policy both --runs 3`，
对照 busy/yield、busy/spin 和 blocking。

## 测试覆盖

单元测试与真实跨进程集成测试覆盖槽位协议、实例身份、schema/ABI 校验、队列空/满、
计数耗尽、owner 崩溃、futex 等待/唤醒、EINTR、伪唤醒和绝对 deadline。
Release 安装消费示例覆盖 channel、queue+wait 和 Pool descriptor 三条独立进程链。

收尾验证完成 Debug 与 ASan/UBSan 各 121 项启用测试，以及显式执行的完整
C01–C23 崩溃矩阵 23/23。ASan preset 使用 `detect_leaks=0`；正常退出路径另由
原生 Valgrind 3.27.1 Memcheck 检查 channel、queue+wait、Pool descriptor 链及资源清理。

默认 CTest 执行 `crash_matrix_smoke` 子集；完整 `crash_matrix` 注册为 Disabled，
通过工具显式运行。先用 `edge_crash_matrix --list` 查看完整用例，再执行：

```bash
crash_out=$(mktemp -d /tmp/edgeruntime-crash.XXXXXX)
build/dev-debug/tools/edge_crash_matrix \
  --producer build/dev-debug/tools/edge_shm_producer \
  --consumer build/dev-debug/tools/edge_shm_consumer \
  --ctl build/dev-debug/tools/edge_shm_ctl \
  --supervisor build/dev-debug/tools/edge_shm_supervisor \
  --out-dir "$crash_out"
```

## 资源生命周期

POSIX shm 名称由 creator 通过 creator-only remove 管理；fd 与 mmap 各自保持独立
生命周期。memfd 通过 broker 向新 peer 分发 fd，broker 退出后结束新连接接入，已有
fd 和映射由持有者关闭或解除。进程活性、现有引用和新 peer 接入分别管理。

共享块复用要求 reader/writer 完成释放及所有权校验；进程在持有借用期间终止，或
所有权存在歧义时，保留现场并返回 `RecoveryBlocked`，交由应用处理故障和实例重建。
