# EdgeRuntime

EdgeRuntime 是一个 Linux C++17 跨进程通信库，提供固定 schema 的 SPSC **latest-value** 共享内存通道，以及独立的固定容量 MPMC bounded task queue。

## 特性

- **固定跨进程 ABI**：共享内存只含显式宽度整数，无指针、无 STL 容器、无虚函数；32/64 位原子访问均为 lock-free。
- **latest-value 语义**：三槽位所有权协议，publish/read 无背压；慢消费者丢弃中间样本、从不阻塞生产者。
- **futex 通知**：进程共享等待/唤醒，只影响唤醒延迟，不参与数据正确性。
- **崩溃恢复**：对通道生命周期各崩溃点有界恢复（重建、槽位回收、重连），身份无法验证时安全关闭（fail closed）。
- **双 transport（v0.2）**：POSIX shm（默认，行为与 v0.1 一致）与 **memfd + SCM_RIGHTS**——通道对象无全局名，consumer 通过 producer 进程内的 per-channel broker socket（`SO_PEERCRED` 同 UID 门控）收到一次性 fd。memfd 对象由仍存活的 fd 或映射保持；creator/broker 退出会阻止新的 fd admission，但不会立即使已传递的 fd 或已有映射失效。
- **optional heartbeat（v0.2）**：`Producer<T>::heartbeat()` 显式声明"正在推进"，consumer 可区分正常空闲（`kDataStale`）与逻辑卡死（`kProducerStalled`，纯观测报告，不回收 owner）。
- **ProducerSupervisor（v0.3）**：长驻 pidfd 监督 + 有界 backoff 重启——监督自 spawn 的 producer 子进程，崩溃自动重启（generation+1），心跳卡死经 SIGTERM→grace→SIGKILL 接管；clean exit 不重启，crash loop 封顶后 GAVE_UP。**不绕过恢复引擎**：接管只是信号，替换由子进程自己的 `Producer::create` 走死 owner 路径。
- **loaned encoded buffers（v0.4）**：`WriteLoan` 直接编码到共享槽位，`ReadLoan` 原地解析校验后的共享字节；move-only RAII 负责 abort/release，借用者崩溃复用既有槽位回收。该保证仅指库内不复制 payload 字节，不等同于任意 C++ 对象或端到端零拷贝。
- **MPMC bounded task queue（首个里程碑）**：`MpmcQueue<T>` 支持固定大小 trivially-copyable payload、多生产者/多消费者和 `try_push`/`try_pop`；每条消息按 FIFO 位置至多成功交付一次。首版仅使用 POSIX 共享内存，生产/消费两侧各自跨进程门控，owner/槽位崩溃按 `RecoveryBlocked` fail-closed，不静默回收。
- **MPMC deadline-aware blocking wait（当前里程碑）**：MPMC ABI major 2 使用 `EDGMPMC2`、320B header 和 64B slot；`EDGMPMC1`/旧 major 在读取新增字段前拒绝。`wait_push_until`/`wait_pop_until` 使用绝对 `steady_clock` deadline 和进程共享 futex；超时、损坏、耗尽和 `RecoveryBlocked` 保持独立错误。
- **MPMC 基线压测（第二个里程碑）**：`edge_mpmc_bench` 使用真实 `fork/exec` 子进程，支持 1P1C/2P2C、64B/1KiB/4KiB 固定 payload、可配置容量与消息数；统计 published/delivered、队列状态、`wait4` CPU 时间和逐 run 延迟分位数。输出必须标注 `VM_ONLY`，不把 MPMC delivered messages 与 SPSC latest-value 的 `missed_samples` 混用。
- **SharedBufferPool 固定块零拷贝基线（第三个里程碑）**：`SharedBufferPool` 支持 4KiB/16KiB 固定块，调用方直接写入/读取 mmap 视图，仅通过 `MpmcQueue<BufferHandle>` 传递固定宽度 descriptor。Pool ABI major 2 使用 192B header、64B block header，block `owner_epoch` 位于 offset 48；`BufferHandle` 固定为 88B。pool generation、schema、owner identity 和 block generation 全部校验，崩溃边界 fail-closed，不静默回收；旧 pool magic/major 不会自动升级。`edge_buffer_pool_bench` 提供 1P1C/2P2C 矩阵、逐 run 延迟摘要与原始 CSV，结果标注 `VM_ONLY`。
- **诊断与取证**：`edge_shm_ctl` 检查通道状态（含 fd-pass 通道经只读 fd）、执行受验证的删除，不导出 payload 内容。

## 构建与测试

环境要求：Ubuntu 22.04 / Linux ≥ 5.15、GCC 11（C++17）、CMake 3.22。

所有 CMake preset 都使用 out-of-source binary directory：构建输出位于源码树的
`build/<preset>/`，不写入源码目录中的模块树；`src/CMakeLists.txt` 是库源码的唯一
归属入口。

```bash
cmake --preset dev-debug && cmake --build --preset dev-debug
ctest --preset dev-debug

# MPMC bounded queue：真实 fork/exec 集成测试
ctest --test-dir build/dev-debug -R mpmc_queue_er16 --output-on-failure

# MPMC 基线压测 smoke（完整压测不纳入默认 CTest）
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

完整示例见 [examples/consume_demo/](examples/consume_demo/)（独立 CMake 项目，验证已安装 target 可被干净消费）。示例覆盖 channel、MPMC queue+wait，以及 `BufferHandle` 经队列传递后由独立进程重新打开 pool 并读取/释放。数据编解码由调用方以 `PayloadCodec<T>` 特化提供，库不引入动态 schema。

## 目录结构

- `include/edge_runtime/<module>/` — canonical 公共 API：`common`、`channel`、`queue`、`buffer`、`process`；安装时只暴露这棵树
- `include/edge_runtime/*.hpp` — 兼容旧版本 include 的 forwarding headers；项目内部代码和文档统一使用 module include
- `src/edge_runtime/<module>/` — 与公共功能域对应的私有头和实现：`channel`、`queue`、`buffer`、`process`，以及仅源码可见的 `transport`、`sync`、`utility`
- `src/CMakeLists.txt` — 唯一的库源码归属入口，声明 `edge_runtime` target 及其按功能域组织的源码；模块目录不再各自拥有 CMake 文件
- `tools/src/` — 诊断、崩溃矩阵与 benchmark 工具源文件
- `tools/include/` — 工具私有头文件，不属于安装公共 API
- `tools/` — 工具 CMake 入口；源码位于 `tools/src/` 与 `tools/include/`，构建产物位于 `build/<preset>/tools/`：`edge_shm_producer` / `edge_shm_consumer` / `edge_shm_ctl`、`edge_crash_matrix`、`edge_shm_bench`、`edge_mpmc_bench`、`edge_buffer_pool_bench`
- `test/` — 单元测试（gtest）与跨进程集成测试
- `examples/` — 下游消费示例（独立 CMake 项目，消费已安装的 target）
- `scripts/` — 本地 CI gate 脚本；`.github/workflows/` 提供相同 gate 的 GitHub Actions 版

## MPMC 验证边界

`mpmc_queue_er16` 覆盖真实跨进程的 1P1C、2P2C、空/满、schema/ABI 拒绝和进程终止
fail-closed 边界。该测试证明正确性和错误分类，不构成吞吐或延迟结论。后续压测必须把
MPMC successfully delivered messages 与 SPSC latest-value 的 `missed_samples` 分开统计，
并将虚拟机结果标记为 `VM_ONLY`。

`edge_mpmc_bench --wait-policy busy|blocking` 可对照 busy retry 与阻塞等待；完整
对照矩阵由 `scripts/run_mpmc_bench_ab.sh --wait-policy both` 执行，不纳入默认 CTest。
`scripts/run_mpmc_bench_matrix.sh` 和 `scripts/run_buffer_pool_bench_matrix.sh` 提供
固定 payload/block、1P1C/2P2C、CSV 和正确性交付门槛。它们是 VM-only 证据工具，不能
推出 SHM 必然快于 socket，也不能把 MPMC delivered messages 与 latest-value 丢样本相比较。

`edge_crash_matrix --list` 显示当前完整崩溃用例；默认 CTest 中的 `crash_matrix` 是
Disabled，不能把 Disabled 计作通过。需要完整矩阵时，应显式传入 producer、consumer、
ctl、supervisor 和私有输出目录，并逐项检查结果与资源清理。`crash_matrix_smoke` 只覆盖
smoke 子集。

## 资源生命周期与限制

POSIX shm 名称在 creator 明确调用 creator-only remove 前可以保留；对象的 fd 和 mmap
生命周期独立于名称。memfd 没有全局 shm 名称，broker 退出后新 peer 不能再取得 fd，
但已经传递的 fd 和已有 mmap 仍由持有者负责关闭/解除映射。creator death、存量 fd/mmap
以及新 peer admission 是三个不同状态，不能互相替代。对仍由外部进程持有 active
reader/writer 的 killed-reader 场景，公共 API 不提供远程回收；无法验证安全所有权时
保持 fail-closed。
