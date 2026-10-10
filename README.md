# EdgeRuntime

面向同机 Linux 进程的 **C++17 共享内存 IPC 库**，用于连续状态、离散任务和大块数据传递。

## 核心能力

| 模块 | 公共接口 | 用途 |
|---|---|---|
| SPSC 状态通道 | `Producer<T>` / `Consumer<T>` | 三槽位 latest-value，读取最新控制目标、设备状态或周期快照 |
| MPMC 有界队列 | `MpmcQueue<T>` | 固定容量 FIFO，多进程任务分发，支持即时操作和 deadline 等待 |
| 固定块缓冲池 | `SharedBufferPool` | 4/16KiB 共享内存缓冲区，通过 `BufferHandle` 跨进程传递 |

SPSC 支持 POSIX shm、memfd + SCM_RIGHTS 和共享槽位的原地读写。
缓冲池可与 `MpmcQueue<BufferHandle>` 组合：大块数据留在共享内存中，队列传递 88B 句柄。

- **同步与所有权**：进程共享 futex、MPMC 生产/消费分侧 gate、RAII 管理共享缓冲区所有权；校验 schema、实例代次和进程身份。
- **故障处理**：SPSC 支持经身份校验的崩溃恢复；MPMC/Pool 在所有权存在歧义时返回 `RecoveryBlocked`。
- **监督与诊断**：`ProducerSupervisor` 基于 pidfd 监督自身启动的 SPSC producer 子进程；`edge_shm_ctl` 查看通道状态并管理实例。

队列负责至多一次成功出队，业务执行结果与 ACK 由应用层管理；共享资源由 creator 和使用方按所有权释放。

## 快速开始

环境：Ubuntu 22.04 / Linux ≥ 5.15、GCC 11、CMake 3.22。构建产物位于 `build/<preset>/`。

```bash
cmake --preset dev-debug && cmake --build --preset dev-debug
ctest --preset dev-debug

# Release 构建与安装
cmake --preset release && cmake --build --preset release
cmake --install build/release --prefix /your/prefix

# 完整流水线：Debug、ASan/UBSan、Release 安装及独立消费示例
scripts/ci.sh
```

下游通过安装包链接：

```cmake
find_package(EdgeRuntime CONFIG REQUIRED)
target_link_libraries(app PRIVATE EdgeRuntime::edge_runtime)
```

使用示例见 [examples/consume_demo/](examples/consume_demo/)，覆盖 channel、queue+wait 和缓冲池句柄传递。

## 测试与性能

Debug、ASan/UBSan 各完成 121 项启用测试；完整 C01–C23 崩溃矩阵 23/23。
关键正常退出路径另经 Valgrind 3.27.1 Memcheck 检查。完整崩溃矩阵由 `edge_crash_matrix` 显式执行。

2026-10-08 的 Release 补测使用自研 benchmark 和真实 `fork/exec`，在 **8 核 Linux 虚拟机（`VM_ONLY`）**上执行：

- **并发与背压**：MPMC 覆盖 1P1C、1P4C、4P1C、4P4C、8P8C、16P16C，64B/4KiB 消息及 busy/yield、blocking 两种策略；缓冲池覆盖 4/16KiB、4P4C/16P16C，并测试小队列、小缓冲池和慢消费者。
- **交付检查**：51 轮 MPMC、18 轮缓冲池共交付 **802.4 万条**，独立逐条核对 producer/sequence，发送数与接收数一致、重复和缺失均为 0。
- **64B MPMC 吞吐**：capacity64、busy/yield、每生产者 20,000 条、每配置 3 轮；1P1C 约 **131–153 万条/秒**，4P4C 约 **85.1–87.7 万条/秒**，16P16C 约 **83.4–85.2 万条/秒**。
- **持续负载**：16P16C 单轮交付 **3200 万条**，工具计时 **50.965 秒**、吞吐约 **62.8 万条/秒**，累计子进程 CPU 时间约 393.7 秒；重复、缺失和错误均为 0。

吞吐按工具父进程从启动子进程到回收并读取报告的计时计算；MPMC 延迟从消息首次提交尝试计至消费完成，包含 Full/Contention 重试等待。
2026-10-09 修正压测工具后，64B、4P4C、capacity64、busy/yield、每生产者 20,000 条的三轮 p99 为 **39.8–44.2µs**（`VM_ONLY`）。

2026-10-07 的 SPSC / Unix Socket 对照使用 64B、同核、100Hz，预热 100 条后统计 900 条，双方样本 gap、torn read 和 queue overlap 均为 0：

| 传输方式 | p50（µs） | p99（µs） |
|---|---:|---:|
| SPSC SHM + futex | 15.925 | 37.598 |
| Unix Domain Socket | 18.915 | 41.788 |

压测工具：`edge_shm_bench`、`edge_mpmc_bench`、`edge_buffer_pool_bench`。
快速矩阵与等待策略对照见 [scripts/](scripts/)；各工具支持配置参数和原始 CSV 输出，便于在目标硬件上复测。

## 目录结构

- `include/edge_runtime/{common,channel,queue,buffer,process}/`：按功能划分的公共接口。
- `src/edge_runtime/`：对应功能的实现与私有头，统一由 `src/CMakeLists.txt` 构建。
- `tools/{include,src}/`：诊断、benchmark 和崩溃矩阵工具。
- `test/`：单元测试与跨进程集成测试。
- `examples/`：独立安装包消费示例。
- `scripts/`、`.github/workflows/`：自动化构建与验证。
