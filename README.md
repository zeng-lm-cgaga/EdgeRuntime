# EdgeRuntime

EdgeRuntime 是一个 Linux C++17 跨进程通信库，提供固定 schema、单生产者/单消费者（SPSC）的 **latest-value** 共享内存通道（`ShmChannel`）。

## 特性

- **固定跨进程 ABI**：共享内存只含显式宽度整数，无指针、无 STL 容器、无虚函数；32/64 位原子访问均为 lock-free。
- **latest-value 语义**：三槽位所有权协议，publish/read 无背压；慢消费者丢弃中间样本、从不阻塞生产者。
- **futex 通知**：进程共享等待/唤醒，只影响唤醒延迟，不参与数据正确性。
- **崩溃恢复**：对通道生命周期各崩溃点有界恢复（重建、槽位回收、重连），身份无法验证时安全关闭（fail closed）。
- **诊断与取证**：`edge_shm_ctl` 检查通道状态、执行受验证的删除，不导出 payload 内容。

## 构建与测试

环境要求：Ubuntu 22.04 / Linux ≥ 5.15、GCC 11（C++17）、CMake 3.22。

```bash
cmake --preset dev-debug && cmake --build --preset dev-debug
ctest --preset dev-debug

# ASan + UBSan
cmake --preset asan-ubsan && cmake --build --preset asan-ubsan && ctest --preset asan-ubsan
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

完整示例见 [examples/consume_demo/](examples/consume_demo/)（独立 CMake 项目，验证已安装 target 可被干净消费）。数据编解码由调用方以 `PayloadCodec<T>` 特化提供，库不引入动态 schema。

## 目录结构

- `include/edge_runtime/` — 公共 API：`Producer<T>` / `Consumer<T>` / `Result<T>` / `PayloadCodec<T>`
- `src/edge_runtime/` — 实现：ABI 布局、共享内存对象、槽位协议、futex、进程身份、恢复引擎
- `tools/` — 示例与工具：`edge_shm_producer` / `edge_shm_consumer` / `edge_shm_ctl`、`edge_crash_matrix`、`edge_shm_bench`
- `test/` — 单元测试（gtest）与跨进程集成测试
- `examples/` — 下游消费示例（独立 CMake 项目，消费已安装的 target）
- `scripts/` — 本地 CI gate 脚本；`.github/workflows/` 提供相同 gate 的 GitHub Actions 版
