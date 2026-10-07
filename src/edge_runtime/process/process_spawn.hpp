#ifndef EDGE_RUNTIME_DETAIL_PROCESS_SPAWN_HPP
#define EDGE_RUNTIME_DETAIL_PROCESS_SPAWN_HPP

#include <string>
#include <vector>

#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/common/result.hpp"

namespace edge_runtime::detail {

// 子进程通过 posix_spawn 启动，stdout 使用非阻塞管道，避免输出阻塞监督循环。
struct SpawnedProcess {
	pid_t pid = -1;
	UniqueFd stdout_read;
};

Result<SpawnedProcess> spawn_process(const std::vector<std::string>& argv) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_PROCESS_SPAWN_HPP
