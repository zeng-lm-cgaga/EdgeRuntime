#ifndef EDGE_RUNTIME_DETAIL_PROCESS_SPAWN_HPP
#define EDGE_RUNTIME_DETAIL_PROCESS_SPAWN_HPP

// Library-side posix_spawn for the v0.3 ProducerSupervisor (design §35.3).
// The libc implementation owns the fork/vfork-to-exec window, so no application
// code or allocator runs in a post-fork child of a multi-threaded process. Both
// pipe ends are O_NONBLOCK so verbose output cannot wedge supervision.

#include <string>
#include <vector>

#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/result.hpp"

namespace edge_runtime::detail {

struct SpawnedProcess {
	pid_t pid = -1;
	UniqueFd stdout_read;  // O_NONBLOCK read end; EOF when the child exits
};

// Spawn argv[0] directly (no PATH search). The child's stdout goes to
// stdout_read; stderr is inherited. The caller reaps the child with waitpid;
// nothing here detaches or double-forks.
Result<SpawnedProcess> spawn_process(const std::vector<std::string>& argv) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_PROCESS_SPAWN_HPP
