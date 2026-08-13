#ifndef EDGE_RUNTIME_DETAIL_PROCESS_SPAWN_HPP
#define EDGE_RUNTIME_DETAIL_PROCESS_SPAWN_HPP

// Library-side fork+exec for the v0.3 ProducerSupervisor (design §35.3).
// Unlike the test fixture spawners, this one is safe for a multi-threaded
// parent: the char*[] argv is built BEFORE fork (only async-signal-safe calls
// run between fork and exec), and both pipe ends are O_NONBLOCK so a verbose
// child can never wedge the supervisor's drain loop.

#include <string>
#include <vector>

#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/result.hpp"

namespace edge_runtime::detail {

struct SpawnedProcess {
	pid_t pid = -1;
	UniqueFd stdout_read;  // O_NONBLOCK read end; EOF when the child exits
};

// fork + exec(argv[0], argv...). The child's stdout goes to stdout_read; its
// stderr is inherited (surfaces in the supervisor's own log stream). The
// caller reaps the child with waitpid; nothing here detaches or double-forks.
Result<SpawnedProcess> spawn_process(const std::vector<std::string>& argv) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_PROCESS_SPAWN_HPP
