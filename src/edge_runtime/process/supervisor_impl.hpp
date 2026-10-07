#ifndef EDGE_RUNTIME_DETAIL_SUPERVISOR_IMPL_HPP
#define EDGE_RUNTIME_DETAIL_SUPERVISOR_IMPL_HPP

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "edge_runtime/channel/channel_observer.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/process/process_spawn.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/process/supervisor.hpp"

namespace edge_runtime::detail {

// SupervisorHandle 保存 epoll、子进程和重启状态；析构前必须回收并等待所有子进程。
struct SupervisorHandle {
	SupervisorOptions options;

	UniqueFd epoll_fd;
	UniqueFd stop_evfd;
	UniqueFd signalfd_fd;
	std::mutex stop_fd_mutex;

	SpawnedProcess child;
	UniqueFd child_pidfd;
	uint64_t child_start_ticks{0};
	bool child_reaped{false};
	bool child_kill_in_progress{false};

	uint32_t spawn_attempts{0};
	uint32_t consecutive_failures{0};
	uint64_t last_spawn_mono_ns{0};
	uint64_t baseline_generation{0};
	ChannelObserverView view;
	bool have_view{false};
	bool gave_up{false};

	enum class ReapDecision : uint8_t { kUndecided, kCleanExit, kRestart };
	ReapDecision reap_decision{ReapDecision::kUndecided};
	int last_exit_status_for_reap{0};

	std::atomic<bool> stop_requested{false};
	std::atomic<bool> running{false};

	std::string stdout_tail;
};

Result<std::shared_ptr<SupervisorHandle>> supervisor_create_impl(
        const SupervisorOptions& options);

Result<SupervisionResult> supervisor_run(const std::shared_ptr<SupervisorHandle>& h) noexcept;
void supervisor_request_stop(const std::shared_ptr<SupervisorHandle>& h) noexcept;
void supervisor_handle_shutdown(const std::shared_ptr<SupervisorHandle>& h) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_SUPERVISOR_IMPL_HPP
