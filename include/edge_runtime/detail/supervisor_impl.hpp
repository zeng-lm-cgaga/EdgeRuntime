#ifndef EDGE_RUNTIME_DETAIL_SUPERVISOR_IMPL_HPP
#define EDGE_RUNTIME_DETAIL_SUPERVISOR_IMPL_HPP

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "edge_runtime/detail/channel_observer.hpp"
#include "edge_runtime/detail/process_identity.hpp"
#include "edge_runtime/detail/process_spawn.hpp"
#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/result.hpp"
#include "edge_runtime/supervisor.hpp"

namespace edge_runtime::detail {

// All run-loop state of a ProducerSupervisor (design §35). Owned by the
// public handle; everything here is CLOEXEC and joined/reaped on destruction.
struct SupervisorHandle {
	SupervisorOptions options;

	// epoll loop fds
	UniqueFd epoll_fd;
	UniqueFd stop_evfd;
	UniqueFd signalfd_fd;

	// child state
	SpawnedProcess child;                 // pid + stdout read end (reset per spawn)
	UniqueFd child_pidfd;                // raw pidfd, also registered in epoll
	uint64_t child_start_ticks{0};
	bool child_reaped{false};
	bool child_kill_in_progress{false};  // takeover/stop sequence armed

	// supervision state
	uint32_t spawn_attempts{0};
	uint32_t consecutive_failures{0};
	uint64_t last_spawn_mono_ns{0};      // for stable_reset_window
	uint64_t baseline_generation{0};     // last CONFIRMED READY instance
	ChannelObserverView view;            // rebuilt per instance (never reused)
	bool have_view{false};
	bool gave_up{false};

	// decision flags (event-time decision, design §35.4: never re-classified
	// at reap time)
	enum class ReapDecision : uint8_t { kUndecided, kCleanExit, kRestart };
	ReapDecision reap_decision{ReapDecision::kUndecided};
	int last_exit_status_for_reap{0};

	std::atomic<bool> stop_requested{false};
	std::atomic<bool> running{false};

	std::string stdout_tail;  // bounded tail of the current child's output
};

Result<std::shared_ptr<SupervisorHandle>> supervisor_create_impl(
        const SupervisorOptions& options);

Result<SupervisionResult> supervisor_run(const std::shared_ptr<SupervisorHandle>& h) noexcept;
void supervisor_request_stop(const std::shared_ptr<SupervisorHandle>& h) noexcept;
void supervisor_handle_shutdown(const std::shared_ptr<SupervisorHandle>& h) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_SUPERVISOR_IMPL_HPP
