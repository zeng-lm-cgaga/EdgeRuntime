#ifndef EDGE_RUNTIME_SUPERVISOR_HPP
#define EDGE_RUNTIME_SUPERVISOR_HPP

// v0.3 ProducerSupervisor (design §35): a long-lived pidfd watcher that
// supervises ONE producer child process per channel — it posix_spawn()s the
// producer, detects crashes and heartbeat stalls, and restarts with a bounded
// backoff. run() blocks the CALLING thread in an epoll loop; request_stop()
// wakes it from any other thread (the library never spawns threads for this,
// §18.1). The supervisor only ever sends signals to its own child; restarts
// go through the child's own Producer<T>::create, so the recovery engine's
// invariants (dead-owner verification, generation+1) are never bypassed.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/result.hpp"

namespace edge_runtime {
namespace detail {
struct SupervisorHandle;
}

// Observation callback for run() (design §35.2 marker stream). Called from the
// run() thread only, never from a library thread; must not call back into the
// supervisor (no reentrancy) and must return quickly.
enum class SupervisorEvent : uint32_t {
	kSupervised = 0,   // child confirmed READY (pid + generation valid)
	kStallDetected,    // heartbeat stale -> takeover sequence armed
	kKilled,           // SIGKILL escalation inside a kill sequence
	kRestartArmed,     // failure counted, backoff delay computed
};

struct SupervisorEventInfo {
	SupervisorEvent event{SupervisorEvent::kSupervised};
	uint64_t pid{0};
	uint64_t generation{0};
	uint32_t attempt{0};    // consecutive failure count (kRestartArmed)
	uint64_t delay_ns{0};   // backoff delay (kRestartArmed)
	uint32_t signal{0};     // delivered signal (kKilled)
};

using SupervisorEventCallback = void (*)(const SupervisorEventInfo&, void* user_data);

struct SupervisorOptions {
	std::string channel_name;
	Transport transport{Transport::kPosixShm};
	std::vector<std::string> producer_argv;  // argv[0] = executable path

	std::chrono::milliseconds initial_delay{100};          // first restart delay
	std::chrono::milliseconds max_delay{10000};            // backoff cap
	uint32_t multiplier{2};                                // integer factor, >= 1
	uint32_t max_restarts{10};                             // retries after initial spawn
	std::chrono::milliseconds stable_reset_window{60000};  // uptime that resets the counter
	std::chrono::milliseconds stall_grace{5000};           // SIGTERM -> SIGKILL window
	std::chrono::milliseconds watch_interval{500};         // stall-check cadence
	std::chrono::milliseconds create_timeout{10000};       // READY+identity deadline

	SupervisorEventCallback on_event{nullptr};
	void* event_user_data{nullptr};
};

enum class SupervisionOutcome : uint32_t {
	kCleanExit = 0,         // child exited on its own (exit 0 / SIGTERM / SIGINT)
	kStopped = 1,           // request_stop()/signal stopped the supervision
	kRestartsExhausted = 2, // crash loop hit max_restarts
};

struct SupervisionResult {
	SupervisionOutcome outcome{SupervisionOutcome::kStopped};
	uint32_t spawn_attempts{0};
	uint32_t restarts{0};
	uint64_t last_generation{0};
	int last_child_pid{-1};
	int last_child_exit_status{0};
	std::string stdout_tail;  // bounded tail of the LAST child's stdout
};

// Move-only handle (same discipline as Producer<T>/Consumer<T>). One
// supervisor per channel; two supervisors on one channel is a configuration
// error this class does not defend against (§35.2).
class ProducerSupervisor {
       public:
	static Result<ProducerSupervisor> create(const SupervisorOptions& options);

	// Blocks the calling thread until the supervision ends (clean exit, stop,
	// or restart exhaustion). Not re-entrant: a second run() on the same
	// handle returns kConcurrentHandleUse.
	Result<SupervisionResult> run();

	// Thread-safe stop request: wakes the epoll loop via eventfd. Safe to call
	// from a signal handler? No — call it from a regular thread. run() receives
	// SIGTERM/SIGINT through signalfd in its calling thread.
	void request_stop() noexcept;

	ProducerSupervisor(const ProducerSupervisor&) = delete;
	ProducerSupervisor& operator=(const ProducerSupervisor&) = delete;
	ProducerSupervisor(ProducerSupervisor&&) noexcept = default;
	ProducerSupervisor& operator=(ProducerSupervisor&&) noexcept = default;

	// Best-effort: if a child is still running, SIGKILL + reap it (never leave
	// an unreaped child behind, §35.4).
	~ProducerSupervisor();

       private:
	explicit ProducerSupervisor(std::shared_ptr<detail::SupervisorHandle> h);

	std::shared_ptr<detail::SupervisorHandle> handle_;
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_SUPERVISOR_HPP
