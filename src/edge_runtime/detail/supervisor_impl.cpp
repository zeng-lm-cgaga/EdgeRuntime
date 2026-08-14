#include "edge_runtime/detail/supervisor_impl.hpp"

#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "edge_runtime/detail/channel_abi.hpp"
#include "edge_runtime/detail/clock.hpp"
#include "edge_runtime/detail/shared_atomic.hpp"

namespace edge_runtime::detail {

namespace {

inline constexpr int kEpollMaxEvents = 8;
inline constexpr uint64_t kStdoutTailLimit = 4096;
inline constexpr uint64_t kAwaitReadySliceNs = 100'000'000;  // 100 ms open retry slice

bool valid_milliseconds(int64_t ms, bool allow_zero) noexcept {
	if (ms < 0 || (!allow_zero && ms == 0)) return false;
	return static_cast<uint64_t>(ms) <= UINT64_MAX / 1'000'000ull;
}

uint64_t ms_to_ns(int64_t ms) noexcept {
	return ms > 0 ? static_cast<uint64_t>(ms) * 1'000'000ull : 0;
}

// epoll_wait timeout in int milliseconds, clamped (design §35.3 deadline
// slicing — all waits go through epoll so request_stop stays responsive).
int ns_to_epoll_ms(uint64_t ns) {
	if (ns == 0) return 0;
	uint64_t ms = ns / 1'000'000ull;
	if (ms == 0) ms = 1;
	if (ms > static_cast<uint64_t>(INT32_MAX)) ms = INT32_MAX;
	return static_cast<int>(ms);
}

Result<void> epoll_add(SupervisorHandle& h, int fd, uint32_t events) noexcept {
	struct epoll_event ev {};
	ev.events = events;
	ev.data.fd = fd;
	if (::epoll_ctl(h.epoll_fd.get(), EPOLL_CTL_ADD, fd, &ev) != 0) {
		const int e = errno;
		return make_errno_error(e, "ProducerSupervisor::epoll_add", std::strerror(e));
	}
	return Result<void>::ok();
}

void append_stdout_tail(std::string* tail, const char* data, size_t size) {
	if (size >= kStdoutTailLimit) {
		tail->assign(data + size - kStdoutTailLimit, kStdoutTailLimit);
		return;
	}
	if (tail->size() + size > kStdoutTailLimit) {
		tail->erase(0, tail->size() + size - kStdoutTailLimit);
	}
	tail->append(data, size);
}

// Drain the child's non-blocking stdout pipe into the bounded tail buffer
// (design §35.3: a full pipe would freeze the child -> false stall -> kill
// loop, so the read end is ALWAYS drained here).
void drain_child_stdout(SupervisorHandle& h) {
	if (h.child.stdout_read.get() < 0) return;
	char buf[4096];
	for (;;) {
		const ssize_t n = ::read(h.child.stdout_read.get(), buf, sizeof(buf));
		if (n > 0) {
			append_stdout_tail(&h.stdout_tail, buf, static_cast<size_t>(n));
			continue;
		}
		if (n == 0) {
			h.child.stdout_read.reset();  // EOF; the pidfd decides death
			return;
		}
		if (errno == EAGAIN || errno == EWOULDBLOCK) return;
		if (errno == EINTR) continue;
		h.child.stdout_read.reset();
		return;
	}
}

// Reap the child once; idempotent. Returns true when the child is known dead
// (either just reaped now or already reaped earlier).
Result<bool> reap_child(SupervisorHandle& h, int* status_out) noexcept {
	if (h.child.pid <= 0) return Result<bool>(true);
	if (h.child_reaped) {
		*status_out = h.last_exit_status_for_reap;
		return Result<bool>(true);
	}
	int status = 0;
	pid_t rc = -1;
	do {
		rc = ::waitpid(h.child.pid, &status, WNOHANG);
	} while (rc < 0 && errno == EINTR);
	if (rc == h.child.pid) {
		h.child_reaped = true;
		h.last_exit_status_for_reap = status;
		h.child_pidfd.reset();  // closes the pidfd; epoll auto-drops it
		*status_out = status;
		return Result<bool>(true);
	}
	if (rc < 0) {
		const int e = errno;
		return make_errno_error(e, "ProducerSupervisor::waitpid", std::strerror(e));
	}
	*status_out = 0;
	return Result<bool>(false);
}

Result<int> reap_child_blocking(pid_t pid) noexcept {
	int status = 0;
	pid_t rc = -1;
	do {
		rc = ::waitpid(pid, &status, 0);
	} while (rc < 0 && errno == EINTR);
	if (rc < 0) {
		const int e = errno;
		return make_errno_error(e, "ProducerSupervisor::waitpid", std::strerror(e));
	}
	return Result<int>(status);
}

Result<void> arm_child_kill(SupervisorHandle& h) noexcept {
	if (h.child.pid <= 0 || h.child_reaped || h.child_kill_in_progress) {
		return Result<void>::ok();
	}
	if (::kill(h.child.pid, SIGTERM) != 0 && errno != ESRCH) {
		const int e = errno;
		return make_errno_error(e, "ProducerSupervisor::kill", std::strerror(e));
	}
	h.child_kill_in_progress = true;
	return Result<void>::ok();
}

bool is_clean_exit_status(int status) {
	if (WIFEXITED(status) && WEXITSTATUS(status) == 0) return true;
	if (WIFSIGNALED(status)) {
		const int sig = WTERMSIG(status);
		return sig == SIGTERM || sig == SIGINT;
	}
	return false;
}

// Backoff delay for the next restart (design §35.4), integer-only math.
uint64_t backoff_delay_ns(const SupervisorHandle& h) {
	uint64_t delay = ms_to_ns(h.options.initial_delay.count());
	uint64_t capped = ms_to_ns(h.options.max_delay.count());
	if (capped == 0) capped = delay;
	// attempt 1 uses initial_delay; each subsequent armed restart multiplies.
	for (uint32_t i = 1; i < h.consecutive_failures && delay < capped; ++i) {
		if (delay > capped / h.options.multiplier) {
			delay = capped;
			break;
		}
		delay *= h.options.multiplier;
	}
	if (delay > capped) delay = capped;
	return delay;
}

// Arm one restart after a failure. max_restarts counts actual retries after
// the initial spawn, so exhaustion is checked before increment/event emission.
bool count_failure(SupervisorHandle& h) {
	const uint64_t now = monotonic_now_ns();
	if (now != 0 && h.last_spawn_mono_ns != 0 && now >= h.last_spawn_mono_ns &&
	    now - h.last_spawn_mono_ns >= ms_to_ns(h.options.stable_reset_window.count())) {
		h.consecutive_failures = 0;  // long-stable child: reset the window
	}
	if (h.consecutive_failures >= h.options.max_restarts) return true;
	++h.consecutive_failures;
	if (h.options.on_event != nullptr) {
		SupervisorEventInfo info;
		info.event = SupervisorEvent::kRestartArmed;
		info.attempt = h.consecutive_failures;
		info.delay_ns = backoff_delay_ns(h);
		h.options.on_event(info, h.options.event_user_data);
	}
	return false;
}

void emit_event(const SupervisorHandle& h, SupervisorEvent event, uint64_t pid,
                uint64_t generation, uint32_t signal) {
	if (h.options.on_event == nullptr) return;
	SupervisorEventInfo info;
	info.event = event;
	info.pid = pid;
	info.generation = generation;
	info.signal = signal;
	h.options.on_event(info, h.options.event_user_data);
}

}  // namespace

Result<std::shared_ptr<SupervisorHandle>> supervisor_create_impl(
        const SupervisorOptions& options) {
	// §35.5 option validation: fail on any impossible configuration.
	if (options.channel_name.empty() ||
	    options.channel_name.size() > kMaxChannelNameLen ||
	    !validate_channel_name(options.channel_name.c_str(), options.channel_name.size())) {
		return make_error(ErrorCode::kInvalidName, "ProducerSupervisor::create",
		                  options.channel_name.c_str());
	}
	if (options.producer_argv.empty() || options.producer_argv[0].empty()) {
		return make_error(ErrorCode::kInvalidOptions, "ProducerSupervisor::create",
		                  "empty producer argv");
	}
	if (options.transport != Transport::kPosixShm &&
	    options.transport != Transport::kMemfdFdPass) {
		return make_error(ErrorCode::kInvalidOptions, "ProducerSupervisor::create",
		                  "unknown transport");
	}
	if (!valid_milliseconds(options.watch_interval.count(), false) ||
	    !valid_milliseconds(options.create_timeout.count(), false)) {
		return make_error(ErrorCode::kInvalidOptions, "ProducerSupervisor::create",
		                  "invalid watch/create timeouts");
	}
	if (!valid_milliseconds(options.initial_delay.count(), true) ||
	    !valid_milliseconds(options.max_delay.count(), true) ||
	    !valid_milliseconds(options.stable_reset_window.count(), false) ||
	    !valid_milliseconds(options.stall_grace.count(), true) ||
	    options.max_delay.count() < options.initial_delay.count()) {
		return make_error(ErrorCode::kInvalidOptions, "ProducerSupervisor::create",
		                  "invalid delay/grace option");
	}
	if (options.multiplier < 1) {
		return make_error(ErrorCode::kInvalidOptions, "ProducerSupervisor::create",
		                  "multiplier < 1");
	}
	auto h = std::make_shared<SupervisorHandle>();
	h->options = options;
	return Result<std::shared_ptr<SupervisorHandle>>(std::move(h));
}

Result<SupervisionResult> supervisor_run(const std::shared_ptr<SupervisorHandle>& h) noexcept {
	if (h->running.exchange(true, std::memory_order_acq_rel)) {
		return make_error(ErrorCode::kConcurrentHandleUse, "ProducerSupervisor::run",
		                  "already running");
	}

	// ---- epoll / eventfd / signalfd setup --------------------------------------
	const int epfd = ::epoll_create1(EPOLL_CLOEXEC);
	if (epfd < 0) {
		const int e = errno;
		h->running.store(false, std::memory_order_release);
		return make_errno_error(e, "ProducerSupervisor::run", std::strerror(e));
	}
	h->epoll_fd = UniqueFd(epfd);

	const int evfd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (evfd < 0) {
		const int e = errno;
		h->epoll_fd.reset();
		h->running.store(false, std::memory_order_release);
		return make_errno_error(e, "ProducerSupervisor::run", std::strerror(e));
	}
	{
		std::lock_guard<std::mutex> lock(h->stop_fd_mutex);
		h->stop_evfd = UniqueFd(evfd);
	}
	auto add_stop = epoll_add(*h, evfd, EPOLLIN);
	if (!add_stop) {
		std::lock_guard<std::mutex> lock(h->stop_fd_mutex);
		h->stop_evfd.reset();
		h->epoll_fd.reset();
		h->running.store(false, std::memory_order_release);
		return add_stop.error();
	}

	// Block SIGTERM/SIGINT in THIS thread and receive them via signalfd
	// (design §35.3). The previous mask is restored when run() returns.
	sigset_t mask, old_mask;
	sigemptyset(&mask);
	sigaddset(&mask, SIGTERM);
	sigaddset(&mask, SIGINT);
	const int mask_rc = ::pthread_sigmask(SIG_BLOCK, &mask, &old_mask);
	if (mask_rc != 0) {
		{
			std::lock_guard<std::mutex> lock(h->stop_fd_mutex);
			h->stop_evfd.reset();
		}
		h->epoll_fd.reset();
		h->running.store(false, std::memory_order_release);
		return make_errno_error(mask_rc, "ProducerSupervisor::run", "pthread_sigmask");
	}
	const int sfd = ::signalfd(-1, &mask, SFD_CLOEXEC | SFD_NONBLOCK);
	if (sfd < 0) {
		const int e = errno;
		(void)::pthread_sigmask(SIG_SETMASK, &old_mask, nullptr);
		{
			std::lock_guard<std::mutex> lock(h->stop_fd_mutex);
			h->stop_evfd.reset();
		}
		h->epoll_fd.reset();
		h->running.store(false, std::memory_order_release);
		return make_errno_error(e, "ProducerSupervisor::run", std::strerror(e));
	}
	h->signalfd_fd = UniqueFd(sfd);
	auto add_signal = epoll_add(*h, sfd, EPOLLIN);
	if (!add_signal) {
		h->signalfd_fd.reset();
		(void)::pthread_sigmask(SIG_SETMASK, &old_mask, nullptr);
		{
			std::lock_guard<std::mutex> lock(h->stop_fd_mutex);
			h->stop_evfd.reset();
		}
		h->epoll_fd.reset();
		h->running.store(false, std::memory_order_release);
		return add_signal.error();
	}

	SupervisionResult result;
	result.outcome = SupervisionOutcome::kStopped;

	// Phase state machine (design §35.4). All blocking happens in epoll_wait
	// with the phase-appropriate slice; grace/backoff deadlines are checked at
	// the top of every iteration.
	enum class Phase : uint8_t { kBackoff, kSpawn, kAwaitReady, kMonitor, kKill, kDone };
	Phase phase = Phase::kSpawn;
	bool kill_then_restart = false;
	uint64_t backoff_deadline = 0;
	uint64_t create_deadline = 0;
	uint64_t grace_deadline = 0;
	bool loop_failed = false;
	Error loop_error;
	const auto set_deadline = [&](uint64_t timeout_ns, uint64_t* out) {
		*out = monotonic_deadline_ns(timeout_ns);
		if (*out != 0) return true;
		loop_error = make_error(ErrorCode::kClockAnomaly, "ProducerSupervisor::run",
		                        "monotonic clock unavailable");
		loop_failed = true;
		return false;
	};
	const auto enter_backoff = [&] {
		if (!set_deadline(backoff_delay_ns(*h), &backoff_deadline)) {
			phase = Phase::kDone;
			return;
		}
		phase = Phase::kBackoff;
	};

	while (phase != Phase::kDone) {
		if (h->stop_requested.load(std::memory_order_acquire)) {
			kill_then_restart = false;
			phase = Phase::kKill;
		}

		switch (phase) {
			case Phase::kBackoff: {
				if (monotonic_now_ns() >= backoff_deadline) {
					phase = Phase::kSpawn;
				}
				break;
			}
			case Phase::kSpawn: {
				// Rebuild the observer view per instance (design §35.3): an old
				// mapping/broker-fd can never see the replacement instance.
				h->view = ChannelObserverView{};
				h->have_view = false;
				h->stdout_tail.clear();
				++h->spawn_attempts;
				auto sp = spawn_process(h->options.producer_argv);
				if (!sp) {
					// A posix_spawn failure is still a consumed spawn attempt.
					if (count_failure(*h)) {
						result.outcome = SupervisionOutcome::kRestartsExhausted;
						phase = Phase::kDone;
						break;
					}
					enter_backoff();
					break;
				}
				h->child = std::move(sp.value());
				h->child_reaped = false;
				h->child_kill_in_progress = false;
				h->reap_decision = SupervisorHandle::ReapDecision::kUndecided;
				h->last_spawn_mono_ns = monotonic_now_ns();
				if (h->last_spawn_mono_ns == 0) {
					loop_error = make_error(ErrorCode::kClockAnomaly,
					                        "ProducerSupervisor::run",
					                        "monotonic clock unavailable");
					loop_failed = true;
					phase = Phase::kDone;
					break;
				}

				auto start = proc_stat_starttime(h->child.pid);
				h->child_start_ticks = start ? start.value() : 0;

				auto watch = LivenessWatch::open(static_cast<uint64_t>(h->child.pid));
				if (!watch) {
					if (::kill(h->child.pid, SIGKILL) != 0 && errno != ESRCH) {
						const int e = errno;
						loop_error = make_errno_error(e, "ProducerSupervisor::kill",
						                              std::strerror(e));
						loop_failed = true;
						phase = Phase::kDone;
						break;
					}
					auto reaped = reap_child_blocking(h->child.pid);
					if (!reaped) {
						loop_error = reaped.error();
						loop_failed = true;
						phase = Phase::kDone;
						break;
					}
					h->child_reaped = true;
					h->last_exit_status_for_reap = reaped.value();
					if (count_failure(*h)) {
						result.outcome = SupervisionOutcome::kRestartsExhausted;
						phase = Phase::kDone;
						break;
					}
					enter_backoff();
					break;
				}
				h->child_pidfd = UniqueFd(watch.value().release());
				auto add_pidfd = epoll_add(*h, h->child_pidfd.get(), EPOLLIN);
				if (!add_pidfd) {
					loop_error = add_pidfd.error();
					loop_failed = true;
					phase = Phase::kDone;
					break;
				}
				if (h->child.stdout_read.get() >= 0) {
					auto add_stdout = epoll_add(*h, h->child.stdout_read.get(), EPOLLIN);
					if (!add_stdout) {
						loop_error = add_stdout.error();
						loop_failed = true;
						phase = Phase::kDone;
						break;
					}
				}
				if (!set_deadline(ms_to_ns(h->options.create_timeout.count()),
				                  &create_deadline)) {
					phase = Phase::kDone;
					break;
				}
				phase = Phase::kAwaitReady;
				break;
			}
			case Phase::kAwaitReady: {
				// One-shot readonly open per slice (retry budget 0). Success must
				// be OUR child's confirmed READY instance at baseline_generation+1.
				auto view = open_channel_readonly(h->options.channel_name,
				                                  h->options.transport, 0);
				if (view) {
					auto* header = observer_header(view.value());
					if (shared_load_acquire(&header->init_state) ==
					    static_cast<uint32_t>(InitState::kReady)) {
						auto pid_abi = identity_snapshot_read(&header->producer);
						const bool ours =
						        pid_abi &&
						        pid_abi.value().pid ==
						                static_cast<uint64_t>(h->child.pid) &&
						        pid_abi.value().proc_start_ticks ==
						                h->child_start_ticks;
						if (ours) {
							const uint64_t gen = observer_generation(header);
							if (h->baseline_generation == 0 ||
							    (h->baseline_generation != UINT64_MAX &&
							     gen == h->baseline_generation + 1)) {
								h->baseline_generation = gen;
								h->view = std::move(view.value());
								h->have_view = true;
								emit_event(*h, SupervisorEvent::kSupervised,
								           static_cast<uint64_t>(h->child.pid), gen, 0);
								phase = Phase::kMonitor;
								break;
							}
						}
					}
					// Opened but not (yet) ours / not the expected generation:
					// keep waiting within create_timeout.
				}
				if (h->child_reaped) {
					// Died before ever confirming READY: a failure (event-time
					// decision, design §35.4 — never re-classified by exit code).
					if (count_failure(*h)) {
						result.outcome = SupervisionOutcome::kRestartsExhausted;
						phase = Phase::kDone;
						break;
					}
					enter_backoff();
					break;
				}
				if (monotonic_now_ns() >= create_deadline) {
					// Never became ready in time: kill, then count as failure.
					// (The grace deadline is armed inside the kill phase.)
					kill_then_restart = true;
					phase = Phase::kKill;
				}
				break;
			}
			case Phase::kMonitor: {
				if (h->child_reaped) {
					// The pidfd event armed the decision; act on it.
					if (h->reap_decision == SupervisorHandle::ReapDecision::kCleanExit) {
						result.outcome = SupervisionOutcome::kCleanExit;
						result.last_child_exit_status = h->last_exit_status_for_reap;
						phase = Phase::kDone;
						break;
					}
					if (count_failure(*h)) {
						result.outcome = SupervisionOutcome::kRestartsExhausted;
						phase = Phase::kDone;
						break;
					}
					enter_backoff();
					break;
				}
				// Stall check (only when we own a confirmed view, §35.3).
				if (h->have_view) {
					auto* header = observer_header(h->view);
					const StallClass sc =
					        classify_stall(header, boottime_now_ns(),
					                       static_cast<uint64_t>(h->child.pid),
					                       h->child_start_ticks);
					if (sc == StallClass::kStalled) {
						// Event-time decision: this death is a RESTART no matter
						// how the child exits after our SIGTERM (design §35.4).
						// (The grace deadline is armed inside the kill phase.)
						h->reap_decision = SupervisorHandle::ReapDecision::kRestart;
						kill_then_restart = true;
						emit_event(*h, SupervisorEvent::kStallDetected,
						           static_cast<uint64_t>(h->child.pid), 0, 0);
						phase = Phase::kKill;
					}
				}
				break;
			}
			case Phase::kKill: {
				// Kill sequence (stop / signal / create-timeout / stall):
				// SIGTERM -> grace -> SIGKILL -> reap -> decide. The grace
				// deadline is armed HERE, together with the first signal —
				// a stale deadline would SIGKILL instantly.
				if (h->child.pid > 0 && !h->child_reaped) {
					if (!h->child_kill_in_progress) {
						auto armed = arm_child_kill(*h);
						if (!armed) {
							loop_error = armed.error();
							loop_failed = true;
							phase = Phase::kDone;
							break;
						}
						if (!set_deadline(ms_to_ns(h->options.stall_grace.count()),
						                  &grace_deadline)) {
							phase = Phase::kDone;
							break;
						}
					}
					if (h->child_kill_in_progress && monotonic_now_ns() >= grace_deadline) {
						if (::kill(h->child.pid, SIGKILL) != 0 && errno != ESRCH) {
							const int e = errno;
							loop_error = make_errno_error(e, "ProducerSupervisor::kill",
							                              std::strerror(e));
							loop_failed = true;
							phase = Phase::kDone;
							break;
						}
						emit_event(*h, SupervisorEvent::kKilled,
						           static_cast<uint64_t>(h->child.pid), 0, SIGKILL);
						// Keep the kill sequence armed but move its deadline out of reach;
						// otherwise the next loop would start a second SIGTERM grace window.
						grace_deadline = UINT64_MAX;
					}
				}
				int status = 0;
				auto reaped = reap_child(*h, &status);
				if (!reaped) {
					loop_error = reaped.error();
					loop_failed = true;
					phase = Phase::kDone;
					break;
				}
				if (reaped.value()) {
					if (kill_then_restart) {
						kill_then_restart = false;
						if (count_failure(*h)) {
							result.outcome = SupervisionOutcome::kRestartsExhausted;
						} else {
							enter_backoff();
							break;
						}
					} else {
						result.outcome = SupervisionOutcome::kStopped;
					}
					phase = Phase::kDone;
				}
				break;
			}
			case Phase::kDone:
				break;
		}

		if (phase == Phase::kDone) break;

		// ---- epoll wait with the phase-appropriate slice -----------------------
		uint64_t slice = ms_to_ns(h->options.watch_interval.count());
		if (phase == Phase::kAwaitReady) {
			slice = kAwaitReadySliceNs;
		} else if (phase == Phase::kBackoff) {
			const uint64_t now = monotonic_now_ns();
			slice = backoff_deadline > now ? backoff_deadline - now : 0;
		} else if (phase == Phase::kKill && h->child_kill_in_progress) {
			const uint64_t now = monotonic_now_ns();
			const uint64_t grace_left = grace_deadline > now ? grace_deadline - now : 0;
			slice = grace_left < slice ? grace_left : slice;
		}
		const int timeout = ns_to_epoll_ms(slice);
		struct epoll_event events[kEpollMaxEvents];
		const int n = ::epoll_wait(h->epoll_fd.get(), events, kEpollMaxEvents, timeout);
		if (n < 0) {
			if (errno == EINTR) continue;
			const int e = errno;
			loop_error = make_errno_error(e, "ProducerSupervisor::epoll_wait",
			                              std::strerror(e));
			loop_failed = true;
			phase = Phase::kDone;
			break;
		}

		for (int i = 0; i < n; ++i) {
			const int fd = events[i].data.fd;
			if (fd == h->stop_evfd.get()) {
				uint64_t counter = 0;
				const ssize_t drain_rc = ::read(fd, &counter, sizeof(counter));
				(void)drain_rc;  // drain; EAGAIN means "already set"
				kill_then_restart = false;
				phase = Phase::kKill;
			} else if (fd == h->signalfd_fd.get()) {
				struct signalfd_siginfo si {};
				const ssize_t drain_rc = ::read(fd, &si, sizeof(si));
				(void)drain_rc;  // drain; SIGTERM/SIGINT both stop
				kill_then_restart = false;
				phase = Phase::kKill;
			} else if (h->child_pidfd.get() >= 0 && fd == h->child_pidfd.get()) {
				// Child exit observed: reap once, decide by the flag armed at
				// event time (never re-classify by exit status here, §35.4).
				int status = 0;
				auto reaped = reap_child(*h, &status);
				if (!reaped) {
					loop_error = reaped.error();
					loop_failed = true;
					phase = Phase::kDone;
					break;
				}
				if (!reaped.value()) continue;
				if (h->reap_decision == SupervisorHandle::ReapDecision::kUndecided) {
					h->reap_decision = is_clean_exit_status(status)
					                           ? SupervisorHandle::ReapDecision::kCleanExit
					                           : SupervisorHandle::ReapDecision::kRestart;
				}
				// In a kill sequence the decision was already armed; the phases
				// react to child_reaped on their next iteration.
			} else if (h->child.stdout_read.get() >= 0 &&
			           fd == h->child.stdout_read.get()) {
				drain_child_stdout(*h);
			}
		}
	}

	// ---- teardown: reap whatever remains, restore the signal mask --------------
	if (h->child.pid > 0 && !h->child_reaped) {
		bool may_reap = true;
		if (::kill(h->child.pid, SIGKILL) != 0 && errno != ESRCH) {
			const int e = errno;
			if (!loop_failed) {
				loop_error = make_errno_error(e, "ProducerSupervisor::kill", std::strerror(e));
				loop_failed = true;
			}
			may_reap = false;
		}
		if (may_reap) {
			auto reaped = reap_child_blocking(h->child.pid);
			if (reaped) {
				h->child_reaped = true;
				h->last_exit_status_for_reap = reaped.value();
			} else if (!loop_failed) {
				loop_error = reaped.error();
				loop_failed = true;
			}
		}
	}
	drain_child_stdout(*h);
	result.stdout_tail = h->stdout_tail;
	result.spawn_attempts = h->spawn_attempts;
	result.restarts = h->spawn_attempts > 0 ? h->spawn_attempts - 1 : 0;
	result.last_generation = h->baseline_generation;
	result.last_child_pid = h->child.pid;
	if (h->child_reaped) result.last_child_exit_status = h->last_exit_status_for_reap;

	h->epoll_fd.reset();
	{
		std::lock_guard<std::mutex> lock(h->stop_fd_mutex);
		h->stop_evfd.reset();
	}
	h->signalfd_fd.reset();
	const int restore_rc = ::pthread_sigmask(SIG_SETMASK, &old_mask, nullptr);
	h->running.store(false, std::memory_order_release);
	if (loop_failed) return loop_error;
	if (restore_rc != 0) {
		return make_errno_error(restore_rc, "ProducerSupervisor::run",
		                        "restore pthread signal mask");
	}
	return Result<SupervisionResult>(std::move(result));
}

void supervisor_request_stop(const std::shared_ptr<SupervisorHandle>& h) noexcept {
	h->stop_requested.store(true, std::memory_order_release);
	std::lock_guard<std::mutex> lock(h->stop_fd_mutex);
	if (h->stop_evfd.get() >= 0) {
		const uint64_t one = 1;
		const ssize_t wr = ::write(h->stop_evfd.get(), &one, sizeof(one));
		(void)wr;  // EAGAIN means the counter is already nonzero: stop armed
	}
}

void supervisor_handle_shutdown(const std::shared_ptr<SupervisorHandle>& h) noexcept {
	// Destructor path: never leave an unreaped child (design §35.4).
	h->stop_requested.store(true, std::memory_order_release);
	{
		std::lock_guard<std::mutex> lock(h->stop_fd_mutex);
		if (h->stop_evfd.get() >= 0) {
			const uint64_t one = 1;
			const ssize_t wr = ::write(h->stop_evfd.get(), &one, sizeof(one));
			(void)wr;
		}
	}
	if (h->child.pid > 0 && !h->child_reaped) {
		if (::kill(h->child.pid, SIGKILL) == 0 || errno == ESRCH) {
			auto reaped = reap_child_blocking(h->child.pid);
			if (reaped) {
				h->child_reaped = true;
				h->last_exit_status_for_reap = reaped.value();
			}
		}
	}
}

}  // namespace edge_runtime::detail

namespace edge_runtime {

Result<ProducerSupervisor> ProducerSupervisor::create(const SupervisorOptions& options) {
	auto h = detail::supervisor_create_impl(options);
	if (!h) return h.error();
	return ProducerSupervisor(std::move(h.value()));
}

Result<SupervisionResult> ProducerSupervisor::run() {
	return detail::supervisor_run(handle_);
}

void ProducerSupervisor::request_stop() noexcept {
	detail::supervisor_request_stop(handle_);
}

ProducerSupervisor::ProducerSupervisor(std::shared_ptr<detail::SupervisorHandle> h)
        : handle_(std::move(h)) {}

ProducerSupervisor::~ProducerSupervisor() {
	if (handle_) detail::supervisor_handle_shutdown(handle_);
}

}  // namespace edge_runtime
