#include "edge_runtime/sync/futex.hpp"

#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace edge_runtime::detail {

namespace {
inline constexpr int kFutexWait = 0;
inline constexpr int kFutexWake = 1;

#if EDGERUNTIME_ENABLE_FAILPOINTS

std::atomic<uint32_t> trace_dropped{0};
std::atomic<uint64_t> trace_attempted{0};
std::atomic<uint64_t> trace_emitted{0};
std::atomic<uint64_t> trace_dropped_total{0};
std::atomic<bool> trace_counter_overflow{false};

bool fd_env_configured(const char* name) noexcept {
	const char* text = ::getenv(name);
	return text != nullptr && text[0] != '\0';
}

void increment_trace_counter(std::atomic<uint64_t>& counter) noexcept {
	uint64_t current = counter.load(std::memory_order_relaxed);
	while (current != UINT64_MAX) {
		if (counter.compare_exchange_weak(
				current, current + 1, std::memory_order_relaxed,
				std::memory_order_relaxed)) {
			return;
		}
	}
	trace_counter_overflow.store(true, std::memory_order_relaxed);
}

uint32_t trace_drop_with_current(uint32_t pending) noexcept {
	return pending == UINT32_MAX ? UINT32_MAX : pending + 1;
}

void note_current_trace_drop(uint32_t pending) noexcept {
	increment_trace_counter(trace_dropped_total);
	uint32_t current = trace_dropped.load(std::memory_order_relaxed);
	while (current != UINT32_MAX) {
		const uint32_t available = UINT32_MAX - current;
		const uint32_t restore = trace_drop_with_current(pending);
		const uint32_t desired = restore > available ? UINT32_MAX : current + restore;
		if (trace_dropped.compare_exchange_weak(
				current, desired, std::memory_order_relaxed, std::memory_order_relaxed)) {
			return;
		}
	}
}

int trace_fd() noexcept {
	const char* text = ::getenv("EDGE_RUNTIME_FUTEX_TRACE_FD");
	if (text == nullptr || text[0] == '\0') return -1;
	char* end = nullptr;
	const long value = std::strtol(text, &end, 10);
	if (end == text || *end != '\0' || value < 0 || value > INT_MAX) return -1;
	const int fd = static_cast<int>(value);
	const int flags = ::fcntl(fd, F_GETFL, 0);
	if (flags < 0) return -1;
	if ((flags & O_NONBLOCK) == 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
		return -1;
	}
	return fd;
}

int trace_status_fd() noexcept {
	const char* text = ::getenv("EDGE_RUNTIME_FUTEX_TRACE_STATUS_FD");
	if (text == nullptr || text[0] == '\0') return -1;
	char* end = nullptr;
	const long value = std::strtol(text, &end, 10);
	if (end == text || *end != '\0' || value < 0 || value > INT_MAX) return -1;
	const int fd = static_cast<int>(value);
	const int flags = ::fcntl(fd, F_GETFL, 0);
	if (flags < 0) return -1;
	if ((flags & O_NONBLOCK) == 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
		return -1;
	}
	return fd;
}

void trace_event(const char* operation, const void* address, uint32_t value, int rc,
			int saved_errno) noexcept {
	const int fd = trace_fd();
	if (fd < 0) return;
	increment_trace_counter(trace_attempted);
	char line[512];
	const uint32_t pending = trace_dropped.exchange(0, std::memory_order_relaxed);
	int offset = 0;
	if (pending != 0) {
		offset = std::snprintf(line, sizeof(line), "FUTEX_TRACE_DROPPED count=%u\n", pending);
		if (offset <= 0 || static_cast<size_t>(offset) >= sizeof(line)) {
			note_current_trace_drop(pending);
			return;
		}
	}
	const int event_length = std::snprintf(
			line + offset, sizeof(line) - static_cast<size_t>(offset),
			"FUTEX_%s rc=%d errno=%d pid=%ld addr=%p value=%u\n", operation, rc, saved_errno,
			static_cast<long>(::getpid()), address, value);
	if (event_length <= 0 || static_cast<size_t>(offset + event_length) >= sizeof(line)) {
		note_current_trace_drop(pending);
		return;
	}
	const size_t length = static_cast<size_t>(offset + event_length);
	const ssize_t written = ::write(fd, line, length);
	if (written != static_cast<ssize_t>(length)) {
		note_current_trace_drop(pending);
		return;
	}
	increment_trace_counter(trace_emitted);
}

#endif
}

// futex 使用相对等待时间；等待循环由上层根据绝对 MONOTONIC 截止时间反复计算。
int futex_wait(const void* addr, uint32_t expected, const struct timespec* rel_timeout) noexcept {
#if EDGERUNTIME_ENABLE_FAILPOINTS
	trace_event("WAIT_CALL", addr, expected, 0, 0);
#endif
	const int rc = static_cast<int>(
	        ::syscall(SYS_futex, addr, kFutexWait, expected, rel_timeout, nullptr, 0));
	const int saved_errno = errno;
#if EDGERUNTIME_ENABLE_FAILPOINTS
	trace_event("WAIT_RETURN", addr, expected, rc, rc < 0 ? saved_errno : 0);
#endif
	errno = saved_errno;
	return rc;
}

int futex_wake(const void* addr, int count) noexcept {
	const int rc = static_cast<int>(::syscall(SYS_futex, addr, kFutexWake, count, nullptr,
	                                           nullptr, 0));
	const int saved_errno = errno;
#if EDGERUNTIME_ENABLE_FAILPOINTS
	trace_event("WAKE_RETURN", addr, static_cast<uint32_t>(count), rc, rc < 0 ? saved_errno : 0);
#endif
	errno = saved_errno;
	return rc;
}

bool futex_trace_flush() noexcept {
#if EDGERUNTIME_ENABLE_FAILPOINTS
	const bool trace_requested = fd_env_configured("EDGE_RUNTIME_FUTEX_TRACE_FD");
	const bool status_requested = fd_env_configured("EDGE_RUNTIME_FUTEX_TRACE_STATUS_FD");
	if (!trace_requested && !status_requested) return true;
	if (trace_requested && trace_fd() < 0) return false;
	const int fd = trace_status_fd();
	if (fd < 0) return false;
	trace_event("TRACE_FLUSH", nullptr, 0, 0, 0);
	const uint64_t attempted = trace_attempted.load(std::memory_order_relaxed);
	const uint64_t emitted = trace_emitted.load(std::memory_order_relaxed);
	const uint64_t dropped = trace_dropped_total.load(std::memory_order_relaxed);
	const bool counters_valid = !trace_counter_overflow.load(std::memory_order_relaxed) &&
				attempted >= emitted && attempted - emitted == dropped;
	char line[256];
	const int length = std::snprintf(
			line, sizeof(line),
			"FUTEX_TRACE_STATUS pid=%ld attempted=%llu emitted=%llu dropped=%llu complete=%d overflow=%d\n",
			static_cast<long>(::getpid()),
			static_cast<unsigned long long>(attempted), static_cast<unsigned long long>(emitted),
			static_cast<unsigned long long>(dropped), counters_valid ? 1 : 0,
			trace_counter_overflow.load(std::memory_order_relaxed) ? 1 : 0);
	if (length <= 0 || static_cast<size_t>(length) >= sizeof(line)) return false;
	const bool written =
			::write(fd, line, static_cast<size_t>(length)) == static_cast<ssize_t>(length);
	return written && counters_valid;
#else
	return true;
#endif
}

}
