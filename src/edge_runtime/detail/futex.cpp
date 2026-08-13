#include "edge_runtime/detail/futex.hpp"

#include <sys/syscall.h>
#include <unistd.h>

namespace edge_runtime::detail {

// FUTEX_WAIT / FUTEX_WAKE (no PRIVATE flag, no REALTIME_ABSOLUTE): the timeout
// is interpreted as a RELATIVE interval, which is what the wait loop recomputes
// from its absolute monotonic deadline on every retry (design §14.2, §15.7).
namespace {
inline constexpr int kFutexWait = 0;
inline constexpr int kFutexWake = 1;
}  // namespace

int futex_wait(const void* addr, uint32_t expected, const struct timespec* rel_timeout) noexcept {
	return static_cast<int>(
	        ::syscall(SYS_futex, addr, kFutexWait, expected, rel_timeout, nullptr, 0));
}

int futex_wake(const void* addr, int count) noexcept {
	return static_cast<int>(::syscall(SYS_futex, addr, kFutexWake, count, nullptr, nullptr, 0));
}

}  // namespace edge_runtime::detail
