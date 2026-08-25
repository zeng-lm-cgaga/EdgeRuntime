#include "edge_runtime/detail/futex.hpp"

#include <sys/syscall.h>
#include <unistd.h>

namespace edge_runtime::detail {

namespace {
inline constexpr int kFutexWait = 0;
inline constexpr int kFutexWake = 1;
}

// futex 使用相对等待时间；等待循环由上层根据绝对 MONOTONIC 截止时间反复计算。
int futex_wait(const void* addr, uint32_t expected, const struct timespec* rel_timeout) noexcept {
	return static_cast<int>(
	        ::syscall(SYS_futex, addr, kFutexWait, expected, rel_timeout, nullptr, 0));
}

int futex_wake(const void* addr, int count) noexcept {
	return static_cast<int>(::syscall(SYS_futex, addr, kFutexWake, count, nullptr, nullptr, 0));
}

}
