#include "edge_runtime/detail/clock.hpp"

#include <time.h>

namespace edge_runtime::detail {

namespace {
uint64_t clock_ns_or_zero(clockid_t id) noexcept {
	struct timespec ts {};
	if (::clock_gettime(id, &ts) != 0) return 0;
	const uint64_t sec = static_cast<uint64_t>(ts.tv_sec);
	const uint64_t nsec = static_cast<uint64_t>(ts.tv_nsec);
	if (sec > UINT64_MAX / 1000000000ull) return 0;  // overflow guard
	return sec * 1000000000ull + nsec;
}
}  // namespace

uint64_t boottime_now_ns() noexcept { return clock_ns_or_zero(CLOCK_BOOTTIME); }

uint64_t monotonic_now_ns() noexcept { return clock_ns_or_zero(CLOCK_MONOTONIC); }

uint64_t monotonic_deadline_ns(uint64_t timeout_ns) noexcept {
	const uint64_t now = monotonic_now_ns();
	if (now > UINT64_MAX - timeout_ns) return UINT64_MAX;  // saturate
	return now + timeout_ns;
}

uint64_t remaining_time_ns(uint64_t deadline_ns) noexcept {
	const uint64_t now = monotonic_now_ns();
	return now >= deadline_ns ? 0 : deadline_ns - now;
}

}  // namespace edge_runtime::detail
