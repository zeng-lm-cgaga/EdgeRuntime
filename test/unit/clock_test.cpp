// Clock helpers behind wait_latest (design §15.7): the absolute MONOTONIC
// deadline and the remaining-time derivation. A deadline of 0 is the clock-
// failure sentinel (monotonic_now_ns() == 0); remaining time is 0 once the
// deadline has passed, never a wraparound.

#include "edge_runtime/detail/clock.hpp"

#include <gtest/gtest.h>

#include <cstdint>

namespace {

using edge_runtime::detail::boottime_now_ns;
using edge_runtime::detail::monotonic_deadline_ns;
using edge_runtime::detail::monotonic_now_ns;
using edge_runtime::detail::remaining_time_ns;

TEST(Clock, MonotonicAndBoottimeAvailable) {
	EXPECT_GT(monotonic_now_ns(), 0u) << "CLOCK_MONOTONIC unavailable";
	EXPECT_GT(boottime_now_ns(), 0u) << "CLOCK_BOOTTIME unavailable";
	// BOOTTIME = MONOTONIC + suspend time, so it can never read smaller.
	EXPECT_GE(boottime_now_ns(), monotonic_now_ns());
}

TEST(Clock, DeadlineSaturatesOnOverflow) {
	// now + UINT64_MAX must not wrap to a tiny value.
	EXPECT_EQ(monotonic_deadline_ns(UINT64_MAX), UINT64_MAX);
	// the common case adds without wrapping.
	EXPECT_GT(monotonic_deadline_ns(1000000000ull), monotonic_now_ns());
}

TEST(Clock, ExpiredDeadlineReturnsZero) {
	EXPECT_EQ(remaining_time_ns(0), 0u);
	EXPECT_EQ(remaining_time_ns(monotonic_now_ns()), 0u);
}

TEST(Clock, RemainingCountsDownFromDeadline) {
	const uint64_t deadline = monotonic_deadline_ns(10ull * 1000000ull);  // 10ms out
	const uint64_t remaining = remaining_time_ns(deadline);
	EXPECT_GT(remaining, 0u);
	EXPECT_LE(remaining, 10ull * 1000000ull);
}

}  // namespace
