

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

	EXPECT_GE(boottime_now_ns(), monotonic_now_ns());
}

TEST(Clock, DeadlineSaturatesOnOverflow) {

	EXPECT_EQ(monotonic_deadline_ns(UINT64_MAX), UINT64_MAX);

	EXPECT_GT(monotonic_deadline_ns(1000000000ull), monotonic_now_ns());
}

TEST(Clock, ExpiredDeadlineReturnsZero) {
	EXPECT_EQ(remaining_time_ns(0), 0u);
	EXPECT_EQ(remaining_time_ns(monotonic_now_ns()), 0u);
}

TEST(Clock, RemainingCountsDownFromDeadline) {
	const uint64_t deadline = monotonic_deadline_ns(10ull * 1000000ull);
	const uint64_t remaining = remaining_time_ns(deadline);
	EXPECT_GT(remaining, 0u);
	EXPECT_LE(remaining, 10ull * 1000000ull);
}

}
