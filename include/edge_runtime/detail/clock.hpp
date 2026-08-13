#ifndef EDGE_RUNTIME_DETAIL_CLOCK_HPP
#define EDGE_RUNTIME_DETAIL_CLOCK_HPP

#include <cstdint>

namespace edge_runtime::detail {

// Sample-age clock (design §15.7): CLOCK_BOOTTIME so suspend time counts toward
// staleness. Returns 0 only on catastrophic clock failure; callers treat 0 as
// ClockAnomaly rather than a valid timestamp.
uint64_t boottime_now_ns() noexcept;

// Timeout/deadline clock (design §15.7): CLOCK_MONOTONIC absolute deadline.
// EINTR/retry loops must re-derive remaining time from this deadline, never
// reset it.
uint64_t monotonic_now_ns() noexcept;

// absolute monotonic deadline = now + timeout (saturating at UINT64_MAX).
uint64_t monotonic_deadline_ns(uint64_t timeout_ns) noexcept;

// remaining monotonic nanoseconds until deadline; 0 when already expired.
uint64_t remaining_time_ns(uint64_t deadline_ns) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CLOCK_HPP
