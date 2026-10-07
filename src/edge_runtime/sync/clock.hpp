#ifndef EDGE_RUNTIME_DETAIL_CLOCK_HPP
#define EDGE_RUNTIME_DETAIL_CLOCK_HPP

#include <cstdint>

namespace edge_runtime::detail {

// BOOTTIME 计入系统挂起时间，用于判断样本和心跳的新鲜度。
uint64_t boottime_now_ns() noexcept;

// MONOTONIC 不受墙上时间调整影响，用于等待截止时间。
uint64_t monotonic_now_ns() noexcept;

uint64_t monotonic_deadline_ns(uint64_t timeout_ns) noexcept;

uint64_t remaining_time_ns(uint64_t deadline_ns) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CLOCK_HPP
