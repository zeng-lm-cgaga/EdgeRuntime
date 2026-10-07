#ifndef EDGE_RUNTIME_DETAIL_FUTEX_HPP
#define EDGE_RUNTIME_DETAIL_FUTEX_HPP

#include <cstdint>
#include <ctime>

namespace edge_runtime::detail {

// Futex 只负责通知，不负责槽所有权；调用方必须用绝对截止时间维护等待循环。
int futex_wait(const void* addr, uint32_t expected, const struct timespec* rel_timeout) noexcept;

int futex_wake(const void* addr, int count) noexcept;

// Debug trace flush; the release implementation reports success without side effects.
bool futex_trace_flush() noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_FUTEX_HPP
