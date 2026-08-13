#ifndef EDGE_RUNTIME_DETAIL_FUTEX_HPP
#define EDGE_RUNTIME_DETAIL_FUTEX_HPP

#include <cstdint>
#include <ctime>

namespace edge_runtime::detail {

// Process-shared futex wrappers (design §14.1). The address must be a 4-byte
// aligned word in shared memory; FUTEX_PRIVATE_FLAG is deliberately absent so
// producer and consumer may live in different processes.
//
// futex_wait returns 0 when woken/spuriously; -1 with errno set otherwise
// (EAGAIN if the value changed, EINTR on signal, ETIMEDOUT on timeout).
// rel_timeout == nullptr waits forever.
int futex_wait(const void* addr, uint32_t expected, const struct timespec* rel_timeout) noexcept;

// Wakes up to `count` waiters on addr. Returns the number woken.
int futex_wake(const void* addr, int count) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_FUTEX_HPP
