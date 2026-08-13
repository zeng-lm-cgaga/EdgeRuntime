#ifndef EDGE_RUNTIME_DETAIL_SHARED_ATOMIC_HPP
#define EDGE_RUNTIME_DETAIL_SHARED_ATOMIC_HPP

#include <cstdint>

namespace edge_runtime::detail {

// All access to shared-memory atomic fields goes through these wrappers
// (design §6.2, §13). T must be a lock-free explicit-width integer type; the
// frozen ABI never contains std::atomic or plain concurrent access.
template <typename T>
inline T shared_load_acquire(const T* addr) noexcept {
	static_assert(sizeof(T) <= 8, "shared atomics are limited to 64-bit fields");
	static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
	              "shared atomic field is not lock-free");
	return __atomic_load_n(addr, __ATOMIC_ACQUIRE);
}

template <typename T>
inline T shared_load_relaxed(const T* addr) noexcept {
	static_assert(sizeof(T) <= 8, "shared atomics are limited to 64-bit fields");
	static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
	              "shared atomic field is not lock-free");
	return __atomic_load_n(addr, __ATOMIC_RELAXED);
}

template <typename T>
inline void shared_store_release(T* addr, T value) noexcept {
	static_assert(sizeof(T) <= 8, "shared atomics are limited to 64-bit fields");
	static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
	              "shared atomic field is not lock-free");
	__atomic_store_n(addr, value, __ATOMIC_RELEASE);
}

template <typename T>
inline void shared_store_relaxed(T* addr, T value) noexcept {
	static_assert(sizeof(T) <= 8, "shared atomics are limited to 64-bit fields");
	static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
	              "shared atomic field is not lock-free");
	__atomic_store_n(addr, value, __ATOMIC_RELAXED);
}

// Strong compare_exchange: acquire on success, relaxed on failure.
template <typename T>
inline bool shared_cas_acquire(T* addr, T expected, T desired) noexcept {
	static_assert(sizeof(T) <= 8, "shared atomics are limited to 64-bit fields");
	static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
	              "shared atomic field is not lock-free");
	return __atomic_compare_exchange_n(addr, &expected, desired, false, __ATOMIC_ACQUIRE,
	                                   __ATOMIC_RELAXED);
}

template <typename T>
inline T shared_exchange_release(T* addr, T value) noexcept {
	static_assert(sizeof(T) <= 8, "shared atomics are limited to 64-bit fields");
	static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
	              "shared atomic field is not lock-free");
	return __atomic_exchange_n(addr, value, __ATOMIC_RELEASE);
}

template <typename T>
inline T shared_fetch_add_relaxed(T* addr, T value) noexcept {
	static_assert(sizeof(T) <= 8, "shared atomics are limited to 64-bit fields");
	static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
	              "shared atomic field is not lock-free");
	return __atomic_fetch_add(addr, value, __ATOMIC_RELAXED);
}

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_SHARED_ATOMIC_HPP
