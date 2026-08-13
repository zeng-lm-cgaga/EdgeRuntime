// U10: process-shared futex wrapper semantics (design §14.1). Tests the raw
// syscall wrapper directly with in-process threads; the cross-process wake
// behavior is exercised by the integration wait tests (I12 publish-wakes-wait)
// and the pattern suite. Covers wake, EAGAIN (value changed before arming),
// relative-timeout ETIMEDOUT, the zero timeout, and EINTR on a signal.

#include "edge_runtime/detail/futex.hpp"

#include <gtest/gtest.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <thread>

namespace {

using edge_runtime::detail::futex_wait;
using edge_runtime::detail::futex_wake;

void noop_sigusr1(int) {}

TEST(Futex, WakeUnblocksWaiter) {
	uint32_t val = 0;
	std::atomic<bool> in_wait{false};
	int rc = -99;
	std::thread waiter([&] {
		in_wait.store(true, std::memory_order_release);
		rc = futex_wait(&val, 0, nullptr);  // relative timeout = infinite
	});
	while (!in_wait.load(std::memory_order_acquire)) {
		std::this_thread::yield();
	}
	// Give the waiter a generous window to reach the blocking syscall. Even if it
	// has not, the store below makes the next futex_wait return EAGAIN, so the
	// thread can never hang — the assertion on woken==1/rc==0 is just weaker then.
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	val = 1;
	const int woken = futex_wake(&val, 1);
	waiter.join();
	EXPECT_EQ(woken, 1);
	EXPECT_EQ(rc, 0);
}

TEST(Futex, EagainOnValueMismatch) {
	// expected (5) differs from the current value (7): futex returns EAGAIN
	// without blocking.
	uint32_t val = 7;
	errno = 0;
	const int rc = futex_wait(&val, 5, nullptr);
	EXPECT_EQ(rc, -1);
	EXPECT_EQ(errno, EAGAIN);
}

TEST(Futex, EtimedoutOnShortTimeout) {
	uint32_t val = 0;
	struct timespec ts {};
	ts.tv_nsec = 50L * 1000000L;  // 50ms
	const auto t0 = std::chrono::steady_clock::now();
	errno = 0;
	const int rc = futex_wait(&val, 0, &ts);
	const int64_t elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
	                                   std::chrono::steady_clock::now() - t0)
	                                   .count();
	EXPECT_EQ(rc, -1);
	EXPECT_EQ(errno, ETIMEDOUT);
	EXPECT_GE(elapsed_ms, 30);    // did not return early
	EXPECT_LE(elapsed_ms, 2000);  // and did not wait forever
}

TEST(Futex, ImmediateZeroTimeout) {
	uint32_t val = 0;
	struct timespec ts {};
	ts.tv_sec = 0;
	ts.tv_nsec = 0;
	errno = 0;
	const int rc = futex_wait(&val, 0, &ts);
	EXPECT_EQ(rc, -1);
	EXPECT_EQ(errno, ETIMEDOUT);
}

TEST(Futex, EinrtOnSignal) {
	// SIGUSR1 with a no-op handler (no SA_RESTART) interrupts a blocked
	// futex_wait with EINTR; the caller is expected to re-wait with its original
	// deadline (design §14.2) — the wrapper must surface, not swallow, EINTR.
	struct sigaction sa {};
	sa.sa_handler = noop_sigusr1;
	::sigemptyset(&sa.sa_mask);
	struct sigaction old {};
	::sigaction(SIGUSR1, &sa, &old);

	uint32_t val = 0;
	std::atomic<bool> in_wait{false};
	int rc = -99;
	int saved_errno = 0;
	std::thread waiter([&] {
		in_wait.store(true, std::memory_order_release);
		struct timespec ts {};
		ts.tv_sec = 5;
		rc = futex_wait(&val, 0, &ts);
		saved_errno = errno;
	});
	while (!in_wait.load(std::memory_order_acquire)) {
		std::this_thread::yield();
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(20));  // blocked by now
	::pthread_kill(waiter.native_handle(), SIGUSR1);
	waiter.join();

	EXPECT_EQ(rc, -1);
	EXPECT_EQ(saved_errno, EINTR);
	::sigaction(SIGUSR1, &old, nullptr);  // restore default disposition
}

}  // namespace
