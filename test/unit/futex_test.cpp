

#include "edge_runtime/sync/futex.hpp"

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
		rc = futex_wait(&val, 0, nullptr);
	});
	while (!in_wait.load(std::memory_order_acquire)) {
		std::this_thread::yield();
	}

	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	val = 1;
	const int woken = futex_wake(&val, 1);
	waiter.join();
	EXPECT_EQ(woken, 1);
	EXPECT_EQ(rc, 0);
}

TEST(Futex, EagainOnValueMismatch) {

	uint32_t val = 7;
	errno = 0;
	const int rc = futex_wait(&val, 5, nullptr);
	EXPECT_EQ(rc, -1);
	EXPECT_EQ(errno, EAGAIN);
}

TEST(Futex, EtimedoutOnShortTimeout) {
	uint32_t val = 0;
	struct timespec ts {};
	ts.tv_nsec = 50L * 1000000L;
	const auto t0 = std::chrono::steady_clock::now();
	errno = 0;
	const int rc = futex_wait(&val, 0, &ts);
	const int64_t elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
	                                   std::chrono::steady_clock::now() - t0)
	                                   .count();
	EXPECT_EQ(rc, -1);
	EXPECT_EQ(errno, ETIMEDOUT);
	EXPECT_GE(elapsed_ms, 30);
	EXPECT_LE(elapsed_ms, 2000);
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
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	::pthread_kill(waiter.native_handle(), SIGUSR1);
	waiter.join();

	EXPECT_EQ(rc, -1);
	EXPECT_EQ(saved_errno, EINTR);
	::sigaction(SIGUSR1, &old, nullptr);
}

}
