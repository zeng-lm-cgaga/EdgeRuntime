// v0.3 §35.3 channel observer unit tests: open_channel_readonly over both
// transports, retry/refusal behavior, classify_stall branches, and the
// zero-write (PROT_READ) guarantee. The READY channel fixtures come from an
// in-process Producer handle (the observer itself never creates anything).

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/detail/channel_abi.hpp"
#include "edge_runtime/detail/channel_observer.hpp"
#include "edge_runtime/detail/clock.hpp"
#include "edge_runtime/detail/shared_atomic.hpp"
#include "edge_runtime/producer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {

using edge_runtime::ChannelOptions;
using edge_runtime::Producer;
using edge_runtime::Transport;
using edge_runtime::detail::ChannelObserverView;
using edge_runtime::detail::classify_stall;
using edge_runtime::detail::open_channel_readonly;
using edge_runtime::detail::observer_header;
using edge_runtime::detail::observer_generation;
using edge_runtime::detail::StallClass;
using edge_test::unique_channel_name;

uint64_t ns_from_ms(uint64_t ms) { return ms * 1'000'000ull; }

TEST(ChannelObserver, OpenPosixReadonly) {
	const std::string name = unique_channel_name("obs1");
	ChannelOptions opts;
	opts.name = name;
	auto p = Producer<TestPayloadV1>::create(opts, TestPayloadV1Schema());
	ASSERT_TRUE(p) << edge_runtime::to_string(p.error().code);

	auto view = open_channel_readonly(name, Transport::kPosixShm, 100);
	ASSERT_TRUE(view) << edge_runtime::to_string(view.error().code);
	EXPECT_EQ(view.value().transport, Transport::kPosixShm);
	EXPECT_GT(view.value().size, 0u);
	EXPECT_EQ(observer_generation(observer_header(view.value())), 1u);
}

TEST(ChannelObserver, OpenFdPassReadonly) {
	const std::string name = unique_channel_name("obs2");
	ChannelOptions opts;
	opts.name = name;
	opts.transport = Transport::kMemfdFdPass;
	auto p = Producer<TestPayloadV1>::create(opts, TestPayloadV1Schema());
	ASSERT_TRUE(p) << edge_runtime::to_string(p.error().code);

	auto view = open_channel_readonly(name, Transport::kMemfdFdPass, 1000);
	ASSERT_TRUE(view) << edge_runtime::to_string(view.error().code);
	EXPECT_EQ(view.value().transport, Transport::kMemfdFdPass);
	EXPECT_EQ(observer_generation(observer_header(view.value())), 1u);
}

TEST(ChannelObserver, AbsentChannelTimesOut) {
	const std::string name = unique_channel_name("obs3");
	auto view = open_channel_readonly(name, Transport::kPosixShm, 150);
	ASSERT_FALSE(view);
	EXPECT_EQ(view.error().code, edge_runtime::ErrorCode::kTimeout);
}

TEST(ChannelObserver, ClassifyStallBranches) {
	// The observer view is PROT_READ — all header mutations in this test go
	// through the PRODUCER's own writable mapping (heartbeat()/publish()),
	// which the observer's MAP_SHARED view of the same object observes.
	const std::string name = unique_channel_name("obs4");
	ChannelOptions opts;
	opts.name = name;
	opts.heartbeat_interval = std::chrono::milliseconds(100);
	auto p = Producer<TestPayloadV1>::create(opts, TestPayloadV1Schema());
	ASSERT_TRUE(p) << edge_runtime::to_string(p.error().code);

	auto view = open_channel_readonly(name, Transport::kPosixShm, 100);
	ASSERT_TRUE(view);
	auto* header = observer_header(view.value());
	const auto pid_abi = edge_runtime::detail::identity_snapshot_read(&header->producer);
	ASSERT_TRUE(pid_abi);
	const uint64_t pid = pid_abi.value().pid;
	const uint64_t start = pid_abi.value().proc_start_ticks;

	// Identity mismatch: never kill someone else's channel.
	EXPECT_EQ(classify_stall(header, edge_runtime::detail::boottime_now_ns(), pid + 1, start),
	          StallClass::kIdentityMismatch);
	// Fresh beat: heartbeat() just stored BOOTTIME, no stall at ~now.
	ASSERT_TRUE(p.value().heartbeat());
	const uint64_t now = edge_runtime::detail::boottime_now_ns();
	EXPECT_EQ(classify_stall(header, now, pid, start), StallClass::kFresh);
	// Stale beat beyond 3x the interval (simulated clock advance): stalled.
	const uint64_t stale_now = now + 4 * ns_from_ms(100);
	EXPECT_EQ(classify_stall(header, stale_now, pid, start), StallClass::kStalled);
	// Stale beat + fresh publish -> rule 2 wins (data advancing beats a stale
	// heartbeat). Real time must actually pass 3x the interval so the beat is
	// stale while the new publish is fresh.
	std::this_thread::sleep_for(std::chrono::milliseconds(350));
	ASSERT_TRUE(p.value().publish(TestPayloadV1{0x5A000001u, 1, 0}));
	const uint64_t now2 = edge_runtime::detail::boottime_now_ns();
	EXPECT_EQ(classify_stall(header, now2, pid, start), StallClass::kFresh);
	// A future observation in shared memory must not underflow into a false stall.
	EXPECT_EQ(classify_stall(header, 1, pid, start), StallClass::kFresh);
}

TEST(ChannelObserver, RetryTimeoutOverflowRejected) {
	auto view = open_channel_readonly("overflow", Transport::kPosixShm, UINT64_MAX);
	ASSERT_FALSE(view);
	EXPECT_EQ(view.error().code, edge_runtime::ErrorCode::kInvalidOptions);
}

TEST(ChannelObserver, HeartbeatDisabledNotApplicable) {
	const std::string name = unique_channel_name("obs5");
	ChannelOptions opts;
	opts.name = name;  // heartbeat_interval defaults to 0 (disabled)
	auto p = Producer<TestPayloadV1>::create(opts, TestPayloadV1Schema());
	ASSERT_TRUE(p);
	auto view = open_channel_readonly(name, Transport::kPosixShm, 100);
	ASSERT_TRUE(view);
	auto* header = observer_header(view.value());
	const auto pid_abi = edge_runtime::detail::identity_snapshot_read(&header->producer);
	ASSERT_TRUE(pid_abi);
	EXPECT_EQ(classify_stall(header, edge_runtime::detail::boottime_now_ns(),
	                         pid_abi.value().pid, pid_abi.value().proc_start_ticks),
	          StallClass::kNotApplicable);
}

}  // namespace
