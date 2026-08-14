#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <vector>

#include "edge_runtime/supervisor.hpp"
#include "test_util.hpp"

namespace {

struct RestartTrace {
	std::vector<uint32_t> attempts;
	std::vector<uint64_t> delays;
};

void record_restart(const edge_runtime::SupervisorEventInfo& info, void* user_data) {
	if (info.event != edge_runtime::SupervisorEvent::kRestartArmed) return;
	auto* trace = static_cast<RestartTrace*>(user_data);
	trace->attempts.push_back(info.attempt);
	trace->delays.push_back(info.delay_ns);
}

edge_runtime::SupervisorOptions false_child_options(const char* suffix) {
	edge_runtime::SupervisorOptions options;
	options.channel_name = edge_test::unique_channel_name(suffix);
	options.producer_argv = {"/bin/false"};
	options.initial_delay = std::chrono::milliseconds(1);
	options.max_delay = std::chrono::milliseconds(4);
	options.multiplier = 2;
	options.max_restarts = 3;
	options.stable_reset_window = std::chrono::seconds(10);
	options.stall_grace = std::chrono::milliseconds(1);
	options.watch_interval = std::chrono::milliseconds(1);
	options.create_timeout = std::chrono::milliseconds(20);
	return options;
}

TEST(SupervisorPolicy, RestartCapCountsActualRetriesAndBackoffStartsAtInitial) {
	auto options = false_child_options("policy_cap");
	RestartTrace trace;
	options.on_event = record_restart;
	options.event_user_data = &trace;
	auto supervisor = edge_runtime::ProducerSupervisor::create(options);
	ASSERT_TRUE(supervisor) << edge_runtime::to_string(supervisor.error().code);
	auto result = supervisor.value().run();
	ASSERT_TRUE(result) << edge_runtime::to_string(result.error().code);
	EXPECT_EQ(result.value().outcome, edge_runtime::SupervisionOutcome::kRestartsExhausted);
	EXPECT_EQ(result.value().spawn_attempts, 4u);
	EXPECT_EQ(result.value().restarts, 3u);
	EXPECT_EQ(trace.attempts, (std::vector<uint32_t>{1, 2, 3}));
	EXPECT_EQ(trace.delays, (std::vector<uint64_t>{1'000'000, 2'000'000, 4'000'000}));
}

TEST(SupervisorPolicy, ZeroRestartBudgetSpawnsOnlyOnce) {
	auto options = false_child_options("policy_zero");
	options.max_restarts = 0;
	RestartTrace trace;
	options.on_event = record_restart;
	options.event_user_data = &trace;
	auto supervisor = edge_runtime::ProducerSupervisor::create(options);
	ASSERT_TRUE(supervisor);
	auto result = supervisor.value().run();
	ASSERT_TRUE(result);
	EXPECT_EQ(result.value().spawn_attempts, 1u);
	EXPECT_EQ(result.value().restarts, 0u);
	EXPECT_TRUE(trace.attempts.empty());
}

TEST(SupervisorPolicy, RejectsDurationConversionOverflow) {
	auto options = false_child_options("policy_overflow");
	options.watch_interval =
	        std::chrono::milliseconds(std::numeric_limits<int64_t>::max());
	auto supervisor = edge_runtime::ProducerSupervisor::create(options);
	ASSERT_FALSE(supervisor);
	EXPECT_EQ(supervisor.error().code, edge_runtime::ErrorCode::kInvalidOptions);
}

TEST(SupervisorPolicy, RejectsZeroStableResetWindow) {
	auto options = false_child_options("policy_stable_zero");
	options.stable_reset_window = std::chrono::milliseconds(0);
	auto supervisor = edge_runtime::ProducerSupervisor::create(options);
	ASSERT_FALSE(supervisor);
	EXPECT_EQ(supervisor.error().code, edge_runtime::ErrorCode::kInvalidOptions);
}

}  // namespace
