// ER3 integration driver (I12–I14): wait_latest semantics across processes.
// Every case fork+execs the SEPARATE tool binaries (design §18.3).
//
//   I12  publish-before/during wait does not lose the wakeup
//   I13  SIGUSR1 interrupts a blocked wait; the ORIGINAL deadline still applies
//   I14  no new data returns at the deadline boundary (not early, not hung):
//          producer alive -> DataStale, producer cleanly gone -> ProducerOffline
//
// The consumer tool reports the outcome in its SUMMARY line (waits, timed_out,
// last_error) and its total runtime distinguishes "woken by publish" from
// "sat out the full timeout".

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "test_util.hpp"

namespace {

using edge_test::monotonic_ms_now;
using edge_test::SpawnedChild;
using edge_test::unique_channel_name;

std::string g_producer_tool;
std::string g_consumer_tool;

std::vector<std::string> producer_args(const std::string& name) {
	return {g_producer_tool, "--name", name, "--schema", "testpayloadv1"};
}

std::vector<std::string> consumer_args(const std::string& name) {
	return {g_consumer_tool, "--name", name, "--schema", "testpayloadv1"};
}

uint64_t summary_field(const std::string& out, const char* field) {
	const std::string needle = std::string(field) + "=";
	const size_t pos = out.find(needle);
	if (pos == std::string::npos) return 0;
	const size_t val_start = pos + needle.size();
	const size_t val_end = out.find_first_of(" \n\r", val_start);
	const std::string val = out.substr(
	        val_start, val_end == std::string::npos ? std::string::npos : val_end - val_start);
	return std::strtoull(val.c_str(), nullptr, 10);
}

bool out_contains(const std::string& out, const char* needle) {
	return out.find(needle) != std::string::npos;
}

TEST(Wait, PublishWakesWaitNoLostWakeup) {  // I12
	const std::string name = unique_channel_name("i12");
	// Consumer blocks in wait_latest(5s) BEFORE any sample exists; the producer
	// then publishes 3 samples. The futex wake must interrupt the wait — the
	// consumer returns the sample immediately (timed_out=0) instead of sitting
	// out the full 5s and classifying a timeout.
	auto ca = consumer_args(name);
	ca.push_back("--use-wait-ms");
	ca.push_back("5000");
	ca.push_back("--expect-last-seq");
	ca.push_back("3");
	ca.push_back("--read-timeout-ms");
	ca.push_back("0");
	ca.push_back("--open-retry-ms");
	ca.push_back("20000");
	SpawnedChild cons;
	const int64_t t0 = monotonic_ms_now();
	ASSERT_TRUE(cons.spawn(ca));
	std::this_thread::sleep_for(std::chrono::milliseconds(400));

	auto pa = producer_args(name);
	pa.push_back("--count");
	pa.push_back("3");
	SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));

	std::string cons_out, prod_out;
	ASSERT_TRUE(cons.wait(20000, &cons_out)) << "consumer hung: " << cons_out;
	const int64_t elapsed = monotonic_ms_now() - t0;
	ASSERT_TRUE(prod.wait(20000, &prod_out)) << "producer hung: " << prod_out;
	ASSERT_EQ(prod.exit_code(), 0) << prod_out;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	EXPECT_EQ(summary_field(cons_out, "torn"), 0u) << cons_out;
	EXPECT_GE(summary_field(cons_out, "last_seq"), 3u) << cons_out;
	EXPECT_EQ(summary_field(cons_out, "timed_out"), 0u)
	        << "wait timed out instead of being woken: " << cons_out;
	// Woken by the publish ~400ms in, not sat for the full 5s timeout.
	EXPECT_LT(elapsed, 4000) << "consumer sat out the full wait timeout (lost wakeup?)";
}

TEST(Wait, EinrtKeepsOriginalDeadline) {  // I13
	const std::string name = unique_channel_name("i13");
	// Producer stays alive but publishes nothing for 8s (sleep-first). Consumer
	// blocks in wait_latest(5s). A SIGUSR1 at ~1.2s interrupts the futex (EINTR);
	// the loop must re-wait with the REMAINING time, so the total runtime stays
	// ~5s. If the deadline were reset, the runtime would stretch to ~6.2s.
	auto pa = producer_args(name);
	pa.push_back("--sleep-first-us");
	pa.push_back("8000000");
	pa.push_back("--count");
	pa.push_back("0");
	SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));

	auto ca = consumer_args(name);
	ca.push_back("--use-wait-ms");
	ca.push_back("5000");
	ca.push_back("--read-timeout-ms");
	ca.push_back("0");
	ca.push_back("--open-retry-ms");
	ca.push_back("20000");
	SpawnedChild cons;
	const int64_t t0 = monotonic_ms_now();
	ASSERT_TRUE(cons.spawn(ca));
	std::this_thread::sleep_for(std::chrono::milliseconds(1200));
	cons.kill(SIGUSR1);

	std::string cons_out;
	ASSERT_TRUE(cons.wait(20000, &cons_out)) << "consumer hung: " << cons_out;
	const int64_t elapsed = monotonic_ms_now() - t0;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	// The signal was absorbed (not a kill): the classification is a clean timeout.
	EXPECT_EQ(summary_field(cons_out, "timed_out"), 1u) << cons_out;
	EXPECT_TRUE(out_contains(cons_out, "last_error=DataStale"))
	        << "producer alive but idle should classify DataStale: " << cons_out;
	// ~5s deadline, NOT ~5s + interrupt delay (6.2s) from a reset.
	EXPECT_GE(elapsed, 4700) << "consumer returned too early: " << cons_out;
	EXPECT_LT(elapsed, 5900) << "deadline appears to have been reset on EINTR: " << cons_out;

	prod.kill(SIGTERM);
	std::string prod_out;
	prod.wait(10000, &prod_out);
}

TEST(Wait, BoundaryAliveProducerDataStale) {  // I14a
	const std::string name = unique_channel_name("i14a");
	// Producer alive but publishing nothing: wait_latest(2s) returns DataStale at
	// the ~2s boundary — not early, not hung.
	auto pa = producer_args(name);
	pa.push_back("--sleep-first-us");
	pa.push_back("6000000");
	pa.push_back("--count");
	pa.push_back("0");
	SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));

	auto ca = consumer_args(name);
	ca.push_back("--use-wait-ms");
	ca.push_back("2000");
	ca.push_back("--read-timeout-ms");
	ca.push_back("0");
	ca.push_back("--open-retry-ms");
	ca.push_back("20000");
	SpawnedChild cons;
	const int64_t t0 = monotonic_ms_now();
	ASSERT_TRUE(cons.spawn(ca));
	std::string cons_out;
	ASSERT_TRUE(cons.wait(15000, &cons_out)) << "consumer hung: " << cons_out;
	const int64_t elapsed = monotonic_ms_now() - t0;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	EXPECT_EQ(summary_field(cons_out, "timed_out"), 1u) << cons_out;
	EXPECT_TRUE(out_contains(cons_out, "last_error=DataStale")) << cons_out;
	EXPECT_GE(elapsed, 1800) << "returned before the boundary: " << cons_out;
	EXPECT_LT(elapsed, 2600) << "returned after the boundary: " << cons_out;

	prod.kill(SIGTERM);
	std::string prod_out;
	prod.wait(10000, &prod_out);
}

TEST(Wait, BoundaryDeadProducerProducerOffline) {  // I14b
	const std::string name = unique_channel_name("i14b");
	// Producer creates the channel then clean-exits (no samples; the destructor
	// marks producer_state OFFLINE). wait_latest(2s) returns ProducerOffline at
	// the ~2s boundary.
	auto pa = producer_args(name);
	pa.push_back("--no-publish");
	SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));
	std::this_thread::sleep_for(std::chrono::milliseconds(400));
	prod.kill(SIGTERM);  // clean shutdown -> producer_state OFFLINE
	std::string prod_out;
	ASSERT_TRUE(prod.wait(10000, &prod_out)) << "producer hung: " << prod_out;
	EXPECT_TRUE(out_contains(prod_out, "DONE published=0")) << prod_out;

	auto ca = consumer_args(name);
	ca.push_back("--use-wait-ms");
	ca.push_back("2000");
	ca.push_back("--read-timeout-ms");
	ca.push_back("0");
	ca.push_back("--open-retry-ms");
	ca.push_back("5000");
	SpawnedChild cons;
	const int64_t t0 = monotonic_ms_now();
	ASSERT_TRUE(cons.spawn(ca));
	std::string cons_out;
	ASSERT_TRUE(cons.wait(15000, &cons_out)) << "consumer hung: " << cons_out;
	const int64_t elapsed = monotonic_ms_now() - t0;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	EXPECT_EQ(summary_field(cons_out, "reads"), 0u)
	        << "no-publish producer must publish nothing: " << cons_out;
	EXPECT_EQ(summary_field(cons_out, "timed_out"), 1u) << cons_out;
	EXPECT_TRUE(out_contains(cons_out, "last_error=ProducerOffline")) << cons_out;
	EXPECT_GE(elapsed, 1800) << "returned before the boundary: " << cons_out;
	EXPECT_LT(elapsed, 2600) << "returned after the boundary: " << cons_out;
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc < 3) {
		std::fprintf(stderr, "usage: wait_test <edge_shm_producer> <edge_shm_consumer>\n");
		return 2;
	}
	g_producer_tool = argv[1];
	g_consumer_tool = argv[2];
	return RUN_ALL_TESTS();
}
