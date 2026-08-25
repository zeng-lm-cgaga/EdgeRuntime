

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

using edge_test::SpawnedChild;
using edge_test::unique_channel_name;

std::string g_producer_tool;
std::string g_consumer_tool;

std::vector<std::string> heartbeat_producer_args(const std::string& name) {
	return {g_producer_tool, "--name", name, "--schema", "testpayloadv1",
	        "--heartbeat-interval-us", "100000", "--heartbeat-only"};
}

std::vector<std::string> consumer_args(const std::string& name) {
	return {g_consumer_tool, "--name", name, "--schema", "testpayloadv1"};
}

bool out_contains(const std::string& out, const char* needle) {
	return out.find(needle) != std::string::npos;
}

TEST(Heartbeat, FrozenProducerClassifiesStalled) {
	const std::string name = unique_channel_name("hb1");
	auto pa = heartbeat_producer_args(name);
	SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));
	std::this_thread::sleep_for(std::chrono::milliseconds(700));

	prod.kill(SIGSTOP);

	auto ca = consumer_args(name);
	ca.push_back("--use-wait-ms");
	ca.push_back("2000");
	ca.push_back("--read-timeout-ms");
	ca.push_back("0");
	ca.push_back("--open-retry-ms");
	ca.push_back("10000");
	SpawnedChild cons;
	ASSERT_TRUE(cons.spawn(ca));

	std::string cons_out;
	ASSERT_TRUE(cons.wait(20000, &cons_out)) << "consumer hung: " << cons_out;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	EXPECT_TRUE(out_contains(cons_out, "last_error=ProducerStalled")) << cons_out;
	EXPECT_TRUE(out_contains(cons_out, "timed_out=1")) << cons_out;

	prod.kill(SIGKILL);
	std::string prod_out;
	prod.wait(10000, &prod_out);
}

TEST(Heartbeat, HealthyHeartbeatStillDataStale) {
	const std::string name = unique_channel_name("hb2");
	auto pa = heartbeat_producer_args(name);
	SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));
	std::this_thread::sleep_for(std::chrono::milliseconds(700));

	auto ca = consumer_args(name);
	ca.push_back("--use-wait-ms");
	ca.push_back("2000");
	ca.push_back("--read-timeout-ms");
	ca.push_back("0");
	ca.push_back("--open-retry-ms");
	ca.push_back("10000");
	SpawnedChild cons;
	ASSERT_TRUE(cons.spawn(ca));

	std::string cons_out;
	ASSERT_TRUE(cons.wait(20000, &cons_out)) << "consumer hung: " << cons_out;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;

	EXPECT_TRUE(out_contains(cons_out, "last_error=DataStale")) << cons_out;
	EXPECT_TRUE(out_contains(cons_out, "timed_out=1")) << cons_out;

	prod.kill(SIGTERM);
	std::string prod_out;
	ASSERT_TRUE(prod.wait(10000, &prod_out)) << "producer hung: " << prod_out;
}

}

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc < 3) {
		std::fprintf(stderr,
		             "usage: heartbeat_test <edge_shm_producer> <edge_shm_consumer>\n");
		return 2;
	}
	g_producer_tool = argv[1];
	g_consumer_tool = argv[2];
	return RUN_ALL_TESTS();
}
