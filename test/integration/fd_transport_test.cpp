

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
std::string g_ctl_tool;

std::vector<std::string> fd_producer_args(const std::string& name) {
	return {g_producer_tool, "--name", name, "--schema", "testpayloadv1",
	        "--transport", "fd"};
}

std::vector<std::string> fd_consumer_args(const std::string& name) {
	return {g_consumer_tool, "--name", name, "--schema", "testpayloadv1",
	        "--transport", "fd"};
}

std::vector<std::string> posix_consumer_args(const std::string& name) {
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

TEST(FdTransport, CrossExecCreateOpenReadInspect) {
	const std::string name = unique_channel_name("fd1");
	auto pa = fd_producer_args(name);
	pa.push_back("--interval-us");
	pa.push_back("20000");
	SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	auto ca = fd_consumer_args(name);
	ca.push_back("--reads");
	ca.push_back("3");
	ca.push_back("--read-interval-ms");
	ca.push_back("20");
	ca.push_back("--open-retry-ms");
	ca.push_back("5000");
	SpawnedChild cons;
	ASSERT_TRUE(cons.spawn(ca));

	std::string cons_out, prod_out;
	ASSERT_TRUE(cons.wait(20000, &cons_out)) << "consumer hung: " << cons_out;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	EXPECT_EQ(summary_field(cons_out, "reads"), 3u) << cons_out;
	EXPECT_EQ(summary_field(cons_out, "torn"), 0u) << cons_out;

	const std::vector<std::string> ctl_args = {g_ctl_tool, "inspect", name};
	const auto ctl_res = edge_test::run_child_capture(ctl_args, 15000);
	ASSERT_EQ(ctl_res.exit_code, 0) << ctl_res.stdout_text;
	EXPECT_TRUE(out_contains(ctl_res.stdout_text, "transport=fd_pass"))
	        << ctl_res.stdout_text;
	EXPECT_TRUE(out_contains(ctl_res.stdout_text, "init=ready")) << ctl_res.stdout_text;

	prod.kill(SIGTERM);
	ASSERT_TRUE(prod.wait(10000, &prod_out)) << "producer hung: " << prod_out;
}

TEST(FdTransport, ConsumerStartsFirstBoundedRetry) {
	const std::string name = unique_channel_name("fd2");
	auto ca = fd_consumer_args(name);
	ca.push_back("--reads");
	ca.push_back("2");
	ca.push_back("--read-interval-ms");
	ca.push_back("20");
	ca.push_back("--open-retry-ms");
	ca.push_back("20000");
	SpawnedChild cons;
	ASSERT_TRUE(cons.spawn(ca));
	std::this_thread::sleep_for(std::chrono::milliseconds(800));

	auto pa = fd_producer_args(name);
	pa.push_back("--count");
	pa.push_back("4");
	pa.push_back("--interval-us");
	pa.push_back("5000");
	SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));

	std::string cons_out, prod_out;
	ASSERT_TRUE(cons.wait(25000, &cons_out)) << "consumer hung: " << cons_out;
	ASSERT_TRUE(prod.wait(15000, &prod_out)) << "producer hung: " << prod_out;
	EXPECT_EQ(prod.exit_code(), 0) << prod_out;
	EXPECT_EQ(cons.exit_code(), 0) << cons_out;
	EXPECT_GE(summary_field(cons_out, "reads"), 2u) << cons_out;
	EXPECT_EQ(summary_field(cons_out, "torn"), 0u) << cons_out;
}

TEST(FdTransport, PosixConsumerGetsExplicitMismatch) {
	const std::string name = unique_channel_name("fd3");
	auto pa = fd_producer_args(name);
	pa.push_back("--interval-us");
	pa.push_back("100000");
	SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));
	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	auto ca = posix_consumer_args(name);
	ca.push_back("--open-retry-ms");
	ca.push_back("0");
	SpawnedChild cons;
	ASSERT_TRUE(cons.spawn(ca));
	std::string cons_out;
	ASSERT_TRUE(cons.wait(15000, &cons_out)) << "consumer hung: " << cons_out;
	EXPECT_NE(cons.exit_code(), 0) << cons_out;
	EXPECT_TRUE(out_contains(cons_out, "code=InvalidOptions")) << cons_out;
	EXPECT_TRUE(out_contains(cons_out, "fd-pass")) << cons_out;

	prod.kill(SIGTERM);
	std::string prod_out;
	ASSERT_TRUE(prod.wait(10000, &prod_out)) << "producer hung: " << prod_out;
}

TEST(FdTransport, CleanShutdownReleasesSocketAndSuccessorRecreates) {
	const std::string name = unique_channel_name("fd4");
	{
		auto pa = fd_producer_args(name);
		pa.push_back("--no-publish");
		SpawnedChild prod;
		ASSERT_TRUE(prod.spawn(pa));
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
		prod.kill(SIGTERM);
		std::string prod_out;
		ASSERT_TRUE(prod.wait(10000, &prod_out)) << "producer hung: " << prod_out;
		EXPECT_TRUE(out_contains(prod_out, "DONE published=0")) << prod_out;
	}

	{
		auto pa = fd_producer_args(name);
		pa.push_back("--interval-us");
		pa.push_back("50000");
		SpawnedChild prod;
		ASSERT_TRUE(prod.spawn(pa));
		std::this_thread::sleep_for(std::chrono::milliseconds(600));
		{
			auto ca = fd_consumer_args(name);
			ca.push_back("--reads");
			ca.push_back("1");
			ca.push_back("--read-interval-ms");
			ca.push_back("20");
			ca.push_back("--open-retry-ms");
			ca.push_back("5000");
			SpawnedChild cons;
			ASSERT_TRUE(cons.spawn(ca));
			std::string cons_out;
			ASSERT_TRUE(cons.wait(15000, &cons_out)) << "consumer hung: " << cons_out;
			EXPECT_EQ(cons.exit_code(), 0) << cons_out;
			EXPECT_GE(summary_field(cons_out, "reads"), 1u) << cons_out;
		}
		prod.kill(SIGTERM);
		std::string prod_out;
		ASSERT_TRUE(prod.wait(10000, &prod_out)) << "producer hung: " << prod_out;
		EXPECT_TRUE(out_contains(prod_out, "GENERATION 2")) << prod_out;
	}
}

}

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc < 4) {
		std::fprintf(stderr,
		             "usage: fd_transport_test <edge_shm_producer> <edge_shm_consumer> "
		             "<edge_shm_ctl>\n");
		return 2;
	}
	g_producer_tool = argv[1];
	g_consumer_tool = argv[2];
	g_ctl_tool = argv[3];
	return RUN_ALL_TESTS();
}
