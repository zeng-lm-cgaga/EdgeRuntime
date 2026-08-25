

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

std::string g_supervisor_tool;
std::string g_producer_tool;
std::string g_consumer_tool;

std::vector<std::string> supervisor_args(const std::string& name,
                                         const std::string& full_producer_argv,
                                         const std::vector<std::string>& extra = {}) {
	std::vector<std::string> out = {g_supervisor_tool, "--name", name,
	                                "--producer-argv", full_producer_argv,
	                                "--watch-interval-ms", "100",
	                                "--stall-grace-ms", "500",
	                                "--initial-delay-ms", "200"};
	for (const std::string& e : extra) {
		out.push_back(e);
	}
	return out;
}

bool out_contains(const std::string& out, const char* needle) {
	return out.find(needle) != std::string::npos;
}

int signal_producer_child(const std::string& sig, const std::string& name) {
	const std::string pattern = "^" + g_producer_tool + " --name " + name + " ";
	const std::string cmd = "pkill -" + sig + " -f \"" + pattern + "\"";
	return std::system(cmd.c_str());
}

TEST(Supervisor, CleanExitNoRestart) {
	const std::string name = unique_channel_name("s1");
	auto sa = supervisor_args(name, g_producer_tool + " --name " + name +
	                                         " --count 2 --interval-us 20000");
	SpawnedChild sup;
	ASSERT_TRUE(sup.spawn(sa));
	std::string out;
	ASSERT_TRUE(sup.wait(20000, &out)) << "supervisor hung: " << out;
	EXPECT_EQ(sup.exit_code(), 0) << out;
	EXPECT_TRUE(out_contains(out, "SUPERVISED pid=")) << out;
	EXPECT_TRUE(out_contains(out, "CLEAN_EXIT")) << out;
	EXPECT_FALSE(out_contains(out, "RESTART")) << out;
}

TEST(Supervisor, CrashRestartsAtGenerationPlusOne) {
	const std::string name = unique_channel_name("s2");
	auto sa = supervisor_args(name, g_producer_tool + " --name " + name +
	                                         " --interval-us 100000",
	                          {"--create-timeout-ms", "5000"});
	SpawnedChild sup;
	ASSERT_TRUE(sup.spawn(sa));
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));

	ASSERT_EQ(signal_producer_child("KILL", name), 0);

	std::this_thread::sleep_for(std::chrono::milliseconds(2500));

	std::vector<std::string> ca = {g_consumer_tool, "--name", name,
	                               "--schema", "testpayloadv1", "--reads", "1",
	                               "--read-interval-ms", "20", "--open-retry-ms",
	                               "8000"};
	SpawnedChild cons;
	ASSERT_TRUE(cons.spawn(ca));
	std::string cons_out;
	ASSERT_TRUE(cons.wait(20000, &cons_out)) << "consumer hung: " << cons_out;
	EXPECT_EQ(cons.exit_code(), 0) << cons_out;
	EXPECT_TRUE(out_contains(cons_out, "SUMMARY reads=1")) << cons_out;

	sup.kill(SIGTERM);
	std::string out;
	ASSERT_TRUE(sup.wait(15000, &out)) << "supervisor hung: " << out;
	EXPECT_TRUE(out_contains(out, "RESTART attempt=1")) << out;

	EXPECT_TRUE(out_contains(out, "gen=2")) << out;
}

TEST(Supervisor, StallDetectedKillAndRestart) {
	const std::string name = unique_channel_name("s3");
	auto sa = supervisor_args(name,
	                          g_producer_tool + " --name " + name +
	                                  " --heartbeat-only --heartbeat-interval-us 100000");
	SpawnedChild sup;
	ASSERT_TRUE(sup.spawn(sa));
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));

	ASSERT_EQ(signal_producer_child("STOP", name), 0);
	std::this_thread::sleep_for(std::chrono::milliseconds(3000));

	sup.kill(SIGTERM);
	std::string out;
	ASSERT_TRUE(sup.wait(15000, &out)) << "supervisor hung: " << out;
	EXPECT_TRUE(out_contains(out, "STALL_DETECTED")) << out;

	EXPECT_TRUE(out_contains(out, "KILLED sig=9")) << out;
	EXPECT_TRUE(out_contains(out, "RESTART attempt=1")) << out;
	EXPECT_TRUE(out_contains(out, "gen=2")) << out;
	EXPECT_TRUE(out_contains(out, "STOPPED")) << out;
}

TEST(Supervisor, CrashLoopCappedByGaveUp) {
	const std::string name = unique_channel_name("s4");

	auto sa = supervisor_args(name, "/bin/false",
	                          {"--max-restarts", "3", "--max-delay-ms", "1000",
	                           "--multiplier", "2",
	                           "--create-timeout-ms", "1000"});
	SpawnedChild sup;
	ASSERT_TRUE(sup.spawn(sa));
	std::string out;
	ASSERT_TRUE(sup.wait(30000, &out)) << "supervisor hung: " << out;
	EXPECT_EQ(sup.exit_code(), 3) << out;
	EXPECT_TRUE(out_contains(out, "GAVE_UP attempts=4 restarts=3")) << out;
	EXPECT_TRUE(out_contains(out, "RESTART attempt=1 delay=200000000ns")) << out;
	EXPECT_TRUE(out_contains(out, "RESTART attempt=2 delay=400000000ns")) << out;
	EXPECT_TRUE(out_contains(out, "RESTART attempt=3 delay=800000000ns")) << out;
	EXPECT_FALSE(out_contains(out, "RESTART attempt=4")) << out;
}

TEST(Supervisor, StopReapsChildCleanly) {
	const std::string name = unique_channel_name("s5");
	auto sa = supervisor_args(name, g_producer_tool + " --name " + name +
	                                         " --interval-us 100000");
	SpawnedChild sup;
	ASSERT_TRUE(sup.spawn(sa));
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));

	sup.kill(SIGTERM);
	std::string out;
	ASSERT_TRUE(sup.wait(15000, &out)) << "supervisor hung: " << out;
	EXPECT_EQ(sup.exit_code(), 0) << out;
	EXPECT_TRUE(out_contains(out, "STOPPED")) << out;
	EXPECT_FALSE(out_contains(out, "RESTART")) << out;
}

}

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc < 4) {
		std::fprintf(stderr,
		             "usage: supervisor_test <edge_shm_supervisor> <edge_shm_producer> "
		             "<edge_shm_consumer>\n");
		return 2;
	}
	g_supervisor_tool = argv[1];
	g_producer_tool = argv[2];
	g_consumer_tool = argv[3];
	return RUN_ALL_TESTS();
}
