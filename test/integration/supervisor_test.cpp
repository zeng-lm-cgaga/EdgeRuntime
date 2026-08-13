// ER13 integration driver (v0.3 §35): ProducerSupervisor restart semantics
// across processes. Every case fork+execs the SEPARATE tool binaries
// (design §18.3):
//
//   S1  clean exit (child finishes by itself) -> CLEAN_EXIT, no restart, exit 0
//   S2  crash (SIGKILL) -> RESTART + SUPERVISED gen=2; a consumer then reads
//       the replacement instance (generation+1 recovery path)
//   S3  stall (SIGSTOP of the heartbeat-only child) -> STALL_DETECTED ->
//       KILLED sig=9 (SIGTERM pending on a stopped process) -> RESTART
//   S4  crash loop -> RESTART x max_restarts -> GAVE_UP, non-zero exit
//   S5  stop: SIGTERM to the supervisor -> STOPPED, exit 0, child reaped

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

// Signal the supervised producer child. The pattern is anchored at ^ so it can
// only match the exec'd producer's own cmdline — never the supervisor's
// (which contains the pattern mid-string) and never this driver's shell.
int signal_producer_child(const std::string& sig, const std::string& name) {
	const std::string pattern = "^" + g_producer_tool + " --name " + name + " ";
	const std::string cmd = "pkill -" + sig + " -f \"" + pattern + "\"";
	return std::system(cmd.c_str());
}

TEST(Supervisor, CleanExitNoRestart) {  // S1
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

TEST(Supervisor, CrashRestartsAtGenerationPlusOne) {  // S2
	const std::string name = unique_channel_name("s2");
	auto sa = supervisor_args(name, g_producer_tool + " --name " + name +
	                                         " --interval-us 100000",
	                          {"--create-timeout-ms", "5000"});
	SpawnedChild sup;
	ASSERT_TRUE(sup.spawn(sa));
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));

	ASSERT_EQ(signal_producer_child("KILL", name), 0);

	std::this_thread::sleep_for(std::chrono::milliseconds(2500));

	// A fresh consumer must be able to open and read the replacement instance.
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
	// gen=2 proves the replacement went through the dead-owner path.
	EXPECT_TRUE(out_contains(out, "gen=2")) << out;
}

TEST(Supervisor, StallDetectedKillAndRestart) {  // S3
	const std::string name = unique_channel_name("s3");
	auto sa = supervisor_args(name,
	                          g_producer_tool + " --name " + name +
	                                  " --heartbeat-only --heartbeat-interval-us 100000");
	SpawnedChild sup;
	ASSERT_TRUE(sup.spawn(sa));
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));

	// Freeze the heartbeat-only child (SIGSTOP victim, the C18 scenario): the
	// supervisor must classify the stall and take over.
	ASSERT_EQ(signal_producer_child("STOP", name), 0);
	std::this_thread::sleep_for(std::chrono::milliseconds(3000));

	sup.kill(SIGTERM);
	std::string out;
	ASSERT_TRUE(sup.wait(15000, &out)) << "supervisor hung: " << out;
	EXPECT_TRUE(out_contains(out, "STALL_DETECTED")) << out;
	// SIGTERM stays pending on the stopped process; the grace escalation kills.
	EXPECT_TRUE(out_contains(out, "KILLED sig=9")) << out;
	EXPECT_TRUE(out_contains(out, "RESTART attempt=1")) << out;
	EXPECT_TRUE(out_contains(out, "gen=2")) << out;
	EXPECT_TRUE(out_contains(out, "STOPPED")) << out;
}

TEST(Supervisor, CrashLoopCappedByGaveUp) {  // S4
	const std::string name = unique_channel_name("s4");
	// /bin/false exits 1 instantly without ever creating the channel: a
	// deterministic crash loop (death before READY counts as a failure).
	auto sa = supervisor_args(name, "/bin/false",
	                          {"--max-restarts", "3", "--max-delay-ms", "200",
	                           "--create-timeout-ms", "1000"});
	SpawnedChild sup;
	ASSERT_TRUE(sup.spawn(sa));
	std::string out;
	ASSERT_TRUE(sup.wait(30000, &out)) << "supervisor hung: " << out;
	EXPECT_EQ(sup.exit_code(), 3) << out;  // GAVE_UP exit code
	EXPECT_TRUE(out_contains(out, "GAVE_UP")) << out;
	EXPECT_TRUE(out_contains(out, "RESTART attempt=1")) << out;
	EXPECT_TRUE(out_contains(out, "RESTART attempt=2")) << out;
	EXPECT_TRUE(out_contains(out, "RESTART attempt=3")) << out;
	EXPECT_FALSE(out_contains(out, "RESTART attempt=4")) << out;
}

TEST(Supervisor, StopReapsChildCleanly) {  // S5
	const std::string name = unique_channel_name("s5");
	auto sa = supervisor_args(name, g_producer_tool + " --name " + name +
	                                         " --interval-us 100000");
	SpawnedChild sup;
	ASSERT_TRUE(sup.spawn(sa));
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));

	sup.kill(SIGTERM);  // supervisor receives it via signalfd -> stop sequence
	std::string out;
	ASSERT_TRUE(sup.wait(15000, &out)) << "supervisor hung: " << out;
	EXPECT_EQ(sup.exit_code(), 0) << out;
	EXPECT_TRUE(out_contains(out, "STOPPED")) << out;
	EXPECT_FALSE(out_contains(out, "RESTART")) << out;
}

}  // namespace

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
