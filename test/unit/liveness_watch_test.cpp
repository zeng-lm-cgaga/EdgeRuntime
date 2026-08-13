// v0.3 §35.3 LivenessWatch unit tests: long-held pidfd readability vs exit,
// CLOEXEC discipline, and ESRCH classification. The watched process is this
// binary re-exec'd as a child that sleeps/exits on demand.

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <thread>

#include "edge_runtime/detail/process_identity.hpp"
#include "edge_runtime/detail/process_spawn.hpp"
#include "edge_runtime/detail/shm_object.hpp"

namespace {

using edge_runtime::detail::LivenessWatch;
using edge_runtime::detail::spawn_process;

pid_t spawn_sleeper(uint32_t ms) {
	auto sp = spawn_process({"/proc/self/exe", "--watch-child", std::to_string(ms)});
	if (!sp) return -1;
	sp.value().stdout_read.reset();  // do not drain; the child prints nothing
	return sp.value().pid;
}

TEST(LivenessWatch, AliveThenExited) {
	const pid_t child = spawn_sleeper(300);
	ASSERT_GT(child, 0);

	auto watch = LivenessWatch::open(static_cast<uint64_t>(child));
	ASSERT_TRUE(watch) << edge_runtime::to_string(watch.error().code);
	EXPECT_FALSE(watch.value().exited()) << "live child must not be readable";

	int status = 0;
	ASSERT_EQ(::waitpid(child, &status, 0), child);
	ASSERT_TRUE(WIFEXITED(status));
	EXPECT_TRUE(watch.value().exited()) << "exited child must make the pidfd readable";
}

TEST(LivenessWatch, CloexecSet) {
	const pid_t child = spawn_sleeper(300);
	ASSERT_GT(child, 0);
	auto watch = LivenessWatch::open(static_cast<uint64_t>(child));
	ASSERT_TRUE(watch);
	const int flags = ::fcntl(watch.value().fd(), F_GETFD);
	ASSERT_GE(flags, 0);
	EXPECT_NE(flags & FD_CLOEXEC, 0) << "pidfd must be CLOEXEC (no fd leak into children)";
	int status = 0;
	ASSERT_EQ(::waitpid(child, &status, 0), child);
}

TEST(LivenessWatch, NonexistentPidFailsClosed) {
	auto watch = LivenessWatch::open(UINT32_MAX);
	ASSERT_FALSE(watch);
}

}  // namespace

int main(int argc, char** argv) {
	// Child mode: sleep argv[2] ms, print nothing, exit 0.
	if (argc >= 3 && std::strcmp(argv[1], "--watch-child") == 0) {
		const uint32_t ms = static_cast<uint32_t>(std::strtoul(argv[2], nullptr, 10));
		std::this_thread::sleep_for(std::chrono::milliseconds(ms));
		return 0;
	}
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
