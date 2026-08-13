// v0.3 §35.3 process_spawn unit tests: fork+exec of a separate binary, stdout
// capture with a non-blocking drain (a verbose child must never wedge the
// supervisor), and exec-failure handling. The helper binary is the unit test
// itself re-invoked with --spawn-child (a SEPARATE exec, never a shared
// mapping — design §18.3).

#include <gtest/gtest.h>

#include <fcntl.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "edge_runtime/detail/process_spawn.hpp"
#include "edge_runtime/detail/shm_object.hpp"

namespace {

using edge_runtime::detail::SpawnedProcess;
using edge_runtime::detail::spawn_process;

// Child mode of THIS binary: prints `n` lines of `len` bytes each.
int run_child_mode(int argc, char** argv) {
	if (argc < 4 || std::strcmp(argv[1], "--spawn-child") != 0) return -1;
	const int lines = std::atoi(argv[2]);
	const int len = std::atoi(argv[3]);
	std::string line(static_cast<size_t>(len), 'x');
	line += "\n";
	for (int i = 0; i < lines; ++i) {
		std::fputs(line.c_str(), stdout);
	}
	std::fflush(stdout);
	return 0;
}

// Drain the non-blocking read end until EOF, with a poll deadline.
std::string drain_with_deadline(const edge_runtime::detail::UniqueFd& read_end, int timeout_ms,
                                bool* timed_out) {
	std::string out;
	*timed_out = false;
	const int64_t deadline = timeout_ms * 1000;
	int64_t spent = 0;
	char buf[4096];
	while (true) {
		struct pollfd pfd {};
		pfd.fd = read_end.get();
		pfd.events = POLLIN;
		const int rc = ::poll(&pfd, 1, 10);
		if (rc > 0) {
			for (;;) {
				const ssize_t n = ::read(read_end.get(), buf, sizeof(buf));
				if (n > 0) {
					out.append(buf, static_cast<size_t>(n));
					continue;
				}
				if (n == 0) return out;  // EOF: child exited
				if (errno == EAGAIN || errno == EWOULDBLOCK) break;
				if (errno == EINTR) continue;
				return out;
			}
		} else if (rc < 0 && errno != EINTR) {
			return out;
		}
		spent += 10'000;
		if (spent >= deadline) {
			*timed_out = true;
			return out;
		}
	}
}

TEST(ProcessSpawn, SpawnCaptureAndReap) {
	const std::string self = "/proc/self/exe";
	auto sp = spawn_process({self, "--spawn-child", "5", "80"});
	ASSERT_TRUE(sp) << edge_runtime::to_string(sp.error().code);
	bool timed_out = false;
	const std::string out = drain_with_deadline(sp.value().stdout_read, 5000, &timed_out);
	ASSERT_FALSE(timed_out);
	EXPECT_EQ(std::count(out.begin(), out.end(), '\n'), 5);

	int status = 0;
	ASSERT_EQ(::waitpid(sp.value().pid, &status, 0), sp.value().pid);
	ASSERT_TRUE(WIFEXITED(status));
	EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(ProcessSpawn, VerboseChildNeverBlocks) {
	// ~200 KiB of output far exceeds the 64 KiB pipe buffer. The NONBLOCK
	// write end makes writes beyond the buffer capacity drop instead of
	// blocking, so the child completes promptly even when the parent drains
	// lazily — a blocking write end would hang this exact scenario (the
	// supervisor's false-stall-kill-loop failure mode, design §35.3). Dropped
	// bytes are the documented tradeoff; the supervisor drains continuously
	// so they only occur in the pathological overrun case.
	const std::string self = "/proc/self/exe";
	auto sp = spawn_process({self, "--spawn-child", "3000", "80"});
	ASSERT_TRUE(sp) << edge_runtime::to_string(sp.error().code);
	bool timed_out = false;
	const std::string out = drain_with_deadline(sp.value().stdout_read, 10000, &timed_out);
	ASSERT_FALSE(timed_out) << "child blocked on a full pipe";
	EXPECT_GT(std::count(out.begin(), out.end(), '\n'), 0);
	int status = 0;
	ASSERT_EQ(::waitpid(sp.value().pid, &status, 0), sp.value().pid);
	ASSERT_TRUE(WIFEXITED(status));
	EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(ProcessSpawn, ExecFailureExits127) {
	auto sp = spawn_process({"/nonexistent/definitely/not/a/binary"});
	ASSERT_TRUE(sp) << edge_runtime::to_string(sp.error().code);
	int status = 0;
	ASSERT_EQ(::waitpid(sp.value().pid, &status, 0), sp.value().pid);
	ASSERT_TRUE(WIFEXITED(status));
	EXPECT_EQ(WEXITSTATUS(status), 127);
}

TEST(ProcessSpawn, EmptyArgvRejected) {
	auto sp = spawn_process({});
	ASSERT_FALSE(sp);
	EXPECT_EQ(sp.error().code, edge_runtime::ErrorCode::kInvalidOptions);
}

}  // namespace

int main(int argc, char** argv) {
	const int child_rc = run_child_mode(argc, argv);
	if (child_rc >= 0) return child_rc;
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
