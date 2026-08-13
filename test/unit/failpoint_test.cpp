// Failpoint infrastructure (design §20.3/§22.3): link-section registry, env
// activation, and the stop/crash/log modes. Activation is read once per
// process, so every behavioural test runs in a forked child that sets its own
// environment; the parent observes the child's fate (exited vs SIGSTOPped).

#include "edge_runtime/detail/failpoint.hpp"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>

namespace {

using edge_runtime::detail::failpoint_list;

// A failpoint wired into the test binary itself; it also proves the section
// registry picks up failpoints from any TU in the link.
void hit_utest_fp() { EDGE_FAILPOINT(UTEST); }

TEST(Failpoint, RegistryEnumeratesWiredIds) {
	const char* const* ids = nullptr;
	const size_t n = failpoint_list(&ids);
	ASSERT_GT(n, 0u) << "the library and this TU both wire failpoints";
	bool found_utest = false;
	for (size_t i = 0; i < n; ++i) {
		if (std::strcmp(ids[i], "UTEST") == 0) found_utest = true;
	}
	EXPECT_TRUE(found_utest);
}

TEST(Failpoint, InertWhenEnvUnset) {
	const pid_t pid = ::fork();
	ASSERT_GE(pid, 0);
	if (pid == 0) {
		::unsetenv("EDGE_FAILPOINT");
		::unsetenv("EDGE_FAILPOINT_MODE");
		::unsetenv("EDGE_FAILPOINT_COUNT");
		hit_utest_fp();  // must return without acting
		::_exit(0);
	}
	int status = 0;
	ASSERT_EQ(::waitpid(pid, &status, 0), pid);
	EXPECT_TRUE(WIFEXITED(status));
	EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(Failpoint, LogModePrintsAndContinues) {
	char tmpl[] = "/tmp/edge_fp_log_XXXXXX";
	const int fd = ::mkstemp(tmpl);
	ASSERT_GE(fd, 0);
	::close(fd);

	const pid_t pid = ::fork();
	ASSERT_GE(pid, 0);
	if (pid == 0) {
		const int out = ::open(tmpl, O_WRONLY | O_TRUNC);
		::dup2(out, STDERR_FILENO);
		::close(out);
		::setenv("EDGE_FAILPOINT", "UTEST", 1);
		::setenv("EDGE_FAILPOINT_MODE", "log", 1);
		::unsetenv("EDGE_FAILPOINT_COUNT");
		hit_utest_fp();  // logs the marker and continues
		::_exit(0);      // _exit would never run if it SIGSTOPped
	}
	int status = 0;
	ASSERT_EQ(::waitpid(pid, &status, 0), pid);
	EXPECT_TRUE(WIFEXITED(status));
	EXPECT_EQ(WEXITSTATUS(status), 0);

	char buf[256];
	const int in = ::open(tmpl, O_RDONLY);
	ASSERT_GE(in, 0);
	const ssize_t n = ::read(in, buf, sizeof(buf) - 1);
	::close(in);
	::unlink(tmpl);
	ASSERT_GT(n, 0);
	buf[static_cast<size_t>(n)] = '\0';
	EXPECT_NE(std::string(buf).find("FAILPOINT hit id=UTEST"), std::string::npos);
}

TEST(Failpoint, StopModeSigsStops) {
	const pid_t pid = ::fork();
	ASSERT_GE(pid, 0);
	if (pid == 0) {
		::setenv("EDGE_FAILPOINT", "UTEST", 1);
		::setenv("EDGE_FAILPOINT_MODE", "stop", 1);
		::unsetenv("EDGE_FAILPOINT_COUNT");
		hit_utest_fp();  // SIGSTOPs the whole process
		::_exit(0);      // unreachable (SIGSTOP cannot be caught/ignored)
	}
	int status = 0;
	ASSERT_EQ(::waitpid(pid, &status, WUNTRACED), pid);
	EXPECT_TRUE(WIFSTOPPED(status));
	EXPECT_EQ(WSTOPSIG(status), SIGSTOP);
	::kill(pid, SIGKILL);
	ASSERT_EQ(::waitpid(pid, &status, 0), pid);
	EXPECT_TRUE(WIFSIGNALED(status));
	EXPECT_EQ(WTERMSIG(status), SIGKILL);
}

TEST(Failpoint, CountFiresOnNthHit) {
	// COUNT=3: the first two hits are no-ops (proven by the marker written
	// before the third), the third SIGSTOPs.
	char marker[] = "/tmp/edge_fp_cnt_XXXXXX";
	const int fd = ::mkstemp(marker);
	ASSERT_GE(fd, 0);
	::close(fd);
	::unlink(marker);

	const pid_t pid = ::fork();
	ASSERT_GE(pid, 0);
	if (pid == 0) {
		::setenv("EDGE_FAILPOINT", "UTEST", 1);
		::setenv("EDGE_FAILPOINT_MODE", "stop", 1);
		::setenv("EDGE_FAILPOINT_COUNT", "3", 1);
		hit_utest_fp();
		hit_utest_fp();
		const int m = ::open(marker, O_CREAT | O_WRONLY, 0600);
		if (m >= 0) {
			const char x = 'x';
			::write(m, &x, 1);
			::close(m);
		}
		hit_utest_fp();
		::_exit(0);
	}
	int status = 0;
	ASSERT_EQ(::waitpid(pid, &status, WUNTRACED), pid);
	EXPECT_TRUE(WIFSTOPPED(status));
	::kill(pid, SIGKILL);
	::waitpid(pid, &status, 0);

	const int r = ::open(marker, O_RDONLY);
	if (r >= 0) {
		::close(r);
		::unlink(marker);
	}
	EXPECT_GE(r, 0) << "first two hits must be no-ops (marker written) before the 3rd stops";
}

}  // namespace
