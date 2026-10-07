

#include "edge_runtime/process/process_identity.hpp"

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdint>

namespace {

using edge_runtime::detail::current_process_identity;
using edge_runtime::detail::identity_matches_current;
using edge_runtime::detail::Liveness;
using edge_runtime::detail::probe_liveness;
using edge_runtime::detail::proc_stat_starttime;

TEST(ProcessIdentity, SelfStarttimeParses) {
	auto r = proc_stat_starttime(getpid());
	ASSERT_TRUE(r) << "starttime parse failed";
	EXPECT_GT(r.value(), 0u);
}

TEST(ProcessIdentity, CurrentIdentityMatches) {
	auto id = current_process_identity();
	EXPECT_EQ(id.pid, static_cast<uint64_t>(getpid()));
	EXPECT_GT(id.proc_start_ticks, 0u);
	EXPECT_TRUE(identity_matches_current(id));
}

TEST(ProcessIdentity, SelfIsAlive) {
	auto id = current_process_identity();
	EXPECT_EQ(probe_liveness(id.pid, id.proc_start_ticks), Liveness::kAlive);
}

TEST(ProcessIdentity, ReapedChildIsExited) {
	const pid_t child = ::fork();
	ASSERT_GE(child, 0);
	if (child == 0) {
		_exit(0);
	}
	int status = 0;
	::waitpid(child, &status, 0);

	EXPECT_EQ(probe_liveness(static_cast<uint64_t>(child), 0), Liveness::kExited);
}

TEST(ProcessIdentity, ImplausiblePidIsExited) {
	EXPECT_EQ(probe_liveness(UINT64_MAX, 0), Liveness::kExited);
}

TEST(ProcessIdentity, StaleStarttimeIsPidReuse) {

	EXPECT_EQ(probe_liveness(1, 0), Liveness::kPidReused);
}

}
