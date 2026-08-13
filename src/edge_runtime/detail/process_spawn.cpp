#include "edge_runtime/detail/process_spawn.hpp"

#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>

namespace edge_runtime::detail {

namespace {
// Between fork() and execv() only async-signal-safe calls are permitted. All
// allocation and string handling therefore happens BEFORE the fork.
std::vector<char*> build_c_argv(const std::vector<std::string>& argv) {
	std::vector<char*> c_argv;
	c_argv.reserve(argv.size() + 1);
	for (const std::string& a : argv) {
		c_argv.push_back(const_cast<char*>(a.c_str()));
	}
	c_argv.push_back(nullptr);
	return c_argv;
}
}  // namespace

Result<SpawnedProcess> spawn_process(const std::vector<std::string>& argv) noexcept {
	if (argv.empty() || argv[0].empty()) {
		return make_error(ErrorCode::kInvalidOptions, "spawn_process", "empty argv");
	}
	std::vector<char*> c_argv = build_c_argv(argv);

	int pipefd[2] = {-1, -1};
	if (::pipe2(pipefd, O_NONBLOCK | O_CLOEXEC) != 0) {
		return make_error(classify_errno(errno), "spawn_process", std::strerror(errno));
	}
	UniqueFd read_end(pipefd[0]);
	UniqueFd write_end(pipefd[1]);

	const pid_t pid = ::fork();
	if (pid < 0) {
		return make_error(classify_errno(errno), "spawn_process", std::strerror(errno));
	}
	if (pid == 0) {
		// Child: stdout -> pipe write end (already O_NONBLOCK|O_CLOEXEC). Also
		// reset the signal mask: the supervisor blocks SIGTERM/SIGINT in ITS
		// thread, and the mask survives fork — without this the child would
		// never receive the SIGTERM a takeover/stop sequence sends it.
		sigset_t empty_mask;
		sigemptyset(&empty_mask);
		(void)::pthread_sigmask(SIG_SETMASK, &empty_mask, nullptr);
		if (::dup2(write_end.get(), STDOUT_FILENO) < 0) ::_exit(127);
		write_end.reset();
		read_end.reset();
		::execv(c_argv[0], c_argv.data());
		std::fprintf(stderr, "spawn_process execv %s: %s\n", c_argv[0], std::strerror(errno));
		::_exit(127);
	}

	write_end.reset();
	SpawnedProcess sp;
	sp.pid = pid;
	sp.stdout_read = std::move(read_end);
	return Result<SpawnedProcess>(std::move(sp));
}

}  // namespace edge_runtime::detail
