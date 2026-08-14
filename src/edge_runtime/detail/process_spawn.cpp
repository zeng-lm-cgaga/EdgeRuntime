#include "edge_runtime/detail/process_spawn.hpp"

#include <fcntl.h>
#include <spawn.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

extern char** environ;

namespace edge_runtime::detail {

namespace {
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
	std::vector<char*> c_argv;
	try {
		c_argv = build_c_argv(argv);
	} catch (...) {
		return make_error(ErrorCode::kSystemError, "spawn_process", "argv allocation failed");
	}

	int pipefd[2] = {-1, -1};
	if (::pipe2(pipefd, O_NONBLOCK | O_CLOEXEC) != 0) {
		const int e = errno;
		return make_errno_error(e, "spawn_process", std::strerror(e));
	}
	UniqueFd read_end(pipefd[0]);
	UniqueFd write_end(pipefd[1]);

	posix_spawn_file_actions_t actions;
	int rc = ::posix_spawn_file_actions_init(&actions);
	if (rc != 0) {
		return make_errno_error(rc, "spawn_process", std::strerror(rc));
	}
	const auto destroy_actions = [&actions] { (void)::posix_spawn_file_actions_destroy(&actions); };
	rc = ::posix_spawn_file_actions_addclose(&actions, read_end.get());
	if (rc == 0) {
		rc = ::posix_spawn_file_actions_adddup2(&actions, write_end.get(), STDOUT_FILENO);
	}
	if (rc == 0 && write_end.get() != STDOUT_FILENO) {
		rc = ::posix_spawn_file_actions_addclose(&actions, write_end.get());
	}
	if (rc != 0) {
		destroy_actions();
		return make_errno_error(rc, "spawn_process", std::strerror(rc));
	}

	posix_spawnattr_t attributes;
	rc = ::posix_spawnattr_init(&attributes);
	if (rc != 0) {
		destroy_actions();
		return make_errno_error(rc, "spawn_process", std::strerror(rc));
	}
	const auto destroy_attributes = [&attributes] { (void)::posix_spawnattr_destroy(&attributes); };
	sigset_t empty_mask;
	sigemptyset(&empty_mask);
	rc = ::posix_spawnattr_setsigmask(&attributes, &empty_mask);
	if (rc == 0) {
		rc = ::posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETSIGMASK);
	}
	if (rc != 0) {
		destroy_attributes();
		destroy_actions();
		return make_errno_error(rc, "spawn_process", std::strerror(rc));
	}

	pid_t pid = -1;
	rc = ::posix_spawn(&pid, c_argv[0], &actions, &attributes, c_argv.data(), environ);
	destroy_attributes();
	destroy_actions();
	if (rc != 0) {
		return make_errno_error(rc, "spawn_process", std::strerror(rc));
	}
	write_end.reset();
	SpawnedProcess sp;
	sp.pid = pid;
	sp.stdout_read = std::move(read_end);
	return Result<SpawnedProcess>(std::move(sp));
}

}  // namespace edge_runtime::detail
