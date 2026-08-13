#ifndef EDGE_TEST_TEST_UTIL_HPP
#define EDGE_TEST_TEST_UTIL_HPP

// Shared helpers for integration/crash drivers (design §18.3): unique channel
// names, and the fork+exec harness used by every cross-process test. The child
// is always a SEPARATE binary (never the driver image) so a reopen is a true
// reopen and not an inherited mapping.

#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace edge_test {

// Unique channel name: <tag>_<pid>_<counter>. pids differ across processes and
// the counter within a process, so concurrent/rerun instances never collide.
inline std::string unique_channel_name(const char* tag) {
	static std::atomic<uint64_t> counter{0};
	const uint64_t n = counter.fetch_add(1);
	char buf[96];
	std::snprintf(buf, sizeof(buf), "%s_%ld_%llu", tag, static_cast<long>(getpid()),
	              static_cast<unsigned long long>(n));
	return buf;
}

inline int64_t monotonic_ms_now() {
	struct timespec ts {};
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

struct ChildResult {
	int exit_code = -1;  // -1: exec/launch failure, or killed on timeout
	pid_t child_pid = 0;
	std::string stdout_text;
	bool timed_out = false;
};

// fork()+exec(argv[0], argv...) with stdout captured on a pipe; the driver
// polls with a hard deadline and SIGKILLs the child on timeout. stderr is
// inherited so the child's diagnostics surface in the test log.
inline ChildResult run_child_capture(const std::vector<std::string>& argv, int timeout_ms) {
	ChildResult result;
	int pipefd[2] = {-1, -1};
	if (::pipe(pipefd) != 0) return result;

	const pid_t pid = ::fork();
	if (pid < 0) {
		::close(pipefd[0]);
		::close(pipefd[1]);
		return result;
	}
	if (pid == 0) {
		::close(pipefd[0]);
		if (::dup2(pipefd[1], STDOUT_FILENO) < 0) _exit(127);
		std::vector<char*> args;
		args.reserve(argv.size() + 1);
		for (const std::string& a : argv) {
			args.push_back(const_cast<char*>(a.c_str()));
		}
		args.push_back(nullptr);
		::execv(args[0], args.data());
		std::fprintf(stderr, "execv %s: %s\n", args[0], std::strerror(errno));
		_exit(127);
	}
	result.child_pid = pid;
	::close(pipefd[1]);

	char buf[4096];
	const int64_t deadline = monotonic_ms_now() + timeout_ms;
	while (true) {
		const int64_t remaining = deadline - monotonic_ms_now();
		if (remaining <= 0) {
			result.timed_out = true;
			break;
		}
		struct pollfd pfd {};
		pfd.fd = pipefd[0];
		pfd.events = POLLIN;
		const int rc = ::poll(&pfd, 1, static_cast<int>(remaining));
		if (rc < 0) {
			if (errno == EINTR) continue;
			break;
		}
		if (rc == 0) {
			result.timed_out = true;
			break;
		}
		if ((pfd.revents & (POLLIN | POLLHUP)) == 0) break;
		const ssize_t n = ::read(pipefd[0], buf, sizeof(buf));
		if (n <= 0) break;
		result.stdout_text.append(buf, static_cast<size_t>(n));
	}
	::close(pipefd[0]);

	if (result.timed_out) ::kill(pid, SIGKILL);
	int status = 0;
	::waitpid(pid, &status, 0);
	if (result.timed_out) {
		result.exit_code = -1;
	} else if (WIFEXITED(status)) {
		result.exit_code = WEXITSTATUS(status);
	}
	return result;
}

// SpawnedChild: like run_child_capture but non-blocking — the child keeps
// running until wait()/kill()/destruction, which is what overlapping-process
// tests (I04 consumer-first, I05/I06 duplicate endpoints) need. stdout is
// captured on a pipe and drained by wait(). The destructor SIGKILLs any child
// not yet reaped, so a leaked live child never outlives the driver.
class SpawnedChild {
       public:
	SpawnedChild() = default;
	SpawnedChild(const SpawnedChild&) = delete;
	SpawnedChild& operator=(const SpawnedChild&) = delete;
	~SpawnedChild() { stop(); }

	// fork+exec; stdout -> pipe. Returns false on any failure (never leaves a
	// half-spawned child).
	bool spawn(const std::vector<std::string>& argv) {
		if (pid_ > 0) return false;
		int pipefd[2] = {-1, -1};
		if (::pipe(pipefd) != 0) return false;
		const pid_t pid = ::fork();
		if (pid < 0) {
			::close(pipefd[0]);
			::close(pipefd[1]);
			return false;
		}
		if (pid == 0) {
			::close(pipefd[0]);
			if (::dup2(pipefd[1], STDOUT_FILENO) < 0) _exit(127);
			std::vector<char*> args;
			args.reserve(argv.size() + 1);
			for (const std::string& a : argv)
				args.push_back(const_cast<char*>(a.c_str()));
			args.push_back(nullptr);
			::execv(args[0], args.data());
			std::fprintf(stderr, "execv %s: %s\n", args[0], std::strerror(errno));
			_exit(127);
		}
		pid_ = pid;
		pipe_read_ = pipefd[0];
		::close(pipefd[1]);
		return true;
	}

	pid_t pid() const { return pid_; }
	bool reaped() const { return reaped_; }

	// Send a signal to the child (no-op if already reaped).
	void kill(int sig = SIGTERM) {
		if (pid_ > 0 && !reaped_) ::kill(pid_, sig);
	}

	// Block until the child exits (draining stdout), or timeout_ms passes. On
	// timeout the child is SIGKILLed and reaped; returns false with partial
	// stdout in *stdout_out. Returns true on clean reap.
	bool wait(int timeout_ms, std::string* stdout_out) {
		stdout_out->clear();
		if (pid_ <= 0 || reaped_) return reaped_;
		char buf[4096];
		const int64_t deadline = monotonic_ms_now() + timeout_ms;
		bool timed_out = false;
		while (!timed_out) {
			const int64_t remaining = deadline - monotonic_ms_now();
			if (remaining <= 0) {
				timed_out = true;
				break;
			}
			struct pollfd pfd {};
			pfd.fd = pipe_read_;
			pfd.events = POLLIN;
			const int rc = ::poll(&pfd, 1, static_cast<int>(remaining));
			if (rc < 0) {
				if (errno == EINTR) continue;
				timed_out = true;
				break;
			}
			if (rc == 0) {
				timed_out = true;
				break;
			}
			if ((pfd.revents & (POLLIN | POLLHUP)) == 0) continue;
			const ssize_t n = ::read(pipe_read_, buf, sizeof(buf));
			if (n <= 0) break;  // EOF (POLLHUP) or error: child closed stdout
			stdout_out->append(buf, static_cast<size_t>(n));
		}
		if (timed_out) ::kill(pid_, SIGKILL);
		// drain any residual output then reap
		while (true) {
			const ssize_t n = ::read(pipe_read_, buf, sizeof(buf));
			if (n <= 0) break;
			stdout_out->append(buf, static_cast<size_t>(n));
		}
		::close(pipe_read_);
		pipe_read_ = -1;
		int status = 0;
		::waitpid(pid_, &status, 0);
		const bool exited = WIFEXITED(status);
		exit_code_ = exited ? WEXITSTATUS(status) : -1;
		reaped_ = true;
		if (timed_out) return false;
		return exited;
	}

	int exit_code() const { return exit_code_; }

       private:
	void stop() {
		if (pid_ > 0 && !reaped_) {
			::kill(pid_, SIGKILL);
			if (pipe_read_ >= 0) ::close(pipe_read_);
			::waitpid(pid_, nullptr, 0);
			reaped_ = true;
		}
	}

	pid_t pid_ = -1;
	int pipe_read_ = -1;
	bool reaped_ = false;
	int exit_code_ = -1;
};

}  // namespace edge_test

#endif  // EDGE_TEST_TEST_UTIL_HPP
