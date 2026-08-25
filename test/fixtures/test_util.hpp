#ifndef EDGE_TEST_TEST_UTIL_HPP
#define EDGE_TEST_TEST_UTIL_HPP

// 测试夹具负责生成隔离通道名、启动独立子进程并收集退出状态和输出。
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
	int exit_code = -1;
	pid_t child_pid = 0;
	std::string stdout_text;
	bool timed_out = false;
};

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

class SpawnedChild {
       public:
	SpawnedChild() = default;
	SpawnedChild(const SpawnedChild&) = delete;
	SpawnedChild& operator=(const SpawnedChild&) = delete;
	~SpawnedChild() { stop(); }

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

	void kill(int sig = SIGTERM) {
		if (pid_ > 0 && !reaped_) ::kill(pid_, sig);
	}

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
			if (n <= 0) break;
			stdout_out->append(buf, static_cast<size_t>(n));
		}
		if (timed_out) ::kill(pid_, SIGKILL);

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

}

#endif
