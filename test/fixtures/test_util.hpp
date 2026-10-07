#ifndef EDGE_TEST_TEST_UTIL_HPP
#define EDGE_TEST_TEST_UTIL_HPP

// 测试夹具负责生成隔离通道名、启动独立子进程并收集退出状态和输出。
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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

class FutexTracePipe {
	public:
	FutexTracePipe() = default;
	FutexTracePipe(const FutexTracePipe&) = delete;
	FutexTracePipe& operator=(const FutexTracePipe&) = delete;
	~FutexTracePipe() {
		if (read_fd_ >= 0) ::close(read_fd_);
		if (write_fd_ >= 0) ::close(write_fd_);
		if (status_read_fd_ >= 0) ::close(status_read_fd_);
		if (status_write_fd_ >= 0) ::close(status_write_fd_);
	}

	bool open() {
		if (read_fd_ >= 0 || write_fd_ >= 0 || status_read_fd_ >= 0 || status_write_fd_ >= 0) {
			return false;
		}
		int trace_pipe[2] = {-1, -1};
		int status_pipe[2] = {-1, -1};
		if (::pipe(trace_pipe) != 0 || ::pipe(status_pipe) != 0) {
			if (trace_pipe[0] >= 0) ::close(trace_pipe[0]);
			if (trace_pipe[1] >= 0) ::close(trace_pipe[1]);
			if (status_pipe[0] >= 0) ::close(status_pipe[0]);
			if (status_pipe[1] >= 0) ::close(status_pipe[1]);
			return false;
		}
		const int trace_read_flags = ::fcntl(trace_pipe[0], F_GETFL, 0);
		const int trace_write_flags = ::fcntl(trace_pipe[1], F_GETFL, 0);
		const int status_read_flags = ::fcntl(status_pipe[0], F_GETFL, 0);
		const int status_write_flags = ::fcntl(status_pipe[1], F_GETFL, 0);
		if (trace_read_flags < 0 || trace_write_flags < 0 || status_read_flags < 0 ||
			status_write_flags < 0 ||
			::fcntl(trace_pipe[0], F_SETFL, trace_read_flags | O_NONBLOCK) != 0 ||
			::fcntl(trace_pipe[1], F_SETFL, trace_write_flags | O_NONBLOCK) != 0 ||
			::fcntl(status_pipe[0], F_SETFL, status_read_flags | O_NONBLOCK) != 0 ||
			::fcntl(status_pipe[1], F_SETFL, status_write_flags | O_NONBLOCK) != 0) {
			::close(trace_pipe[0]);
			::close(trace_pipe[1]);
			::close(status_pipe[0]);
			::close(status_pipe[1]);
			return false;
		}
		read_fd_ = trace_pipe[0];
		write_fd_ = trace_pipe[1];
		status_read_fd_ = status_pipe[0];
		status_write_fd_ = status_pipe[1];
		return true;
	}

	int read_fd() const { return read_fd_; }
	int write_fd() const { return write_fd_; }
	int status_read_fd() const { return status_read_fd_; }
	int status_write_fd() const { return status_write_fd_; }

	bool set_trace_capacity(int bytes) {
		if (write_fd_ < 0 || bytes <= 0) return false;
		if (::fcntl(write_fd_, F_SETPIPE_SZ, bytes) < 0) return false;
		const int actual = ::fcntl(write_fd_, F_GETPIPE_SZ, 0);
		return actual >= bytes;
	}

	void drain() {
		if (read_fd_ < 0) return;
		char buffer[4096];
		for (;;) {
			const ssize_t count = ::read(read_fd_, buffer, sizeof(buffer));
			if (count > 0) {
				transcript_.append(buffer, static_cast<size_t>(count));
				continue;
			}
			if (count < 0 && errno == EINTR) continue;
			if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
			return;
		}
	}

	void drain_status() {
		if (status_read_fd_ < 0) return;
		char buffer[4096];
		for (;;) {
			const ssize_t count = ::read(status_read_fd_, buffer, sizeof(buffer));
			if (count > 0) {
				status_transcript_.append(buffer, static_cast<size_t>(count));
				continue;
			}
			if (count < 0 && errno == EINTR) continue;
			if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
			return;
		}
	}

	bool wait_for_status(pid_t pid, uint64_t* attempted, uint64_t* emitted, uint64_t* dropped,
	                     bool* complete, int timeout_ms) {
		const int64_t deadline = monotonic_ms_now() + timeout_ms;
		for (;;) {
			drain_status();
			if (find_status(pid, attempted, emitted, dropped, complete)) return true;
			const int64_t remaining = deadline - monotonic_ms_now();
			if (remaining <= 0 || status_read_fd_ < 0) return false;
			struct pollfd descriptor {status_read_fd_, POLLIN, 0};
			const int rc = ::poll(&descriptor, 1, static_cast<int>(remaining));
			if (rc < 0 && errno == EINTR) continue;
			if (rc <= 0) return false;
		}
	}

	bool wait_for(const char* needle, int timeout_ms) {
		return wait_for_count(needle, 1, timeout_ms);
	}

	bool wait_for_count(const char* needle, size_t expected, int timeout_ms) {
		const int64_t deadline = monotonic_ms_now() + timeout_ms;
		for (;;) {
			drain();
			if (count(needle) >= expected) return true;
			const int64_t remaining = deadline - monotonic_ms_now();
			if (remaining <= 0 || read_fd_ < 0) return false;
			struct pollfd descriptor {read_fd_, POLLIN, 0};
			const int rc = ::poll(&descriptor, 1, static_cast<int>(remaining));
			if (rc < 0 && errno == EINTR) continue;
			if (rc <= 0) return false;
		}
	}

	bool wait_for_pid_event(pid_t pid, const char* needle, size_t expected, int timeout_ms) {
		const int64_t deadline = monotonic_ms_now() + timeout_ms;
		for (;;) {
			drain();
			if (count_pid_events(pid, needle) >= expected) return true;
			const int64_t remaining = deadline - monotonic_ms_now();
			if (remaining <= 0 || read_fd_ < 0) return false;
			struct pollfd descriptor {read_fd_, POLLIN, 0};
			const int rc = ::poll(&descriptor, 1, static_cast<int>(remaining));
			if (rc < 0 && errno == EINTR) continue;
			if (rc <= 0) return false;
		}
	}

	bool no_new(const char* needle, int quiet_ms) {
		drain();
		const size_t start = transcript_.size();
		const int64_t deadline = monotonic_ms_now() + quiet_ms;
		for (;;) {
			const int64_t remaining = deadline - monotonic_ms_now();
			if (remaining <= 0) break;
			struct pollfd descriptor {read_fd_, POLLIN, 0};
			const int rc = ::poll(&descriptor, 1, static_cast<int>(remaining));
			if (rc < 0 && errno == EINTR) continue;
			if (rc <= 0) break;
			drain();
			if (transcript_.find(needle, start) != std::string::npos) return false;
		}
		drain();
		return transcript_.find(needle, start) == std::string::npos;
	}

	size_t count(const char* needle) const {
		if (needle == nullptr || needle[0] == '\0') return 0;
		size_t result = 0;
		size_t offset = 0;
		const size_t length = std::strlen(needle);
		while (offset < transcript_.size()) {
			const size_t found = transcript_.find(needle, offset);
			if (found == std::string::npos) break;
			++result;
			offset = found + length;
		}
		return result;
	}

	size_t count_pid_events(pid_t pid, const char* needle) const {
		if (needle == nullptr || needle[0] == '\0') return 0;
		const std::string pid_token = "pid=" + std::to_string(static_cast<long>(pid)) + " ";
		size_t result = 0;
		size_t offset = 0;
		while (offset < transcript_.size()) {
			const size_t line_end = transcript_.find('\n', offset);
			const size_t end = line_end == std::string::npos ? transcript_.size() : line_end;
			const std::string line = transcript_.substr(offset, end - offset);
			if (line.find(pid_token) != std::string::npos && line.find(needle) != std::string::npos) {
				++result;
			}
			if (line_end == std::string::npos) break;
			offset = line_end + 1;
		}
		return result;
	}

	bool has_dropped_events() const {
		return transcript_.find("FUTEX_TRACE_DROPPED") != std::string::npos;
	}

	const std::string& status_transcript() const { return status_transcript_; }

	const std::string& transcript() const { return transcript_; }

	private:
	static bool parse_status_value(const std::string& line, const char* key, uint64_t* value) {
		const size_t offset = line.find(key);
		if (offset == std::string::npos) return false;
		errno = 0;
		char* end = nullptr;
		const char* start = line.c_str() + offset + std::strlen(key);
		const unsigned long long parsed = std::strtoull(start, &end, 10);
		if (end == start || errno == ERANGE || (*end != '\0' && *end != ' ')) return false;
		*value = static_cast<uint64_t>(parsed);
		return true;
	}

	bool find_status(pid_t pid, uint64_t* attempted, uint64_t* emitted, uint64_t* dropped,
				bool* complete) const {
		const std::string pid_token = "pid=" + std::to_string(static_cast<long>(pid)) + " ";
		*complete = false;
		bool found = false;
		size_t offset = 0;
		while (offset < status_transcript_.size()) {
			const size_t line_end = status_transcript_.find('\n', offset);
			if (line_end == std::string::npos) break;
			const size_t end = line_end;
			const std::string line = status_transcript_.substr(offset, end - offset);
			if (line.find("FUTEX_TRACE_STATUS") != std::string::npos &&
					line.find(pid_token) != std::string::npos) {
				uint64_t status_complete = 0;
				uint64_t status_overflow = 0;
				const bool valid = parse_status_value(line, "attempted=", attempted) &&
									parse_status_value(line, "emitted=", emitted) &&
									parse_status_value(line, "dropped=", dropped) &&
									parse_status_value(line, "complete=", &status_complete) &&
									parse_status_value(line, "overflow=", &status_overflow);
				found = valid && status_complete == 1 && status_overflow == 0;
				*complete = found;
			}
			offset = line_end + 1;
		}
		return found;
	}

	int read_fd_ = -1;
	int write_fd_ = -1;
	int status_read_fd_ = -1;
	int status_write_fd_ = -1;
	std::string transcript_;
	std::string status_transcript_;
};

inline bool set_futex_trace_fd(int fd) {
	char value[32];
	const int length = std::snprintf(value, sizeof(value), "%d", fd);
	if (length <= 0 || static_cast<size_t>(length) >= sizeof(value)) return false;
	return ::setenv("EDGE_RUNTIME_FUTEX_TRACE_FD", value, 1) == 0;
}

inline void clear_futex_trace_fd() { (void)::unsetenv("EDGE_RUNTIME_FUTEX_TRACE_FD"); }

inline bool set_futex_trace_status_fd(int fd) {
	char value[32];
	const int length = std::snprintf(value, sizeof(value), "%d", fd);
	if (length <= 0 || static_cast<size_t>(length) >= sizeof(value)) return false;
	return ::setenv("EDGE_RUNTIME_FUTEX_TRACE_STATUS_FD", value, 1) == 0;
}

inline void clear_futex_trace_status_fd() {
	(void)::unsetenv("EDGE_RUNTIME_FUTEX_TRACE_STATUS_FD");
}

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
