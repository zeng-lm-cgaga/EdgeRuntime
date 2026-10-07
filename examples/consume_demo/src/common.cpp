#include "consume_demo/common.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>

#include "edge_runtime/common/error.hpp"
#include "edge_runtime/common/schema.hpp"

namespace consume_demo {

namespace {

using Clock = std::chrono::steady_clock;

bool read_one(int fd, char* token, std::chrono::milliseconds timeout) {
	if (fd < 0 || token == nullptr) return false;
	const auto deadline = Clock::now() + timeout;
	for (;;) {
		const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
		        deadline - Clock::now());
		if (remaining.count() <= 0) return false;
		struct pollfd descriptor{};
		descriptor.fd = fd;
		descriptor.events = POLLIN;
		int rc = 0;
		do {
			rc = ::poll(&descriptor, 1, static_cast<int>(remaining.count()));
		} while (rc < 0 && errno == EINTR);
		if (rc <= 0) return false;
		const ssize_t count = ::read(fd, token, 1);
		if (count == 1) return true;
		if (count < 0 && errno == EINTR) continue;
		return false;
	}
}

bool wait_for_exit(pid_t pid, int* status, std::chrono::milliseconds timeout) {
	const auto deadline = Clock::now() + timeout;
	for (;;) {
	const pid_t result = ::waitpid(pid, status, WNOHANG);
		if (result == pid) return true;
		if (result < 0 && errno != EINTR) return false;
		if (Clock::now() >= deadline) return false;
		::poll(nullptr, 0, 10);
	}
}

bool close_if_open(int* fd) {
	if (fd == nullptr || *fd < 0) return true;
	int rc = 0;
	do {
		rc = ::close(*fd);
	} while (rc < 0 && errno == EINTR);
	*fd = -1;
	return rc == 0 || errno == EBADF;
}

}  // namespace

std::string executable_path() {
	std::array<char, 4096> buffer{};
	const ssize_t length = ::readlink("/proc/self/exe", buffer.data(), buffer.size() - 1);
	if (length <= 0) return {};
	buffer[static_cast<size_t>(length)] = '\0';
	return std::string(buffer.data());
}

std::string unique_name(const char* prefix) {
	const auto ticks = std::chrono::duration_cast<std::chrono::nanoseconds>(
	        Clock::now().time_since_epoch());
	return std::string(prefix) + "_" +
	       std::to_string(static_cast<unsigned long long>(::getpid())) + "_" +
	       std::to_string(static_cast<unsigned long long>(ticks.count()));
}

bool write_token(int fd, char token) {
	if (fd < 0) return false;
	for (;;) {
		const ssize_t count = ::write(fd, &token, 1);
		if (count == 1) return true;
		if (count < 0 && errno == EINTR) continue;
		return false;
	}
}

bool read_token(int fd, char expected, std::chrono::milliseconds timeout) {
	char token = '\0';
	if (!read_one(fd, &token, timeout)) return false;
	if (token != expected) {
		std::fprintf(stderr, "consume_demo: expected token %c, got %c\n", expected, token);
		return false;
	}
	return true;
}

bool wait_child(ChildProcess* child, std::chrono::milliseconds timeout) {
	if (child == nullptr || child->pid <= 0) return false;
	int status = 0;
	if (!wait_for_exit(static_cast<pid_t>(child->pid), &status, timeout)) return false;
	child->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
	const bool ok = child->exit_code == 0;
	child->pid = -1;
	return ok;
}

void stop_child(ChildProcess* child) {
	if (child == nullptr || child->pid <= 0) return;
	(void)::kill(static_cast<pid_t>(child->pid), SIGKILL);
	int status = 0;
	do {
	} while (::waitpid(static_cast<pid_t>(child->pid), &status, 0) < 0 && errno == EINTR);
	child->exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
	child->pid = -1;
}

void close_child(ChildProcess* child) {
	if (child == nullptr) return;
	(void)close_if_open(&child->event_read);
	(void)close_if_open(&child->command_write);
}

ChildProcess spawn_child(const std::string& mode, const std::vector<std::string>& args,
						const std::vector<int>& inherited_fds,
						const std::vector<int>& keep_fds) {
	ChildProcess child;
	child.pid = static_cast<int>(::fork());
	if (child.pid < 0) return child;
	if (child.pid == 0) {
		for (const int fd : inherited_fds) {
			bool keep = false;
			for (const int keep_fd : keep_fds) {
				if (fd == keep_fd) {
					keep = true;
					break;
				}
			}
			if (!keep) (void)::close(fd);
		}
		const std::string executable = executable_path();
		std::vector<std::string> values;
		values.reserve(args.size() + 2);
		values.push_back(executable);
		values.push_back(mode);
		values.insert(values.end(), args.begin(), args.end());
		std::vector<char*> argv;
		argv.reserve(values.size() + 1);
		for (std::string& value : values) argv.push_back(value.data());
		argv.push_back(nullptr);
		::execv(executable.c_str(), argv.data());
		::_exit(127);
	}
	return child;
}

edge_runtime::SchemaDescriptor point_schema() {
	edge_runtime::SchemaDescriptor schema;
	schema.fingerprint.fill(std::byte{0x31});
	schema.version = 1;
	schema.debug_name = "consume_demo.point";
	return schema;
}

edge_runtime::SchemaDescriptor queue_schema() {
	edge_runtime::SchemaDescriptor schema;
	schema.fingerprint.fill(std::byte{0x32});
	schema.version = 1;
	schema.debug_name = "consume_demo.queue";
	return schema;
}

edge_runtime::SchemaDescriptor buffer_schema() {
	edge_runtime::SchemaDescriptor schema;
	schema.fingerprint.fill(std::byte{0x33});
	schema.version = 1;
	schema.debug_name = "consume_demo.buffer";
	return schema;
}

bool encode_point(const Point& point, std::byte* output, size_t capacity) noexcept {
	if (output == nullptr || capacity < 8) return false;
	std::memcpy(output, &point.x, sizeof(point.x));
	std::memcpy(output + sizeof(point.x), &point.y, sizeof(point.y));
	return true;
}

bool decode_point(const std::byte* input, size_t size, Point* point) noexcept {
	if (input == nullptr || point == nullptr || size < 8) return false;
	std::memcpy(&point->x, input, sizeof(point->x));
	std::memcpy(&point->y, input + sizeof(point->x), sizeof(point->y));
	return true;
}

}  // namespace consume_demo
