#include "broker_observer_fixture.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <utility>

#include "edge_runtime/sync/clock.hpp"

namespace edge_test::broker_observer {

Child::Child() = default;

Child::~Child() {
	if (pid_ > 0) {
		(void)::kill(pid_, SIGKILL);
		while (::waitpid(pid_, nullptr, 0) < 0 && errno == EINTR) {
		}
	}
}

bool Child::start(const std::vector<std::string>& arguments) {
	int input[2]{};
	int output[2]{};
	if (pid_ > 0 || arguments.empty() || ::pipe2(input, O_CLOEXEC) != 0) return false;
	if (arguments.size() >= 3 && (arguments[1] == "fake" || arguments[1] == "backlog"))
		owned_path_ = std::make_unique<OwnedPath>(arguments[2]);
	edge_runtime::detail::UniqueFd input_read(input[0]);
	input_.reset(input[1]);
	if (::pipe2(output, O_CLOEXEC) != 0) return false;
	output_.reset(output[0]);
	edge_runtime::detail::UniqueFd output_write(output[1]);
	std::vector<char*> argv;
	for (const auto& argument : arguments) argv.push_back(const_cast<char*>(argument.c_str()));
	argv.push_back(nullptr);
	pid_ = ::fork();
	if (pid_ < 0) return false;
	if (pid_ == 0) {
		if (::dup2(input_read.get(), STDIN_FILENO) < 0 ||
		    ::dup2(output_write.get(), STDOUT_FILENO) < 0)
			_exit(126);
		::execv(argv[0], argv.data());
		_exit(127);
	}
	return true;
}

std::string Child::line(int timeout_ms) {
	const uint64_t deadline = edge_runtime::detail::monotonic_deadline_ns(
	        static_cast<uint64_t>(timeout_ms) * 1'000'000);
	std::string result;
	for (;;) {
		const uint64_t remaining = edge_runtime::detail::remaining_time_ns(deadline);
		if (remaining == 0) return {};
		struct pollfd descriptor {
			output_.get(), POLLIN, 0
		};
		const int ready =
		        ::poll(&descriptor, 1, static_cast<int>((remaining + 999'999) / 1'000'000));
		if (ready < 0 && errno == EINTR) continue;
		if (ready <= 0) return {};
		char character = 0;
		if (::read(output_.get(), &character, 1) != 1) return {};
		if (character == '\n') {
			if (result == "READY" && owned_path_ != nullptr)
				(void)owned_path_->capture();
			std::fprintf(stderr, "peer[%d]: %s\n", pid_, result.c_str());
			return result;
		}
		result += character;
	}
}

bool Child::command(char command) { return ::write(input_.get(), &command, 1) == 1; }

bool Child::wait(int timeout_ms) {
	const uint64_t deadline = edge_runtime::detail::monotonic_deadline_ns(
	        static_cast<uint64_t>(timeout_ms) * 1'000'000);
	while (pid_ > 0) {
		int status = 0;
		const pid_t result = ::waitpid(pid_, &status, WNOHANG);
		if (result == pid_) {
			pid_ = -1;
			exit_code_ = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
			return true;
		}
		if (result < 0 && errno != EINTR) return false;
		if (edge_runtime::detail::remaining_time_ns(deadline) == 0) return false;
		::usleep(1'000);
	}
	return true;
}

pid_t Child::pid() const { return pid_; }
int Child::exit_code() const { return exit_code_; }

OwnedPath::OwnedPath(std::string path) : path_(std::move(path)) {}

OwnedPath::~OwnedPath() {
	struct stat current {};
	if (ino_ != 0 && ::lstat(path_.c_str(), &current) == 0 &&
	    static_cast<uint64_t>(current.st_dev) == dev_ &&
	    static_cast<uint64_t>(current.st_ino) == ino_)
		(void)::unlink(path_.c_str());
}

bool OwnedPath::capture() {
	struct stat current {};
	if (::lstat(path_.c_str(), &current) != 0) return false;
	dev_ = static_cast<uint64_t>(current.st_dev);
	ino_ = static_cast<uint64_t>(current.st_ino);
	return true;
}

std::string unique_name() {
	static uint64_t sequence = 0;
	return "fix002_" + std::to_string(::getpid()) + "_" +
	       std::to_string(edge_runtime::detail::monotonic_now_ns()) + "_" +
	       std::to_string(++sequence);
}

std::string peer_binary() { return BROKER_OBSERVER_PEER; }

edge_runtime::detail::UniqueFd connect_peer(const std::string& path) {
	edge_runtime::detail::UniqueFd socket(::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0));
	struct sockaddr_un address {};
	address.sun_family = AF_UNIX;
	if (socket.get() < 0 || path.size() >= sizeof(address.sun_path)) return {};
	std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
	if (::connect(socket.get(), reinterpret_cast<struct sockaddr*>(&address),
	              sizeof(address)) != 0)
		return {};
	return socket;
}

size_t fd_count() {
	DIR* directory = ::opendir("/proc/self/fd");
	if (directory == nullptr) return 0;
	size_t count = 0;
	while (const dirent* entry = ::readdir(directory)) {
		if (entry->d_name[0] != '.') ++count;
	}
	(void)::closedir(directory);
	return count;
}

uint64_t elapsed_ms(uint64_t start_ns) {
	return (edge_runtime::detail::monotonic_now_ns() - start_ns) / 1'000'000;
}

bool parse_report(const std::string& text, ClientReport* report) {
	char code[64]{};
	unsigned long long duration = 0;
	long long delta = 0;
	unsigned int interruptions = 0;
	if (std::sscanf(text.c_str(), "RESULT %63s %llu %lld %d %d %d %u", code, &duration, &delta,
	                &report->signals, &report->access, &report->cloexec, &interruptions) != 7)
		return false;
	report->code = code;
	report->duration_ms = static_cast<uint64_t>(duration);
	report->fd_delta = static_cast<int64_t>(delta);
	report->syscall_interruptions = interruptions;
	return true;
}

}
