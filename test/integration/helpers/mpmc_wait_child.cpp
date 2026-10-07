#include <cinttypes>
#include <chrono>
#include <climits>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <time.h>
#include <unistd.h>

#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime/sync/futex.hpp"
#include "mpmc_payload.hpp"

namespace {

const char* arg_value(int argc, char** argv, const char* name) {
	for (int i = 1; i + 1 < argc; ++i) {
		if (std::strcmp(argv[i], name) == 0) return argv[i + 1];
	}
	return nullptr;
}

uint64_t arg_u64(int argc, char** argv, const char* name, uint64_t fallback) {
	const char* value = arg_value(argc, argv, name);
	return value == nullptr ? fallback : std::strtoull(value, nullptr, 10);
}

int arg_fd(int argc, char** argv, const char* name) {
	const char* value = arg_value(argc, argv, name);
	if (value == nullptr) return -1;
	char* end = nullptr;
	const long parsed = std::strtol(value, &end, 10);
	if (end == value || *end != '\0' || parsed < 0 || parsed > INT_MAX) return -2;
	return static_cast<int>(parsed);
}

bool has_flag(int argc, char** argv, const char* name) {
	for (int i = 1; i < argc; ++i) {
		if (std::strcmp(argv[i], name) == 0) return true;
	}
	return false;
}

void noop_sigusr1(int) {}

bool write_deadline_event(int fd, int64_t start_ms, int64_t deadline_ms) {
	if (fd < 0) return true;
	char line[160];
	const int length = std::snprintf(line, sizeof(line),
					"WAIT_DEADLINE start_ms=%" PRId64 " deadline_ms=%" PRId64 " pid=%ld\n", start_ms,
					deadline_ms, static_cast<long>(::getpid()));
	if (length <= 0 || static_cast<size_t>(length) >= sizeof(line)) return false;
	return ::write(fd, line, static_cast<size_t>(length)) == static_cast<ssize_t>(length);
}

int64_t monotonic_ms_now() {
	struct timespec ts {};
	::clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

int print_error(const char* operation, const edge_runtime::Error& error) {
	std::printf("WAIT_%s_FAIL code=%s ctx=%s\n", operation,
	            edge_runtime::to_string(error.code), error.context);
	std::fflush(stdout);
	return 3;
}

int print_wait_error(const char* operation, const edge_runtime::Error& error, int64_t start_ms,
			int64_t deadline_ms) {
	const int64_t end_ms = monotonic_ms_now();
	std::printf("WAIT_%s_FAIL code=%s ctx=%s start_ms=%" PRId64 " deadline_ms=%" PRId64
	            " end_ms=%" PRId64 " elapsed_ms=%" PRId64 "\n",
	            operation, edge_runtime::to_string(error.code), error.context, start_ms, deadline_ms,
	            end_ms, end_ms - start_ms);
	std::fflush(stdout);
	return 3;
}

}  // namespace

int main(int argc, char** argv) {
	const char* role = arg_value(argc, argv, "--role");
	const char* name = arg_value(argc, argv, "--name");
	if (role == nullptr || name == nullptr ||
	    (std::strcmp(role, "producer") != 0 && std::strcmp(role, "consumer") != 0)) {
		std::fprintf(stderr,
					"usage: mpmc_wait_child --role producer|consumer --name NAME "
					"--capacity N --timeout-ms N [--id N] [--sequence N] "
					"[--interruptible] [--no-notify] [--deadline-event-fd FD]\n");
		return 2;
	}
	if (has_flag(argc, argv, "--no-notify") &&
	    ::setenv("EDGE_RUNTIME_MPMC_DISABLE_NOTIFY", "1", 1) != 0) {
		return 2;
	}
	if (has_flag(argc, argv, "--interruptible")) {
		struct sigaction action {};
		action.sa_handler = noop_sigusr1;
		::sigemptyset(&action.sa_mask);
		if (::sigaction(SIGUSR1, &action, nullptr) != 0) return 2;
	}

	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = static_cast<uint32_t>(arg_u64(argc, argv, "--capacity", 0));
	auto queue = edge_runtime::MpmcQueue<MpmcTestPayload>::open(options, MpmcTestSchema());
	if (!queue) return print_error("OPEN", queue.error());

	const uint64_t timeout_ms = arg_u64(argc, argv, "--timeout-ms", 0);
	const int deadline_event_fd = arg_fd(argc, argv, "--deadline-event-fd");
	if (deadline_event_fd == -2) return 2;
	const int64_t start_ms = monotonic_ms_now();
	const int64_t deadline_ms = start_ms + static_cast<int64_t>(timeout_ms);
	const auto deadline = std::chrono::steady_clock::now() +
	                      std::chrono::milliseconds(timeout_ms);
	if (!write_deadline_event(deadline_event_fd, start_ms, deadline_ms)) return 2;
	if (std::strcmp(role, "consumer") == 0) {
		auto result = queue.value().wait_pop_until(deadline);
		const bool trace_complete = edge_runtime::detail::futex_trace_flush();
		if (!trace_complete) return 5;
		if (!result) return print_wait_error("POP", result.error(), start_ms, deadline_ms);
		if (result.value().magic != 0x4D504D43u) {
			std::printf("WAIT_POP_FAIL code=PayloadCorrupt ctx=bad magic\n");
			return 4;
		}
		std::printf("WAIT_POP_OK producer=%" PRIu32 " sequence=%" PRIu64 "\n",
		            result.value().producer, result.value().sequence);
		std::fflush(stdout);
		return 0;
	}

	const uint32_t producer = static_cast<uint32_t>(arg_u64(argc, argv, "--id", 11));
	const uint64_t sequence = arg_u64(argc, argv, "--sequence", 7);
	const MpmcTestPayload value{sequence, producer, 0x4D504D43u};
	auto result = queue.value().wait_push_until(value, deadline);
	const bool trace_complete = edge_runtime::detail::futex_trace_flush();
	if (!trace_complete) return 5;
	if (!result) return print_wait_error("PUSH", result.error(), start_ms, deadline_ms);
	std::printf("WAIT_PUSH_OK producer=%" PRIu32 " sequence=%" PRIu64 "\n", producer,
	            sequence);
	std::fflush(stdout);
	return 0;
}
