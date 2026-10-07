#include <sched.h>

#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <string>

#include <unistd.h>

#include "edge_runtime/common/error.hpp"
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
	if (value == nullptr) return fallback;
	return std::strtoull(value, nullptr, 10);
}

bool has_flag(int argc, char** argv, const char* name) {
	for (int i = 1; i < argc; ++i) {
		if (std::strcmp(argv[i], name) == 0) return true;
	}
	return false;
}

using Queue = edge_runtime::MpmcQueue<MpmcTestPayload>;

int print_error(const char* operation, const edge_runtime::Error& error) {
	std::printf("%s_FAIL code=%s ctx=%s\n", operation,
	            edge_runtime::to_string(error.code), error.context);
	std::fflush(stdout);
	return 3;
}

int run_producer(Queue& queue, uint32_t producer, uint64_t count) {
	uint64_t pushed = 0;
	for (uint64_t i = 0; i < count;) {
		MpmcTestPayload value{i, producer, 0x4D504D43u};
		auto result = queue.try_push(value);
		if (result) {
			++i;
			++pushed;
			continue;
		}
		if (result.error().code == edge_runtime::ErrorCode::kQueueFull ||
		    result.error().code == edge_runtime::ErrorCode::kQueueContention) {
			::sched_yield();
			continue;
		}
		return print_error("PUSH", result.error());
	}
	std::printf("PRODUCED producer=%u count=%" PRIu64 "\n", producer, pushed);
	std::fflush(stdout);
	return 0;
}

int run_consumer(Queue& queue, uint64_t count) {
	uint64_t consumed = 0;
	while (consumed < count) {
		auto result = queue.try_pop();
		if (result) {
			const MpmcTestPayload& value = result.value();
			if (value.magic != 0x4D504D43u) {
				std::printf("BAD_PAYLOAD magic=%" PRIu32 "\n", value.magic);
				return 4;
			}
			std::printf("ITEM producer=%" PRIu32 " sequence=%" PRIu64 "\n", value.producer,
			            value.sequence);
			++consumed;
			continue;
		}
		if (result.error().code == edge_runtime::ErrorCode::kQueueEmpty ||
		    result.error().code == edge_runtime::ErrorCode::kQueueContention) {
			::sched_yield();
			continue;
		}
		return print_error("POP", result.error());
	}
	std::printf("CONSUMED count=%" PRIu64 "\n", consumed);
	std::fflush(stdout);
	return 0;
}

int run_probe_consumer(Queue& queue) {
	auto result = queue.try_pop();
	if (!result) return print_error("POP", result.error());
	const MpmcTestPayload& value = result.value();
	if (value.magic != 0x4D504D43u) {
		std::printf("BAD_PAYLOAD magic=%" PRIu32 "\n", value.magic);
		return 4;
	}
	std::printf("ITEM producer=%" PRIu32 " sequence=%" PRIu64 "\n", value.producer,
	            value.sequence);
	std::fflush(stdout);
	return 0;
}

int run_probe_producer(Queue& queue, uint32_t producer) {
	const MpmcTestPayload value{202, producer, 0x4D504D43u};
	auto result = queue.try_push(value);
	if (!result) return print_error("PUSH", result.error());
	std::printf("PUSH producer=%" PRIu32 " sequence=202\n", producer);
	std::fflush(stdout);
	return 0;
}

bool write_ready_token(int fd) {
	const char token[] = "FIRST_TRY_QUEUE_CONTENTION\n";
	size_t written = 0;
	while (written < sizeof(token) - 1u) {
		const ssize_t count = ::write(fd, token + written, sizeof(token) - 1u - written);
		if (count > 0) {
			written += static_cast<size_t>(count);
			continue;
		}
		if (count < 0 && errno == EINTR) continue;
		return false;
	}
	return true;
}

int run_wait_probe_producer(Queue& queue, uint32_t producer, int ready_fd) {
	const MpmcTestPayload value{0, producer, 0x4D504D43u};
	auto first = queue.try_push(value);
	if (first) {
		std::printf("FIRST_TRY_FAIL unexpected_success\n");
		return 4;
	}
	if (first.error().code != edge_runtime::ErrorCode::kQueueContention) {
		return print_error("FIRST_TRY", first.error());
	}
	if (!write_ready_token(ready_fd)) return 5;
	auto result = queue.wait_push_until(
			value, std::chrono::steady_clock::now() + std::chrono::seconds(5));
	if (!result) return print_error("WAIT_PUSH", result.error());
	if (!edge_runtime::detail::futex_trace_flush()) return 5;
	std::printf("WAIT_PRODUCED producer=%" PRIu32 " sequence=0\n", producer);
	std::fflush(stdout);
	return 0;
}

int run_wait_producer(Queue& queue, uint32_t producer) {
	const MpmcTestPayload value{0, producer, 0x4D504D43u};
	auto result = queue.wait_push_until(
		value, std::chrono::steady_clock::now() + std::chrono::seconds(5));
	if (!result) return print_error("WAIT_PUSH", result.error());
	std::printf("WAIT_PRODUCED producer=%" PRIu32 " sequence=0\n", producer);
	std::fflush(stdout);
	return 0;
}

int run_wait_consumer(Queue& queue) {
	auto result = queue.wait_pop_until(std::chrono::steady_clock::now() + std::chrono::seconds(5));
	if (!result) return print_error("WAIT_POP", result.error());
	const MpmcTestPayload& value = result.value();
	if (value.magic != 0x4D504D43u) {
		std::printf("BAD_PAYLOAD magic=%" PRIu32 "\n", value.magic);
		return 4;
	}
	std::printf("ITEM producer=%" PRIu32 " sequence=%" PRIu64 "\n", value.producer,
	            value.sequence);
	std::fflush(stdout);
	return 0;
}

int run_status(Queue& queue) {
	auto status = queue.status();
	if (!status) return print_error("STATUS", status.error());
	std::printf("STATUS enqueue_state=%" PRIu32 " dequeue_state=%" PRIu32
	            " enqueue_pid=%" PRIu64 " dequeue_pid=%" PRIu64
	            " enqueue_alive=%d dequeue_alive=%d\n",
	            status.value().enqueue_lock_state, status.value().dequeue_lock_state,
	            status.value().enqueue_owner_pid, status.value().dequeue_owner_pid,
	            status.value().enqueue_owner_alive ? 1 : 0,
	            status.value().dequeue_owner_alive ? 1 : 0);
	std::fflush(stdout);
	return 0;
}

}  // namespace

int main(int argc, char** argv) {
	const char* role = arg_value(argc, argv, "--role");
	const char* name = arg_value(argc, argv, "--name");
	if (role == nullptr || name == nullptr) {
		std::fprintf(
				stderr,
				"usage: mpmc_child --role producer|consumer|probe-producer|wait-probe-producer "
				"--capacity N --count N [--id N] [--schema-mismatch]\n");
		return 2;
	}
	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = static_cast<uint32_t>(arg_u64(argc, argv, "--capacity", 0));
	const auto schema = has_flag(argc, argv, "--schema-mismatch")
	                            ? MpmcTestMismatchSchema()
	                            : MpmcTestSchema();
	auto queue = Queue::open(options, schema);
	if (!queue) return print_error("OPEN", queue.error());
	if (std::strcmp(role, "status") == 0) return run_status(queue.value());
	if (std::strcmp(role, "probe-consumer") == 0) return run_probe_consumer(queue.value());
	if (std::strcmp(role, "probe-producer") == 0) {
		return run_probe_producer(
				queue.value(), static_cast<uint32_t>(arg_u64(argc, argv, "--id", 0)));
	}
	if (std::strcmp(role, "wait-probe-producer") == 0) {
		const char* ready_fd_text = arg_value(argc, argv, "--ready-fd");
		if (ready_fd_text == nullptr) return 2;
		return run_wait_probe_producer(
				queue.value(), static_cast<uint32_t>(arg_u64(argc, argv, "--id", 0)),
				static_cast<int>(std::strtol(ready_fd_text, nullptr, 10)));
	}
	const uint64_t count = arg_u64(argc, argv, "--count", 0);
	const uint32_t id = static_cast<uint32_t>(arg_u64(argc, argv, "--id", 0));
	if (std::strcmp(role, "wait-producer") == 0) return run_wait_producer(queue.value(), id);
	if (std::strcmp(role, "wait-consumer") == 0) return run_wait_consumer(queue.value());
	if (std::strcmp(role, "producer") == 0) return run_producer(queue.value(), id, count);
	if (std::strcmp(role, "consumer") == 0) return run_consumer(queue.value(), count);
	return 2;
}
