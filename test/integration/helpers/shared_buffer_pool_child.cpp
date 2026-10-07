#include <sched.h>

#include <cerrno>
#include <cinttypes>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>

#include <signal.h>
#include <time.h>

#include <unistd.h>

#include "edge_runtime/common/error.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "shared_buffer_pool_payload.hpp"

namespace {

using Queue = edge_runtime::MpmcQueue<edge_runtime::BufferHandle>;

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

bool has_flag(int argc, char** argv, const char* name) {
	for (int i = 1; i < argc; ++i) {
		if (std::strcmp(argv[i], name) == 0) return true;
	}
	return false;
}

int arg_fd(int argc, char** argv, const char* name) {
	const char* value = arg_value(argc, argv, name);
	if (value == nullptr) return -1;
	char* end = nullptr;
	const long parsed = std::strtol(value, &end, 10);
	if (end == value || *end != '\0' || parsed < 0 || parsed > INT_MAX) return -2;
	return static_cast<int>(parsed);
}

int64_t monotonic_ms_now() {
	struct timespec ts {};
	::clock_gettime(CLOCK_MONOTONIC, &ts);
	return static_cast<int64_t>(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

void yield_briefly() {
	::sched_yield();
}

edge_runtime::SharedBufferPoolOptions pool_options(const std::string& name,
                                                   bool mismatch = false) {
	edge_runtime::SharedBufferPoolOptions options;
	options.name = name;
	options.block_size = 4096;
	options.block_count = 1;
	options.schema = mismatch ? SharedBufferPoolMismatchSchema() : SharedBufferPoolTestSchema();
	return options;
}

edge_runtime::MpmcQueueOptions queue_options(const std::string& name) {
	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = 4;
	return options;
}

int print_error(const char* operation, const edge_runtime::Error& error) {
	std::printf("%s_FAIL code=%s ctx=%s\n", operation,
	            edge_runtime::to_string(error.code), error.context);
	std::fflush(stdout);
	return 3;
}

int open_queue(const std::string& name, std::optional<Queue>* result) {
	const int64_t deadline = monotonic_ms_now() + 10000;
	while (monotonic_ms_now() < deadline) {
		auto opened = Queue::open(queue_options(name), SharedBufferHandleQueueSchema());
		if (opened) {
			result->emplace(std::move(opened.value()));
			return 0;
		}
		if (opened.error().code != edge_runtime::ErrorCode::kNotFound &&
		    opened.error().code != edge_runtime::ErrorCode::kInitializationIncomplete) {
			return print_error("QUEUE_OPEN", opened.error());
		}
		yield_briefly();
	}
	std::printf("QUEUE_OPEN_FAIL code=Timeout ctx=queue did not become ready\n");
	return 3;
}

int open_pool(const std::string& name, std::optional<edge_runtime::SharedBufferPool>* result,
              bool mismatch = false) {
	const int64_t deadline = monotonic_ms_now() + 10000;
	while (monotonic_ms_now() < deadline) {
		auto opened = edge_runtime::SharedBufferPool::open(pool_options(name, mismatch));
		if (opened) {
			result->emplace(std::move(opened.value()));
			return 0;
		}
		if (mismatch || (opened.error().code != edge_runtime::ErrorCode::kNotFound &&
		                 opened.error().code !=
		                         edge_runtime::ErrorCode::kInitializationIncomplete)) {
			return print_error("OPEN", opened.error());
		}
		yield_briefly();
	}
	std::printf("OPEN_FAIL code=Timeout ctx=pool did not become ready\n");
	return 3;
}

unsigned char expected_byte(uint64_t round, size_t offset) {
	return static_cast<unsigned char>((round * 17u + offset) % 251u);
}

bool fill_payload(edge_runtime::WriteBuffer* buffer, uint64_t round) {
	if (buffer == nullptr || buffer->size() < sizeof(round)) return false;
	std::memcpy(buffer->data(), &round, sizeof(round));
	for (size_t i = sizeof(round); i < buffer->size(); ++i) {
		buffer->data()[i] = std::byte{expected_byte(round, i)};
	}
	return true;
}

bool check_payload(const edge_runtime::ReadBuffer& buffer, uint64_t expected_round) {
	if (buffer.size() < sizeof(expected_round)) return false;
	uint64_t round = 0;
	std::memcpy(&round, buffer.data(), sizeof(round));
	if (round != expected_round) return false;
	for (size_t i = sizeof(round); i < buffer.size(); ++i) {
		if (std::to_integer<unsigned char>(buffer.data()[i]) != expected_byte(round, i)) {
			return false;
		}
	}
	return true;
}

bool write_event(int fd, const char* text) {
	if (fd < 0 || text == nullptr) return false;
	const size_t length = std::strlen(text);
	size_t written = 0;
	while (written < length) {
		const ssize_t result = ::write(fd, text + written, length - written);
		if (result > 0) {
			written += static_cast<size_t>(result);
			continue;
		}
		if (result < 0 && errno == EINTR) continue;
		return false;
	}
	return true;
}

int run_chain_producer(const std::string& pool_name, const std::string& queue_name,
		uint64_t count, int status_event_fd) {
	auto pool = edge_runtime::SharedBufferPool::create(pool_options(pool_name));
	if (!pool) return print_error("POOL_CREATE", pool.error());
	std::optional<Queue> queue;
	if (const int rc = open_queue(queue_name, &queue); rc != 0) return rc;

	uint64_t previous_generation = 0;
	for (uint64_t round = 0; round < count; ++round) {
		std::optional<edge_runtime::WriteBuffer> write;
		const int64_t deadline = monotonic_ms_now() + 30000;
		while (!write && monotonic_ms_now() < deadline) {
			auto candidate = pool.value().try_acquire_write();
			if (candidate) {
				write.emplace(std::move(candidate.value()));
				break;
			}
			if (candidate.error().code != edge_runtime::ErrorCode::kBufferPoolFull &&
			    candidate.error().code != edge_runtime::ErrorCode::kBufferPoolContention) {
				return print_error("WRITE_ACQUIRE", candidate.error());
			}
			yield_briefly();
		}
		if (!write) {
			std::printf("WRITE_ACQUIRE_FAIL code=Timeout ctx=block did not become free\n");
			return 3;
		}
		if (!fill_payload(&*write, round)) {
			std::printf("PAYLOAD_FAIL ctx=write buffer too small\n");
			return 4;
		}
		auto published = write->publish();
		if (!published) return print_error("PUBLISH", published.error());
		if (published.value().block_generation != round + 1 ||
		    (previous_generation != 0 &&
		     published.value().block_generation != previous_generation + 1)) {
			std::printf("GENERATION_FAIL got=%" PRIu64 " round=%" PRIu64 "\n",
			            published.value().block_generation, round);
			return 4;
		}
		previous_generation = published.value().block_generation;

		bool queued = false;
		while (!queued && monotonic_ms_now() < deadline) {
			auto pushed = queue->try_push(published.value());
			if (pushed) {
				queued = true;
				break;
			}
			if (pushed.error().code != edge_runtime::ErrorCode::kQueueFull &&
			    pushed.error().code != edge_runtime::ErrorCode::kQueueContention) {
				return print_error("QUEUE_PUSH", pushed.error());
			}
			yield_briefly();
		}
		if (!queued) {
			std::printf("QUEUE_PUSH_FAIL code=Timeout ctx=queue did not accept handle\n");
			return 3;
		}
	}

	const int64_t deadline = monotonic_ms_now() + 30000;
	bool reported_status_contention = false;
	while (monotonic_ms_now() < deadline) {
		auto status = pool.value().status();
		if (!status) {
			if (status.error().code != edge_runtime::ErrorCode::kBufferPoolContention) {
				return print_error("POOL_STATUS", status.error());
			}
			if (!reported_status_contention && status_event_fd >= 0) {
				if (!write_event(status_event_fd, "POOL_STATUS_CONTENTION\n")) return 5;
				reported_status_contention = true;
			}
			yield_briefly();
			continue;
		}
		if (status.value().free_blocks == status.value().block_count &&
		    status.value().published_blocks == 0 && status.value().writing_blocks == 0 &&
		    status.value().reading_blocks == 0) {
			auto removed = pool.value().remove_if_creator();
			if (!removed) return print_error("POOL_REMOVE", removed.error());
			std::printf("PRODUCED count=%" PRIu64 " generations=%" PRIu64 "\n", count,
			            previous_generation);
			std::fflush(stdout);
			return 0;
		}
		yield_briefly();
	}
	std::printf("POOL_DRAIN_FAIL code=Timeout ctx=consumer did not release all blocks\n");
	return 3;
}

int run_chain_consumer(const std::string& pool_name, const std::string& queue_name,
                       uint64_t count) {
	std::optional<edge_runtime::SharedBufferPool> pool;
	if (const int rc = open_pool(pool_name, &pool); rc != 0) return rc;
	std::optional<Queue> queue;
	if (const int rc = open_queue(queue_name, &queue); rc != 0) return rc;

	uint64_t previous_generation = 0;
	for (uint64_t round = 0; round < count; ++round) {
		edge_runtime::BufferHandle handle{};
		bool popped = false;
		const int64_t deadline = monotonic_ms_now() + 30000;
		while (!popped && monotonic_ms_now() < deadline) {
			auto item = queue->try_pop();
			if (item) {
				handle = item.value();
				popped = true;
				break;
			}
			if (item.error().code != edge_runtime::ErrorCode::kQueueEmpty &&
			    item.error().code != edge_runtime::ErrorCode::kQueueContention) {
				return print_error("QUEUE_POP", item.error());
			}
			yield_briefly();
		}
		if (!popped) {
			std::printf("QUEUE_POP_FAIL code=Timeout ctx=queue did not deliver handle\n");
			return 3;
		}
		if (handle.block_generation != round + 1 ||
		    (previous_generation != 0 && handle.block_generation != previous_generation + 1)) {
			std::printf("GENERATION_FAIL got=%" PRIu64 " round=%" PRIu64 "\n",
			            handle.block_generation, round);
			return 4;
		}
		previous_generation = handle.block_generation;
		auto read = pool->acquire_read(handle);
		if (!read) return print_error("READ_ACQUIRE", read.error());
		if (!check_payload(read.value(), round)) {
			std::printf("PAYLOAD_FAIL round=%" PRIu64 "\n", round);
			return 4;
		}
		read.value().release();
	}
	std::printf("CONSUMED count=%" PRIu64 " generations=%" PRIu64 "\n", count,
	            previous_generation);
	std::fflush(stdout);
	return 0;
}

int run_open(const std::string& pool_name, bool mismatch) {
	std::optional<edge_runtime::SharedBufferPool> pool;
	return open_pool(pool_name, &pool, mismatch);
}

int run_status(const std::string& pool_name) {
	std::optional<edge_runtime::SharedBufferPool> pool;
	if (const int rc = open_pool(pool_name, &pool); rc != 0) return rc;
	auto status = pool->status();
	if (!status) return print_error("STATUS", status.error());
	std::printf("STATUS_OK free=%u writing=%u published=%u reading=%u blocked=%u\n",
	            status.value().free_blocks, status.value().writing_blocks,
	            status.value().published_blocks, status.value().reading_blocks,
	            status.value().recovery_blocked_blocks);
	std::fflush(stdout);
	return 0;
}

edge_runtime::BufferHandle parse_handle(int argc, char** argv) {
	edge_runtime::BufferHandle handle{};
	handle.pool_generation = arg_u64(argc, argv, "--pool-generation", 0);
	handle.block_generation = arg_u64(argc, argv, "--block-generation", 0);
	handle.instance_nonce_hi = arg_u64(argc, argv, "--nonce-hi", 0);
	handle.instance_nonce_lo = arg_u64(argc, argv, "--nonce-lo", 0);
	handle.offset = arg_u64(argc, argv, "--offset", 0);
	handle.block_id = static_cast<uint32_t>(arg_u64(argc, argv, "--block-id", 0));
	handle.length = static_cast<uint32_t>(arg_u64(argc, argv, "--length", 0));
	handle.schema_version = 1;
	std::memcpy(handle.schema_fingerprint, kSharedBufferPoolFingerprint.data(),
	            sizeof(handle.schema_fingerprint));
	return handle;
}

int run_crash_writer(const std::string& pool_name) {
	std::optional<edge_runtime::SharedBufferPool> pool;
	if (const int rc = open_pool(pool_name, &pool); rc != 0) return rc;
	auto write = pool->try_acquire_write();
	if (!write) return print_error("WRITE_ACQUIRE", write.error());
	if (!fill_payload(&write.value(), 0)) return 4;
	std::printf("READY\n");
	std::fflush(stdout);
	::raise(SIGSTOP);
	for (;;) ::pause();
}

int run_crash_reader(int argc, char** argv, const std::string& pool_name) {
	std::optional<edge_runtime::SharedBufferPool> pool;
	if (const int rc = open_pool(pool_name, &pool); rc != 0) return rc;
	const auto handle = parse_handle(argc, argv);
	auto read = pool->acquire_read(handle);
	if (!read) return print_error("READ_ACQUIRE", read.error());
	std::printf("READY\n");
	std::fflush(stdout);
	::raise(SIGSTOP);
	for (;;) ::pause();
}

int run_stale_reader(int argc, char** argv, const std::string& pool_name) {
	std::optional<edge_runtime::SharedBufferPool> pool;
	if (const int rc = open_pool(pool_name, &pool); rc != 0) return rc;
	const auto handle = parse_handle(argc, argv);
	std::printf("READY\n");
	std::fflush(stdout);
	auto read = pool->acquire_read(handle);
	if (!read) return print_error("READ_ACQUIRE", read.error());
	const unsigned int first_byte =
	        std::to_integer<unsigned int>(read.value().data()[0]);
	std::printf("READ_OK first_byte=0x%02X\n", first_byte);
	read.value().release();
	std::fflush(stdout);
	return 0;
}

int run_recycle(int argc, char** argv, const std::string& pool_name) {
	std::optional<edge_runtime::SharedBufferPool> pool;
	if (const int rc = open_pool(pool_name, &pool); rc != 0) return rc;
	const auto old_handle = parse_handle(argc, argv);
	auto old_read = pool->acquire_read(old_handle);
	if (!old_read) return print_error("OLD_READ", old_read.error());
	if (std::to_integer<unsigned int>(old_read.value().data()[0]) != 0x11u) {
		std::printf("RECYCLE_FAIL ctx=old payload mismatch\n");
		return 4;
	}
	old_read.value().release();

	auto write = pool->try_acquire_write();
	if (!write) return print_error("RECYCLE_WRITE", write.error());
	std::memset(write.value().data(), 0x22, write.value().size());
	auto published = write.value().publish();
	if (!published) return print_error("RECYCLE_PUBLISH", published.error());
	std::printf("RECYCLE_OK old_generation=%" PRIu64 " new_generation=%" PRIu64
	            " first_byte=0x22\n",
	            old_handle.block_generation, published.value().block_generation);
	std::fflush(stdout);
	return 0;
}

}  // namespace

int main(int argc, char** argv) {
	const char* mode = arg_value(argc, argv, "--mode");
	const char* pool_name = arg_value(argc, argv, "--pool-name");
	if (mode == nullptr || pool_name == nullptr) {
		std::fprintf(stderr, "usage: shared_buffer_pool_child --mode MODE --pool-name NAME ...\n");
		return 2;
	}
	const std::string pool = pool_name;
	const std::string queue = arg_value(argc, argv, "--queue-name") == nullptr
	                                  ? std::string()
	                                  : arg_value(argc, argv, "--queue-name");
	const uint64_t count = arg_u64(argc, argv, "--count", 8);
	const int status_event_fd = arg_fd(argc, argv, "--status-event-fd");
	if (status_event_fd == -2) return 2;
	if (std::strcmp(mode, "producer") == 0) {
		if (queue.empty()) return 2;
		return run_chain_producer(pool, queue, count, status_event_fd);
	}
	if (std::strcmp(mode, "consumer") == 0) {
		if (queue.empty()) return 2;
		return run_chain_consumer(pool, queue, count);
	}
	if (std::strcmp(mode, "open-mismatch") == 0) return run_open(pool, true);
	if (std::strcmp(mode, "open") == 0) return run_open(pool, false);
	if (std::strcmp(mode, "status") == 0) return run_status(pool);
	if (std::strcmp(mode, "crash-writer") == 0) return run_crash_writer(pool);
	if (std::strcmp(mode, "crash-reader") == 0) return run_crash_reader(argc, argv, pool);
	if (std::strcmp(mode, "stale-reader") == 0) return run_stale_reader(argc, argv, pool);
	if (std::strcmp(mode, "recycle") == 0) return run_recycle(argc, argv, pool);
	if (has_flag(argc, argv, "--help")) return 0;
	return 2;
}
