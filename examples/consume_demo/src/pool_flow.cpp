#include "consume_demo/pool_flow.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <poll.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>

#include "consume_demo/common.hpp"
#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"

namespace consume_demo {

namespace {

using HandleQueue = edge_runtime::MpmcQueue<edge_runtime::BufferHandle>;

bool resources_absent(const edge_runtime::MpmcQueueOptions& queue_options,
	                  const edge_runtime::SharedBufferPoolOptions& pool_options) {
	auto queue = HandleQueue::open(queue_options, queue_schema());
	auto pool = edge_runtime::SharedBufferPool::open(pool_options);
	const bool queue_absent = !queue && queue.error().code == edge_runtime::ErrorCode::kNotFound;
	const bool pool_absent = !pool && pool.error().code == edge_runtime::ErrorCode::kNotFound;
	return queue_absent && pool_absent;
}

bool settle_published(edge_runtime::SharedBufferPool* pool,
	                  const edge_runtime::BufferHandle* descriptor) {
	if (pool == nullptr || descriptor == nullptr) return true;
	auto read = pool->acquire_read(*descriptor);
	if (read) {
		read.value().release();
		return true;
	}
	if (read.error().code == edge_runtime::ErrorCode::kBufferStateMismatch) {
		// A normal peer may already have released the block before reporting failure.
		return true;
	}
	std::fprintf(stderr, "published descriptor cleanup failed: %s\n",
	             edge_runtime::to_string(read.error().code));
	return false;
}

bool cleanup(ChildProcess* child, HandleQueue* queue,
	         edge_runtime::SharedBufferPool* pool, edge_runtime::WriteBuffer* write,
	         const edge_runtime::BufferHandle* descriptor) {
	bool ok = true;
	if (child != nullptr && child->pid > 0) stop_child(child);
	if (write != nullptr && write->active()) write->abort();
	if (!settle_published(pool, descriptor)) ok = false;
	if (pool != nullptr) {
		const auto removed = pool->remove_if_creator();
		if (!removed) {
			std::fprintf(stderr, "pool cleanup failed: %s\n",
			             edge_runtime::to_string(removed.error().code));
			ok = false;
		}
	}
	if (queue != nullptr) {
		const auto removed = queue->remove_if_creator();
		if (!removed) {
			std::fprintf(stderr, "handle queue cleanup failed: %s\n",
			             edge_runtime::to_string(removed.error().code));
			ok = false;
		}
	}
	close_child(child);
	return ok;
}

}  // namespace

int run_pool_child(const std::string& queue_name, const std::string& pool_name, int event_fd,
	               bool fail_after_pop, bool fail_after_read) {
	edge_runtime::MpmcQueueOptions queue_options;
	queue_options.name = queue_name;
	queue_options.capacity = 1;
	edge_runtime::SharedBufferPoolOptions pool_options;
	pool_options.name = pool_name;
	pool_options.block_size = 4096;
	pool_options.block_count = 1;
	pool_options.schema = buffer_schema();
	auto queue_result = HandleQueue::open(queue_options, queue_schema());
	auto pool_result = edge_runtime::SharedBufferPool::open(pool_options);
	if (!queue_result || !pool_result) {
		(void)write_token(event_fd, 'F');
		return 3;
	}
	HandleQueue queue = std::move(queue_result.value());
	edge_runtime::SharedBufferPool pool = std::move(pool_result.value());
	if (!write_token(event_fd, 'R')) return 3;
	const auto received = queue.wait_pop_until(std::chrono::steady_clock::now() +
	                                           std::chrono::seconds(3));
	if (!received) {
		(void)write_token(event_fd, 'F');
		return 3;
	}
	if (fail_after_pop) {
		(void)write_token(event_fd, 'F');
		return 3;
	}
	auto read = pool.acquire_read(received.value());
	if (!read || read.value().size() < 16) {
		if (read) read.value().release();
		(void)write_token(event_fd, 'F');
		return 3;
	}
	const char expected[] = "installed-pool";
	const bool payload_ok =
		std::memcmp(read.value().data(), expected, sizeof(expected)) == 0 &&
		!fail_after_read;
	if (!payload_ok) {
		read.value().release();
		(void)write_token(event_fd, 'F');
		if (fail_after_read) (void)::poll(nullptr, 0, 300);
		return 3;
	}
	read.value().release();
	if (!write_token(event_fd, 'S')) return 3;
	return 0;
}

bool run_pool_flow(PoolFlowScenario scenario) {
	const std::string queue_name = unique_name("er_demo_handles");
	const std::string pool_name = unique_name("er_demo_pool");
	edge_runtime::MpmcQueueOptions queue_options;
	queue_options.name = queue_name;
	queue_options.capacity = 1;
	edge_runtime::SharedBufferPoolOptions pool_options;
	pool_options.name = pool_name;
	pool_options.block_size = 4096;
	pool_options.block_count = 1;
	pool_options.schema = buffer_schema();

	// Construct in dependency order so every successful create has an owner that can clean it.
	auto queue_result = HandleQueue::create(queue_options, queue_schema());
	if (!queue_result) {
		const bool expected = scenario == PoolFlowScenario::kQueueCreateFailure;
		const bool absent = resources_absent(queue_options, pool_options);
		if (expected && absent) {
			std::printf("buffer failure: queue create failed, resources absent\n");
		}
		return expected && absent;
	}
	HandleQueue queue = std::move(queue_result.value());

	auto pool_result = edge_runtime::SharedBufferPool::create(pool_options);
	if (!pool_result) {
		const auto removed = queue.remove_if_creator();
		if (!removed) {
			std::fprintf(stderr, "queue cleanup after pool create failed: %s\n",
			             edge_runtime::to_string(removed.error().code));
		}
		const bool expected = scenario == PoolFlowScenario::kPoolCreateFailure;
		const bool absent = removed && resources_absent(queue_options, pool_options);
		if (expected && absent) {
			std::printf("buffer failure: pool create failed, resources absent\n");
		}
		return expected && absent;
	}
	edge_runtime::SharedBufferPool pool = std::move(pool_result.value());
	if (scenario == PoolFlowScenario::kQueueCreateFailure ||
	    scenario == PoolFlowScenario::kPoolCreateFailure) {
		(void)cleanup(nullptr, &queue, &pool, nullptr, nullptr);
		return false;
	}

	int events[2] = {-1, -1};
	if (::pipe(events) != 0) {
		if (events[0] >= 0) (void)::close(events[0]);
		if (events[1] >= 0) (void)::close(events[1]);
		(void)cleanup(nullptr, &queue, &pool, nullptr, nullptr);
		return false;
	}
	const bool fail_after_pop = scenario == PoolFlowScenario::kPeerFailsAfterPop;
	const bool fail_after_read = scenario == PoolFlowScenario::kPeerFailsAfterRead;
	const char* child_mode = fail_after_pop ? "pool-child-fail-after-pop"
		: (fail_after_read ? "pool-child-fail-after-read" : "pool-child");
	const ChildProcess spawned = spawn_child(
		child_mode,
		{queue_name, pool_name, std::to_string(events[1])}, {events[0], events[1]}, {events[1]});
	ChildProcess child = spawned;
	const int child_pid = child.pid;
	(void)::close(events[1]);
	events[1] = -1;
	child.event_read = events[0];
	if (child.pid <= 0) {
		(void)::close(events[0]);
		child.event_read = -1;
		(void)cleanup(&child, &queue, &pool, nullptr, nullptr);
		return false;
	}
	if (!read_token(child.event_read, 'R', std::chrono::seconds(3))) {
		(void)cleanup(&child, &queue, &pool, nullptr, nullptr);
		return false;
	}
	auto write = pool.try_acquire_write();
	if (!write) {
		(void)cleanup(&child, &queue, &pool, nullptr, nullptr);
		return false;
	}
	edge_runtime::WriteBuffer buffer = std::move(write.value());
	const char payload[] = "installed-pool";
	if (buffer.size() < sizeof(payload)) {
		(void)cleanup(&child, &queue, &pool, &buffer, nullptr);
		return false;
	}
	std::memcpy(buffer.data(), payload, sizeof(payload));
	const auto descriptor = buffer.publish();
	if (!descriptor) {
		(void)cleanup(&child, &queue, &pool, &buffer, nullptr);
		return false;
	}
	edge_runtime::BufferHandle published_descriptor = descriptor.value();
	const auto pushed = queue.wait_push_until(
		published_descriptor, std::chrono::steady_clock::now() + std::chrono::seconds(2));
	if (!pushed) {
		(void)cleanup(&child, &queue, &pool, &buffer, &published_descriptor);
		return false;
	}

	if (fail_after_pop || fail_after_read) {
		const bool reported_failure = read_token(child.event_read, 'F', std::chrono::seconds(3));
		const bool exited_successfully = wait_child(&child, std::chrono::seconds(3));
		const bool expected_failure = reported_failure && !exited_successfully &&
		                              child.exit_code == 3;
		const bool cleaned = cleanup(&child, &queue, &pool, &buffer, &published_descriptor);
		const bool absent = resources_absent(queue_options, pool_options);
		if (!expected_failure || !cleaned || !absent) return false;
		const char* failure_stage = fail_after_read ? "after-read" : "after-pop";
		std::printf("buffer failure: stage=%s parent_pid=%d child_pid=%d child_exit=3, "
		            "published descriptor settled, cleanup=creator-unlinked\n",
		            failure_stage, static_cast<int>(::getpid()), child_pid);
		return true;
	}

	if (!read_token(child.event_read, 'S', std::chrono::seconds(3))) {
		(void)wait_child(&child, std::chrono::seconds(1));
		(void)cleanup(&child, &queue, &pool, &buffer, &published_descriptor);
		return false;
	}
	if (!wait_child(&child, std::chrono::seconds(3))) {
		(void)cleanup(&child, &queue, &pool, &buffer, &published_descriptor);
		return false;
	}
	const auto status = pool.status();
	if (!status || status.value().free_blocks != 1 || status.value().reading_blocks != 0) {
		(void)cleanup(&child, &queue, &pool, &buffer, &published_descriptor);
		return false;
	}
	const bool cleaned = cleanup(&child, &queue, &pool, &buffer, &published_descriptor);
	const bool absent = resources_absent(queue_options, pool_options);
	if (!cleaned || !absent) return false;
	std::printf("buffer: parent_pid=%d child_pid=%d child_exit=0 descriptor over MPMC, "
	            "independent read/release, cleanup=creator-unlinked\n",
	            static_cast<int>(::getpid()), child_pid);
	return true;
}

}  // namespace consume_demo
