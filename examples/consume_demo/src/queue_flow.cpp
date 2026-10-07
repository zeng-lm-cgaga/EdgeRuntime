#include "consume_demo/queue_flow.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <sys/types.h>
#include <unistd.h>

#include "consume_demo/common.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"

namespace consume_demo {

namespace {

using Queue = edge_runtime::MpmcQueue<QueueMessage>;

bool retry_push(Queue* queue, const QueueMessage& message,
				std::chrono::steady_clock::time_point deadline) {
	for (;;) {
		auto result = queue->wait_push_until(message, deadline);
		if (result) return true;
		if (result.error().code != edge_runtime::ErrorCode::kQueueContention ||
		    std::chrono::steady_clock::now() >= deadline) {
			return false;
		}
	}
}

void cleanup(ChildProcess* child, Queue* queue) {
	if (child != nullptr && child->pid > 0) stop_child(child);
	if (queue != nullptr) {
		const auto removed = queue->remove_if_creator();
		if (!removed) {
			std::fprintf(stderr, "queue cleanup failed: %s\n",
			             edge_runtime::to_string(removed.error().code));
		}
	}
	close_child(child);
}

}  // namespace

int run_queue_child(const std::string& name, int event_fd, int command_fd) {
	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = 1;
	auto opened = Queue::open(options, queue_schema());
	if (!opened) {
		(void)write_token(event_fd, 'F');
		return 3;
	}
	Queue queue = std::move(opened.value());
	const auto empty = queue.wait_pop_until(std::chrono::steady_clock::now() +
	                                        std::chrono::milliseconds(150));
	if (empty || empty.error().code != edge_runtime::ErrorCode::kTimeout) {
		(void)write_token(event_fd, 'F');
		return 3;
	}
	if (!write_token(event_fd, 'E') || !read_token(command_fd, 'P', std::chrono::seconds(3))) {
		return 3;
	}
	const auto received = queue.wait_pop_until(std::chrono::steady_clock::now() +
	                                           std::chrono::seconds(3));
	if (!received || received.value().sequence != 1 || received.value().value != 42) {
		(void)write_token(event_fd, 'F');
		return 3;
	}
	if (!write_token(event_fd, 'S')) return 3;
	return 0;
}

bool run_queue_flow() {
	const std::string name = unique_name("er_demo_queue");
	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = 1;
	auto created = Queue::create(options, queue_schema());
	if (!created) {
		std::fprintf(stderr, "queue create failed: %s\n",
		             edge_runtime::to_string(created.error().code));
		return false;
	}
	Queue queue = std::move(created.value());
	int events[2] = {-1, -1};
	int commands[2] = {-1, -1};
	if (::pipe(events) != 0 || ::pipe(commands) != 0) {
		if (events[0] >= 0) (void)::close(events[0]);
		if (events[1] >= 0) (void)::close(events[1]);
		if (commands[0] >= 0) (void)::close(commands[0]);
		if (commands[1] >= 0) (void)::close(commands[1]);
		cleanup(nullptr, &queue);
		return false;
	}
	const ChildProcess spawned = spawn_child(
	        "queue-child", {name, std::to_string(events[1]), std::to_string(commands[0])},
	        {events[0], events[1], commands[0], commands[1]}, {events[1], commands[0]});
	ChildProcess child = spawned;
	const int child_pid = child.pid;
	(void)::close(events[1]);
	(void)::close(commands[0]);
	events[1] = -1;
	commands[0] = -1;
	child.event_read = events[0];
	child.command_write = commands[1];
	if (child.pid <= 0) {
		(void)::close(events[0]);
		(void)::close(commands[1]);
		cleanup(&child, &queue);
		return false;
	}
	if (!read_token(child.event_read, 'E', std::chrono::seconds(3))) {
		cleanup(&child, &queue);
		return false;
	}
	const auto first_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
	if (!retry_push(&queue, QueueMessage{1, 42, 0}, first_deadline)) {
		cleanup(&child, &queue);
		return false;
	}
	const auto full_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(180);
	const auto full = queue.wait_push_until(QueueMessage{2, 84, 0}, full_deadline);
	if (full || full.error().code != edge_runtime::ErrorCode::kTimeout) {
		std::fprintf(stderr, "queue full boundary did not timeout\n");
		cleanup(&child, &queue);
		return false;
	}
	if (!write_token(child.command_write, 'P') ||
	    !read_token(child.event_read, 'S', std::chrono::seconds(3)) ||
	    !wait_child(&child, std::chrono::seconds(3))) {
		cleanup(&child, &queue);
		return false;
	}
	close_child(&child);
	const auto removed = queue.remove_if_creator();
	if (!removed) return false;
	const auto reopened = Queue::open(options, queue_schema());
	if (reopened || reopened.error().code != edge_runtime::ErrorCode::kNotFound) return false;
	std::printf("queue: parent_pid=%d child_pid=%d child_exit=0 empty/full deadlines, wait "
	            "push/pop, cleanup=creator-unlinked\n",
	            static_cast<int>(::getpid()), child_pid);
	return true;
}

}  // namespace consume_demo
