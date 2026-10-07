#include "consume_demo/channel_flow.hpp"

#include <cstdio>
#include <fcntl.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>

#include "consume_demo/common.hpp"
#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/loan.hpp"
#include "edge_runtime/channel/producer.hpp"

namespace consume_demo {

namespace {

using Consumer = edge_runtime::Consumer<Point>;
using Producer = edge_runtime::Producer<Point>;

void cleanup(ChildProcess* child, Producer* producer, const edge_runtime::ChannelOptions& options) {
	if (child != nullptr && child->pid > 0) stop_child(child);
	if (producer != nullptr) {
		const auto removed = producer->remove_if_owner();
		if (!removed) {
			std::fprintf(stderr, "channel cleanup failed: %s\n",
			             edge_runtime::to_string(removed.error().code));
		}
	}
	(void)options;
	close_child(child);
}

}  // namespace

int run_channel_child(const std::string& name, int event_fd) {
	edge_runtime::ChannelOptions options;
	options.name = name;
	auto opened = Consumer::open(options, point_schema());
	if (!opened) {
		(void)write_token(event_fd, 'F');
		return 3;
	}
	Consumer consumer = std::move(opened.value());
	if (!write_token(event_fd, 'R')) return 3;
	auto read_loan = consumer.wait_loan_latest(std::chrono::milliseconds(2500));
	Point point{};
	if (!read_loan || !decode_point(read_loan.value().data(), read_loan.value().size(), &point) ||
	    point.x != 3 || point.y != 4 || read_loan.value().sequence() == 0) {
		(void)write_token(event_fd, 'F');
		return 3;
	}
	read_loan.value().release();
	if (!write_token(event_fd, 'S')) return 3;
	return 0;
}

bool run_channel_flow() {
	const std::string name = unique_name("er_demo_channel");
	edge_runtime::ChannelOptions options;
	options.name = name;
	auto created = Producer::create(options, point_schema());
	if (!created) {
		std::fprintf(stderr, "channel create failed: %s\n",
		             edge_runtime::to_string(created.error().code));
		return false;
	}
	Producer producer = std::move(created.value());
	int events[2] = {-1, -1};
	if (::pipe(events) != 0) {
		cleanup(nullptr, &producer, options);
		return false;
	}
	const ChildProcess spawned = spawn_child(
	        "channel-child", {name, std::to_string(events[1])}, {events[0], events[1]}, {events[1]});
	ChildProcess child = spawned;
	const int child_pid = child.pid;
	(void)::close(events[1]);
	events[1] = -1;
	child.event_read = events[0];
	if (child.pid <= 0) {
		(void)::close(events[0]);
		events[0] = -1;
		cleanup(&child, &producer, options);
		return false;
	}
	if (!read_token(child.event_read, 'R', std::chrono::seconds(3))) {
		cleanup(&child, &producer, options);
		return false;
	}
	auto loan = producer.loan();
	if (!loan || !encode_point(Point{3, 4}, loan.value().data(), loan.value().size()) ||
	    !loan.value().commit()) {
		std::fprintf(stderr, "channel loan/commit failed\n");
		cleanup(&child, &producer, options);
		return false;
	}
	if (!read_token(child.event_read, 'S', std::chrono::seconds(3)) ||
	    !wait_child(&child, std::chrono::seconds(3))) {
		cleanup(&child, &producer, options);
		return false;
	}
	close_child(&child);
	const auto removed = producer.remove_if_owner();
	if (!removed) return false;
	const auto reopened = Consumer::open(options, point_schema());
	if (reopened || reopened.error().code != edge_runtime::ErrorCode::kNotFound) return false;
	std::printf("channel: parent_pid=%d child_pid=%d child_exit=0 fork/exec loan sequence "
	            "verified cleanup=creator-unlinked\n",
	            static_cast<int>(::getpid()), child_pid);
	return true;
}

}  // namespace consume_demo
