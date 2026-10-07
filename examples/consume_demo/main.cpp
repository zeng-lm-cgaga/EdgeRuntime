#include <cstdio>
#include <csignal>
#include <cstdlib>
#include <string>

#include "consume_demo/channel_flow.hpp"
#include "consume_demo/common.hpp"
#include "consume_demo/pool_flow.hpp"
#include "consume_demo/queue_flow.hpp"

namespace {

int parse_fd(const char* text) {
	if (text == nullptr) return -1;
	char* end = nullptr;
	const long value = std::strtol(text, &end, 10);
	if (end == text || *end != '\0' || value < 0 || value > 1'000'000) return -1;
	return static_cast<int>(value);
}

}  // namespace

int main(int argc, char** argv) {
	(void)std::signal(SIGPIPE, SIG_IGN);
	if (argc >= 2 && std::string(argv[1]) == "channel-child" && argc == 4) {
		return consume_demo::run_channel_child(argv[2], parse_fd(argv[3]));
	}
	if (argc >= 2 && std::string(argv[1]) == "queue-child" && argc == 5) {
		return consume_demo::run_queue_child(argv[2], parse_fd(argv[3]), parse_fd(argv[4]));
	}
	if (argc >= 2 && std::string(argv[1]) == "pool-child" && argc == 5) {
		return consume_demo::run_pool_child(argv[2], argv[3], parse_fd(argv[4]));
	}
	if (argc >= 2 && std::string(argv[1]) == "pool-child-fail-after-pop" && argc == 5) {
		return consume_demo::run_pool_child(argv[2], argv[3], parse_fd(argv[4]), true);
	}
	if (argc >= 2 && std::string(argv[1]) == "pool-child-fail-after-read" && argc == 5) {
		return consume_demo::run_pool_child(argv[2], argv[3], parse_fd(argv[4]), false, true);
	}
	if (argc != 1) {
		std::fprintf(stderr, "usage: %s\n", argv[0]);
		return 2;
	}

	if (consume_demo::executable_path().empty() || !consume_demo::run_channel_flow() ||
	    !consume_demo::run_queue_flow() || !consume_demo::run_pool_flow()) {
		return 1;
	}
	std::printf("OK installed EdgeRuntime public API fork/exec audit\n");
	return 0;
}
