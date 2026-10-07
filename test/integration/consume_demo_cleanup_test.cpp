#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>

#include "consume_demo/common.hpp"
#include "consume_demo/pool_flow.hpp"

namespace {

int parse_fd(const char* text) {
	if (text == nullptr) return -1;
	char* end = nullptr;
	const long value = std::strtol(text, &end, 10);
	if (end == text || *end != '\0' || value < 0 || value > 1'000'000) return -1;
	return static_cast<int>(value);
}

bool set_create_failure(const char* kind) {
	return ::setenv("ER_CONSUME_DEMO_FAIL_CREATE", kind, 1) == 0;
}

void clear_create_failure() { (void)::unsetenv("ER_CONSUME_DEMO_FAIL_CREATE"); }

}  // namespace

extern "C" int __real_shm_open(const char* name, int flags, mode_t mode);

extern "C" int __wrap_shm_open(const char* name, int flags, mode_t mode) {
	const char* failure = std::getenv("ER_CONSUME_DEMO_FAIL_CREATE");
	if (name != nullptr && failure != nullptr && (flags & O_CREAT) != 0) {
		const bool fail_queue = std::strcmp(failure, "queue") == 0 &&
		                        std::strstr(name, ".mpmc.") != nullptr;
		const bool fail_pool = std::strcmp(failure, "pool") == 0 &&
		                       std::strstr(name, ".bufferpool.") != nullptr;
		if (fail_queue || fail_pool) {
			errno = EMFILE;
			return -1;
		}
	}
	return __real_shm_open(name, flags, mode);
}

int main(int argc, char** argv) {
	(void)std::signal(SIGPIPE, SIG_IGN);
	if (argc == 5 && std::string(argv[1]) == "pool-child") {
		return consume_demo::run_pool_child(argv[2], argv[3], parse_fd(argv[4]));
	}
	if (argc == 5 && std::string(argv[1]) == "pool-child-fail-after-pop") {
		return consume_demo::run_pool_child(argv[2], argv[3], parse_fd(argv[4]), true);
	}
	if (argc == 5 && std::string(argv[1]) == "pool-child-fail-after-read") {
		return consume_demo::run_pool_child(argv[2], argv[3], parse_fd(argv[4]), false, true);
	}
	if (argc != 1) return 2;

	if (!set_create_failure("queue")) return 1;
	const bool queue_failure = consume_demo::run_pool_flow(
		consume_demo::PoolFlowScenario::kQueueCreateFailure);
	clear_create_failure();
	if (!queue_failure) return 1;

	if (!set_create_failure("pool")) return 1;
	const bool pool_failure = consume_demo::run_pool_flow(
		consume_demo::PoolFlowScenario::kPoolCreateFailure);
	clear_create_failure();
	if (!pool_failure) return 1;

	if (!consume_demo::run_pool_flow(consume_demo::PoolFlowScenario::kPeerFailsAfterPop)) return 1;
	if (!consume_demo::run_pool_flow(consume_demo::PoolFlowScenario::kPeerFailsAfterRead)) return 1;
	std::printf("consume_demo cleanup regressions: partial-create, pre-read, and after-read "
				"peer failures clean\n");
	return 0;
}
