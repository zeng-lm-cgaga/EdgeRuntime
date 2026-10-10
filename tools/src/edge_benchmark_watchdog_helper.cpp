// Benchmark-only owner supervisor used to exercise timeout cleanup when the
// worker parent exits before its fork/exec children.  It uses only public
// EdgeRuntime APIs and is never part of the installed library.
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/common/schema.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"

namespace {

using edge_runtime::BufferHandle;
using edge_runtime::ErrorCode;
using Queue = edge_runtime::MpmcQueue<BufferHandle>;

struct Payload {
	uint64_t magic{0x455257444F47504Cull};
	uint64_t sequence{0};
	std::array<std::byte, 48> body{};
};

using PayloadQueue = edge_runtime::MpmcQueue<Payload>;

static_assert(std::is_trivially_copyable_v<Payload>);
static_assert(sizeof(Payload) == 64);

constexpr uint64_t kMagic = 0x455257444F47504Cull;
volatile sig_atomic_t g_stop_requested = 0;

void stop_signal_handler(int) noexcept { g_stop_requested = 1; }

bool install_stop_handler() noexcept {
	struct sigaction action {};
	action.sa_handler = stop_signal_handler;
	if (::sigemptyset(&action.sa_mask) != 0) return false;
	return ::sigaction(SIGTERM, &action, nullptr) == 0;
}

const char* value_of(int argc, char** argv, const char* key) noexcept {
	for (int i = 1; i + 1 < argc; ++i) {
		if (std::strcmp(argv[i], key) == 0) return argv[i + 1];
	}
	return nullptr;
}

bool has_flag(int argc, char** argv, const char* key) noexcept {
	for (int i = 1; i < argc; ++i) {
		if (std::strcmp(argv[i], key) == 0) return true;
	}
	return false;
}

uint64_t number_of(int argc, char** argv, const char* key, uint64_t fallback) noexcept {
	const char* value = value_of(argc, argv, key);
	if (value == nullptr) return fallback;
	char* end = nullptr;
	const unsigned long long parsed = std::strtoull(value, &end, 10);
	if (end == value || *end != '\0') return fallback;
	return static_cast<uint64_t>(parsed);
}

uint64_t monotonic_ns() noexcept {
	struct timespec ts {};
	(void)::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
	return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
	       static_cast<uint64_t>(ts.tv_nsec);
}

bool write_full(int fd, const void* data, size_t size) noexcept {
	const auto* bytes = static_cast<const unsigned char*>(data);
	while (size != 0) {
		const ssize_t count = ::write(fd, bytes, size);
		if (count > 0) {
			bytes += count;
			size -= static_cast<size_t>(count);
			continue;
		}
		if (count < 0 && errno == EINTR) continue;
		return false;
	}
	return true;
}

void report_ready(int fd) noexcept {
	const char value = 'R';
	(void)write_full(fd, &value, sizeof(value));
	(void)::close(fd);
}

std::string self_executable() {
	char buffer[4096]{};
	const ssize_t length = ::readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
	if (length <= 0) return {};
	buffer[length] = '\0';
	return buffer;
}

std::array<std::byte, 32> fingerprint(uint8_t seed) noexcept {
	std::array<std::byte, 32> value{};
	for (size_t i = 0; i < value.size(); ++i) {
		value[i] = std::byte{static_cast<unsigned char>(seed + static_cast<uint8_t>(i * 7u))};
	}
	return value;
}

edge_runtime::SchemaDescriptor queue_schema() noexcept {
	return {fingerprint(0x51), 1, "EdgeRuntimeWatchdogPayload"};
}

edge_runtime::SchemaDescriptor pool_schema(uint32_t block_size) noexcept {
	return {fingerprint(static_cast<uint8_t>(0x70u ^ block_size)), 1,
	        "EdgeRuntimeWatchdogPoolPayload"};
}

Payload make_payload() noexcept {
	Payload value{};
	value.magic = kMagic;
	value.sequence = 1;
	for (size_t i = 0; i < value.body.size(); ++i) {
		value.body[i] = std::byte{static_cast<unsigned char>(i ^ 0xA5u)};
	}
	return value;
}

pid_t spawn_exec(const std::string& executable, const std::vector<std::string>& arguments) {
	const pid_t pid = ::fork();
	if (pid != 0) return pid;
	std::vector<char*> child_argv;
	child_argv.reserve(arguments.size() + 1);
	for (const auto& argument : arguments) {
		child_argv.push_back(const_cast<char*>(argument.c_str()));
	}
	child_argv.push_back(nullptr);
	::execv(executable.c_str(), child_argv.data());
	::_exit(127);
}

bool group_alive(pid_t pgid) noexcept {
	if (pgid <= 0) return false;
	if (::kill(-pgid, 0) == 0) return true;
	return errno == EPERM;
}

bool wait_ready_pipe(int fd, size_t expected, uint64_t timeout_ms) {
	size_t ready = 0;
	const uint64_t deadline = monotonic_ns() + timeout_ms * 1000000ull;
	while (ready < expected) {
		if (monotonic_ns() >= deadline) return false;
		struct pollfd descriptor {fd, POLLIN | POLLHUP, 0};
		const int result = ::poll(&descriptor, 1, 50);
		if (result < 0 && errno == EINTR) continue;
		if (result < 0) return false;
		if (result == 0) continue;
		char values[16]{};
		const ssize_t count = ::read(fd, values, sizeof(values));
		if (count == 0) return false;
		if (count < 0) {
			if (errno == EINTR) continue;
			return false;
		}
		for (ssize_t i = 0; i < count; ++i) {
			if (values[i] != 'R') return false;
			++ready;
		}
	}
	return true;
}

bool write_ready_file(const std::string& path, const std::string& kind,
	                      const std::string& pool_name, const std::string& queue_name,
	                      pid_t worker_parent, pid_t worker_pgid,
	                      const std::vector<pid_t>& workers) {
	FILE* file = std::fopen(path.c_str(), "w");
	if (file == nullptr) return false;
	bool ok = std::fprintf(file, "READY kind=%s pool=%s queue=%s worker_parent=%ld worker_pgid=%ld\n",
	                       kind.c_str(), pool_name.c_str(), queue_name.c_str(),
	                       static_cast<long>(worker_parent), static_cast<long>(worker_pgid)) >= 0;
	for (pid_t worker : workers) {
		if (!ok) break;
		ok = std::fprintf(file, "WORKER pid=%ld\n", static_cast<long>(worker)) >= 0;
	}
	if (std::fclose(file) != 0) ok = false;
	return ok;
}

int run_mpmc_worker(int argc, char** argv) {
	if (!install_stop_handler()) return 2;
	if (has_flag(argc, argv, "--ignore-term-worker")) (void)::signal(SIGTERM, SIG_IGN);
	const char* name = value_of(argc, argv, "--queue-name");
	const int ready_fd = static_cast<int>(number_of(argc, argv, "--ready-fd", 0));
	const bool producer = std::strcmp(value_of(argc, argv, "--worker-role"), "producer") == 0;
	if (name == nullptr) return 2;
	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = 1;
	auto queue = edge_runtime::MpmcQueue<Payload>::open(options, queue_schema());
	if (!queue) return 3;
	report_ready(ready_fd);
	if (producer) {
		const Payload payload = make_payload();
		while (!g_stop_requested) {
			auto pushed = queue.value().try_push(payload);
			if (pushed || pushed.error().code != ErrorCode::kQueueFull) break;
			::sched_yield();
		}
	}
	while (!g_stop_requested) ::usleep(10000);
	return 0;
}

int run_pool_worker(int argc, char** argv) {
	if (!install_stop_handler()) return 2;
	if (has_flag(argc, argv, "--ignore-term-worker")) (void)::signal(SIGTERM, SIG_IGN);
	const char* pool_name = value_of(argc, argv, "--pool-name");
	const char* queue_name = value_of(argc, argv, "--queue-name");
	const int ready_fd = static_cast<int>(number_of(argc, argv, "--ready-fd", 0));
	const bool producer = std::strcmp(value_of(argc, argv, "--worker-role"), "producer") == 0;
	const uint32_t block_size = static_cast<uint32_t>(number_of(argc, argv, "--block-size", 4096));
	const uint32_t block_count = static_cast<uint32_t>(number_of(argc, argv, "--block-count", 2));
	if (pool_name == nullptr || queue_name == nullptr) return 2;
	edge_runtime::SharedBufferPoolOptions pool_options;
	pool_options.name = pool_name;
	pool_options.block_size = block_size;
	pool_options.block_count = block_count;
	pool_options.schema = pool_schema(block_size);
	edge_runtime::MpmcQueueOptions queue_options;
	queue_options.name = queue_name;
	queue_options.capacity = 1;
	auto pool = edge_runtime::SharedBufferPool::open(pool_options);
	auto queue = Queue::open(queue_options, queue_schema());
	if (!pool || !queue) return 3;
	report_ready(ready_fd);
	if (producer) {
		for (;;) {
			auto write = pool.value().try_acquire_write();
			if (!write) {
				if (write.error().code != ErrorCode::kBufferPoolFull &&
				    write.error().code != ErrorCode::kBufferPoolContention) break;
				if (g_stop_requested) break;
				::sched_yield();
				continue;
			}
			auto published = write.value().publish();
			if (!published) break;
			while (!g_stop_requested) {
				auto pushed = queue.value().try_push(published.value());
				if (pushed) return 0;
				if (pushed.error().code != ErrorCode::kQueueFull &&
				    pushed.error().code != ErrorCode::kQueueContention)
					break;
				::sched_yield();
			}
			if (g_stop_requested) {
				auto settled = pool.value().acquire_read(published.value());
				if (settled) settled.value().release();
			}
			break;
		}
	}
	while (!g_stop_requested) ::usleep(10000);
	return 0;
}

int run_worker_parent(int argc, char** argv, const std::string& executable) {
	const char* kind = value_of(argc, argv, "--kind");
	const char* pool_name = value_of(argc, argv, "--pool-name");
	const char* queue_name = value_of(argc, argv, "--queue-name");
	const char* ready_path = value_of(argc, argv, "--ready-file");
	if (kind == nullptr || queue_name == nullptr || ready_path == nullptr) return 2;
	int ready_pipe[2] = {-1, -1};
	if (::pipe(ready_pipe) != 0) return 2;
	std::vector<pid_t> workers;
	for (const char* role : {"consumer", "producer"}) {
		std::vector<std::string> args = {executable, "--role", "worker", "--kind", kind,
		                                "--worker-role", role, "--queue-name", queue_name,
		                                "--ready-fd", std::to_string(ready_pipe[1])};
		if (std::strcmp(kind, "pool") == 0) {
			args.push_back("--pool-name");
			args.push_back(pool_name == nullptr ? "" : pool_name);
			args.push_back("--block-size");
			args.push_back("4096");
			args.push_back("--block-count");
			args.push_back("2");
		}
		if (has_flag(argc, argv, "--ignore-term-worker") && std::strcmp(role, "consumer") == 0) {
			args.push_back("--ignore-term-worker");
		}
		const pid_t pid = spawn_exec(executable, args);
		if (pid < 0) {
			(void)::close(ready_pipe[0]);
			(void)::close(ready_pipe[1]);
			return 2;
		}
		workers.push_back(pid);
	}
	(void)::close(ready_pipe[1]);
	if (!wait_ready_pipe(ready_pipe[0], workers.size(), 5000)) {
		(void)::close(ready_pipe[0]);
		return 3;
	}
	(void)::close(ready_pipe[0]);
	if (!write_ready_file(ready_path, kind, pool_name == nullptr ? "" : pool_name, queue_name,
	                      ::getpid(), ::getpgrp(), workers)) {
		return 3;
	}
	// The test intentionally leaves the two independently exec'd workers behind.
	::_exit(0);
}

bool drain_pool(edge_runtime::SharedBufferPool* pool, Queue* queue) {
	const uint64_t deadline = monotonic_ns() + 2000ull * 1000000ull;
	while (monotonic_ns() < deadline) {
		auto popped = queue->try_pop();
		if (popped) {
			auto read = pool->acquire_read(popped.value());
			if (!read) return false;
			read.value().release();
			continue;
		}
		if (popped.error().code == ErrorCode::kQueueEmpty) return true;
		if (popped.error().code != ErrorCode::kQueueContention) return false;
		::sched_yield();
	}
	return false;
}

void terminate_worker_group(pid_t pgid, bool* kill_sent) {
	*kill_sent = false;
	if (pgid <= 0) return;
	auto reap_children = []() noexcept {
		int status = 0;
		while (::waitpid(-1, &status, WNOHANG) > 0) {
		}
	};
	(void)::kill(-pgid, SIGCONT);
	(void)::kill(-pgid, SIGTERM);
	const uint64_t deadline = monotonic_ns() + 600ull * 1000000ull;
	while (group_alive(pgid) && monotonic_ns() < deadline) {
		reap_children();
		::usleep(10000);
	}
	reap_children();
	if (group_alive(pgid)) {
		if (::kill(-pgid, SIGKILL) == 0) *kill_sent = true;
		const uint64_t kill_deadline = monotonic_ns() + 1000ull * 1000000ull;
		while (group_alive(pgid) && monotonic_ns() < kill_deadline) {
			reap_children();
			::usleep(10000);
		}
		reap_children();
	}
}

int run_supervisor(int argc, char** argv, const std::string& executable) {
	if (!install_stop_handler()) return 2;
	const char* kind_arg = value_of(argc, argv, "--kind");
	const char* ready_path_arg = value_of(argc, argv, "--ready-file");
	if (kind_arg == nullptr || ready_path_arg == nullptr) return 2;
	const std::string kind = kind_arg;
	const std::string base = "er_watchdog_" + std::to_string(static_cast<long>(::getpid())) +
	                         "_" + std::to_string(static_cast<unsigned long long>(monotonic_ns()));
	const std::string pool_name = base + "_pool";
	const std::string queue_name = base + "_queue";
	edge_runtime::MpmcQueueOptions queue_options;
	queue_options.name = queue_name;
	queue_options.capacity = 1;
	std::optional<PayloadQueue> payload_queue;
	std::optional<Queue> queue;
	std::optional<edge_runtime::SharedBufferPool> pool;
	if (kind == "mpmc") {
		auto created = edge_runtime::MpmcQueue<Payload>::create(queue_options, queue_schema());
		if (!created) return 2;
		payload_queue.emplace(std::move(created.value()));
	} else if (kind == "pool") {
		edge_runtime::SharedBufferPoolOptions pool_options;
		pool_options.name = pool_name;
		pool_options.block_size = 4096;
		pool_options.block_count = 2;
		pool_options.schema = pool_schema(4096);
		auto created_pool = edge_runtime::SharedBufferPool::create(pool_options);
		if (!created_pool) return 2;
		pool.emplace(std::move(created_pool.value()));
		auto created_queue = Queue::create(queue_options, queue_schema());
		if (!created_queue) {
			(void)pool->remove_if_creator();
			return 2;
		}
		queue.emplace(std::move(created_queue.value()));
	} else {
		return 2;
	}

	(void)::prctl(PR_SET_CHILD_SUBREAPER, 1);
	const pid_t worker_parent = ::fork();
	if (worker_parent < 0) return 2;
	if (worker_parent == 0) {
		(void)::setpgid(0, 0);
		std::vector<std::string> args = {executable, "--role", "worker-parent", "--kind", kind,
		                                "--queue-name", queue_name, "--ready-file", ready_path_arg};
		if (kind == "pool") {
			args.push_back("--pool-name");
			args.push_back(pool_name);
		}
		if (has_flag(argc, argv, "--ignore-term-worker")) args.push_back("--ignore-term-worker");
		std::vector<char*> child_argv;
		for (auto& arg : args) child_argv.push_back(arg.data());
		child_argv.push_back(nullptr);
		::execv(executable.c_str(), child_argv.data());
		::_exit(127);
	}
	(void)::setpgid(worker_parent, worker_parent);
	const pid_t worker_pgid = worker_parent;
	int worker_parent_status = 0;
	bool worker_parent_reaped = false;
	while (!g_stop_requested && !worker_parent_reaped) {
		const pid_t result = ::waitpid(worker_parent, &worker_parent_status, WNOHANG);
		if (result == worker_parent) {
			worker_parent_reaped = true;
			break;
		}
		if (result < 0 && errno != EINTR) break;
		::usleep(10000);
	}
	if (g_stop_requested && !worker_parent_reaped) {
		bool ignored_kill = false;
		terminate_worker_group(worker_pgid, &ignored_kill);
		while (::waitpid(worker_parent, &worker_parent_status, 0) < 0 && errno == EINTR) {
		}
		worker_parent_reaped = true;
	}
	if (worker_parent_reaped && !g_stop_requested) {
		if (::access(ready_path_arg, F_OK) != 0) g_stop_requested = 1;
		while (!g_stop_requested) ::usleep(10000);
	}
	std::printf("WORKER_PARENT pid=%ld exit=%d\n", static_cast<long>(worker_parent),
	            WIFEXITED(worker_parent_status) ? WEXITSTATUS(worker_parent_status) : -1);
	std::fflush(stdout);
	bool worker_kill_sent = false;
	terminate_worker_group(worker_pgid, &worker_kill_sent);
	int status = 0;
	while (::waitpid(-1, &status, WNOHANG) > 0) {
	}
	bool cleanup_ok = true;
	if (kind == "pool") cleanup_ok = drain_pool(&pool.value(), &queue.value());
	bool removed_queue = false;
	bool queue_absent = false;
	std::string reopened_queue_status;
	if (kind == "pool") {
		const auto removed = queue->remove_if_creator();
		removed_queue = static_cast<bool>(removed);
		auto reopened = Queue::open(queue_options, queue_schema());
		queue_absent = !reopened && reopened.error().code == ErrorCode::kNotFound;
		reopened_queue_status =
		        reopened ? "FOUND" : edge_runtime::to_string(reopened.error().code);
	} else {
		const auto removed = payload_queue->remove_if_creator();
		removed_queue = static_cast<bool>(removed);
		auto reopened = PayloadQueue::open(queue_options, queue_schema());
		queue_absent = !reopened && reopened.error().code == ErrorCode::kNotFound;
		reopened_queue_status =
		        reopened ? "FOUND" : edge_runtime::to_string(reopened.error().code);
	}
	bool pool_absent = true;
	bool removed_pool = true;
	if (kind == "pool") {
		removed_pool = static_cast<bool>(pool->remove_if_creator());
		edge_runtime::SharedBufferPoolOptions pool_options;
		pool_options.name = pool_name;
		pool_options.block_size = 4096;
		pool_options.block_count = 2;
		pool_options.schema = pool_schema(4096);
		auto reopened_pool = edge_runtime::SharedBufferPool::open(pool_options);
		pool_absent = !reopened_pool && reopened_pool.error().code == ErrorCode::kNotFound;
		std::printf("CLEANUP kind=%s queue=%s pool=%s reopen_queue=%s reopen_pool=%s "
		            "parent_exit=%s worker_kill=%s drain=%s\n",
		            kind.c_str(), removed_queue ? "SUCCESS" : "FAIL",
		            removed_pool ? "SUCCESS" : "FAIL",
		            reopened_queue_status.c_str(),
		            reopened_pool ? "FOUND" : edge_runtime::to_string(reopened_pool.error().code),
		            worker_parent_reaped && WIFEXITED(worker_parent_status) ? "true" : "false",
		            worker_kill_sent ? "true" : "false", cleanup_ok ? "SETTLED" : "INCOMPLETE");
	} else {
		std::printf("CLEANUP kind=%s queue=%s reopen_queue=%s parent_exit=%s worker_kill=%s\n",
		            kind.c_str(), removed_queue ? "SUCCESS" : "FAIL",
		            reopened_queue_status.c_str(),
		            worker_parent_reaped && WIFEXITED(worker_parent_status) ? "true" : "false",
		            worker_kill_sent ? "true" : "false");
	}
	std::fflush(stdout);
	return cleanup_ok && removed_queue && queue_absent && removed_pool && pool_absent ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
	const std::string executable = self_executable();
	if (executable.empty()) return 2;
	const char* role = value_of(argc, argv, "--role");
	if (role != nullptr) {
		if (std::strcmp(role, "worker") == 0) {
			if (has_flag(argc, argv, "--ignore-term-worker")) (void)::signal(SIGTERM, SIG_IGN);
			const char* kind = value_of(argc, argv, "--kind");
			if (kind != nullptr && std::strcmp(kind, "pool") == 0) return run_pool_worker(argc, argv);
			return run_mpmc_worker(argc, argv);
		}
		if (std::strcmp(role, "worker-parent") == 0) return run_worker_parent(argc, argv, executable);
	}
	return run_supervisor(argc, argv, executable);
}
