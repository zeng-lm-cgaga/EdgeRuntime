// 监督器工具把 ProducerSupervisor 的状态事件转换为稳定的一行标记，便于驱动核对。
#include <atomic>
#include <csignal>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "edge_runtime/common/error.hpp"
#include "edge_runtime/process/supervisor.hpp"
#include "edge_runtime_tool/tool_common.hpp"

namespace {

std::vector<std::string> split_ws(const std::string& s) {
	std::vector<std::string> out;
	std::string cur;
	for (const char c : s) {
		if (c == ' ' || c == '\t') {
			if (!cur.empty()) {
				out.push_back(cur);
				cur.clear();
			}
		} else {
			cur.push_back(c);
		}
	}
	if (!cur.empty()) out.push_back(cur);
	return out;
}

int64_t arg_ms(int argc, char** argv, const char* flag, int64_t dflt) {
	const char* v = edge_tool::arg_value(argc, argv, flag);
	return v != nullptr ? std::strtoll(v, nullptr, 10) : dflt;
}

void on_event(const edge_runtime::SupervisorEventInfo& info, void*) {
	switch (info.event) {
		case edge_runtime::SupervisorEvent::kSupervised:
			std::printf("SUPERVISED pid=%" PRIu64 " gen=%" PRIu64 "\n", info.pid,
			            info.generation);
			break;
		case edge_runtime::SupervisorEvent::kStallDetected:
			std::printf("STALL_DETECTED pid=%" PRIu64 "\n", info.pid);
			break;
		case edge_runtime::SupervisorEvent::kKilled:
			std::printf("KILLED sig=%u pid=%" PRIu64 "\n", info.signal, info.pid);
			break;
		case edge_runtime::SupervisorEvent::kRestartArmed:
			std::printf("RESTART attempt=%u delay=%" PRIu64 "ns\n", info.attempt,
			            info.delay_ns);
			break;
	}
	std::fflush(stdout);
}

}

int main(int argc, char** argv) {
	const char* name = edge_tool::arg_value(argc, argv, "--name");
	const char* producer_argv_str = edge_tool::arg_value(argc, argv, "--producer-argv");
	if (name == nullptr || producer_argv_str == nullptr) {
		std::fprintf(stderr,
		             "usage: edge_shm_supervisor --name <name> --producer-argv \"bin --args\" "
		             "[--transport fd|posix] [--initial-delay-ms N] [--max-delay-ms N] "
		             "[--multiplier N] [--max-restarts N] [--stable-reset-ms N] "
		             "[--stall-grace-ms N] [--watch-interval-ms N] [--create-timeout-ms N] "
		             "[--child-env K=V]... [--forward-stdout 0|1]\n");
		return 2;
	}

	edge_runtime::SupervisorOptions opts;
	opts.channel_name = name;
	opts.producer_argv = split_ws(producer_argv_str);
	{
		const char* t = edge_tool::arg_value(argc, argv, "--transport");
		if (t != nullptr && std::strcmp(t, "fd") == 0) {
			opts.transport = edge_runtime::Transport::kMemfdFdPass;
		}
	}
	opts.initial_delay = std::chrono::milliseconds(arg_ms(argc, argv, "--initial-delay-ms", 100));
	opts.max_delay = std::chrono::milliseconds(arg_ms(argc, argv, "--max-delay-ms", 10000));
	opts.multiplier = static_cast<uint32_t>(arg_ms(argc, argv, "--multiplier", 2));
	opts.max_restarts = static_cast<uint32_t>(arg_ms(argc, argv, "--max-restarts", 10));
	opts.stable_reset_window =
	        std::chrono::milliseconds(arg_ms(argc, argv, "--stable-reset-ms", 60000));
	opts.stall_grace = std::chrono::milliseconds(arg_ms(argc, argv, "--stall-grace-ms", 5000));
	opts.watch_interval =
	        std::chrono::milliseconds(arg_ms(argc, argv, "--watch-interval-ms", 500));
	opts.create_timeout =
	        std::chrono::milliseconds(arg_ms(argc, argv, "--create-timeout-ms", 10000));
	const bool forward_stdout =
	        edge_tool::arg_u64(argc, argv, "--forward-stdout", 0) != 0;
	opts.on_event = on_event;

	for (int i = 1; i + 1 < argc; ++i) {
		if (std::strcmp(argv[i], "--child-env") == 0) {
			const std::string kv = argv[i + 1];
			const size_t eq = kv.find('=');
			if (eq == std::string::npos) {
				std::fprintf(stderr, "bad --child-env (need K=V): %s\n", kv.c_str());
				return 2;
			}
			(void)::setenv(kv.substr(0, eq).c_str(), kv.substr(eq + 1).c_str(), 1);
		}
	}

	auto sup = edge_runtime::ProducerSupervisor::create(opts);
	if (!sup) {
		const auto& e = sup.error();
		std::printf("SUPERVISOR_FAIL code=%s ctx=%s\n", edge_runtime::to_string(e.code),
		            e.context);
		std::fflush(stdout);
		return 1;
	}

	auto result = sup.value().run();
	if (!result) {
		std::printf("SUPERVISOR_FAIL code=%s ctx=%s\n",
		            edge_runtime::to_string(result.error().code),
		            result.error().context);
		std::fflush(stdout);
		return 1;
	}
	const auto& r = result.value();
	switch (r.outcome) {
		case edge_runtime::SupervisionOutcome::kCleanExit:
			std::printf("CLEAN_EXIT pid=%d\n", r.last_child_pid);
			break;
		case edge_runtime::SupervisionOutcome::kStopped:
			std::printf("STOPPED\n");
			break;
		case edge_runtime::SupervisionOutcome::kRestartsExhausted:
			std::printf("GAVE_UP attempts=%u restarts=%u\n", r.spawn_attempts, r.restarts);
			break;
	}
	if (forward_stdout && !r.stdout_tail.empty()) {
		std::printf("CHILD_STDOUT_TAIL %s\n", r.stdout_tail.c_str());
	}
	std::fflush(stdout);
	return r.outcome == edge_runtime::SupervisionOutcome::kRestartsExhausted ? 3 : 0;
}
