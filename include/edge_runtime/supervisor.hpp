#ifndef EDGE_RUNTIME_SUPERVISOR_HPP
#define EDGE_RUNTIME_SUPERVISOR_HPP

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/result.hpp"

namespace edge_runtime {
namespace detail {
struct SupervisorHandle;
}

enum class SupervisorEvent : uint32_t {
	kSupervised = 0,
	kStallDetected,
	kKilled,
	kRestartArmed,
};

struct SupervisorEventInfo {
	SupervisorEvent event{SupervisorEvent::kSupervised};
	uint64_t pid{0};
	uint64_t generation{0};
	uint32_t attempt{0};
	uint64_t delay_ns{0};
	uint32_t signal{0};
};

using SupervisorEventCallback = void (*)(const SupervisorEventInfo&, void* user_data);

struct SupervisorOptions {
	std::string channel_name;
	Transport transport{Transport::kPosixShm};
	std::vector<std::string> producer_argv;

	std::chrono::milliseconds initial_delay{100};
	std::chrono::milliseconds max_delay{10000};
	uint32_t multiplier{2};
	uint32_t max_restarts{10};
	std::chrono::milliseconds stable_reset_window{60000};
	std::chrono::milliseconds stall_grace{5000};
	std::chrono::milliseconds watch_interval{500};
	std::chrono::milliseconds create_timeout{10000};

	SupervisorEventCallback on_event{nullptr};
	void* event_user_data{nullptr};
};

enum class SupervisionOutcome : uint32_t {
	kCleanExit = 0,
	kStopped = 1,
	kRestartsExhausted = 2,
};

struct SupervisionResult {
	SupervisionOutcome outcome{SupervisionOutcome::kStopped};
	uint32_t spawn_attempts{0};
	uint32_t restarts{0};
	uint64_t last_generation{0};
	int last_child_pid{-1};
	int last_child_exit_status{0};
	std::string stdout_tail;
};

class ProducerSupervisor {
       public:
	// 创建监督器；run() 负责阻塞等待子进程和通道状态变化。
	static Result<ProducerSupervisor> create(const SupervisorOptions& options);

	// 运行监督循环，返回干净退出、主动停止或重启次数耗尽。
	Result<SupervisionResult> run();

	// 唤醒监督循环并请求停止，不直接终止被监督进程。
	void request_stop() noexcept;

	ProducerSupervisor(const ProducerSupervisor&) = delete;
	ProducerSupervisor& operator=(const ProducerSupervisor&) = delete;
	ProducerSupervisor(ProducerSupervisor&&) noexcept = default;
	ProducerSupervisor& operator=(ProducerSupervisor&&) noexcept = default;

	// 析构时回收事件文件描述符并等待已启动的子进程结束。
	~ProducerSupervisor();

       private:
	explicit ProducerSupervisor(std::shared_ptr<detail::SupervisorHandle> h);

	std::shared_ptr<detail::SupervisorHandle> handle_;
};

}  // 命名空间 edge_runtime

#endif  // EDGE_RUNTIME_SUPERVISOR_HPP
