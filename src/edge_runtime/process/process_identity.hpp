#ifndef EDGE_RUNTIME_DETAIL_PROCESS_IDENTITY_HPP
#define EDGE_RUNTIME_DETAIL_PROCESS_IDENTITY_HPP

#include <cstdint>

#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/common/result.hpp"

namespace edge_runtime::detail {

// 进程身份由 pid、/proc 启动时间和本次启动标识共同组成，防止 PID 复用误判。
struct ProcessIdentity {
	uint64_t pid = 0;
	uint64_t proc_start_ticks = 0;
	uint64_t boot_id_hash_hi = 0;
	uint64_t boot_id_hash_lo = 0;
};

enum class Liveness : uint8_t {
	kAlive = 0,
	kExited = 1,
	kPidReused = 2,
	kUnverifiable = 3,
};

Result<uint64_t> proc_stat_starttime(int pid) noexcept;

void current_boot_id_hash(uint64_t* hi, uint64_t* lo) noexcept;

ProcessIdentity current_process_identity() noexcept;

Liveness probe_liveness(uint64_t pid, uint64_t expected_start_ticks) noexcept;

bool identity_matches_current(const ProcessIdentity& id) noexcept;

class LivenessWatch {
       public:
	// 长驻 pidfd 供 ProducerSupervisor 监听；普通恢复路径使用一次性探测。
	static Result<LivenessWatch> open(uint64_t pid) noexcept;
	int fd() const noexcept { return pidfd_.get(); }

	int release() noexcept { return pidfd_.release(); }

	bool exited() const noexcept;

	LivenessWatch() = default;
	LivenessWatch(const LivenessWatch&) = delete;
	LivenessWatch& operator=(const LivenessWatch&) = delete;
	LivenessWatch(LivenessWatch&& other) noexcept = default;
	LivenessWatch& operator=(LivenessWatch&& other) noexcept = default;

       private:
	UniqueFd pidfd_;
};

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_PROCESS_IDENTITY_HPP
