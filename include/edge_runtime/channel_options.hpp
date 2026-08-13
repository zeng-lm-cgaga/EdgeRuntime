#ifndef EDGE_RUNTIME_CHANNEL_OPTIONS_HPP
#define EDGE_RUNTIME_CHANNEL_OPTIONS_HPP

#include <chrono>
#include <cstdint>
#include <string>

namespace edge_runtime {

// Object transport (v0.2, design §33): how the shared-memory object is created
// and handed to the consumer. Frozen at create; consumer must match.
enum class Transport : uint32_t {
	kPosixShm = 0,     // v0.1 named POSIX shm (default; behavior unchanged)
	kMemfdFdPass = 1,  // memfd + SCM_RIGHTS via a per-channel broker socket
};

struct ChannelOptions {
	std::string name;
	std::chrono::milliseconds stale_timeout{100};
	std::chrono::milliseconds reconnect_timeout{1000};
	bool enable_payload_checksum{true};
	Transport transport{Transport::kPosixShm};
	// v0.2 optional heartbeat (design §34): producer-side making-progress
	// interval; 0 disables heartbeat (v0.1 behavior, abi_minor 0).
	std::chrono::nanoseconds heartbeat_interval{0};
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_CHANNEL_OPTIONS_HPP
