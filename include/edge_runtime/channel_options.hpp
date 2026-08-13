#ifndef EDGE_RUNTIME_CHANNEL_OPTIONS_HPP
#define EDGE_RUNTIME_CHANNEL_OPTIONS_HPP

#include <chrono>
#include <string>

namespace edge_runtime {

struct ChannelOptions {
	std::string name;
	std::chrono::milliseconds stale_timeout{100};
	std::chrono::milliseconds reconnect_timeout{1000};
	bool enable_payload_checksum{true};
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_CHANNEL_OPTIONS_HPP
