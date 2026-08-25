#ifndef EDGE_RUNTIME_DETAIL_CHANNEL_OBSERVER_HPP
#define EDGE_RUNTIME_DETAIL_CHANNEL_OBSERVER_HPP

#include <cstddef>
#include <cstdint>
#include <string>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/result.hpp"

namespace edge_runtime::detail {

// 监督器只保留只读映射和实例身份，用于观察 READY、心跳和代数变化。
struct ChannelObserverView {
	std::string channel_name;
	Transport transport{Transport::kPosixShm};
	UniqueFd fd;
	MappedRegion mapping;
	uint64_t dev = 0;
	uint64_t ino = 0;
	uint64_t size = 0;
};

Result<ChannelObserverView> open_channel_readonly(const std::string& channel_name,
                                                  Transport transport,
                                                  uint64_t retry_ms) noexcept;

inline ChannelHeaderAbi* observer_header(ChannelObserverView& view) noexcept {
	return reinterpret_cast<ChannelHeaderAbi*>(
	        static_cast<std::byte*>(view.mapping.get()) + kChannelHeaderOffset);
}

enum class StallClass : uint8_t {
	kNotReady = 0,
	kIdentityMismatch,
	kNotApplicable,
	kFresh,
	kStalled,
};

StallClass classify_stall(const ChannelHeaderAbi* header, uint64_t now_boot_ns,
                          uint64_t expected_pid, uint64_t expected_start_ticks) noexcept;

uint64_t observer_generation(const ChannelHeaderAbi* header) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CHANNEL_OBSERVER_HPP
