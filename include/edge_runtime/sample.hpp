#ifndef EDGE_RUNTIME_SAMPLE_HPP
#define EDGE_RUNTIME_SAMPLE_HPP

#include <array>
#include <cstddef>
#include <cstdint>

namespace edge_runtime {

struct PublishInfo {
	uint64_t generation{0};
	uint64_t sequence{0};
	uint64_t publish_boot_ns{0};
};

// 解码后的样本及其来源信息；missed_samples 表示两次观察之间跳过的序号数。
template <typename T>
struct Sample {
	T value{};
	uint64_t generation{0};
	std::array<std::byte, 16> instance_nonce{};
	uint64_t sequence{0};
	uint64_t publish_boot_ns{0};
	uint64_t receive_boot_ns{0};
	uint64_t missed_samples{0};
};

struct ReconnectInfo {
	uint64_t old_generation{0};
	uint64_t new_generation{0};
	std::array<std::byte, 16> old_instance_nonce{};
	std::array<std::byte, 16> new_instance_nonce{};
	bool schema_changed{false};
};

struct ChannelStatus {
	bool ready{false};
	uint32_t init_state{0};
	uint32_t producer_state{0};
	uint32_t consumer_state{0};
	uint16_t abi_major{0};
	uint16_t abi_minor{0};
	uint32_t slot_count{0};
	uint32_t payload_size{0};
	uint64_t mapping_size{0};
	uint64_t generation{0};
	uint64_t instance_nonce_hi{0};
	uint64_t instance_nonce_lo{0};
	uint64_t publish_count{0};
	uint64_t read_count{0};
	uint64_t last_publish_boot_ns{0};
	uint64_t producer_pid{0};
	uint64_t consumer_pid{0};
	bool producer_alive{false};
	bool consumer_alive{false};

	// 心跳字段为零表示未启用或尚未写入。
	uint64_t heartbeat_boot_ns{0};
	uint64_t producer_heartbeat_interval_ns{0};
};

}  // 命名空间 edge_runtime

#endif  // EDGE_RUNTIME_SAMPLE_HPP
