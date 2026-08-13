#ifndef EDGE_RUNTIME_SAMPLE_HPP
#define EDGE_RUNTIME_SAMPLE_HPP

#include <array>
#include <cstddef>
#include <cstdint>

namespace edge_runtime {

// Returned by publish() (design §16.2).
struct PublishInfo {
	uint64_t generation{0};
	uint64_t sequence{0};
	uint64_t publish_boot_ns{0};
};

// A decoded sample plus its provenance (design §16.2). value is only valid when
// the codec's decode() succeeded; missed_samples saturates at gap overflow.
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

// Result of a successful reconnect to a new producer instance (design §15.5).
struct ReconnectInfo {
	uint64_t old_generation{0};
	uint64_t new_generation{0};
	std::array<std::byte, 16> old_instance_nonce{};
	std::array<std::byte, 16> new_instance_nonce{};
	bool schema_changed{false};
};

// Diagnostic snapshot of the channel (design §15.6 / §16.2 status()). Log text
// is never a control input; the ErrorCode + this struct are.
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
	// v0.2 optional heartbeat (design §34)
	uint64_t heartbeat_boot_ns{0};
	uint64_t producer_heartbeat_interval_ns{0};
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_SAMPLE_HPP
