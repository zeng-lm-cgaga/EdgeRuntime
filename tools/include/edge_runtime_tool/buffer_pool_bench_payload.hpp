#ifndef EDGE_RUNTIME_BUFFER_POOL_BENCH_PAYLOAD_HPP
#define EDGE_RUNTIME_BUFFER_POOL_BENCH_PAYLOAD_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "edge_runtime/common/schema.hpp"

namespace edge_buffer_pool_bench {

inline constexpr uint64_t kPayloadMagic = 0x45524250424E4331ull;  // "ERBPBNC1"
inline constexpr uint64_t kFnvOffset = 14695981039346656037ull;
inline constexpr uint64_t kFnvPrime = 1099511628211ull;

struct PayloadHeader {
	uint64_t magic{0};
	uint32_t producer{0};
	uint32_t reserved{0};
	uint64_t sequence{0};
	uint64_t send_time_ns{0};
	uint64_t checksum{0};
};

static_assert(sizeof(PayloadHeader) == 40, "pool benchmark payload header size");
static_assert(std::is_trivially_copyable_v<PayloadHeader>);
static_assert(std::is_trivially_destructible_v<PayloadHeader>);

inline unsigned char body_byte(uint32_t producer, uint64_t sequence, size_t offset) noexcept {
	const uint64_t pattern = static_cast<uint64_t>(producer) * 0x9E3779B97F4A7C15ull +
	                         sequence * 0xD1B54A32D192ED03ull + offset;
	return static_cast<unsigned char>(pattern & 0xFFu);
}

inline uint64_t checksum(const std::byte* data, size_t size) noexcept {
	const size_t checksum_offset = offsetof(PayloadHeader, checksum);
	uint64_t result = kFnvOffset;
	for (size_t i = 0; i < size; ++i) {
		if (i >= checksum_offset && i < checksum_offset + sizeof(uint64_t)) continue;
		result ^= static_cast<uint64_t>(std::to_integer<unsigned char>(data[i]));
		result *= kFnvPrime;
	}
	return result;
}

inline bool fill(std::byte* data, size_t size, uint32_t producer, uint64_t sequence,
                 uint64_t send_time_ns) noexcept {
	if (data == nullptr || size < sizeof(PayloadHeader) || send_time_ns == 0) return false;
	const PayloadHeader header{kPayloadMagic, producer, 0, sequence, send_time_ns, 0};
	std::memcpy(data, &header, sizeof(header));
	for (size_t i = sizeof(PayloadHeader); i < size; ++i) {
		data[i] = std::byte{body_byte(producer, sequence, i)};
	}
	const uint64_t value = checksum(data, size);
	std::memcpy(data + offsetof(PayloadHeader, checksum), &value, sizeof(value));
	return true;
}

inline bool validate(const std::byte* data, size_t size, uint32_t producer_count,
                     uint64_t messages_per_producer, uint64_t now_ns,
                     uint64_t* latency_ns, uint32_t* producer, uint64_t* sequence) noexcept {
	if (data == nullptr || size < sizeof(PayloadHeader) || latency_ns == nullptr ||
	    producer == nullptr || sequence == nullptr) {
		return false;
	}
	PayloadHeader header{};
	std::memcpy(&header, data, sizeof(header));
	if (header.magic != kPayloadMagic || header.reserved != 0 ||
	    header.producer >= producer_count || header.sequence >= messages_per_producer ||
	    header.send_time_ns == 0 || header.send_time_ns > now_ns ||
	    checksum(data, size) != header.checksum) {
		return false;
	}
	for (size_t i = sizeof(PayloadHeader); i < size; ++i) {
		if (std::to_integer<unsigned char>(data[i]) !=
		    body_byte(header.producer, header.sequence, i)) {
			return false;
		}
	}
	*latency_ns = now_ns - header.send_time_ns;
	*producer = header.producer;
	*sequence = header.sequence;
	return true;
}

inline std::array<std::byte, 32> fingerprint(uint64_t seed) noexcept {
	uint64_t state = kFnvOffset ^ seed;
	std::array<std::byte, 32> result{};
	for (size_t i = 0; i < result.size(); ++i) {
		state ^= state >> 29;
		state *= 0x94D049BB133111EBull;
		state ^= state >> 31;
		const unsigned char value =
		        static_cast<unsigned char>((state >> ((i % 8) * 8)) & 0xFFu);
		result[i] = std::byte{value == 0 ? static_cast<unsigned char>(0xA7u) : value};
	}
	return result;
}

inline edge_runtime::SchemaDescriptor pool_schema(uint32_t block_size) noexcept {
	return {fingerprint(0x504F4F4Cull ^ block_size), 1, "EdgeRuntimeBufferPoolBenchPayload"};
}

inline edge_runtime::SchemaDescriptor queue_schema() noexcept {
	return {fingerprint(0x514555455545ull), 1, "EdgeRuntimeBufferPoolBenchHandle"};
}

}  // namespace edge_buffer_pool_bench

#endif  // EDGE_RUNTIME_BUFFER_POOL_BENCH_PAYLOAD_HPP
