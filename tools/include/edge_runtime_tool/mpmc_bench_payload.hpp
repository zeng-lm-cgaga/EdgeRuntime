#ifndef EDGE_RUNTIME_MPMC_BENCH_PAYLOAD_HPP
#define EDGE_RUNTIME_MPMC_BENCH_PAYLOAD_HPP

// MPMC benchmark records are fixed-size, self-checking, and intentionally independent of the
// SPSC latest-value payload contract.
#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "edge_runtime/common/schema.hpp"

namespace edge_mpmc_bench {

inline constexpr uint64_t kPayloadMagic = 0x45524D50434D4231ull;  // "ERMPCMB1"
inline constexpr uint64_t kFnvOffset = 14695981039346656037ull;
inline constexpr uint64_t kFnvPrime = 1099511628211ull;
inline constexpr size_t kPayloadHeaderBytes = 40;

template <size_t kSize>
struct alignas(8) Payload {
	static_assert(kSize == 64 || kSize == 1024 || kSize == 4096,
	              "benchmark payload must be 64, 1024, or 4096 bytes");

	uint64_t magic;
	uint32_t producer;
	uint32_t reserved;
	uint64_t sequence;
	uint64_t send_time_ns;
	uint64_t checksum;
	std::array<std::byte, kSize - kPayloadHeaderBytes> body;
};

static_assert(sizeof(Payload<64>) == 64, "benchmark payload must have exact size");
static_assert(sizeof(Payload<1024>) == 1024, "benchmark payload must have exact size");
static_assert(sizeof(Payload<4096>) == 4096, "benchmark payload must have exact size");
static_assert(std::is_trivially_copyable_v<Payload<64>>,
              "benchmark payload must be trivially copyable");
static_assert(std::is_trivially_copyable_v<Payload<1024>>,
              "benchmark payload must be trivially copyable");
static_assert(std::is_trivially_copyable_v<Payload<4096>>,
              "benchmark payload must be trivially copyable");
static_assert(std::is_trivially_destructible_v<Payload<64>>,
              "benchmark payload must be trivially destructible");
static_assert(std::is_trivially_destructible_v<Payload<1024>>,
              "benchmark payload must be trivially destructible");
static_assert(std::is_trivially_destructible_v<Payload<4096>>,
              "benchmark payload must be trivially destructible");

template <size_t kSize>
uint64_t checksum(const Payload<kSize>& payload) noexcept {
	const auto* bytes = reinterpret_cast<const unsigned char*>(&payload);
	constexpr size_t checksum_offset = offsetof(Payload<kSize>, checksum);
	uint64_t result = kFnvOffset;
	for (size_t i = 0; i < kSize; ++i) {
		if (i >= checksum_offset && i < checksum_offset + sizeof(payload.checksum)) continue;
		result ^= static_cast<uint64_t>(bytes[i]);
		result *= kFnvPrime;
	}
	return result;
}

template <size_t kSize>
Payload<kSize> make_payload(uint32_t producer, uint64_t sequence, uint64_t send_time_ns) noexcept {
	Payload<kSize> payload{};
	payload.magic = kPayloadMagic;
	payload.producer = producer;
	payload.reserved = 0;
	payload.sequence = sequence;
	payload.send_time_ns = send_time_ns;
	for (size_t i = 0; i < payload.body.size(); ++i) {
		const uint64_t pattern = static_cast<uint64_t>(producer) * 0x9E3779B97F4A7C15ull +
		                         sequence * 0xD1B54A32D192ED03ull + i;
		payload.body[i] = std::byte{static_cast<unsigned char>(pattern & 0xFFu)};
	}
	payload.checksum = checksum(payload);
	return payload;
}

template <size_t kSize>
bool valid_payload(const Payload<kSize>& payload, uint32_t producer_count,
                   uint64_t messages_per_producer, uint64_t now_ns,
                   uint64_t* latency_ns) noexcept {
	if (payload.magic != kPayloadMagic || payload.reserved != 0 ||
	    payload.producer >= producer_count || payload.sequence >= messages_per_producer ||
	    payload.send_time_ns == 0 || payload.send_time_ns > now_ns) {
		return false;
	}
	const Payload<kSize> expected =
	        make_payload<kSize>(payload.producer, payload.sequence, payload.send_time_ns);
	if (expected.body != payload.body || expected.checksum != payload.checksum) return false;
	*latency_ns = now_ns - payload.send_time_ns;
	return true;
}

inline uint64_t schema_seed(size_t payload_size) noexcept {
	uint64_t seed = kFnvOffset;
	const auto* bytes = reinterpret_cast<const unsigned char*>(&payload_size);
	for (size_t i = 0; i < sizeof(payload_size); ++i) {
		seed ^= static_cast<uint64_t>(bytes[i]);
		seed *= kFnvPrime;
	}
	return seed;
}

inline std::array<std::byte, 32> schema_fingerprint(size_t payload_size) noexcept {
	uint64_t state = schema_seed(payload_size);
	std::array<std::byte, 32> fingerprint{};
	for (size_t i = 0; i < fingerprint.size(); ++i) {
		state ^= state >> 29;
		state *= 0x94D049BB133111EBull;
		state ^= state >> 31;
		const unsigned char value = static_cast<unsigned char>((state >> ((i % 8) * 8)) & 0xFFu);
		fingerprint[i] = std::byte{value == 0 ? static_cast<unsigned char>(0xA7u) : value};
	}
	return fingerprint;
}

template <size_t kSize>
edge_runtime::SchemaDescriptor schema() noexcept {
	return {schema_fingerprint(kSize), 1, "EdgeRuntimeMpmcBenchPayload"};
}

}  // namespace edge_mpmc_bench

#endif  // EDGE_RUNTIME_MPMC_BENCH_PAYLOAD_HPP
