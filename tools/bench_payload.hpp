#ifndef EDGE_RUNTIME_TOOL_BENCH_PAYLOAD_HPP
#define EDGE_RUNTIME_TOOL_BENCH_PAYLOAD_HPP

// Benchmark payload + codec (design §21). Unlike the TestPayloadV1 fixture, a
// BenchPayloadV1<N> carries the producer's CLOCK_MONOTONIC_RAW publish
// timestamp so a cross-process benchmark can compute end-to-end latency on the
// receiving side (latency_ns = receive_ns - publish_ns, §21.3). The payload
// size N is the matrix dimension (64 B ... 64 KiB); the codec is byte-exact LE
// with no padding, and the trailing fill bytes carry a position pattern so a
// torn copy is observable even in the padding (complements the channel
// checksum). The 32-byte schema fingerprint is derived deterministically from
// N, so producer and consumer agree on it without any config handshake, and a
// consumer opening the wrong size is rejected as a schema mismatch.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "edge_runtime/schema.hpp"

namespace bench {

// 24-byte header carried in every payload size; the codec's kEncodedSize == kSize
// and the tail (indices 24..kSize-1) is a derived position pattern, not carried
// struct state — so sizeof() stays the header size and kEncodedSize owns the
// slot width.
template <size_t kSize>
struct BenchPayloadV1 {
	static_assert(kSize >= 24 && kSize <= 65536, "bench payload size out of range");
	uint32_t magic = 0x5B000001u;
	uint64_t counter = 0;     // pattern: sample_sequence - 1
	uint64_t publish_ns = 0;  // CLOCK_MONOTONIC_RAW right before publish
	uint32_t flags = 0;       // only bits [1:0] valid
};

inline constexpr uint32_t kBenchPayloadMagic = 0x5B000001u;

// Local FNV-1a 64 (deterministic, non-cryptographic — §8.2) used only to derive
// the per-size fingerprint; keeps bench_payload.hpp free of library internals.
inline uint64_t fnv1a64_bytes(const std::byte* data, size_t n) {
	uint64_t h = 14695981039346656037ull;
	for (size_t i = 0; i < n; ++i) {
		h ^= static_cast<uint64_t>(std::to_integer<unsigned char>(data[i]));
		h *= 1099511628211ull;
	}
	return h;
}

inline uint64_t bench_size_hash(size_t size) {
	std::array<std::byte, 8> le{};
	uint64_t s = static_cast<uint64_t>(size);
	for (size_t i = 0; i < 8; ++i) {
		le[i] = static_cast<std::byte>((s >> (8u * i)) & 0xFFu);
	}
	return fnv1a64_bytes(le.data(), le.size());
}

// 32-byte fingerprint for a payload size. Distinct per size; never all-zero.
inline std::array<std::byte, 32> bench_fingerprint(size_t size) {
	const uint64_t h = bench_size_hash(size);
	std::array<std::byte, 32> fp{};
	for (size_t i = 0; i < fp.size(); ++i) {
		const uint8_t b = static_cast<uint8_t>((h >> (8u * ((i * 5u) % 8u))) ^
		                                       static_cast<uint64_t>(i * 131u));
		fp[i] = std::byte(b == 0u ? 0x7Bu : b);
	}
	return fp;
}

}  // namespace bench

namespace edge_runtime {

template <size_t kSize>
struct PayloadCodec<bench::BenchPayloadV1<kSize>> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = kSize;
	using EncodedBuffer = std::array<std::byte, kSize>;

	static bool encode(const bench::BenchPayloadV1<kSize>& v, std::byte* dst,
	                   size_t cap) noexcept {
		if (cap < kSize) return false;
		std::memcpy(dst, &v.magic, 4);
		std::memcpy(dst + 4, &v.counter, 8);
		std::memcpy(dst + 12, &v.publish_ns, 8);
		std::memcpy(dst + 20, &v.flags, 4);
		for (size_t i = 24; i < kSize; ++i) {
			dst[i] = static_cast<std::byte>(0xA5u ^ static_cast<unsigned>(i & 0xFFu));
		}
		return true;
	}

	static bool decode(const std::byte* src, size_t size,
	                   bench::BenchPayloadV1<kSize>* out) noexcept {
		if (size < kSize) return false;
		std::memcpy(&out->magic, src, 4);
		std::memcpy(&out->counter, src + 4, 8);
		std::memcpy(&out->publish_ns, src + 12, 8);
		std::memcpy(&out->flags, src + 20, 4);
		if (out->magic != bench::kBenchPayloadMagic) return false;
		if ((out->flags & ~0x3u) != 0) return false;  // reserved bits must stay zero
		for (size_t i = 24; i < kSize; ++i) {
			const std::byte expect =
			        static_cast<std::byte>(0xA5u ^ static_cast<unsigned>(i & 0xFFu));
			if (src[i] != expect) return false;  // torn padding detected
		}
		return true;
	}
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_TOOL_BENCH_PAYLOAD_HPP
