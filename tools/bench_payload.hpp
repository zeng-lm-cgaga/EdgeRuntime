#ifndef EDGE_RUNTIME_TOOL_BENCH_PAYLOAD_HPP
#define EDGE_RUNTIME_TOOL_BENCH_PAYLOAD_HPP

// 基准载荷携带发布时间和位置模式，用于跨进程测量端到端延迟并发现撕裂复制。
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "edge_runtime/schema.hpp"

namespace bench {

template <size_t kSize>
struct BenchPayloadV1 {
	static_assert(kSize >= 24 && kSize <= 65536, "bench payload size out of range");
	uint32_t magic = 0x5B000001u;
	uint64_t counter = 0;
	uint64_t publish_ns = 0;
	uint32_t flags = 0;
};

inline constexpr uint32_t kBenchPayloadMagic = 0x5B000001u;

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

}

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
		if ((out->flags & ~0x3u) != 0) return false;
		for (size_t i = 24; i < kSize; ++i) {
			const std::byte expect =
			        static_cast<std::byte>(0xA5u ^ static_cast<unsigned>(i & 0xFFu));
			if (src[i] != expect) return false;
		}
		return true;
	}
};

}

#endif
