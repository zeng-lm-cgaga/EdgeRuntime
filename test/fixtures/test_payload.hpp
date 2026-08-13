#ifndef EDGE_TEST_TEST_PAYLOAD_HPP
#define EDGE_TEST_TEST_PAYLOAD_HPP

// Test payloads + codecs used by unit/integration/crash suites. byte-exact
// little-endian, no padding enters the checksummed region; decode() validates
// the magic and the reserved flag bits so torn reads are observable (ER2 I02).
// The 32-byte schema fingerprint is a FIXED test constant shared verbatim by
// every driver/helper binary; the *_FingerprintHex strings are the same bytes
// as lowercase hex for the CLI tools.

#include <array>
#include <cstddef>
#include <cstdint>

#include "edge_runtime/schema.hpp"

struct TestPayloadV1 {
	uint32_t magic = 0x5A000001u;  // family + version tag
	uint64_t counter = 0;
	uint32_t flags = 0;  // only bits [1:0] are valid
};

inline constexpr std::array<std::byte, 32> kTestPayloadV1Fingerprint = {
        std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}, std::byte{0x05},
        std::byte{0x06}, std::byte{0x07}, std::byte{0x08}, std::byte{0x09}, std::byte{0x0A},
        std::byte{0x0B}, std::byte{0x0C}, std::byte{0x0D}, std::byte{0x0E}, std::byte{0x0F},
        std::byte{0x10}, std::byte{0x11}, std::byte{0x12}, std::byte{0x13}, std::byte{0x14},
        std::byte{0x15}, std::byte{0x16}, std::byte{0x17}, std::byte{0x18}, std::byte{0x19},
        std::byte{0x1A}, std::byte{0x1B}, std::byte{0x1C}, std::byte{0x1D}, std::byte{0x1E},
        std::byte{0x1F}, std::byte{0x20},
};

inline constexpr uint32_t kTestPayloadV1Version = 1;
inline constexpr const char* kTestPayloadV1Name = "TestPayloadV1";

// The same fingerprint as a lowercase-hex string, for CLI tools that take a
// --schema-hex argument (kept in sync with the array above).
inline constexpr const char* kTestPayloadV1FingerprintHex =
        "0102030405060708090a0b0c0d0e0f10"
        "1112131415161718191a1b1c1d1e1f20";

// Second payload type with a DIFFERENT encoded size (8 != 16): opening a
// consumer<TestPayloadV2> against a producer<TestPayloadV1> channel must be
// rejected as kSchemaMismatch (ER2 I08).
struct TestPayloadV2 {
	uint32_t magic = 0x5A000002u;
	uint32_t value = 0;
};

inline constexpr std::array<std::byte, 32> kTestPayloadV2Fingerprint = {
        std::byte{0x41}, std::byte{0x42}, std::byte{0x43}, std::byte{0x44}, std::byte{0x45},
        std::byte{0x46}, std::byte{0x47}, std::byte{0x48}, std::byte{0x49}, std::byte{0x4A},
        std::byte{0x4B}, std::byte{0x4C}, std::byte{0x4D}, std::byte{0x4E}, std::byte{0x4F},
        std::byte{0x50}, std::byte{0x51}, std::byte{0x52}, std::byte{0x53}, std::byte{0x54},
        std::byte{0x55}, std::byte{0x56}, std::byte{0x57}, std::byte{0x58}, std::byte{0x59},
        std::byte{0x5A}, std::byte{0x5B}, std::byte{0x5C}, std::byte{0x5D}, std::byte{0x5E},
        std::byte{0x5F}, std::byte{0x60},
};

inline constexpr const char* kTestPayloadV2FingerprintHex =
        "4142434445464748494a4b4c4d4e4f50"
        "5152535455565758595a5b5c5d5e5f60";

inline constexpr uint32_t kTestPayloadV2Version = 1;
inline constexpr const char* kTestPayloadV2Name = "TestPayloadV2";

inline edge_runtime::SchemaDescriptor TestPayloadV1Schema() {
	return edge_runtime::SchemaDescriptor{kTestPayloadV1Fingerprint, kTestPayloadV1Version,
	                                      kTestPayloadV1Name};
}

inline edge_runtime::SchemaDescriptor TestPayloadV2Schema() {
	return edge_runtime::SchemaDescriptor{kTestPayloadV2Fingerprint, kTestPayloadV2Version,
	                                      kTestPayloadV2Name};
}

namespace edge_runtime {

template <>
struct PayloadCodec<TestPayloadV1> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 16;  // 4 + 8 + 4, no padding
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;

	static bool encode(const TestPayloadV1& v, std::byte* dst, size_t cap) noexcept {
		if (cap < kEncodedSize) return false;
		const uint32_t magic = v.magic;
		const uint64_t counter = v.counter;
		const uint32_t flags = v.flags;
		for (size_t i = 0; i < 4; ++i) {
			dst[i] = static_cast<std::byte>((magic >> (8u * i)) & 0xFFu);
		}
		for (size_t i = 0; i < 8; ++i) {
			dst[4 + i] = static_cast<std::byte>((counter >> (8u * i)) & 0xFFu);
		}
		for (size_t i = 0; i < 4; ++i) {
			dst[12 + i] = static_cast<std::byte>((flags >> (8u * i)) & 0xFFu);
		}
		return true;
	}

	static bool decode(const std::byte* src, size_t size, TestPayloadV1* out) noexcept {
		if (size < kEncodedSize) return false;
		uint32_t magic = 0;
		uint64_t counter = 0;
		uint32_t flags = 0;
		for (size_t i = 0; i < 4; ++i) {
			magic |= static_cast<uint32_t>(std::to_integer<unsigned char>(src[i]))
			         << (8u * i);
		}
		for (size_t i = 0; i < 8; ++i) {
			counter |= static_cast<uint64_t>(std::to_integer<unsigned char>(src[4 + i]))
			           << (8u * i);
		}
		for (size_t i = 0; i < 4; ++i) {
			flags |= static_cast<uint32_t>(std::to_integer<unsigned char>(src[12 + i]))
			         << (8u * i);
		}
		if (magic != 0x5A000001u) return false;
		if ((flags & ~0x3u) != 0) return false;  // reserved bits must stay zero
		out->magic = magic;
		out->counter = counter;
		out->flags = flags;
		return true;
	}
};

template <>
struct PayloadCodec<TestPayloadV2> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 8;  // 4 + 4, no padding
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;

	static bool encode(const TestPayloadV2& v, std::byte* dst, size_t cap) noexcept {
		if (cap < kEncodedSize) return false;
		const uint32_t magic = v.magic;
		const uint32_t value = v.value;
		for (size_t i = 0; i < 4; ++i) {
			dst[i] = static_cast<std::byte>((magic >> (8u * i)) & 0xFFu);
		}
		for (size_t i = 0; i < 4; ++i) {
			dst[4 + i] = static_cast<std::byte>((value >> (8u * i)) & 0xFFu);
		}
		return true;
	}

	static bool decode(const std::byte* src, size_t size, TestPayloadV2* out) noexcept {
		if (size < kEncodedSize) return false;
		uint32_t magic = 0;
		uint32_t value = 0;
		for (size_t i = 0; i < 4; ++i) {
			magic |= static_cast<uint32_t>(std::to_integer<unsigned char>(src[i]))
			         << (8u * i);
		}
		for (size_t i = 0; i < 4; ++i) {
			value |= static_cast<uint32_t>(std::to_integer<unsigned char>(src[4 + i]))
			         << (8u * i);
		}
		if (magic != 0x5A000002u) return false;
		out->magic = magic;
		out->value = value;
		return true;
	}
};

}  // namespace edge_runtime

#endif  // EDGE_TEST_TEST_PAYLOAD_HPP
