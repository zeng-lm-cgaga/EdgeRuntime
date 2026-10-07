#ifndef EDGE_TEST_MPMC_PAYLOAD_HPP
#define EDGE_TEST_MPMC_PAYLOAD_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "edge_runtime/common/schema.hpp"

struct MpmcTestPayload {
	uint64_t sequence = 0;
	uint32_t producer = 0;
	uint32_t magic = 0x4D504D43u;
};

static_assert(std::is_trivially_copyable_v<MpmcTestPayload>);
static_assert(std::is_trivially_destructible_v<MpmcTestPayload>);

inline constexpr std::array<std::byte, 32> kMpmcTestFingerprint = {
		std::byte{0x90}, std::byte{0x91}, std::byte{0x92}, std::byte{0x93}, std::byte{0x94},
		std::byte{0x95}, std::byte{0x96}, std::byte{0x97}, std::byte{0x98}, std::byte{0x99},
		std::byte{0x9A}, std::byte{0x9B}, std::byte{0x9C}, std::byte{0x9D}, std::byte{0x9E},
		std::byte{0x9F}, std::byte{0xA0}, std::byte{0xA1}, std::byte{0xA2}, std::byte{0xA3},
		std::byte{0xA4}, std::byte{0xA5}, std::byte{0xA6}, std::byte{0xA7}, std::byte{0xA8},
		std::byte{0xA9}, std::byte{0xAA}, std::byte{0xAB}, std::byte{0xAC}, std::byte{0xAD},
		std::byte{0xAE}, std::byte{0xAF},
};

inline constexpr std::array<std::byte, 32> kMpmcTestMismatchFingerprint = {
		std::byte{0x10}, std::byte{0x11}, std::byte{0x12}, std::byte{0x13}, std::byte{0x14},
		std::byte{0x15}, std::byte{0x16}, std::byte{0x17}, std::byte{0x18}, std::byte{0x19},
		std::byte{0x1A}, std::byte{0x1B}, std::byte{0x1C}, std::byte{0x1D}, std::byte{0x1E},
		std::byte{0x1F}, std::byte{0x20}, std::byte{0x21}, std::byte{0x22}, std::byte{0x23},
		std::byte{0x24}, std::byte{0x25}, std::byte{0x26}, std::byte{0x27}, std::byte{0x28},
		std::byte{0x29}, std::byte{0x2A}, std::byte{0x2B}, std::byte{0x2C}, std::byte{0x2D},
		std::byte{0x2E}, std::byte{0x2F},
};

inline edge_runtime::SchemaDescriptor MpmcTestSchema() {
	return {kMpmcTestFingerprint, 1, "MpmcTestPayload"};
}

inline edge_runtime::SchemaDescriptor MpmcTestMismatchSchema() {
	return {kMpmcTestMismatchFingerprint, 1, "MpmcTestPayloadMismatch"};
}

#endif  // EDGE_TEST_MPMC_PAYLOAD_HPP
