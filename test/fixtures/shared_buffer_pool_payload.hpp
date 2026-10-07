#ifndef EDGE_TEST_SHARED_BUFFER_POOL_PAYLOAD_HPP
#define EDGE_TEST_SHARED_BUFFER_POOL_PAYLOAD_HPP

#include <array>
#include <cstddef>

#include "edge_runtime/common/schema.hpp"

inline constexpr std::array<std::byte, 32> kSharedBufferPoolFingerprint = {
        std::byte{0xB0}, std::byte{0xB1}, std::byte{0xB2}, std::byte{0xB3}, std::byte{0xB4},
        std::byte{0xB5}, std::byte{0xB6}, std::byte{0xB7}, std::byte{0xB8}, std::byte{0xB9},
        std::byte{0xBA}, std::byte{0xBB}, std::byte{0xBC}, std::byte{0xBD}, std::byte{0xBE},
        std::byte{0xBF}, std::byte{0xC0}, std::byte{0xC1}, std::byte{0xC2}, std::byte{0xC3},
        std::byte{0xC4}, std::byte{0xC5}, std::byte{0xC6}, std::byte{0xC7}, std::byte{0xC8},
        std::byte{0xC9}, std::byte{0xCA}, std::byte{0xCB}, std::byte{0xCC}, std::byte{0xCD},
        std::byte{0xCE}, std::byte{0xCF},
};

inline constexpr std::array<std::byte, 32> kSharedBufferPoolMismatchFingerprint = {
        std::byte{0xD0}, std::byte{0xD1}, std::byte{0xD2}, std::byte{0xD3}, std::byte{0xD4},
        std::byte{0xD5}, std::byte{0xD6}, std::byte{0xD7}, std::byte{0xD8}, std::byte{0xD9},
        std::byte{0xDA}, std::byte{0xDB}, std::byte{0xDC}, std::byte{0xDD}, std::byte{0xDE},
        std::byte{0xDF}, std::byte{0xE0}, std::byte{0xE1}, std::byte{0xE2}, std::byte{0xE3},
        std::byte{0xE4}, std::byte{0xE5}, std::byte{0xE6}, std::byte{0xE7}, std::byte{0xE8},
        std::byte{0xE9}, std::byte{0xEA}, std::byte{0xEB}, std::byte{0xEC}, std::byte{0xED},
        std::byte{0xEE}, std::byte{0xEF},
};

inline constexpr std::array<std::byte, 32> kSharedBufferHandleQueueFingerprint = {
        std::byte{0x70}, std::byte{0x71}, std::byte{0x72}, std::byte{0x73}, std::byte{0x74},
        std::byte{0x75}, std::byte{0x76}, std::byte{0x77}, std::byte{0x78}, std::byte{0x79},
        std::byte{0x7A}, std::byte{0x7B}, std::byte{0x7C}, std::byte{0x7D}, std::byte{0x7E},
        std::byte{0x7F}, std::byte{0x80}, std::byte{0x81}, std::byte{0x82}, std::byte{0x83},
        std::byte{0x84}, std::byte{0x85}, std::byte{0x86}, std::byte{0x87}, std::byte{0x88},
        std::byte{0x89}, std::byte{0x8A}, std::byte{0x8B}, std::byte{0x8C}, std::byte{0x8D},
        std::byte{0x8E}, std::byte{0x8F},
};

inline edge_runtime::SchemaDescriptor SharedBufferPoolTestSchema() {
	return {kSharedBufferPoolFingerprint, 1, "SharedBufferPoolTestPayload"};
}

inline edge_runtime::SchemaDescriptor SharedBufferPoolMismatchSchema() {
	return {kSharedBufferPoolMismatchFingerprint, 1, "SharedBufferPoolMismatch"};
}

inline edge_runtime::SchemaDescriptor SharedBufferHandleQueueSchema() {
	return {kSharedBufferHandleQueueFingerprint, 1, "SharedBufferHandleQueue"};
}

#endif  // EDGE_TEST_SHARED_BUFFER_POOL_PAYLOAD_HPP
