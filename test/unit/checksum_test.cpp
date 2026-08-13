// fnv1a64: deterministic, documented, non-crypto (design §8.2). Used for the
// payload checksum, the bootstrap checksum and the control journal; any change
// to these golden vectors would invalidate every previously-collected evidence.

#include "edge_runtime/detail/checksum.hpp"

#include <gtest/gtest.h>

#include <cstdint>

namespace {

using edge_runtime::detail::fnv1a64;

TEST(Checksum, KnownVectors) {
	// FNV-1a 64-bit golden values (standard test inputs).
	const std::byte empty = std::byte{0};
	EXPECT_EQ(fnv1a64(&empty, 0), 0xcbf29ce484222325ull);
	EXPECT_EQ(fnv1a64(reinterpret_cast<const std::byte*>("a"), 1), 0xaf63dc4c8601ec8cull);
	EXPECT_EQ(fnv1a64(reinterpret_cast<const std::byte*>("foobar"), 6), 0x85944171f73967e8ull);
}

TEST(Checksum, DeterministicAndSensitive) {
	const std::byte a[4] = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}};
	std::byte b[4] = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}};
	EXPECT_EQ(fnv1a64(a, 4), fnv1a64(b, 4));
	b[2] = std::byte{0xFF};  // single byte flip changes the digest
	EXPECT_NE(fnv1a64(a, 4), fnv1a64(b, 4));
}

TEST(Checksum, LengthSensitive) {
	// same prefix, different length -> different digest
	const std::byte x[3] = {std::byte{1}, std::byte{2}, std::byte{3}};
	const std::byte y[4] = {std::byte{1}, std::byte{2}, std::byte{3}, std::byte{0}};
	EXPECT_NE(fnv1a64(x, 3), fnv1a64(y, 4));
}

}  // namespace
