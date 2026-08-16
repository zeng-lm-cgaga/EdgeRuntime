#include "edge_runtime/schema.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>

namespace {

struct ValidPayload {};
struct ThrowingEncodePayload {};
struct ThrowingDecodePayload {};
struct MissingDecodePayload {};
struct WrongReturnPayload {};

}  // namespace

namespace edge_runtime {

template <>
struct PayloadCodec<ValidPayload> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 1;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;
	static bool encode(const ValidPayload&, std::byte*, size_t) noexcept { return true; }
	static bool decode(const std::byte*, size_t, ValidPayload*) noexcept { return true; }
};

template <>
struct PayloadCodec<ThrowingEncodePayload> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 1;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;
	static bool encode(const ThrowingEncodePayload&, std::byte*, size_t) { return true; }
	static bool decode(const std::byte*, size_t, ThrowingEncodePayload*) noexcept { return true; }
};

template <>
struct PayloadCodec<ThrowingDecodePayload> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 1;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;
	static bool encode(const ThrowingDecodePayload&, std::byte*, size_t) noexcept { return true; }
	static bool decode(const std::byte*, size_t, ThrowingDecodePayload*) { return true; }
};

template <>
struct PayloadCodec<MissingDecodePayload> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 1;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;
	static bool encode(const MissingDecodePayload&, std::byte*, size_t) noexcept { return true; }
};

template <>
struct PayloadCodec<WrongReturnPayload> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 1;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;
	static int encode(const WrongReturnPayload&, std::byte*, size_t) noexcept { return 1; }
	static int decode(const std::byte*, size_t, WrongReturnPayload*) noexcept { return 1; }
};

}  // namespace edge_runtime

namespace {

using edge_runtime::detail::PayloadCodecContract;

static_assert(edge_runtime::kSupportedPayload<ValidPayload>);
static_assert(!edge_runtime::kSupportedPayload<ThrowingEncodePayload>);
static_assert(!PayloadCodecContract<ThrowingEncodePayload>::kEncodeNoexcept);
static_assert(!edge_runtime::kSupportedPayload<ThrowingDecodePayload>);
static_assert(!PayloadCodecContract<ThrowingDecodePayload>::kDecodeNoexcept);
static_assert(!edge_runtime::kSupportedPayload<MissingDecodePayload>);
static_assert(!PayloadCodecContract<MissingDecodePayload>::kHasRequiredMembers);
static_assert(!edge_runtime::kSupportedPayload<WrongReturnPayload>);
static_assert(!PayloadCodecContract<WrongReturnPayload>::kEncodeReturnsBool);
static_assert(!PayloadCodecContract<WrongReturnPayload>::kDecodeReturnsBool);

TEST(SchemaContract, DetectsValidAndInvalidCodecs) {
	EXPECT_TRUE(edge_runtime::kSupportedPayload<ValidPayload>);
	EXPECT_FALSE(edge_runtime::kSupportedPayload<ThrowingEncodePayload>);
	EXPECT_FALSE(edge_runtime::kSupportedPayload<ThrowingDecodePayload>);
	EXPECT_FALSE(edge_runtime::kSupportedPayload<MissingDecodePayload>);
	EXPECT_FALSE(edge_runtime::kSupportedPayload<WrongReturnPayload>);
}

}  // namespace
