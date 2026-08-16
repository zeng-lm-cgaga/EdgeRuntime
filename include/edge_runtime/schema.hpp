#ifndef EDGE_RUNTIME_SCHEMA_HPP
#define EDGE_RUNTIME_SCHEMA_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

namespace edge_runtime {

// 32-byte fixed schema fingerprint (design §8.4). The library never computes
// the fingerprint; callers supply it and the library only copies/compares 32
// bytes. SHA-256 derivation lives in test fixtures and tools, not the library.
struct SchemaDescriptor {
	std::array<std::byte, 32> fingerprint{};
	uint32_t version{0};
	const char* debug_name{nullptr};
};

// User specializations must define (design §16.1):
//   static constexpr bool kDefined = true;
//   static constexpr uint32_t kEncodedSize = <frozen>;
//   using EncodedBuffer = std::array<std::byte, kEncodedSize>;
//   static bool encode(const T&, std::byte*, size_t) noexcept;
//   static bool decode(const std::byte*, size_t, T*) noexcept;
template <typename T>
struct PayloadCodec;

namespace detail {

template <typename T, typename = void>
struct PayloadCodecContract {
	static constexpr bool kHasRequiredMembers = false;
	static constexpr bool kDefined = false;
	static constexpr bool kEncodedSizeTypeValid = false;
	static constexpr bool kEncodedSizeValid = false;
	static constexpr bool kEncodedBufferValid = false;
	static constexpr bool kEncodeReturnsBool = false;
	static constexpr bool kDecodeReturnsBool = false;
	static constexpr bool kEncodeNoexcept = false;
	static constexpr bool kDecodeNoexcept = false;
	static constexpr bool kSatisfied = false;
};

template <typename T>
struct PayloadCodecContract<
        T, std::void_t<decltype(PayloadCodec<T>::kDefined),
                       decltype(PayloadCodec<T>::kEncodedSize),
                       typename PayloadCodec<T>::EncodedBuffer,
                       decltype(PayloadCodec<T>::encode(std::declval<const T&>(),
                                                        std::declval<std::byte*>(),
                                                        std::declval<size_t>())),
                       decltype(PayloadCodec<T>::decode(std::declval<const std::byte*>(),
                                                        std::declval<size_t>(),
                                                        std::declval<T*>()))>> {
	using Codec = PayloadCodec<T>;
	using EncodedBuffer = typename Codec::EncodedBuffer;
	using EncodeResult = decltype(Codec::encode(std::declval<const T&>(),
	                                           std::declval<std::byte*>(),
	                                           std::declval<size_t>()));
	using DecodeResult = decltype(Codec::decode(std::declval<const std::byte*>(),
	                                           std::declval<size_t>(),
	                                           std::declval<T*>()));

	static constexpr bool kHasRequiredMembers = true;
	static constexpr bool kDefinedTypeValid =
	        std::is_same_v<std::remove_cv_t<decltype(Codec::kDefined)>, bool>;
	static constexpr bool kDefined = kDefinedTypeValid && Codec::kDefined;
	static constexpr bool kEncodedSizeTypeValid =
	        std::is_same_v<std::remove_cv_t<decltype(Codec::kEncodedSize)>, uint32_t>;
	static constexpr bool kEncodedSizeValid =
	        kEncodedSizeTypeValid && Codec::kEncodedSize > 0 && Codec::kEncodedSize <= 64 * 1024;
	static constexpr bool kEncodedBufferValid =
	        std::is_same_v<EncodedBuffer, std::array<std::byte, Codec::kEncodedSize>>;
	static constexpr bool kEncodeReturnsBool = std::is_same_v<EncodeResult, bool>;
	static constexpr bool kDecodeReturnsBool = std::is_same_v<DecodeResult, bool>;
	static constexpr bool kEncodeNoexcept = noexcept(Codec::encode(
	        std::declval<const T&>(), std::declval<std::byte*>(), std::declval<size_t>()));
	static constexpr bool kDecodeNoexcept = noexcept(Codec::decode(
	        std::declval<const std::byte*>(), std::declval<size_t>(), std::declval<T*>()));
	static constexpr bool kSatisfied =
	        kDefined && kEncodedSizeValid && kEncodedBufferValid && kEncodeReturnsBool &&
	        kDecodeReturnsBool && kEncodeNoexcept && kDecodeNoexcept;
};

template <typename T>
constexpr void validate_payload_codec() {
	using Contract = PayloadCodecContract<T>;
	static_assert(Contract::kHasRequiredMembers,
	              "PayloadCodec<T> must define kDefined, kEncodedSize, EncodedBuffer, "
	              "encode(), and decode()");
	static_assert(!Contract::kHasRequiredMembers || Contract::kDefined,
	              "PayloadCodec<T>::kDefined must be constexpr bool true");
	static_assert(!Contract::kHasRequiredMembers || Contract::kEncodedSizeTypeValid,
	              "PayloadCodec<T>::kEncodedSize must be constexpr uint32_t");
	static_assert(!Contract::kHasRequiredMembers || Contract::kEncodedSizeValid,
	              "PayloadCodec<T>::kEncodedSize must be in [1, 65536]");
	static_assert(!Contract::kHasRequiredMembers || Contract::kEncodedBufferValid,
	              "PayloadCodec<T>::EncodedBuffer must be std::array<std::byte, kEncodedSize>");
	static_assert(!Contract::kHasRequiredMembers || Contract::kEncodeReturnsBool,
	              "PayloadCodec<T>::encode must return bool");
	static_assert(!Contract::kHasRequiredMembers || Contract::kDecodeReturnsBool,
	              "PayloadCodec<T>::decode must return bool");
	static_assert(!Contract::kHasRequiredMembers || Contract::kEncodeNoexcept,
	              "PayloadCodec<T>::encode must be noexcept");
	static_assert(!Contract::kHasRequiredMembers || Contract::kDecodeNoexcept,
	              "PayloadCodec<T>::decode must be noexcept");
}

}  // namespace detail

template <typename T>
inline constexpr bool kSupportedPayload = detail::PayloadCodecContract<T>::kSatisfied;

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_SCHEMA_HPP
