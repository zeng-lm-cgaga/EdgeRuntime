#ifndef EDGE_RUNTIME_SCHEMA_HPP
#define EDGE_RUNTIME_SCHEMA_HPP

#include <array>
#include <cstddef>
#include <cstdint>

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

template <typename T>
inline constexpr bool kSupportedPayload =
        PayloadCodec<T>::kDefined&& PayloadCodec<T>::kEncodedSize > 0 &&
        PayloadCodec<T>::kEncodedSize <= 64 * 1024;

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_SCHEMA_HPP
