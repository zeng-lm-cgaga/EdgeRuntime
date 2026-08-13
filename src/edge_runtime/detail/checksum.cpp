#include "edge_runtime/detail/checksum.hpp"

#include <cstdint>

namespace edge_runtime::detail {

namespace {
constexpr uint64_t kOffsetBasis = 14695981039346656037ULL;
constexpr uint64_t kPrime = 1099511628211ULL;
}  // namespace

uint64_t fnv1a64(const std::byte* data, size_t size) noexcept {
	uint64_t hash = kOffsetBasis;
	for (size_t i = 0; i < size; ++i) {
		hash ^= static_cast<uint8_t>(data[i]);
		hash *= kPrime;
	}
	return hash;
}

}  // namespace edge_runtime::detail
