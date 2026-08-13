#ifndef EDGE_RUNTIME_DETAIL_CHECKSUM_HPP
#define EDGE_RUNTIME_DETAIL_CHECKSUM_HPP

#include <cstddef>
#include <cstdint>

namespace edge_runtime::detail {

// Deterministic, documented non-crypto checksum (design §8.2). Used for
// payload_checksum, bootstrap checksum and the control journal. Never an
// integrity/authenticity guarantee.
uint64_t fnv1a64(const std::byte* data, size_t size) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CHECKSUM_HPP
