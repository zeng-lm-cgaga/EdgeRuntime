#ifndef EDGE_RUNTIME_DETAIL_CHECKSUM_HPP
#define EDGE_RUNTIME_DETAIL_CHECKSUM_HPP

#include <cstddef>
#include <cstdint>

namespace edge_runtime::detail {

// 用于 ABI 记录和载荷校验的确定性非加密校验值。
uint64_t fnv1a64(const std::byte* data, size_t size) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CHECKSUM_HPP
