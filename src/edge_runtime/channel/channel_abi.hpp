#ifndef EDGE_RUNTIME_DETAIL_CHANNEL_ABI_HPP
#define EDGE_RUNTIME_DETAIL_CHANNEL_ABI_HPP

#include <cstddef>
#include <cstdint>

#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/channel/control_lock.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/channel/sample.hpp"
#include "edge_runtime/common/schema.hpp"

namespace edge_runtime::detail {

// 共享内存中的身份记录采用奇偶 role_epoch，读者遇到写入中的奇数值时必须重试。
inline constexpr int kIdentitySnapshotRetries = 8;

Result<ProcessIdentityAbi> identity_snapshot_read(ProcessIdentityAbi* abi) noexcept;

Result<uint64_t> identity_snapshot_write(ProcessIdentityAbi* abi,
                                         const ProcessIdentityAbi& snap) noexcept;

uint64_t bootstrap_checksum_of(const BootstrapHeaderAbi& boot) noexcept;

// 打开流程先校验引导区，再校验完整头部；任一不一致都拒绝映射。
Result<void> validate_bootstrap_parse(const BootstrapHeaderAbi& boot, uint64_t shm_size) noexcept;
Result<void> validate_header_parse(const ChannelHeaderAbi& header, const SchemaDescriptor& schema,
                                   uint32_t payload_size) noexcept;

Result<void> validate_header_shape(const ChannelHeaderAbi& h) noexcept;

bool next_generation_from_journal(const ControlJournalV1& journal, uint64_t* out) noexcept;

ControlJournalV1 make_control_journal(const std::string& channel_name, JournalState state,
                                      uint64_t old_gen, uint64_t new_gen, uint64_t old_nonce_hi,
                                      uint64_t old_nonce_lo, uint64_t new_nonce_hi,
                                      uint64_t new_nonce_lo,
                                      const ProcessIdentity& creator) noexcept;

Result<void> pread_full(int fd, void* buf, size_t size, uint64_t offset) noexcept;
Result<void> pwrite_full(int fd, const void* buf, size_t size, uint64_t offset) noexcept;

Result<ChannelStatus> read_channel_status(std::byte* base) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CHANNEL_ABI_HPP
