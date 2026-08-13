#ifndef EDGE_RUNTIME_DETAIL_CHANNEL_ABI_HPP
#define EDGE_RUNTIME_DETAIL_CHANNEL_ABI_HPP

#include <cstddef>
#include <cstdint>

#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/control_lock.hpp"
#include "edge_runtime/detail/process_identity.hpp"
#include "edge_runtime/detail/shared_atomic.hpp"
#include "edge_runtime/sample.hpp"
#include "edge_runtime/schema.hpp"

namespace edge_runtime::detail {

inline constexpr int kIdentitySnapshotRetries = 8;

// Seqlock-style snapshot of a ProcessIdentityAbi (design §8.2). Fails closed
// with RecoveryBlocked if the epoch stays odd/unstable past the retry bound.
Result<ProcessIdentityAbi> identity_snapshot_read(ProcessIdentityAbi* abi) noexcept;

// Publishes `snap` under the epoch protocol: odd marker, relaxed field stores,
// next even epoch (release). Returns the published even role_epoch. The
// resulting epoch is monotonic (current + 2), so re-registration after a dead
// owner keeps the even-epoch invariant.
Result<uint64_t> identity_snapshot_write(ProcessIdentityAbi* abi,
                                         const ProcessIdentityAbi& snap) noexcept;

// Checksum over the bootstrap with init_state + checksum zeroed (design §8.2).
uint64_t bootstrap_checksum_of(const BootstrapHeaderAbi& boot) noexcept;

Result<void> validate_bootstrap_parse(const BootstrapHeaderAbi& boot, uint64_t shm_size) noexcept;
Result<void> validate_header_parse(const ChannelHeaderAbi& header, const SchemaDescriptor& schema,
                                   uint32_t payload_size) noexcept;

// Next generation for create/replace from the journal's last completed record
// (design §15.3); 1 when nothing is recorded. Fails on overflow.
bool next_generation_from_journal(const ControlJournalV1& journal, uint64_t* out) noexcept;

// Zero-initialized journal record bound to the channel name hash.
ControlJournalV1 make_control_journal(const std::string& channel_name, JournalState state,
                                      uint64_t old_gen, uint64_t new_gen, uint64_t old_nonce_hi,
                                      uint64_t old_nonce_lo, uint64_t new_nonce_hi,
                                      uint64_t new_nonce_lo,
                                      const ProcessIdentity& creator) noexcept;

// Full pread/pwrite loops (EINTR-safe) for fixed-size ABI records.
Result<void> pread_full(int fd, void* buf, size_t size, uint64_t offset) noexcept;
Result<void> pwrite_full(int fd, const void* buf, size_t size, uint64_t offset) noexcept;

// Diagnostic snapshot of the current channel state (design §15.6 / status()).
Result<ChannelStatus> read_channel_status(std::byte* base) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CHANNEL_ABI_HPP
