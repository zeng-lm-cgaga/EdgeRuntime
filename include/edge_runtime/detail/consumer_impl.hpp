#ifndef EDGE_RUNTIME_DETAIL_CONSUMER_IMPL_HPP
#define EDGE_RUNTIME_DETAIL_CONSUMER_IMPL_HPP

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/consumer.hpp"
#include "edge_runtime/detail/process_identity.hpp"
#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/result.hpp"
#include "edge_runtime/sample.hpp"
#include "edge_runtime/schema.hpp"

namespace edge_runtime::detail {

// State owned by a Consumer<T> handle. Read-path cursors (last_sequence,
// first_sample_in_generation) live here so the impl can mutate them without
// touching T; read_in_use enforces the single-reader rule (§12.3).
struct ConsumerHandle {
	ShmObject shm;
	std::string channel_name;
	uint64_t generation{0};
	uint64_t role_epoch{0};  // consumer's own even role_epoch
	uint64_t instance_nonce_hi{0};
	uint64_t instance_nonce_lo{0};
	std::array<std::byte, 32> schema_fingerprint{};
	uint32_t schema_version{0};
	uint32_t payload_size{0};
	ProcessIdentity self{};

	uint64_t last_sequence{0};
	bool first_sample_in_generation{true};
	std::atomic<bool> read_in_use{false};  // overlap guard, not shared memory
};

// Full §9.2 open sequence: bootstrap/header validation, control-lock
// revalidation of inode+generation+nonce, then consumer identity
// registration (rejecting a live consumer).
Result<std::shared_ptr<ConsumerHandle>> consumer_open_impl(const ChannelOptions& options,
                                                           const SchemaDescriptor& schema,
                                                           uint32_t payload_size);

// §12.1 latest-value read with the §12.2 linearization recheck. The public
// template owns a PayloadCodec<T>::EncodedBuffer and passes its bytes in; the
// impl copies the frozen slot payload into it, validates the checksum over the
// copy AFTER releasing the slot, and returns metadata only — it never touches
// T and never decodes. (ReadSnapshot itself lives in the public consumer.hpp.)
Result<ReadSnapshot> consumer_try_read_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                                   std::byte* encoded_out,
                                                   uint32_t encoded_cap) noexcept;

// §14.2 blocking latest-value read against an absolute MONOTONIC deadline.
// Only NoNewSample/ReadContention are waitable; every other error (including
// PayloadCorrupt/DecodeFailed/StaleHandle/ClockAnomaly) passes through
// unchanged. EAGAIN/EINTR/spurious wakeups loop WITHOUT resetting the deadline;
// on ETIMEDOUT the producer liveness is classified (§15.5): alive-but-idle ->
// kDataStale, offline/dead -> kProducerOffline, identity unknown -> kRecoveryBlocked.
Result<ReadSnapshot> consumer_wait_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                               std::byte* encoded_out, uint32_t encoded_cap,
                                               uint64_t timeout_ns) noexcept;

Result<ChannelStatus> consumer_status_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept;

// Best-effort clean shutdown (design §15.2): marks consumer_state OFFLINE if
// the consumer identity is still ours. Destructor path; never reports errors.
void consumer_shutdown_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CONSUMER_IMPL_HPP
