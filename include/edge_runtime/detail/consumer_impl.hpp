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

// ConsumerHandle 保存映射身份和读取游标；operation_in_use 防止同一句柄重入。
struct ConsumerHandle {
	ShmObject shm;
	std::string channel_name;
	Transport transport{Transport::kPosixShm};
	uint64_t generation{0};
	uint64_t role_epoch{0};
	uint64_t instance_nonce_hi{0};
	uint64_t instance_nonce_lo{0};
	std::array<std::byte, 32> schema_fingerprint{};
	uint32_t schema_version{0};
	uint32_t payload_size{0};
	ProcessIdentity self{};

	uint64_t reconnect_timeout_ms{1000};
	uint64_t last_sequence{0};
	bool first_sample_in_generation{true};
	std::atomic<bool> operation_in_use{false};
	std::atomic<bool> shutdown_pending{false};
	std::atomic<bool> shutdown_started{false};
};

Result<std::shared_ptr<ConsumerHandle>> consumer_open_impl(const ChannelOptions& options,
                                                           const SchemaDescriptor& schema,
                                                           uint32_t payload_size);

Result<ReadSnapshot> consumer_try_read_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                                   std::byte* encoded_out,
                                                   uint32_t encoded_cap) noexcept;

Result<ReadSnapshot> consumer_wait_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                               std::byte* encoded_out, uint32_t encoded_cap,
                                               uint64_t timeout_ns) noexcept;

Result<ReconnectInfo> consumer_reconnect_impl(
        const std::shared_ptr<ConsumerHandle>& handle) noexcept;

Result<ChannelStatus> consumer_status_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept;

void consumer_shutdown_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CONSUMER_IMPL_HPP
