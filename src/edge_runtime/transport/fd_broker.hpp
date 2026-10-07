#ifndef EDGE_RUNTIME_DETAIL_FD_BROKER_HPP
#define EDGE_RUNTIME_DETAIL_FD_BROKER_HPP

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/common/schema.hpp"

namespace edge_runtime::detail {

// Broker 只传递共享内存对象的文件描述符；请求和回复均为固定大小并带校验和。
inline constexpr char kFdBrokerRequestMagic[] = "EDGRQ1";
inline constexpr char kFdBrokerReplyMagic[] = "EDGRP1";
inline constexpr uint32_t kFdBrokerVersion = 1;
inline constexpr size_t kFdBrokerRequestSize = 64;
inline constexpr size_t kFdBrokerReplySize = 64;
inline constexpr uint32_t kFdBrokerFlagReadonly = 1u << 0;

enum class FdBrokerStatus : uint32_t {
	kOk = 0,
	kNotReady = 1,
	kChannelMismatch = 2,
	kRefused = 3,
	kSystem = 4,
};

struct alignas(8) FdBrokerRequestAbi {
	char magic[8];
	uint32_t version;
	uint32_t flags;
	uint32_t channel_hash;
	uint32_t reserved0;
	uint8_t schema_fingerprint[32];
	uint64_t checksum;
};
static_assert(sizeof(FdBrokerRequestAbi) == kFdBrokerRequestSize, "fd broker request size");
static_assert(offsetof(FdBrokerRequestAbi, version) == 8, "req version");
static_assert(offsetof(FdBrokerRequestAbi, flags) == 12, "req flags");
static_assert(offsetof(FdBrokerRequestAbi, channel_hash) == 16, "req channel_hash");
static_assert(offsetof(FdBrokerRequestAbi, schema_fingerprint) == 24, "req fingerprint");
static_assert(offsetof(FdBrokerRequestAbi, checksum) == 56, "req checksum");

struct alignas(8) FdBrokerReplyAbi {
	char magic[8];
	uint32_t status;
	uint32_t reserved0;
	uint64_t mapping_size;
	uint64_t generation;
	uint64_t nonce_hi;
	uint64_t nonce_lo;
	uint16_t abi_major;
	uint16_t abi_minor;
	uint32_t reserved1;
	uint64_t checksum;
};
static_assert(sizeof(FdBrokerReplyAbi) == kFdBrokerReplySize, "fd broker reply size");
static_assert(offsetof(FdBrokerReplyAbi, status) == 8, "reply status");
static_assert(offsetof(FdBrokerReplyAbi, mapping_size) == 16, "reply mapping_size");
static_assert(offsetof(FdBrokerReplyAbi, generation) == 24, "reply generation");
static_assert(offsetof(FdBrokerReplyAbi, nonce_hi) == 32, "reply nonce_hi");
static_assert(offsetof(FdBrokerReplyAbi, nonce_lo) == 40, "reply nonce_lo");
static_assert(offsetof(FdBrokerReplyAbi, abi_major) == 48, "reply abi_major");
static_assert(offsetof(FdBrokerReplyAbi, abi_minor) == 50, "reply abi_minor");
static_assert(offsetof(FdBrokerReplyAbi, checksum) == 56, "reply checksum");

uint64_t fd_broker_checksum(const void* record, size_t size) noexcept;

Result<UniqueFd> memfd_create_object(const std::string& channel_name);

Result<UniqueFd> fd_broker_bind(const std::string& socket_path);

void fd_broker_serve_loop(int listen_fd, int shm_fd, std::byte* base,
                          const uint32_t* channel_hash,
                          const std::array<std::byte, 32>* schema_fingerprint,
                          std::atomic<bool>* stop) noexcept;

Result<UniqueFd> fd_broker_request_fd(const std::string& socket_path, uint32_t channel_hash,
                                      const SchemaDescriptor& schema, bool readonly,
                                      FdBrokerReplyAbi* reply_out, uint64_t retry_ms) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_FD_BROKER_HPP
