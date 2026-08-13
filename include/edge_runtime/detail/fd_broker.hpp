#ifndef EDGE_RUNTIME_DETAIL_FD_BROKER_HPP
#define EDGE_RUNTIME_DETAIL_FD_BROKER_HPP

// v0.2 fd-pass transport (design §33): a per-channel Unix socket served by a
// single joinable thread inside the producer process hands out the memfd via
// SCM_RIGHTS. Both request and reply are fixed-width 64-byte records (magic +
// version + checksum, same discipline as ControlJournalV1) so the protocol
// carries no pointers, no dynamic lengths, and no STL types.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/result.hpp"
#include "edge_runtime/schema.hpp"

namespace edge_runtime::detail {

inline constexpr char kFdBrokerRequestMagic[] = "EDGRQ1";  // 6 chars + 2 NUL
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

// 64-byte fixed-width request (design §33.4). An all-zero fingerprint means
// "no schema filter" (used by edge_shm_ctl, which cannot know the schema).
struct alignas(8) FdBrokerRequestAbi {
	char magic[8];                   // 0
	uint32_t version;                // 8
	uint32_t flags;                  // 12  (bit0: readonly)
	uint32_t channel_hash;           // 16
	uint32_t reserved0;              // 20
	uint8_t schema_fingerprint[32];  // 24
	uint64_t checksum;               // 56
};
static_assert(sizeof(FdBrokerRequestAbi) == kFdBrokerRequestSize, "fd broker request size");
static_assert(offsetof(FdBrokerRequestAbi, version) == 8, "req version");
static_assert(offsetof(FdBrokerRequestAbi, flags) == 12, "req flags");
static_assert(offsetof(FdBrokerRequestAbi, channel_hash) == 16, "req channel_hash");
static_assert(offsetof(FdBrokerRequestAbi, schema_fingerprint) == 24, "req fingerprint");
static_assert(offsetof(FdBrokerRequestAbi, checksum) == 56, "req checksum");

// 64-byte fixed-width reply. Deliberately does NOT carry payload_size,
// schema_version or a bootstrap copy: the consumer re-validates all of them
// from the mapped header after receiving the fd (validate_bootstrap_parse +
// validate_header_parse), so the wire record cannot drift from the segment it
// describes. The trailing checksum protects the record itself.
struct alignas(8) FdBrokerReplyAbi {
	char magic[8];              // 0
	uint32_t status;            // 8  (FdBrokerStatus)
	uint32_t reserved0;         // 12
	uint64_t mapping_size;      // 16
	uint64_t generation;        // 24
	uint64_t nonce_hi;          // 32
	uint64_t nonce_lo;          // 40
	uint16_t abi_major;         // 48
	uint16_t abi_minor;         // 50
	uint32_t reserved1;         // 52
	uint64_t checksum;          // 56
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

// FNV-1a over the whole record with the trailing checksum field zeroed.
uint64_t fd_broker_checksum(const void* record, size_t size) noexcept;

// Create the channel object as an anonymous memfd (design §33.3). Returns the
// only fd; the object dies with it. No global name is involved.
Result<UniqueFd> memfd_create_object(const std::string& channel_name);

// Bind the broker socket (design §33.5). On EADDRINUSE the existing socket is
// probed with connect(): a live peer means someone owns the channel -> error;
// a dead peer means a stale socket -> unlink and rebind once. Must be called
// BEFORE the READY commit, still holding the control lock.
Result<UniqueFd> fd_broker_bind(const std::string& socket_path);

// The broker serving loop (runs on the producer's single joinable thread,
// design §18.1/§33.4). Accepts connections, enforces same-UID via SO_PEERCRED,
// gates on the mapped bootstrap init_state == READY, validates channel_hash and
// (when non-zero) schema fingerprint, and replies with a dup/reopen of shm_fd
// via SCM_RIGHTS plus the fixed reply record. Blocks on accept until `stop` is
// set and the listen fd is shutdown() — never closes the fd itself.
void fd_broker_serve_loop(int listen_fd, int shm_fd, std::byte* base,
                          const uint32_t* channel_hash,
                          const std::array<std::byte, 32>* schema_fingerprint,
                          std::atomic<bool>* stop) noexcept;

// Client side (design §33.4/§33.6): connect, send the request, receive reply +
// fd in one recvmsg. Bounded retry budget `retry_ms` covers ENOENT/ECONNREFUSED
// and kNotReady replies (a producer that has not committed READY yet). A
// received fd has FD_CLOEXEC set explicitly (SCM_RIGHTS does not guarantee it).
// reply_out receives the validated reply record.
Result<UniqueFd> fd_broker_request_fd(const std::string& socket_path, uint32_t channel_hash,
                                      const SchemaDescriptor& schema, bool readonly,
                                      FdBrokerReplyAbi* reply_out, uint64_t retry_ms) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_FD_BROKER_HPP
