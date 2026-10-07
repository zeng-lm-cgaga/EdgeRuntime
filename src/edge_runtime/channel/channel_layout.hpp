#ifndef EDGE_RUNTIME_DETAIL_CHANNEL_LAYOUT_HPP
#define EDGE_RUNTIME_DETAIL_CHANNEL_LAYOUT_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#include "edge_runtime/utility/checked_math.hpp"

namespace edge_runtime::detail {

// 这些常量和结构体组成跨进程共享内存 ABI，字段顺序和大小不可随意调整。
inline constexpr char kBootstrapMagic[] = "EDGBOOT1";
inline constexpr char kChannelHeaderMagic[] = "EDGERT01";
inline constexpr uint32_t kAbiMajor = 1;
inline constexpr uint32_t kAbiMinor = 0;
inline constexpr uint32_t kAbiMinorMax = 1;
inline constexpr uint32_t kHeartbeatStallFactor = 3;
inline constexpr uint32_t kEndianMarker = 0x01020304u;
inline constexpr uint32_t kSlotCount = 3;
inline constexpr uint32_t kMaxPayloadSize = 64u * 1024u;
inline constexpr uint32_t kMaxChannelNameLen = 64;
inline constexpr uint64_t kMaxSampleSequence = (uint64_t{1} << 62) - 1;
inline constexpr uint32_t kInvalidSlot = 0xFFFFFFFFu;

inline constexpr uint64_t kBootstrapHeaderOffset = 0;
inline constexpr uint64_t kChannelHeaderOffset = 128;
inline constexpr uint64_t kFirstSlotOffset = 128 + 320;

enum class InitState : uint32_t {
	kEmpty = 0,
	kInitializing = 1,
	kReady = 2,
	kAborted = 3,
};

// EndpointState 表示生产者或消费者在共享控制面上的生命周期状态。
enum class EndpointState : uint32_t {
	kOffline = 0,
	kOnline = 1,
	kStopping = 2,
	kFault = 3,
};

struct alignas(64) BootstrapHeaderAbi {
	char magic[8];
	uint16_t abi_major;
	uint16_t abi_minor;
	uint32_t header_size;
	uint64_t expected_mapping_size;
	uint64_t creator_nonce_hi;
	uint64_t creator_nonce_lo;
	uint64_t creator_pid;
	uint64_t creator_proc_start_ticks;
	uint64_t creator_boot_id_hash_hi;
	uint64_t creator_boot_id_hash_lo;
	uint32_t init_state;
	uint32_t reserved0;
	uint64_t bootstrap_checksum;
	uint8_t reserved[40];
};
// 引导区先独立写入并校验，READY 后消费者才允许读取完整头部和槽。
static_assert(sizeof(BootstrapHeaderAbi) == 128, "BootstrapHeaderAbi size");
static_assert(alignof(BootstrapHeaderAbi) == 64, "BootstrapHeaderAbi align");
static_assert(offsetof(BootstrapHeaderAbi, abi_major) == 8, "bootstrap abi_major");
static_assert(offsetof(BootstrapHeaderAbi, abi_minor) == 10, "bootstrap abi_minor");
static_assert(offsetof(BootstrapHeaderAbi, header_size) == 12, "bootstrap header_size");
static_assert(offsetof(BootstrapHeaderAbi, expected_mapping_size) == 16, "bootstrap map size");
static_assert(offsetof(BootstrapHeaderAbi, creator_nonce_hi) == 24, "bootstrap nonce_hi");
static_assert(offsetof(BootstrapHeaderAbi, creator_nonce_lo) == 32, "bootstrap nonce_lo");
static_assert(offsetof(BootstrapHeaderAbi, creator_pid) == 40, "bootstrap pid");
static_assert(offsetof(BootstrapHeaderAbi, creator_proc_start_ticks) == 48, "bootstrap start");
static_assert(offsetof(BootstrapHeaderAbi, creator_boot_id_hash_hi) == 56, "bootstrap boot hi");
static_assert(offsetof(BootstrapHeaderAbi, creator_boot_id_hash_lo) == 64, "bootstrap boot lo");
static_assert(offsetof(BootstrapHeaderAbi, init_state) == 72, "bootstrap init_state");
static_assert(offsetof(BootstrapHeaderAbi, bootstrap_checksum) == 80, "bootstrap checksum");

struct alignas(64) ProcessIdentityAbi {
	alignas(8) uint64_t role_epoch;
	alignas(8) uint64_t pid;
	uint64_t proc_start_ticks;
	uint64_t boot_id_hash_hi;
	uint64_t boot_id_hash_lo;
	uint64_t reserved[3];
};
static_assert(sizeof(ProcessIdentityAbi) == 64, "ProcessIdentityAbi size");
static_assert(alignof(ProcessIdentityAbi) == 64, "ProcessIdentityAbi align");
static_assert(offsetof(ProcessIdentityAbi, role_epoch) == 0, "pid role_epoch");
static_assert(offsetof(ProcessIdentityAbi, pid) == 8, "pid pid");
static_assert(offsetof(ProcessIdentityAbi, proc_start_ticks) == 16, "pid start");
static_assert(offsetof(ProcessIdentityAbi, boot_id_hash_hi) == 24, "pid boot hi");
static_assert(offsetof(ProcessIdentityAbi, boot_id_hash_lo) == 32, "pid boot lo");

struct alignas(64) ChannelHeaderAbi {
	char magic[8];
	uint16_t abi_major;
	uint16_t abi_minor;
	uint32_t header_size;
	uint32_t endian_marker;
	uint32_t slot_count;
	uint32_t payload_size;
	uint32_t max_payload_size;
	uint32_t schema_version;
	uint64_t mapping_size;
	uint8_t schema_fingerprint[32];
	uint64_t generation;
	uint64_t instance_nonce_hi;
	uint64_t instance_nonce_lo;
	ProcessIdentityAbi producer;
	ProcessIdentityAbi consumer;

	alignas(8) uint64_t latest_ticket;
	alignas(4) uint32_t notify_epoch;
	alignas(4) uint32_t init_state;
	alignas(4) uint32_t producer_state;
	alignas(4) uint32_t consumer_state;
	alignas(8) uint64_t publish_count;
	alignas(8) uint64_t read_count;
	alignas(8) uint64_t last_publish_boot_ns;

	alignas(8) uint64_t heartbeat_boot_ns;
	alignas(8) uint64_t producer_heartbeat_interval_ns;
};
// 头部保存 Schema、实例身份、槽状态和通知纪元；heartbeat 字段只在启用时有效。
static_assert(sizeof(ChannelHeaderAbi) == 320, "ChannelHeaderAbi size");
static_assert(alignof(ChannelHeaderAbi) == 64, "ChannelHeaderAbi align");
static_assert(offsetof(ChannelHeaderAbi, abi_major) == 8, "hdr abi_major");
static_assert(offsetof(ChannelHeaderAbi, abi_minor) == 10, "hdr abi_minor");
static_assert(offsetof(ChannelHeaderAbi, header_size) == 12, "hdr header_size");
static_assert(offsetof(ChannelHeaderAbi, endian_marker) == 16, "hdr endian");
static_assert(offsetof(ChannelHeaderAbi, slot_count) == 20, "hdr slot_count");
static_assert(offsetof(ChannelHeaderAbi, payload_size) == 24, "hdr payload_size");
static_assert(offsetof(ChannelHeaderAbi, max_payload_size) == 28, "hdr max_payload");
static_assert(offsetof(ChannelHeaderAbi, schema_version) == 32, "hdr schema_version");
static_assert(offsetof(ChannelHeaderAbi, mapping_size) == 40, "hdr mapping_size");
static_assert(offsetof(ChannelHeaderAbi, schema_fingerprint) == 48, "hdr fingerprint");
static_assert(offsetof(ChannelHeaderAbi, generation) == 80, "hdr generation");
static_assert(offsetof(ChannelHeaderAbi, instance_nonce_hi) == 88, "hdr nonce_hi");
static_assert(offsetof(ChannelHeaderAbi, instance_nonce_lo) == 96, "hdr nonce_lo");
static_assert(offsetof(ChannelHeaderAbi, producer) == 128, "hdr producer");
static_assert(offsetof(ChannelHeaderAbi, consumer) == 192, "hdr consumer");
static_assert(offsetof(ChannelHeaderAbi, latest_ticket) == 256, "hdr latest_ticket");
static_assert(offsetof(ChannelHeaderAbi, notify_epoch) == 264, "hdr notify_epoch");
static_assert(offsetof(ChannelHeaderAbi, init_state) == 268, "hdr init_state");
static_assert(offsetof(ChannelHeaderAbi, producer_state) == 272, "hdr producer_state");
static_assert(offsetof(ChannelHeaderAbi, consumer_state) == 276, "hdr consumer_state");
static_assert(offsetof(ChannelHeaderAbi, publish_count) == 280, "hdr publish_count");
static_assert(offsetof(ChannelHeaderAbi, read_count) == 288, "hdr read_count");
static_assert(offsetof(ChannelHeaderAbi, last_publish_boot_ns) == 296, "hdr last_publish");
static_assert(offsetof(ChannelHeaderAbi, heartbeat_boot_ns) == 304, "hdr heartbeat_boot_ns");
static_assert(offsetof(ChannelHeaderAbi, producer_heartbeat_interval_ns) == 312,
              "hdr heartbeat_interval");

struct alignas(64) SlotHeaderAbi {
	alignas(4) uint32_t state;
	uint32_t payload_size;
	uint64_t sample_sequence;
	uint64_t publish_boot_ns;
	uint64_t payload_checksum;
	alignas(8) uint64_t reader_role_epoch;
	uint8_t reserved[24];
};
static_assert(sizeof(SlotHeaderAbi) == 64, "SlotHeaderAbi size");
static_assert(alignof(SlotHeaderAbi) == 64, "SlotHeaderAbi align");
static_assert(offsetof(SlotHeaderAbi, state) == 0, "slot state");
static_assert(offsetof(SlotHeaderAbi, payload_size) == 4, "slot payload_size");
static_assert(offsetof(SlotHeaderAbi, sample_sequence) == 8, "slot sequence");
static_assert(offsetof(SlotHeaderAbi, publish_boot_ns) == 16, "slot publish_ns");
static_assert(offsetof(SlotHeaderAbi, payload_checksum) == 24, "slot checksum");
static_assert(offsetof(SlotHeaderAbi, reader_role_epoch) == 32, "slot role_epoch");

constexpr uint64_t make_ticket(uint64_t sequence, uint32_t slot_index) noexcept {
	return (sequence << 2) | static_cast<uint64_t>(slot_index & 3u);
}

constexpr uint32_t ticket_slot(uint64_t ticket) noexcept {
	return static_cast<uint32_t>(ticket & 3u);
}

constexpr uint64_t ticket_sequence(uint64_t ticket) noexcept { return ticket >> 2; }

constexpr uint64_t kSlotHeaderSize = sizeof(SlotHeaderAbi);

// 三个槽的起始偏移和步长都经过溢出检查，防止异常 payload 计算出越界映射。
inline bool mapping_size_for_payload(uint32_t payload_size, uint64_t* out) noexcept {
	uint64_t slot_stride = 0;
	if (!round_up_to_multiple_u64(kSlotHeaderSize + payload_size, 64, &slot_stride)) {
		return false;
	}
	uint64_t slots_total = 0;
	if (!checked_mul_u64(kSlotCount, slot_stride, &slots_total)) return false;
	return checked_add_u64(kFirstSlotOffset, slots_total, out);
}

inline bool slot_byte_offset(uint32_t slot_index, uint64_t slot_stride, uint64_t* out) noexcept {
	if (slot_index >= kSlotCount) return false;
	if (!checked_mul_u64(slot_index, slot_stride, out)) return false;
	return checked_add_u64(kFirstSlotOffset, *out, out);
}

bool validate_channel_name(const char* name, size_t len) noexcept;

std::string channel_shm_name(const std::string& channel_name);

std::string channel_lock_path(const std::string& channel_name);

std::string channel_socket_path(const std::string& channel_name);

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CHANNEL_LAYOUT_HPP
