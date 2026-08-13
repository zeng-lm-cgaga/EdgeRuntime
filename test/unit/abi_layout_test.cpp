// U01–U04: frozen ABI layout, naming, ticket packing, mapping formula.
// The offsets/sizes here are living evidence of the v0.1 contract (§8); the
// structs themselves are also static_assert'd in channel_layout.hpp, this test
// asserts the values the design document freezes.

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "edge_runtime/detail/channel_layout.hpp"

namespace {

using edge_runtime::detail::BootstrapHeaderAbi;
using edge_runtime::detail::channel_lock_path;
using edge_runtime::detail::channel_shm_name;
using edge_runtime::detail::ChannelHeaderAbi;
using edge_runtime::detail::kAbiMinor;
using edge_runtime::detail::kAbiMinorMax;
using edge_runtime::detail::kChannelHeaderOffset;
using edge_runtime::detail::kFirstSlotOffset;
using edge_runtime::detail::kMaxPayloadSize;
using edge_runtime::detail::kMaxSampleSequence;
using edge_runtime::detail::kSlotCount;
using edge_runtime::detail::kSlotHeaderSize;
using edge_runtime::detail::make_ticket;
using edge_runtime::detail::mapping_size_for_payload;
using edge_runtime::detail::ProcessIdentityAbi;
using edge_runtime::detail::slot_byte_offset;
using edge_runtime::detail::SlotHeaderAbi;
using edge_runtime::detail::ticket_sequence;
using edge_runtime::detail::ticket_slot;
using edge_runtime::detail::validate_channel_name;

TEST(AbiLayout, FrozenRecordSizes) {  // U04
	EXPECT_EQ(sizeof(BootstrapHeaderAbi), 128u);
	EXPECT_EQ(sizeof(ChannelHeaderAbi), 320u);
	EXPECT_EQ(sizeof(SlotHeaderAbi), 64u);
	EXPECT_EQ(sizeof(ProcessIdentityAbi), 64u);
	EXPECT_EQ(alignof(BootstrapHeaderAbi), 64u);
	EXPECT_EQ(alignof(ChannelHeaderAbi), 64u);
	EXPECT_EQ(alignof(SlotHeaderAbi), 64u);
	EXPECT_EQ(alignof(ProcessIdentityAbi), 64u);
}

TEST(AbiLayout, BootstrapOffsets) {  // U04
	EXPECT_EQ(offsetof(BootstrapHeaderAbi, magic), 0u);
	EXPECT_EQ(offsetof(BootstrapHeaderAbi, abi_major), 8u);
	EXPECT_EQ(offsetof(BootstrapHeaderAbi, header_size), 12u);
	EXPECT_EQ(offsetof(BootstrapHeaderAbi, expected_mapping_size), 16u);
	EXPECT_EQ(offsetof(BootstrapHeaderAbi, creator_boot_id_hash_hi), 56u);
	EXPECT_EQ(offsetof(BootstrapHeaderAbi, init_state), 72u);
	EXPECT_EQ(offsetof(BootstrapHeaderAbi, bootstrap_checksum), 80u);
}

TEST(AbiLayout, HeaderOffsets) {  // U04
	EXPECT_EQ(offsetof(ChannelHeaderAbi, magic), 0u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, endian_marker), 16u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, payload_size), 24u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, generation), 80u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, producer), 128u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, consumer), 192u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, latest_ticket), 256u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, notify_epoch), 264u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, producer_state), 272u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, publish_count), 280u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, last_publish_boot_ns), 296u);
	// v0.2 §34: heartbeat fields occupy the former trailing padding; the struct
	// size stays 320 so every v0.1 offset above is unchanged.
	EXPECT_EQ(offsetof(ChannelHeaderAbi, heartbeat_boot_ns), 304u);
	EXPECT_EQ(offsetof(ChannelHeaderAbi, producer_heartbeat_interval_ns), 312u);
	EXPECT_EQ(sizeof(ChannelHeaderAbi), 320u);
	EXPECT_EQ(kAbiMinor, 0u);     // written when heartbeat is disabled
	EXPECT_EQ(kAbiMinorMax, 1u);  // minor 1 == optional heartbeat
}

TEST(AbiLayout, SlotOffsets) {  // U04
	EXPECT_EQ(offsetof(SlotHeaderAbi, state), 0u);
	EXPECT_EQ(offsetof(SlotHeaderAbi, payload_checksum), 24u);
	EXPECT_EQ(offsetof(SlotHeaderAbi, reader_role_epoch), 32u);
	EXPECT_EQ(kSlotHeaderSize, 64u);
	EXPECT_EQ(kChannelHeaderOffset, 128u);
	EXPECT_EQ(kFirstSlotOffset, 448u);
}

TEST(ChannelLayout, NameValidation) {  // U01
	// allowed charset: [a-zA-Z0-9_.-]
	EXPECT_TRUE(validate_channel_name("sensor.1", 8));
	EXPECT_TRUE(validate_channel_name("a-b_c.d", 7));
	EXPECT_TRUE(validate_channel_name("x", 1));
	EXPECT_TRUE(validate_channel_name("-a", 2));
	EXPECT_TRUE(validate_channel_name("a-", 2));
	EXPECT_TRUE(validate_channel_name("a_b_c_", 6));
	// empty / null
	EXPECT_FALSE(validate_channel_name("", 0));
	EXPECT_FALSE(validate_channel_name(nullptr, 0));
	// path traversal guard: "/" and consecutive ".."
	EXPECT_FALSE(validate_channel_name("a/b", 3));
	EXPECT_FALSE(validate_channel_name("..", 2));
	EXPECT_FALSE(validate_channel_name("a..b", 4));
	EXPECT_FALSE(validate_channel_name("a.b..c", 6));
	// whitespace and control chars rejected
	EXPECT_FALSE(validate_channel_name("a b", 3));
	EXPECT_FALSE(validate_channel_name("a\tb", 3));
	// length bound
	std::string max_name(64, 'a');
	EXPECT_TRUE(validate_channel_name(max_name.c_str(), max_name.size()));
	std::string too_long(65, 'a');
	EXPECT_FALSE(validate_channel_name(too_long.c_str(), too_long.size()));
}

TEST(ChannelLayout, TicketPacking) {       // U03
	EXPECT_EQ(make_ticket(0, 0), 0u);  // ticket 0 == unpublished
	EXPECT_EQ(ticket_sequence(make_ticket(7, 2)), 7u);
	EXPECT_EQ(ticket_slot(make_ticket(7, 2)), 2u);
	// maximum sequence still fits, slot bits preserved
	const uint64_t max_ticket = make_ticket(kMaxSampleSequence, 3);
	EXPECT_EQ(ticket_sequence(max_ticket), kMaxSampleSequence);
	EXPECT_EQ(ticket_slot(max_ticket), 3u);
}

TEST(ChannelLayout, MappingFormula) {  // U02
	uint64_t m = 0;
	// payload 0 -> stride 64 -> 448 + 3*64 = 640
	ASSERT_TRUE(mapping_size_for_payload(0, &m));
	EXPECT_EQ(m, kFirstSlotOffset + 3 * 64u);
	// payload 64 -> stride 128
	ASSERT_TRUE(mapping_size_for_payload(64, &m));
	EXPECT_EQ(m, kFirstSlotOffset + 3 * 128u);
	// max payload 65536 -> stride 65600 (no slack: 65600 % 64 == 0)
	ASSERT_TRUE(mapping_size_for_payload(kMaxPayloadSize, &m));
	EXPECT_EQ(m, kFirstSlotOffset + 3 * 65600u);
	// payload that forces rounding
	ASSERT_TRUE(mapping_size_for_payload(65, &m));
	EXPECT_EQ(m, kFirstSlotOffset + 3 * (64 + 128u));
}

TEST(ChannelLayout, SlotByteOffset) {  // U02
	uint64_t o = 0;
	const uint64_t stride = 128;
	ASSERT_TRUE(slot_byte_offset(0, stride, &o));
	EXPECT_EQ(o, kFirstSlotOffset);
	ASSERT_TRUE(slot_byte_offset(1, stride, &o));
	EXPECT_EQ(o, kFirstSlotOffset + stride);
	ASSERT_TRUE(slot_byte_offset(2, stride, &o));
	EXPECT_EQ(o, kFirstSlotOffset + 2 * stride);
	EXPECT_FALSE(slot_byte_offset(3, stride, &o));  // out of range
}

TEST(ChannelLayout, Naming) {  // U01
	const std::string uid = std::to_string(getuid());
	EXPECT_EQ(channel_shm_name("abc"), "/edgeruntime." + uid + ".abc");
	EXPECT_EQ(channel_lock_path("abc"), "/run/user/" + uid + "/edgeruntime/abc.lock");
}

}  // namespace
