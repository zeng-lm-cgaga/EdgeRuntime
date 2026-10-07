#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>

#include "edge_runtime/queue/mpmc_layout.hpp"

namespace {

using edge_runtime::detail::MpmcQueueHeaderAbi;
using edge_runtime::detail::MpmcSlotHeaderAbi;
using edge_runtime::detail::mpmc_mapping_size_for;
using edge_runtime::detail::mpmc_slot_offset;
using edge_runtime::detail::mpmc_checked_ticket_bounds;

TEST(MpmcLayout, FrozenHeaderAndSlotShape) {
	EXPECT_EQ(sizeof(MpmcQueueHeaderAbi), 320u);
	EXPECT_EQ(alignof(MpmcQueueHeaderAbi), 64u);
	EXPECT_EQ(offsetof(MpmcQueueHeaderAbi, schema_fingerprint), 48u);
	EXPECT_EQ(offsetof(MpmcQueueHeaderAbi, creator), 104u);
	EXPECT_EQ(offsetof(MpmcQueueHeaderAbi, enqueue_position), 136u);
	EXPECT_EQ(offsetof(MpmcQueueHeaderAbi, enqueue_owner), 168u);
	EXPECT_EQ(offsetof(MpmcQueueHeaderAbi, header_checksum), 232u);
	EXPECT_EQ(offsetof(MpmcQueueHeaderAbi, notify_epoch), 240u);
	EXPECT_EQ(offsetof(MpmcQueueHeaderAbi, enqueue_owner_epoch), 256u);
	EXPECT_EQ(offsetof(MpmcQueueHeaderAbi, dequeue_owner_epoch), 264u);
	EXPECT_EQ(sizeof(MpmcSlotHeaderAbi), 64u);
	EXPECT_EQ(alignof(MpmcSlotHeaderAbi), 64u);
	EXPECT_EQ(offsetof(MpmcSlotHeaderAbi, sequence), 0u);
	EXPECT_EQ(offsetof(MpmcSlotHeaderAbi, owner), 16u);
	EXPECT_EQ(offsetof(MpmcSlotHeaderAbi, ticket), 48u);
	EXPECT_EQ(offsetof(MpmcSlotHeaderAbi, owner_epoch), 56u);
}

TEST(MpmcLayout, MappingAndSlotOffsetsAreChecked) {
	uint64_t size = 0;
	ASSERT_TRUE(mpmc_mapping_size_for(4, 16, &size));
	EXPECT_EQ(size, 320u + 4u * 128u);
	uint64_t offset = 0;
	EXPECT_TRUE(mpmc_slot_offset(0, 4, 128, &offset));
	EXPECT_EQ(offset, 320u);
	EXPECT_TRUE(mpmc_slot_offset(3, 4, 128, &offset));
	EXPECT_EQ(offset, 320u + 3u * 128u);
	EXPECT_FALSE(mpmc_slot_offset(4, 4, 128, &offset));
	EXPECT_FALSE(mpmc_mapping_size_for(0, 16, &size));
	EXPECT_FALSE(mpmc_mapping_size_for(4, 0, &size));
}

TEST(MpmcLayout, TicketAdvanceBoundsRejectWraparound) {
	uint64_t next_ticket = 0;
	uint64_t reclaim_sequence = 0;
	EXPECT_TRUE(mpmc_checked_ticket_bounds(10, 4, &next_ticket, &reclaim_sequence));
	EXPECT_EQ(next_ticket, 11u);
	EXPECT_EQ(reclaim_sequence, 14u);

	EXPECT_TRUE(mpmc_checked_ticket_bounds(UINT64_MAX - 4u, 4, &next_ticket,
	                                       &reclaim_sequence));
	EXPECT_EQ(next_ticket, UINT64_MAX - 3u);
	EXPECT_EQ(reclaim_sequence, UINT64_MAX);
	EXPECT_FALSE(mpmc_checked_ticket_bounds(UINT64_MAX - 3u, 4, &next_ticket,
	                                        &reclaim_sequence));
	EXPECT_FALSE(mpmc_checked_ticket_bounds(UINT64_MAX, 1, &next_ticket, &reclaim_sequence));
	EXPECT_FALSE(mpmc_checked_ticket_bounds(0, 0, &next_ticket, &reclaim_sequence));
}

}  // namespace
