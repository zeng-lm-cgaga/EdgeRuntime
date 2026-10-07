

#include "edge_runtime/channel/slot_protocol.hpp"

#include <gtest/gtest.h>

#include <cstdint>

#include "edge_runtime/channel/channel_layout.hpp"

namespace {

using edge_runtime::detail::checked_next_sequence;
using edge_runtime::detail::kMaxSampleSequence;
using edge_runtime::detail::saturated_gap;
using edge_runtime::detail::slot_abort_write;
using edge_runtime::detail::slot_claim_readable;
using edge_runtime::detail::slot_claim_writable;
using edge_runtime::detail::slot_mark_reading;
using edge_runtime::detail::slot_publish;
using edge_runtime::detail::slot_release_read;
using edge_runtime::detail::SlotHeaderAbi;
using edge_runtime::detail::SlotState;

TEST(SlotProtocol, NextSequence) {
	uint64_t out = 0;
	ASSERT_TRUE(checked_next_sequence(0, &out));
	EXPECT_EQ(out, 1u);
	ASSERT_TRUE(checked_next_sequence(edge_runtime::detail::make_ticket(5, 2), &out));
	EXPECT_EQ(out, 6u);
	ASSERT_TRUE(checked_next_sequence(edge_runtime::detail::make_ticket(5, 0), &out));
	EXPECT_EQ(out, 6u);

	EXPECT_FALSE(checked_next_sequence(edge_runtime::detail::make_ticket(kMaxSampleSequence, 1),
	                                   &out));
}

TEST(SlotProtocol, SaturatedGap) {
	EXPECT_EQ(saturated_gap(1, 2), 0u);
	EXPECT_EQ(saturated_gap(1, 5), 3u);
	EXPECT_EQ(saturated_gap(0, 1), 0u);
	EXPECT_EQ(saturated_gap(9, 9), 0u);
	EXPECT_EQ(saturated_gap(5, 2), 0u);
	EXPECT_EQ(saturated_gap(0, UINT64_MAX), UINT64_MAX - 1);
}

TEST(SlotProtocol, ProducerTransitions) {
	SlotHeaderAbi slot{};

	const uint32_t free = static_cast<uint32_t>(SlotState::kFree);
	ASSERT_TRUE(slot_claim_writable(&slot, free));
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kWriting));

	EXPECT_FALSE(slot_claim_writable(&slot, free));
	EXPECT_FALSE(slot_claim_writable(&slot, static_cast<uint32_t>(SlotState::kPublished)));
	EXPECT_FALSE(slot_claim_readable(&slot));

	slot_publish(&slot);
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kPublished));

	slot.state = static_cast<uint32_t>(SlotState::kWriting);
	slot_abort_write(&slot);
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kFree));
}

TEST(SlotProtocol, ConsumerClaimCycle) {
	SlotHeaderAbi slot{};
	slot.state = static_cast<uint32_t>(SlotState::kPublished);

	ASSERT_TRUE(slot_claim_readable(&slot));
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kReadingClaiming));
	EXPECT_EQ(slot.reader_role_epoch, 0u);

	constexpr uint64_t kEpoch = 42;
	slot_mark_reading(&slot, kEpoch);
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kReading));
	EXPECT_EQ(slot.reader_role_epoch, kEpoch);

	EXPECT_FALSE(slot_claim_readable(&slot));
	EXPECT_FALSE(slot_claim_writable(&slot, static_cast<uint32_t>(SlotState::kFree)));
	EXPECT_FALSE(slot_claim_writable(&slot, static_cast<uint32_t>(SlotState::kPublished)));

	slot_release_read(&slot);
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kPublished));

	EXPECT_EQ(slot.reader_role_epoch, 0u);
}

TEST(SlotProtocol, ClaimClearsStaleRoleEpoch) {

	SlotHeaderAbi slot{};
	slot.state = static_cast<uint32_t>(SlotState::kPublished);
	slot.reader_role_epoch = 0xDEADu;

	ASSERT_TRUE(slot_claim_readable(&slot));
	slot_mark_reading(&slot, 7);
	EXPECT_EQ(slot.reader_role_epoch, 7u);
	slot_release_read(&slot);
	EXPECT_EQ(slot.reader_role_epoch, 0u);
}

}
