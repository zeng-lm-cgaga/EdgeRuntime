// U07/U09/U14: sequence arithmetic, saturated gap, and the slot ownership
// state machine (design §10). Transitions are exercised on a local
// SlotHeaderAbi (not shared memory), so no cross-process machinery is needed
// here; the cross-process semantics are covered by the pattern integration
// tests.

#include "edge_runtime/detail/slot_protocol.hpp"

#include <gtest/gtest.h>

#include <cstdint>

#include "edge_runtime/detail/channel_layout.hpp"

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

TEST(SlotProtocol, NextSequence) {  // U07
	uint64_t out = 0;
	ASSERT_TRUE(checked_next_sequence(0, &out));
	EXPECT_EQ(out, 1u);  // ticket 0 -> first sequence
	ASSERT_TRUE(checked_next_sequence(edge_runtime::detail::make_ticket(5, 2), &out));
	EXPECT_EQ(out, 6u);
	ASSERT_TRUE(checked_next_sequence(edge_runtime::detail::make_ticket(5, 0), &out));
	EXPECT_EQ(out, 6u);  // slot index does not affect the sequence
	// maximum sequence must not wrap to 0
	EXPECT_FALSE(checked_next_sequence(edge_runtime::detail::make_ticket(kMaxSampleSequence, 1),
	                                   &out));
}

TEST(SlotProtocol, SaturatedGap) {  // U07
	EXPECT_EQ(saturated_gap(1, 5), 4u);
	EXPECT_EQ(saturated_gap(0, 1), 1u);
	EXPECT_EQ(saturated_gap(9, 9), 0u);                   // same sequence (defensive)
	EXPECT_EQ(saturated_gap(5, 2), 0u);                   // stale/older (defensive)
	EXPECT_EQ(saturated_gap(0, UINT64_MAX), UINT64_MAX);  // saturate, no wrap
}

TEST(SlotProtocol, ProducerTransitions) {  // U09
	SlotHeaderAbi slot{};              // zeroed -> FREE

	const uint32_t free = static_cast<uint32_t>(SlotState::kFree);
	ASSERT_TRUE(slot_claim_writable(&slot, free));
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kWriting));

	// a WRITING slot is not reclaimable until published: the CAS targets a FREE
	// or PUBLISHED slot, and the actual WRITING state never matches
	EXPECT_FALSE(slot_claim_writable(&slot, free));
	EXPECT_FALSE(slot_claim_writable(&slot, static_cast<uint32_t>(SlotState::kPublished)));
	EXPECT_FALSE(slot_claim_readable(&slot));

	slot_publish(&slot);
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kPublished));

	// abort rollback returns a WRITING slot to FREE, never PUBLISHED
	slot.state = static_cast<uint32_t>(SlotState::kWriting);
	slot_abort_write(&slot);
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kFree));
}

TEST(SlotProtocol, ConsumerClaimCycle) {  // U09/U14
	SlotHeaderAbi slot{};
	slot.state = static_cast<uint32_t>(SlotState::kPublished);

	ASSERT_TRUE(slot_claim_readable(&slot));
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kReadingClaiming));
	EXPECT_EQ(slot.reader_role_epoch, 0u);

	constexpr uint64_t kEpoch = 42;
	slot_mark_reading(&slot, kEpoch);
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kReading));
	EXPECT_EQ(slot.reader_role_epoch, kEpoch);

	// a READING slot cannot be claimed by producer or a second consumer
	EXPECT_FALSE(slot_claim_readable(&slot));
	EXPECT_FALSE(slot_claim_writable(&slot, static_cast<uint32_t>(SlotState::kFree)));
	EXPECT_FALSE(slot_claim_writable(&slot, static_cast<uint32_t>(SlotState::kPublished)));

	slot_release_read(&slot);
	EXPECT_EQ(slot.state, static_cast<uint32_t>(SlotState::kPublished));
	// the reader role epoch is cleared so the next claim starts from 0 (§10.2)
	EXPECT_EQ(slot.reader_role_epoch, 0u);
}

TEST(SlotProtocol, ClaimClearsStaleRoleEpoch) {  // U14
	// A slot left READING by a dead consumer must be reclaimable by the recovery
	// path; the normal read path re-claims PUBLISHED slots only after
	// release_read cleared the epoch. Simulate the reclaim: force PUBLISHED and
	// claim again — the epoch written on claim must be the fresh one.
	SlotHeaderAbi slot{};
	slot.state = static_cast<uint32_t>(SlotState::kPublished);
	slot.reader_role_epoch = 0xDEADu;

	ASSERT_TRUE(slot_claim_readable(&slot));
	slot_mark_reading(&slot, 7);
	EXPECT_EQ(slot.reader_role_epoch, 7u);  // fresh epoch, not the stale one
	slot_release_read(&slot);
	EXPECT_EQ(slot.reader_role_epoch, 0u);
}

}  // namespace
