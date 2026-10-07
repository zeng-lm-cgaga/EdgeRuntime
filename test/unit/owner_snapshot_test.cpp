#include <gtest/gtest.h>

#include <cstdint>

#include "edge_runtime/buffer/shared_buffer_pool_layout.hpp"
#include "edge_runtime/queue/mpmc_layout.hpp"
#include "edge_runtime/sync/owner_snapshot.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"

namespace {

using edge_runtime::detail::BufferPoolOwnerAbi;
using edge_runtime::detail::MpmcOwnerAbi;
using edge_runtime::detail::MpmcSlotHeaderAbi;
using edge_runtime::detail::OwnerSnapshotKind;
using edge_runtime::detail::OwnerSnapshotPoint;
using edge_runtime::detail::OwnerStateKind;
using edge_runtime::detail::SharedBufferBlockState;

OwnerStateKind classify(uint32_t state) noexcept {
	switch (static_cast<SharedBufferBlockState>(state)) {
		case SharedBufferBlockState::kFree:
		case SharedBufferBlockState::kPublished:
			return OwnerStateKind::kInactive;
		case SharedBufferBlockState::kWritingClaiming:
		case SharedBufferBlockState::kReadingClaiming:
			return OwnerStateKind::kClaiming;
		case SharedBufferBlockState::kWriting:
		case SharedBufferBlockState::kReading:
			return OwnerStateKind::kOwned;
		case SharedBufferBlockState::kEpochExhausted:
			return OwnerStateKind::kEpochExhausted;
		case SharedBufferBlockState::kOwnerProtocolFault:
			return OwnerStateKind::kProtocolFault;
	}
	return OwnerStateKind::kUnexpected;
}

bool valid(const BufferPoolOwnerAbi& owner) noexcept {
	return owner.pid != 0 && owner.proc_start_ticks != 0 && owner.boot_id_hash_hi != 0 &&
	       owner.boot_id_hash_lo != 0;
}

BufferPoolOwnerAbi owner(uint64_t pid, uint64_t start, uint64_t boot_hi,
                         uint64_t boot_lo) noexcept {
	return {pid, start, boot_hi, boot_lo};
}

TEST(OwnerSnapshot, AcceptsStableCompleteTupleAfterACompleteReplacement) {
	edge_runtime::detail::SharedBufferBlockHeaderAbi block{};
	edge_runtime::detail::shared_store_seq_cst(
	        &block.state, static_cast<uint32_t>(SharedBufferBlockState::kWriting));
	edge_runtime::detail::shared_store_seq_cst(&block.owner_epoch, uint64_t{2});
	edge_runtime::detail::store_owner_seq_cst(
	        &block.owner, owner(11, 101, 1001, 10001));
	bool replaced = false;
	const auto hook = [&](OwnerSnapshotPoint point) noexcept {
		if (!replaced && point == OwnerSnapshotPoint::kAfterState1) {
			edge_runtime::detail::shared_store_seq_cst(
			        &block.state, static_cast<uint32_t>(SharedBufferBlockState::kFree));
			edge_runtime::detail::shared_store_seq_cst(
			        &block.state, static_cast<uint32_t>(SharedBufferBlockState::kWritingClaiming));
			edge_runtime::detail::shared_store_seq_cst(&block.owner_epoch, uint64_t{3});
			const BufferPoolOwnerAbi replacement = owner(22, 202, 2002, 20002);
			edge_runtime::detail::store_owner_seq_cst(&block.owner, replacement);
			replaced = true;
		} else if (replaced && point == OwnerSnapshotPoint::kAfterEpoch1) {
			edge_runtime::detail::shared_store_seq_cst(&block.owner_epoch, uint64_t{4});
			edge_runtime::detail::shared_store_seq_cst(
			        &block.state, static_cast<uint32_t>(SharedBufferBlockState::kWriting));
		}
	};

	const auto result = edge_runtime::detail::snapshot_owner_seq_cst<BufferPoolOwnerAbi>(
	        &block.state, &block.owner_epoch, &block.owner, classify, valid, hook);
	EXPECT_EQ(result.kind, OwnerSnapshotKind::kStable);
	EXPECT_EQ(result.owner.pid, 22u);
	EXPECT_EQ(result.owner.proc_start_ticks, 202u);
	EXPECT_EQ(result.owner.boot_id_hash_hi, 2002u);
	EXPECT_EQ(result.owner.boot_id_hash_lo, 20002u);
	EXPECT_EQ(result.epoch, 4u);
}

OwnerStateKind classify_mpmc_slot(uint32_t state) noexcept {
	switch (static_cast<edge_runtime::detail::MpmcSlotState>(state)) {
		case edge_runtime::detail::MpmcSlotState::kFree:
		case edge_runtime::detail::MpmcSlotState::kReady:
			return OwnerStateKind::kInactive;
		case edge_runtime::detail::MpmcSlotState::kEnqueueClaiming:
		case edge_runtime::detail::MpmcSlotState::kDequeueClaiming:
			return OwnerStateKind::kClaiming;
		case edge_runtime::detail::MpmcSlotState::kEnqueueOwned:
		case edge_runtime::detail::MpmcSlotState::kDequeueOwned:
			return OwnerStateKind::kOwned;
		case edge_runtime::detail::MpmcSlotState::kEpochExhausted:
			return OwnerStateKind::kEpochExhausted;
		case edge_runtime::detail::MpmcSlotState::kOwnerProtocolFault:
			return OwnerStateKind::kProtocolFault;
	}
	return OwnerStateKind::kUnexpected;
}

bool valid_mpmc_owner(const MpmcOwnerAbi& value) noexcept {
	return value.pid != 0 && value.proc_start_ticks != 0 && value.boot_id_hash_hi != 0 &&
	       value.boot_id_hash_lo != 0;
}

TEST(OwnerSnapshot, MpmcSlotUsesTheSameCompleteOwnerTupleProtocol) {
	MpmcSlotHeaderAbi slot{};
	edge_runtime::detail::shared_store_seq_cst(
	        &slot.state, static_cast<uint32_t>(edge_runtime::detail::MpmcSlotState::kEnqueueOwned));
	edge_runtime::detail::shared_store_seq_cst(&slot.owner_epoch, uint64_t{2});
	const MpmcOwnerAbi expected{33, 303, 3003, 30003};
	edge_runtime::detail::store_owner_seq_cst(&slot.owner, expected);
	const auto no_op = [](OwnerSnapshotPoint) noexcept {};

	const auto result = edge_runtime::detail::snapshot_owner_seq_cst<MpmcOwnerAbi>(
	        &slot.state, &slot.owner_epoch, &slot.owner, classify_mpmc_slot, valid_mpmc_owner,
	        no_op);
	EXPECT_EQ(result.kind, OwnerSnapshotKind::kStable);
	EXPECT_EQ(result.owner.pid, expected.pid);
	EXPECT_EQ(result.owner.proc_start_ticks, expected.proc_start_ticks);
	EXPECT_EQ(result.owner.boot_id_hash_hi, expected.boot_id_hash_hi);
	EXPECT_EQ(result.owner.boot_id_hash_lo, expected.boot_id_hash_lo);
}

TEST(OwnerSnapshot, ReplacementAfterEpochSnapshotIsClaimingContention) {
	uint32_t state = static_cast<uint32_t>(SharedBufferBlockState::kWriting);
	uint64_t epoch = 2;
	BufferPoolOwnerAbi shared_owner = owner(11, 101, 1001, 10001);
	bool replaced = false;
	const auto hook = [&](OwnerSnapshotPoint point) noexcept {
		if (!replaced && point == OwnerSnapshotPoint::kAfterEpoch1) {
			edge_runtime::detail::shared_store_seq_cst(
			        &state, static_cast<uint32_t>(SharedBufferBlockState::kWritingClaiming));
			edge_runtime::detail::shared_store_seq_cst(&epoch, uint64_t{3});
			edge_runtime::detail::store_owner_seq_cst(
			        &shared_owner, owner(22, 202, 2002, 20002));
			replaced = true;
		}
	};

	const auto result = edge_runtime::detail::snapshot_owner_seq_cst<BufferPoolOwnerAbi>(
	        &state, &epoch, &shared_owner, classify, valid, hook);
	EXPECT_EQ(result.kind, OwnerSnapshotKind::kClaiming);
}

TEST(OwnerSnapshot, OddEpochAfterStateSnapshotIsUnstableNotCorrupt) {
	uint32_t state = static_cast<uint32_t>(SharedBufferBlockState::kWriting);
	uint64_t epoch = 2;
	BufferPoolOwnerAbi shared_owner = owner(11, 101, 1001, 10001);
	bool changed = false;
	const auto hook = [&](OwnerSnapshotPoint point) noexcept {
		if (point == OwnerSnapshotPoint::kAfterState1 && !changed) {
			edge_runtime::detail::shared_store_seq_cst(&epoch, uint64_t{3});
			changed = true;
		}
	};

	const auto result = edge_runtime::detail::snapshot_owner_seq_cst<BufferPoolOwnerAbi>(
	        &state, &epoch, &shared_owner, classify, valid, hook);
	EXPECT_EQ(result.kind, OwnerSnapshotKind::kUnstable);
}

TEST(OwnerSnapshot, StableInactiveInvalidEpochIsCorrupt) {
	for (const uint64_t epoch : {uint64_t{3}, UINT64_MAX}) {
		uint32_t state = static_cast<uint32_t>(SharedBufferBlockState::kFree);
		BufferPoolOwnerAbi shared_owner{};
		const auto no_op = [](OwnerSnapshotPoint) noexcept {};
		const auto result = edge_runtime::detail::snapshot_owner_seq_cst<BufferPoolOwnerAbi>(
				&state, &epoch, &shared_owner, classify, valid, no_op);
		EXPECT_EQ(result.kind, OwnerSnapshotKind::kCorrupt);
		EXPECT_EQ(result.epoch, epoch);
	}
}

TEST(OwnerSnapshot, EpochCapacityReservesWriterAndReaderWithoutWrap) {
	uint64_t epoch = UINT64_MAX - 5u;
	EXPECT_EQ(edge_runtime::detail::owner_epoch_capacity(&epoch, 4u),
	          edge_runtime::detail::OwnerCommitResult::kReady);

	uint64_t writer_epoch = 0;
	EXPECT_EQ(edge_runtime::detail::begin_owner_commit(&epoch, 4u, &writer_epoch),
	          edge_runtime::detail::OwnerCommitResult::kReady);
	EXPECT_EQ(writer_epoch, UINT64_MAX - 3u);
	edge_runtime::detail::shared_store_seq_cst(&epoch, writer_epoch);

	EXPECT_EQ(edge_runtime::detail::owner_epoch_capacity(&epoch, 2u),
	          edge_runtime::detail::OwnerCommitResult::kReady);
	uint64_t reader_epoch = 0;
	EXPECT_EQ(edge_runtime::detail::begin_owner_commit(&epoch, 2u, &reader_epoch),
	          edge_runtime::detail::OwnerCommitResult::kReady);
	EXPECT_EQ(reader_epoch, UINT64_MAX - 1u);
	edge_runtime::detail::shared_store_seq_cst(&epoch, reader_epoch);
	EXPECT_EQ(edge_runtime::detail::owner_epoch_capacity(&epoch, 2u),
	          edge_runtime::detail::OwnerCommitResult::kEpochExhausted);
}

TEST(OwnerSnapshot, WriterStopsWhenOnlyReaderCapacityRemains) {
	uint64_t epoch = UINT64_MAX - 3u;
	EXPECT_EQ(edge_runtime::detail::owner_epoch_capacity(&epoch, 4u),
	          edge_runtime::detail::OwnerCommitResult::kEpochExhausted);
	EXPECT_EQ(edge_runtime::detail::owner_epoch_capacity(&epoch, 2u),
	          edge_runtime::detail::OwnerCommitResult::kReady);
	uint64_t committed = 0;
	EXPECT_EQ(edge_runtime::detail::begin_owner_commit(&epoch, 4u, &committed),
	          edge_runtime::detail::OwnerCommitResult::kEpochExhausted);
	EXPECT_EQ(epoch, UINT64_MAX - 3u);
}

TEST(OwnerSnapshot, OddEpochIsProtocolFaultForCapacityChecks) {
	uint64_t epoch = UINT64_MAX - 4u;
	EXPECT_EQ(edge_runtime::detail::owner_epoch_capacity(&epoch, 2u),
	          edge_runtime::detail::OwnerCommitResult::kCorrupt);
}

}  // namespace
