#ifndef EDGE_RUNTIME_DETAIL_OWNER_SNAPSHOT_HPP
#define EDGE_RUNTIME_DETAIL_OWNER_SNAPSHOT_HPP

#include <cstdint>

#include "edge_runtime/sync/shared_atomic.hpp"

namespace edge_runtime::detail {

enum class OwnerSnapshotKind {
	kInactive,
	kClaiming,
	kStable,
	kUnstable,
	kCorrupt,
	kEpochExhausted,
};

enum class OwnerStateKind {
	kInactive,
	kClaiming,
	kOwned,
	kEpochExhausted,
	kProtocolFault,
	kUnexpected,
};

enum class OwnerSnapshotPoint {
	kAfterState1,
	kAfterEpoch1,
};

template <typename Owner>
struct OwnerSnapshot {
	OwnerSnapshotKind kind{OwnerSnapshotKind::kCorrupt};
	uint32_t state{0};
	Owner owner{};
	uint64_t epoch{0};
};

template <typename Owner>
Owner load_owner_seq_cst(const Owner* owner) noexcept {
	return {shared_load_seq_cst(&owner->pid),
	        shared_load_seq_cst(&owner->proc_start_ticks),
	        shared_load_seq_cst(&owner->boot_id_hash_hi),
	        shared_load_seq_cst(&owner->boot_id_hash_lo)};
}

template <typename Owner>
void store_owner_seq_cst(Owner* owner, const Owner& value) noexcept {
	shared_store_seq_cst(&owner->pid, value.pid);
	shared_store_seq_cst(&owner->proc_start_ticks, value.proc_start_ticks);
	shared_store_seq_cst(&owner->boot_id_hash_hi, value.boot_id_hash_hi);
	shared_store_seq_cst(&owner->boot_id_hash_lo, value.boot_id_hash_lo);
}

constexpr unsigned kOwnerSnapshotRetries = 4;
constexpr uint64_t kMaxCommittedOwnerEpoch = UINT64_MAX - 1u;

enum class OwnerCommitResult {
	kReady,
	kEpochExhausted,
	kCorrupt,
};

inline OwnerCommitResult begin_owner_commit(uint64_t* epoch, uint64_t reserve_after_commit,
                                            uint64_t* committed_epoch) noexcept {
	const uint64_t current = shared_load_seq_cst(epoch);
	if ((current & 1u) != 0u || current > kMaxCommittedOwnerEpoch) {
		return OwnerCommitResult::kCorrupt;
	}
	if (reserve_after_commit > kMaxCommittedOwnerEpoch - current) {
		return OwnerCommitResult::kEpochExhausted;
	}
	if (!shared_cas_seq_cst(epoch, current, current + 1u)) {
		return OwnerCommitResult::kCorrupt;
	}
	*committed_epoch = current + 2u;
	return OwnerCommitResult::kReady;
}

inline OwnerCommitResult owner_epoch_capacity(const uint64_t* epoch,
                                              uint64_t reserve_after_commit) noexcept {
	const uint64_t current = shared_load_seq_cst(epoch);
	if ((current & 1u) != 0u || current > kMaxCommittedOwnerEpoch) {
		return OwnerCommitResult::kCorrupt;
	}
	return reserve_after_commit <= kMaxCommittedOwnerEpoch - current
	               ? OwnerCommitResult::kReady
	               : OwnerCommitResult::kEpochExhausted;
}

template <typename Owner, typename Classifier, typename Validator, typename Hook>
OwnerSnapshot<Owner> snapshot_owner_seq_cst(const uint32_t* state, const uint64_t* epoch,
                                             const Owner* owner, Classifier classify,
                                             Validator valid, Hook hook) noexcept {
	for (unsigned attempt = 0; attempt < kOwnerSnapshotRetries; ++attempt) {
		const uint32_t state_before = shared_load_seq_cst(state);
		const OwnerStateKind state_kind = classify(state_before);
		switch (state_kind) {
			case OwnerStateKind::kInactive: {
				hook(OwnerSnapshotPoint::kAfterState1);
				const uint64_t epoch_before = shared_load_seq_cst(epoch);
				hook(OwnerSnapshotPoint::kAfterEpoch1);
				const uint32_t state_after = shared_load_seq_cst(state);
				const uint64_t epoch_after = shared_load_seq_cst(epoch);
				const OwnerStateKind state_after_kind = classify(state_after);
				const bool stable_inactive =
						state_after == state_before && epoch_before == epoch_after;
				if (stable_inactive) {
					if ((epoch_after & 1u) != 0u || epoch_after > kMaxCommittedOwnerEpoch) {
						return {OwnerSnapshotKind::kCorrupt, state_after, {}, epoch_after};
					}
					return {OwnerSnapshotKind::kInactive, state_after, {}, epoch_after};
				}
				if (state_after_kind == OwnerStateKind::kClaiming) {
					return {OwnerSnapshotKind::kClaiming, state_after, {}, epoch_after};
				}
				if (state_after_kind == OwnerStateKind::kEpochExhausted) {
					return {OwnerSnapshotKind::kEpochExhausted, state_after, {}, epoch_after};
				}
				const bool invalid_state = state_after_kind == OwnerStateKind::kProtocolFault ||
						state_after_kind == OwnerStateKind::kUnexpected;
				if (invalid_state) {
					return {OwnerSnapshotKind::kCorrupt, state_after, {}, epoch_after};
				}
				continue;
			}
			case OwnerStateKind::kClaiming:
				return {OwnerSnapshotKind::kClaiming, state_before, {},
				        shared_load_seq_cst(epoch)};
			case OwnerStateKind::kEpochExhausted:
				return {OwnerSnapshotKind::kEpochExhausted, state_before, {},
				        shared_load_seq_cst(epoch)};
			case OwnerStateKind::kProtocolFault:
			case OwnerStateKind::kUnexpected:
				return {OwnerSnapshotKind::kCorrupt, state_before, {},
				        shared_load_seq_cst(epoch)};
			case OwnerStateKind::kOwned:
				break;
		}

		hook(OwnerSnapshotPoint::kAfterState1);
		const uint64_t epoch_before = shared_load_seq_cst(epoch);
		hook(OwnerSnapshotPoint::kAfterEpoch1);
		if ((epoch_before & 1u) != 0u) continue;
		const Owner candidate = load_owner_seq_cst(owner);
		const uint64_t epoch_after_fields = shared_load_seq_cst(epoch);
		const uint32_t state_after = shared_load_seq_cst(state);
		const uint64_t epoch_after_state = shared_load_seq_cst(epoch);
		const OwnerStateKind state_after_kind = classify(state_after);
		if (state_after_kind == OwnerStateKind::kOwned &&
		    epoch_before == epoch_after_fields && epoch_after_fields == epoch_after_state &&
		    (epoch_after_state & 1u) == 0u) {
			if (!valid(candidate)) {
				return {OwnerSnapshotKind::kCorrupt, state_after, candidate, epoch_after_state};
			}
			return {OwnerSnapshotKind::kStable, state_after, candidate, epoch_after_state};
		}
		if (state_after_kind == OwnerStateKind::kEpochExhausted) {
			return {OwnerSnapshotKind::kEpochExhausted, state_after, {}, epoch_after_state};
		}
		if (state_after_kind == OwnerStateKind::kProtocolFault ||
		    state_after_kind == OwnerStateKind::kUnexpected) {
			return {OwnerSnapshotKind::kCorrupt, state_after, {}, epoch_after_state};
		}
	}
	const uint32_t final_state = shared_load_seq_cst(state);
	const OwnerStateKind final_kind = classify(final_state);
	if (final_kind == OwnerStateKind::kEpochExhausted) {
		return {OwnerSnapshotKind::kEpochExhausted, final_state, {},
		        shared_load_seq_cst(epoch)};
	}
	if (final_kind == OwnerStateKind::kProtocolFault ||
	    final_kind == OwnerStateKind::kUnexpected) {
		return {OwnerSnapshotKind::kCorrupt, final_state, {}, shared_load_seq_cst(epoch)};
	}
	return {OwnerSnapshotKind::kUnstable, final_state, {}, shared_load_seq_cst(epoch)};
}

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_OWNER_SNAPSHOT_HPP
