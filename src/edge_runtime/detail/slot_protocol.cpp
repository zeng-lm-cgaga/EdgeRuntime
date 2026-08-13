#include "edge_runtime/detail/slot_protocol.hpp"

namespace edge_runtime::detail {

bool checked_next_sequence(uint64_t current_ticket, uint64_t* out) noexcept {
	const uint64_t current_seq = ticket_sequence(current_ticket);
	if (current_seq >= kMaxSampleSequence) return false;  // no wrap to 0
	*out = current_seq + 1;
	return true;
}

uint64_t saturated_gap(uint64_t last_sequence, uint64_t current_sequence) noexcept {
	if (current_sequence <= last_sequence) return 0;  // defensive: same/older
	if (current_sequence - last_sequence >= UINT64_MAX) return UINT64_MAX;
	return current_sequence - last_sequence;
}

bool slot_claim_writable(SlotHeaderAbi* slot, uint32_t observed_state) noexcept {
	return shared_cas_acquire(&slot->state, observed_state,
	                          static_cast<uint32_t>(SlotState::kWriting));
}

void slot_publish(SlotHeaderAbi* slot) noexcept {
	shared_store_release(&slot->state, static_cast<uint32_t>(SlotState::kPublished));
}

void slot_abort_write(SlotHeaderAbi* slot) noexcept {
	// A failed write never leaves metadata/payload readable as PUBLISHED; FREE
	// lets the producer reclaim the slot wholesale (design §11.3).
	shared_store_relaxed(&slot->state, static_cast<uint32_t>(SlotState::kFree));
}

bool slot_claim_readable(SlotHeaderAbi* slot) noexcept {
	return shared_cas_acquire(&slot->state, static_cast<uint32_t>(SlotState::kPublished),
	                          static_cast<uint32_t>(SlotState::kReadingClaiming));
}

void slot_mark_reading(SlotHeaderAbi* slot, uint64_t consumer_role_epoch) noexcept {
	shared_store_relaxed(&slot->reader_role_epoch, consumer_role_epoch);
	shared_store_release(&slot->state, static_cast<uint32_t>(SlotState::kReading));
}

void slot_release_read(SlotHeaderAbi* slot) noexcept {
	shared_store_relaxed(&slot->reader_role_epoch, uint64_t{0});
	shared_store_release(&slot->state, static_cast<uint32_t>(SlotState::kPublished));
}

}  // namespace edge_runtime::detail
