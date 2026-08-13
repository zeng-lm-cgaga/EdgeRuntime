#ifndef EDGE_RUNTIME_DETAIL_SLOT_PROTOCOL_HPP
#define EDGE_RUNTIME_DETAIL_SLOT_PROTOCOL_HPP

#include <cstdint>

#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/shared_atomic.hpp"

namespace edge_runtime::detail {

// Slot ownership states (design §10.2). Frozen values; do not reorder.
enum class SlotState : uint32_t {
	kFree = 0,
	kWriting = 1,
	kPublished = 2,
	kReadingClaiming = 3,
	kReading = 4,
};

// Next sample sequence from the current latest ticket. Ticket 0 (nothing
// published yet) yields sequence 1; otherwise current+1. Returns false when
// the sequence would exceed kMaxSampleSequence (design §10.3, U07).
bool checked_next_sequence(uint64_t current_ticket, uint64_t* out) noexcept;

// Missed-sample gap with saturation (design §12.1, U07): the delta between two
// sample sequences, clamped to UINT64_MAX on overflow rather than wrapping.
uint64_t saturated_gap(uint64_t last_sequence, uint64_t current_sequence) noexcept;

// Producer: CAS (FREE|PUBLISHED) -> WRITING, acquire on success. Returns true
// when the slot was claimed. observed must be a recent relaxed read.
bool slot_claim_writable(SlotHeaderAbi* slot, uint32_t observed_state) noexcept;

// Producer: publish the WRITING slot as PUBLISHED (release).
void slot_publish(SlotHeaderAbi* slot) noexcept;

// Producer: roll back a WRITING slot to FREE (design §11.3) after a failed
// mid-write. Never publishes a partially-written slot.
void slot_abort_write(SlotHeaderAbi* slot) noexcept;

// Consumer: CAS PUBLISHED -> READING_CLAIMING (acquire). Returns true when the
// slot was claimed.
bool slot_claim_readable(SlotHeaderAbi* slot) noexcept;

// Consumer: second phase of the claim — record the reader role epoch, then
// store_release state -> READING (design §10.2).
void slot_mark_reading(SlotHeaderAbi* slot, uint64_t consumer_role_epoch) noexcept;

// The single read-path release (design §10.2): clear the reader role epoch
// (relaxed) then store_release state -> PUBLISHED. After this call the caller
// must not touch the slot again.
void slot_release_read(SlotHeaderAbi* slot) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_SLOT_PROTOCOL_HPP
