#ifndef EDGE_RUNTIME_DETAIL_SLOT_PROTOCOL_HPP
#define EDGE_RUNTIME_DETAIL_SLOT_PROTOCOL_HPP

#include <cstdint>

#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/shared_atomic.hpp"

namespace edge_runtime::detail {

// 槽状态值是跨进程协议的一部分，修改顺序会破坏已有共享内存对象。
enum class SlotState : uint32_t {
	kFree = 0,
	kWriting = 1,
	kPublished = 2,
	kReadingClaiming = 3,
	kReading = 4,
};

bool checked_next_sequence(uint64_t current_ticket, uint64_t* out) noexcept;

uint64_t saturated_gap(uint64_t last_sequence, uint64_t current_sequence) noexcept;

bool slot_claim_writable(SlotHeaderAbi* slot, uint32_t observed_state) noexcept;

void slot_publish(SlotHeaderAbi* slot) noexcept;

void slot_abort_write(SlotHeaderAbi* slot) noexcept;

bool slot_claim_readable(SlotHeaderAbi* slot) noexcept;

void slot_mark_reading(SlotHeaderAbi* slot, uint64_t consumer_role_epoch) noexcept;

void slot_release_read(SlotHeaderAbi* slot) noexcept;

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_SLOT_PROTOCOL_HPP
