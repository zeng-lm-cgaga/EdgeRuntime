#ifndef EDGE_RUNTIME_DETAIL_CHANNEL_OBSERVER_HPP
#define EDGE_RUNTIME_DETAIL_CHANNEL_OBSERVER_HPP

// v0.3 §35.3: a payload-agnostic, strictly READ-ONLY, lock-free view of a
// channel for the ProducerSupervisor (and edge_shm_ctl). The observer never
// takes the ControlLock — a long/periodic hold would deadlock the producer's
// own create transaction (flock) — and never writes to the mapping
// (PROT_READ). The view binds to ONE instance: unlink/new-inode or a new
// broker fd makes an old view permanently stale, so callers must rebuild the
// view after every (re)spawn of a supervised producer.

#include <cstddef>
#include <cstdint>
#include <string>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/result.hpp"

namespace edge_runtime::detail {

struct ChannelObserverView {
	std::string channel_name;
	Transport transport{Transport::kPosixShm};
	UniqueFd fd;
	MappedRegion mapping;  // PROT_READ only
	uint64_t dev = 0;
	uint64_t ino = 0;
	uint64_t size = 0;
};

// Open (with a bounded retry budget covering "not yet created / mid-create /
// broker not ready") and validate the structural envelope: bootstrap +
// validate_header_shape. Schema-free by design.
Result<ChannelObserverView> open_channel_readonly(const std::string& channel_name,
                                                  Transport transport,
                                                  uint64_t retry_ms) noexcept;

// Header pointer inside an open view (base + kChannelHeaderOffset).
inline ChannelHeaderAbi* observer_header(ChannelObserverView& view) noexcept {
	return reinterpret_cast<ChannelHeaderAbi*>(
	        static_cast<std::byte*>(view.mapping.get()) + kChannelHeaderOffset);
}

// §35.3 stall classification, the same field rules as §34.3 but against an
// EXPECTED producer identity (the supervised child) instead of an arbitrary
// probe: identity mismatch means "not our child" and must never trigger a
// kill.
enum class StallClass : uint8_t {
	kNotReady = 0,      // init_state != READY: nothing to judge yet
	kIdentityMismatch,  // header producer != expected pid/starttime: never kill
	kNotApplicable,     // heartbeat disabled (interval == 0)
	kFresh,             // publish fresh or heartbeat within 3x interval
	kStalled,           // heartbeat enabled, stale beyond 3x interval, no fresh publish
};

StallClass classify_stall(const ChannelHeaderAbi* header, uint64_t now_boot_ns,
                          uint64_t expected_pid, uint64_t expected_start_ticks) noexcept;

uint64_t observer_generation(const ChannelHeaderAbi* header) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CHANNEL_OBSERVER_HPP
