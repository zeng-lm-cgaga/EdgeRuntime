#include "edge_runtime/channel/channel_observer.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "edge_runtime/channel/channel_abi.hpp"
#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "edge_runtime/transport/fd_broker.hpp"
#include "edge_runtime/utility/checked_math.hpp"

namespace edge_runtime::detail {

namespace {

inline constexpr uint64_t kRetryIntervalNs = 10'000'000;

// 观察路径只读打开对象；fd 传递和 POSIX 名称两种方式最终统一为只读映射。
Result<ChannelObserverView> open_once(const std::string& channel_name, Transport transport,
                                      uint64_t deadline_ns, bool allow_retry) {
	ChannelObserverView view;
	view.channel_name = channel_name;
	view.transport = transport;

	UniqueFd fd;
	FdBrokerReplyAbi reply{};
	uint64_t dev = 0, ino = 0, size = 0;
	if (transport == Transport::kMemfdFdPass) {
		const uint32_t hash = channel_name_hash(channel_name.c_str(), channel_name.size());
		SchemaDescriptor any{};
		auto rf = fd_broker_request_fd_until(channel_socket_path(channel_name), hash, any,
		                                     true, &reply, deadline_ns, allow_retry);
		if (!rf) return rf.error();
		fd = std::move(rf.value());
		auto fst = memfd_fstat_and_capture(fd, &dev, &ino, &size);
		if (!fst) return fst.error();
	} else {
		auto fo = shm_open_existing(channel_shm_name(channel_name));
		if (!fo) return fo.error();
		fd = std::move(fo.value());
		auto fst = shm_fstat_and_capture(fd, &dev, &ino, &size);
		if (!fst) return fst.error();
	}

	BootstrapHeaderAbi boot{};
	auto rboot = pread_full(fd.get(), &boot, sizeof(boot), 0);
	if (!rboot) return rboot.error();
	auto vboot = validate_bootstrap_parse(boot, size);
	if (!vboot) return vboot.error();
	if (boot.expected_mapping_size != size) {
		return make_error(ErrorCode::kInitializationIncomplete, "open_channel_readonly",
		                  "mapping size mismatch (mid-create?)");
	}
	if (boot.init_state != static_cast<uint32_t>(InitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, "open_channel_readonly",
		                  "bootstrap not ready");
	}
	if (size < kChannelHeaderOffset + sizeof(ChannelHeaderAbi)) {
		return make_error(ErrorCode::kCorruptHeader, "open_channel_readonly",
		                  "mapping below header size");
	}

	auto mm = mmap_region_readonly(fd, size);
	if (!mm) return mm.error();
	view.mapping = std::move(mm.value());

	const auto* mapped_boot = static_cast<const BootstrapHeaderAbi*>(view.mapping.get());
	auto* header = observer_header(view);
	if (shared_load_acquire(&mapped_boot->init_state) !=
	            static_cast<uint32_t>(InitState::kReady) ||
	    shared_load_acquire(&header->init_state) != static_cast<uint32_t>(InitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, "open_channel_readonly",
		                  "mapped bootstrap/header not ready");
	}
	auto mapped_boot_valid = validate_bootstrap_parse(*mapped_boot, size);
	if (!mapped_boot_valid) return mapped_boot_valid.error();
	auto vshape = validate_header_shape(*header);
	if (!vshape) return vshape.error();
	if (header->mapping_size != size || mapped_boot->expected_mapping_size != size ||
	    header->abi_major != mapped_boot->abi_major ||
	    header->abi_minor != mapped_boot->abi_minor ||
	    (header->instance_nonce_hi == 0 && header->instance_nonce_lo == 0) ||
	    header->instance_nonce_hi != mapped_boot->creator_nonce_hi ||
	    header->instance_nonce_lo != mapped_boot->creator_nonce_lo ||
	    boot.creator_nonce_hi != mapped_boot->creator_nonce_hi ||
	    boot.creator_nonce_lo != mapped_boot->creator_nonce_lo) {
		return make_error(ErrorCode::kCorruptHeader, "open_channel_readonly",
		                  "bootstrap/header instance disagreement");
	}
	if (transport == Transport::kMemfdFdPass &&
	    (reply.generation != header->generation ||
	     reply.nonce_hi != header->instance_nonce_hi ||
	     reply.nonce_lo != header->instance_nonce_lo ||
	     reply.mapping_size != header->mapping_size || reply.abi_major != header->abi_major ||
	     reply.abi_minor != header->abi_minor)) {
		return make_error(ErrorCode::kTransportFailed, "open_channel_readonly",
		                  "broker reply disagrees with mapped instance");
	}

	view.fd = std::move(fd);
	view.dev = dev;
	view.ino = ino;
	view.size = size;
	return Result<ChannelObserverView>(std::move(view));
}

}

Result<ChannelObserverView> open_channel_readonly(const std::string& channel_name,
                                                  Transport transport, uint64_t retry_ms) noexcept {
	uint64_t retry_ns = 0;
	if (!checked_mul_u64(retry_ms, 1'000'000ull, &retry_ns)) {
		return make_error(ErrorCode::kInvalidOptions, "open_channel_readonly",
		                  "retry timeout out of range");
	}
	if (transport == Transport::kMemfdFdPass && retry_ms == 0)
		retry_ns = kFdBrokerSingleAttemptTimeoutMs * 1'000'000;
	const uint64_t deadline = monotonic_deadline_ns(retry_ns);
	if (deadline == 0) {
		return make_error(ErrorCode::kClockAnomaly, "open_channel_readonly",
		                  "monotonic clock unavailable");
	}

	const auto retryable = [](ErrorCode code) {
		return code == ErrorCode::kNotFound ||
		       code == ErrorCode::kInitializationIncomplete ||
		       code == ErrorCode::kProducerOffline;
	};
	Error last_error(ErrorCode::kSystemError, "open_channel_readonly");
	for (;;) {
		if (retry_ms != 0 && remaining_time_ns(deadline) == 0) {
			return make_error(ErrorCode::kTimeout, "open_channel_readonly",
			                  "retry budget exhausted");
		}
		auto view = open_once(channel_name, transport, deadline, retry_ms != 0);
		if (view) {
			if (retry_ms != 0 && remaining_time_ns(deadline) == 0)
				return make_error(ErrorCode::kTimeout, "open_channel_readonly",
				                  "retry budget exhausted");
			return view;
		}
		last_error = view.error();
		if (!retryable(last_error.code)) return view.error();
		const uint64_t remaining = remaining_time_ns(deadline);
		if (retry_ms == 0 || remaining == 0) {
			return make_error(ErrorCode::kTimeout, "open_channel_readonly",
			                  "retry budget exhausted");
		}
		const struct timespec ts {
			0, static_cast<long>(std::min(remaining, kRetryIntervalNs))
		};
		(void)::nanosleep(&ts, nullptr);
	}
}

StallClass classify_stall(const ChannelHeaderAbi* header, uint64_t now_boot_ns,
                          uint64_t expected_pid, uint64_t expected_start_ticks) noexcept {
	if (shared_load_acquire(&header->init_state) != static_cast<uint32_t>(InitState::kReady)) {
		return StallClass::kNotReady;
	}
	auto pid_res = identity_snapshot_read(const_cast<ProcessIdentityAbi*>(&header->producer));
	if (!pid_res) return StallClass::kIdentityMismatch;
	const ProcessIdentityAbi& p = pid_res.value();
	if (p.pid != expected_pid || p.proc_start_ticks != expected_start_ticks) {
		return StallClass::kIdentityMismatch;
	}

	const uint64_t interval = shared_load_acquire(&header->producer_heartbeat_interval_ns);
	if (interval == 0) return StallClass::kNotApplicable;
	const uint64_t last_publish = shared_load_acquire(&header->last_publish_boot_ns);
	const uint64_t last_beat = shared_load_acquire(&header->heartbeat_boot_ns);
	if (now_boot_ns == 0) return StallClass::kNotApplicable;
	if (last_publish != 0 && !elapsed_exceeds(now_boot_ns, last_publish, interval)) {
		return StallClass::kFresh;
	}
	const uint64_t stall_limit = saturating_mul_u64(interval, kHeartbeatStallFactor);
	if (last_beat == 0 || elapsed_exceeds(now_boot_ns, last_beat, stall_limit)) {
		return StallClass::kStalled;
	}
	return StallClass::kFresh;
}

uint64_t observer_generation(const ChannelHeaderAbi* header) noexcept {
	return shared_load_acquire(&header->generation);
}

}
