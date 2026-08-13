#include "edge_runtime/detail/channel_observer.hpp"

#include <cerrno>
#include <cstring>

#include "edge_runtime/detail/channel_abi.hpp"
#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/clock.hpp"
#include "edge_runtime/detail/fd_broker.hpp"
#include "edge_runtime/detail/shared_atomic.hpp"

namespace edge_runtime::detail {

namespace {

inline constexpr uint64_t kRetryIntervalNs = 10'000'000;  // 10 ms

// One open attempt without retry; the retry loop below wraps it.
Result<ChannelObserverView> open_once(const std::string& channel_name, Transport transport) {
	ChannelObserverView view;
	view.channel_name = channel_name;
	view.transport = transport;

	UniqueFd fd;
	uint64_t dev = 0, ino = 0, size = 0;
	if (transport == Transport::kMemfdFdPass) {
		// v0.2 §33.6: broker readonly fd; all-zero fingerprint = no schema
		// filter (the observer cannot know the schema).
		const uint32_t hash =
		        channel_name_hash(channel_name.c_str(), channel_name.size());
		SchemaDescriptor any{};
		FdBrokerReplyAbi reply{};
		auto rf = fd_broker_request_fd(channel_socket_path(channel_name), hash, any,
		                               /*readonly=*/true, &reply, 0);
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

	// Bootstrap envelope, same sequence as §9.2 but schema-free.
	BootstrapHeaderAbi boot{};
	auto rboot = pread_full(fd.get(), &boot, sizeof(boot), 0);
	if (!rboot) return rboot.error();
	auto vboot = validate_bootstrap_parse(boot, size);
	if (!vboot) return vboot.error();
	if (boot.expected_mapping_size != size) {
		return make_error(ErrorCode::kInitializationIncomplete, "open_channel_readonly",
		                  "mapping size mismatch (mid-create?)");
	}

	auto mm = mmap_region_readonly(fd, size);
	if (!mm) return mm.error();
	view.mapping = std::move(mm.value());

	auto* header = observer_header(view);
	auto vshape = validate_header_shape(*header);
	if (!vshape) return vshape.error();

	view.fd = std::move(fd);
	view.dev = dev;
	view.ino = ino;
	view.size = size;
	return Result<ChannelObserverView>(std::move(view));
}

}  // namespace

Result<ChannelObserverView> open_channel_readonly(const std::string& channel_name,
                                                  Transport transport,
                                                  uint64_t retry_ms) noexcept {
	const uint64_t deadline = monotonic_deadline_ns(retry_ms * 1'000'000ull);
	if (deadline == 0) {
		return make_error(ErrorCode::kClockAnomaly, "open_channel_readonly",
		                  "monotonic clock unavailable");
	}
	// Retryable "not ready yet" family (design §35.3): absent object, mid-create
	// bootstrap, unreachable/not-ready broker.
	const auto retryable = [](ErrorCode code) {
		return code == ErrorCode::kNotFound ||
		       code == ErrorCode::kInitializationIncomplete ||
		       code == ErrorCode::kProducerOffline;
	};
	Error last_error(ErrorCode::kSystemError, "open_channel_readonly");
	for (;;) {
		auto view = open_once(channel_name, transport);
		if (view) return view;
		last_error = view.error();
		if (!retryable(last_error.code)) return view.error();
		if (remaining_time_ns(deadline) == 0) {
			return make_error(ErrorCode::kTimeout, "open_channel_readonly",
			                  "retry budget exhausted");
		}
		const struct timespec ts {0, static_cast<long>(kRetryIntervalNs)};
		(void)::nanosleep(&ts, nullptr);
	}
}

StallClass classify_stall(const ChannelHeaderAbi* header, uint64_t now_boot_ns,
                          uint64_t expected_pid, uint64_t expected_start_ticks) noexcept {
	// §35.3: judge only instances that belong to OUR supervised child.
	if (shared_load_acquire(&header->init_state) !=
	    static_cast<uint32_t>(InitState::kReady)) {
		return StallClass::kNotReady;
	}
	auto pid_res = identity_snapshot_read(
	        const_cast<ProcessIdentityAbi*>(&header->producer));
	if (!pid_res) return StallClass::kIdentityMismatch;
	const ProcessIdentityAbi& p = pid_res.value();
	if (p.pid != expected_pid || p.proc_start_ticks != expected_start_ticks) {
		return StallClass::kIdentityMismatch;
	}
	// §34.3 rules, in order, against the frozen identity above.
	const uint64_t interval =
	        shared_load_acquire(&header->producer_heartbeat_interval_ns);
	if (interval == 0) return StallClass::kNotApplicable;
	const uint64_t last_publish = shared_load_acquire(&header->last_publish_boot_ns);
	const uint64_t last_beat = shared_load_acquire(&header->heartbeat_boot_ns);
	if (now_boot_ns == 0) return StallClass::kNotApplicable;  // clock down: never kill
	if (last_publish != 0 && now_boot_ns - last_publish <= interval) {
		return StallClass::kFresh;
	}
	if (last_beat == 0 || now_boot_ns - last_beat > interval * kHeartbeatStallFactor) {
		return StallClass::kStalled;
	}
	return StallClass::kFresh;
}

uint64_t observer_generation(const ChannelHeaderAbi* header) noexcept {
	return shared_load_acquire(&header->generation);
}

}  // namespace edge_runtime::detail
