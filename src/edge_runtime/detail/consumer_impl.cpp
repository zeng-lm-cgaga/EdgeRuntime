#include "edge_runtime/detail/consumer_impl.hpp"

#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <ctime>
#include <utility>

#include "edge_runtime/detail/channel_abi.hpp"
#include "edge_runtime/detail/checksum.hpp"
#include "edge_runtime/detail/clock.hpp"
#include "edge_runtime/detail/failpoint.hpp"
#include "edge_runtime/detail/fd_broker.hpp"
#include "edge_runtime/detail/futex.hpp"
#include "edge_runtime/detail/slot_protocol.hpp"

namespace edge_runtime::detail {

namespace {
// ER4: under the control lock, reclaim slots left READING/READING_CLAIMING by a
// PROVEN-DEAD consumer (design §15.4/§10.2). The old role epoch decides:
// READING only when its epoch equals the old epoch; READING_CLAIMING when its
// epoch is 0 or equals the old epoch (the control lock + single-reader
// invariant prove no other legitimate reader exists). Any unknown epoch is
// corruption — fail closed, never guess-repair.
Result<void> reclaim_dead_consumer_slots(std::byte* base, uint64_t old_role_epoch,
                                         uint32_t payload_size) noexcept {
	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(kSlotHeaderSize + payload_size, 64, &stride)) {
		return make_error(ErrorCode::kInvalidOptions, "Consumer::open", "stride overflow");
	}
	for (uint32_t i = 0; i < kSlotCount; ++i) {
		uint64_t offset = 0;
		if (!slot_byte_offset(i, stride, &offset)) {
			return make_error(ErrorCode::kCorruptSlot, "Consumer::open",
			                  "slot offset overflow");
		}
		auto* slot = reinterpret_cast<SlotHeaderAbi*>(base + offset);
		const uint32_t st = shared_load_relaxed(&slot->state);
		if (st == static_cast<uint32_t>(SlotState::kReadingClaiming)) {
			const uint64_t epoch = shared_load_relaxed(&slot->reader_role_epoch);
			if (epoch != 0 && epoch != old_role_epoch) {
				return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
				                  "claiming slot epoch unknown");
			}
			slot_release_read(slot);
		} else if (st == static_cast<uint32_t>(SlotState::kReading)) {
			const uint64_t epoch = shared_load_relaxed(&slot->reader_role_epoch);
			if (epoch != old_role_epoch) {
				return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
				                  "reading slot epoch unknown");
			}
			slot_release_read(slot);
		}
		// FREE / WRITING / PUBLISHED are not reader-owned and are left alone
		// (WRITING belongs to the producer, mid-write).
	}
	return Result<void>::ok();
}
}  // namespace

Result<std::shared_ptr<ConsumerHandle>> consumer_open_impl(const ChannelOptions& options,
                                                           const SchemaDescriptor& schema,
                                                           uint32_t payload_size) {
	if (options.name.empty() || options.name.size() > kMaxChannelNameLen ||
	    !validate_channel_name(options.name.c_str(), options.name.size())) {
		return make_error(ErrorCode::kInvalidName, "Consumer::open", options.name.c_str());
	}
	if (payload_size == 0 || payload_size > kMaxPayloadSize) {
		return make_error(ErrorCode::kInvalidOptions, "Consumer::open",
		                  "payload size out of range");
	}
	bool fingerprint_set = false;
	for (const std::byte b : schema.fingerprint) {
		if (b != std::byte{0}) {
			fingerprint_set = true;
			break;
		}
	}
	if (!fingerprint_set) {
		return make_error(ErrorCode::kInvalidOptions, "Consumer::open",
		                  "schema fingerprint unset");
	}

	const std::string shm_name = channel_shm_name(options.name);
	const std::string lock_path = channel_lock_path(options.name);
	const std::string socket_path = channel_socket_path(options.name);
	const uint32_t name_hash = channel_name_hash(options.name.c_str(), options.name.size());
	const bool fd_mode = options.transport == Transport::kMemfdFdPass;

	// ---- open + fstat ----------------------------------------------------------
	// The transports are distinguished at object acquisition only (v0.2 §33.2):
	// everything downstream — bootstrap/header validation, slot protocol, control
	// lock — is byte-identical for both.
	UniqueFd fd;
	uint64_t dev = 0, ino = 0, size = 0;
	FdBrokerReplyAbi fd_reply{};
	if (fd_mode) {
		// Receive the one-shot fd from the broker (bounded retry, §33.6).
		auto rf = fd_broker_request_fd(socket_path, name_hash, schema, /*readonly=*/false,
		                               &fd_reply,
		                               static_cast<uint64_t>(
		                                       options.reconnect_timeout.count()));
		if (!rf) return rf.error();
		fd = std::move(rf.value());
		auto fst = memfd_fstat_and_capture(fd, &dev, &ino, &size);
		if (!fst) return fst.error();
	} else {
		auto fd_res = shm_open_existing(shm_name);
		if (!fd_res) {
			// Explicit transport-mismatch diagnosis (v0.2 §33.2): a fd-pass
			// channel has no shm name but does have a broker socket.
			struct stat st {};
			if (fd_res.error().code == ErrorCode::kNotFound &&
			    ::stat(socket_path.c_str(), &st) == 0) {
				return make_error(ErrorCode::kInvalidOptions, "Consumer::open",
				                  "channel is fd-pass; use Transport::kMemfdFdPass");
			}
			return fd_res.error();  // ENOENT -> NotFound
		}
		fd = std::move(fd_res.value());
		auto fst = shm_fstat_and_capture(fd, &dev, &ino, &size);
		if (!fst) return fst.error();
	}
	if (size < sizeof(BootstrapHeaderAbi)) {
		return make_error(ErrorCode::kInitializationIncomplete, "Consumer::open",
		                  "segment below bootstrap size");
	}

	// ---- pread + validate bootstrap ---------------------------------------------
	BootstrapHeaderAbi boot{};
	auto rboot = pread_full(fd.get(), &boot, sizeof(boot), 0);
	if (!rboot) return rboot.error();
	auto vboot = validate_bootstrap_parse(boot, size);
	if (!vboot) return vboot.error();
	if (boot.expected_mapping_size != size) {
		return make_error(ErrorCode::kInitializationIncomplete, "Consumer::open",
		                  "mapping size mismatch (producer still initializing?)");
	}
	if (boot.init_state != static_cast<uint32_t>(InitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, "Consumer::open",
		                  "producer not ready");
	}
	// Segment must have been created in the current boot (design §7.2).
	{
		uint64_t bhi = 0, blo = 0;
		current_boot_id_hash(&bhi, &blo);
		if (boot.creator_boot_id_hash_hi != bhi || boot.creator_boot_id_hash_lo != blo) {
			return make_error(ErrorCode::kCorruptHeader, "Consumer::open",
			                  "segment from previous boot");
		}
	}

	// ---- mmap + recheck + header validation -------------------------------------
	auto mm = mmap_region(fd, size);
	if (!mm) return mm.error();
	MappedRegion mapping = std::move(mm.value());
	auto* base = static_cast<std::byte*>(mapping.get());
	auto* boot_map = reinterpret_cast<BootstrapHeaderAbi*>(base);

	if (shared_load_acquire(&boot_map->init_state) !=
	    static_cast<uint32_t>(InitState::kReady)) {
		return make_error(ErrorCode::kInitializationIncomplete, "Consumer::open",
		                  "not ready on recheck");
	}

	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	auto vh = validate_header_parse(*header, schema, payload_size);
	if (!vh) return vh.error();

	// ---- control lock + revalidate inode -----------------------------------------
	auto lock_res = ControlLock::acquire(lock_path);
	if (!lock_res) return lock_res.error();
	ControlLock lock = std::move(lock_res.value());

	auto jr = lock.read_journal();
	if (!jr) return jr.error();
	const ControlJournalV1 journal = jr.value();
	if (journal.channel_hash != 0 && journal.channel_hash != name_hash) {
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
		                  "journal channel mismatch");
	}

	if (!fd_mode) {
		auto again = shm_open_existing(shm_name);
		if (!again) return again.error();
		uint64_t adev = 0, aino = 0;
		auto afst = shm_fstat_and_capture(again.value(), &adev, &aino, nullptr);
		if (!afst) return afst.error();
		if (adev != dev || aino != ino) {
			return make_error(ErrorCode::kNameRaceDetected, "Consumer::open",
			                  "instance replaced during open");
		}
	} else {
		// v0.2 §33.6: the received fd is a one-shot grant and cannot be swapped
		// under us — the name-inode recheck above does not exist in fd mode.
		// Cross-check the wire record against the mapped header as the
		// equivalent consistency guard.
		if (fd_reply.generation != header->generation ||
		    fd_reply.nonce_hi != header->instance_nonce_hi ||
		    fd_reply.nonce_lo != header->instance_nonce_lo ||
		    fd_reply.mapping_size != header->mapping_size) {
			return make_error(ErrorCode::kTransportFailed, "Consumer::open",
			                  "broker reply disagrees with header");
		}
	}

	// ---- register consumer identity -----------------------------------------------
	auto old_cid = identity_snapshot_read(&header->consumer);
	if (!old_cid) {
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
		                  "consumer identity unreadable");
	}
	if (old_cid.value().pid != 0) {
		const uint32_t cstate = shared_load_acquire(&header->consumer_state);
		const bool actively_owned =
		        cstate == static_cast<uint32_t>(EndpointState::kOnline) ||
		        cstate == static_cast<uint32_t>(EndpointState::kStopping) ||
		        cstate == static_cast<uint32_t>(EndpointState::kFault);
		if (actively_owned) {
			const Liveness liv = probe_liveness(old_cid.value().pid,
			                                    old_cid.value().proc_start_ticks);
			if (liv == Liveness::kAlive) {
				return make_error(ErrorCode::kConsumerAlreadyOwned,
				                  "Consumer::open", "consumer alive");
			}
			if (liv == Liveness::kUnverifiable) {
				return make_error(ErrorCode::kRecoveryBlocked, "Consumer::open",
				                  "cannot verify old consumer");
			}
			// Dead active consumer (design §15.4): reclaim its READING slots under
			// the control lock before registering, so the producer never has to keep
			// routing around a leaked slot forever.
			auto reclaim = reclaim_dead_consumer_slots(base, old_cid.value().role_epoch,
			                                           payload_size);
			if (!reclaim) return reclaim.error();
		}
		// OFFLINE (clean shutdown) or dead consumer: register a fresh identity.
	}

	const ProcessIdentity self = current_process_identity();
	ProcessIdentityAbi cid{};
	cid.pid = self.pid;
	cid.proc_start_ticks = self.proc_start_ticks;
	cid.boot_id_hash_hi = self.boot_id_hash_hi;
	cid.boot_id_hash_lo = self.boot_id_hash_lo;
	auto cw = identity_snapshot_write(&header->consumer, cid);
	if (!cw) return cw.error();
	const uint64_t role_epoch = cw.value();
	shared_store_release(&header->consumer_state,
	                     static_cast<uint32_t>(EndpointState::kOnline));
	EDGE_FAILPOINT(C15);  // crash matrix: consumer registered, handle not built

	// ---- build handle ---------------------------------------------------------------
	auto handle = std::make_shared<ConsumerHandle>();
	handle->shm.name = options.name;
	handle->shm.fd = std::move(fd);
	handle->shm.mapping = std::move(mapping);
	handle->shm.dev = dev;
	handle->shm.ino = ino;
	handle->shm.size = size;
	handle->channel_name = options.name;
	handle->transport = options.transport;
	handle->generation = header->generation;
	handle->role_epoch = role_epoch;
	handle->instance_nonce_hi = header->instance_nonce_hi;
	handle->instance_nonce_lo = header->instance_nonce_lo;
	handle->schema_fingerprint = schema.fingerprint;
	handle->schema_version = schema.version;
	handle->payload_size = payload_size;
	handle->self = self;
	return Result<std::shared_ptr<ConsumerHandle>>(std::move(handle));
}

namespace {
inline constexpr uint32_t kMaxReadRetries = 8;  // bounded chase (design §12.1)

// RAII overlap guard for the single-reader rule (§12.3).
struct ReadGuard {
	std::atomic<bool>* in_use = nullptr;
	bool armed = false;
	~ReadGuard() {
		if (armed) in_use->store(false, std::memory_order_release);
	}
};

// Pack the two u64 nonce halves into the 16-byte instance nonce (host order;
// endianness is pinned by the endian_marker and the same-process-producer rule).
void pack_nonce(const ConsumerHandle& handle, std::array<std::byte, 16>* out) {
	uint64_t hi = handle.instance_nonce_hi;
	uint64_t lo = handle.instance_nonce_lo;
	std::memcpy(out->data(), &hi, 8);
	std::memcpy(out->data() + 8, &lo, 8);
}
}  // namespace

Result<ReadSnapshot> consumer_try_read_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                                   std::byte* encoded_out,
                                                   uint32_t encoded_cap) noexcept {
	ReadGuard guard;
	guard.in_use = &handle->read_in_use;
	if (handle->read_in_use.exchange(true, std::memory_order_acquire)) {
		return make_error(ErrorCode::kConcurrentHandleUse, "Consumer::try_read_latest",
		                  "concurrent read");
	}
	guard.armed = true;

	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);

	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(kSlotHeaderSize + handle->payload_size, 64, &stride)) {
		return make_error(ErrorCode::kInvalidOptions, "Consumer::try_read_latest",
		                  "stride overflow");
	}

	for (uint32_t attempt = 0; attempt < kMaxReadRetries; ++attempt) {
		const uint64_t ticket = shared_load_acquire(&header->latest_ticket);
		if (ticket == 0 || ticket_sequence(ticket) <= handle->last_sequence) {
			return make_error(ErrorCode::kNoNewSample, "Consumer::try_read_latest",
			                  "no new sample");
		}
		const uint32_t slot_index = ticket_slot(ticket);
		uint64_t offset = 0;
		if (!slot_byte_offset(slot_index, stride, &offset)) {
			return make_error(ErrorCode::kCorruptSlot, "Consumer::try_read_latest",
			                  "ticket slot out of range");
		}
		auto* slot = reinterpret_cast<SlotHeaderAbi*>(base + offset);
		if (!slot_claim_readable(slot)) continue;  // producer overwrote; retry
		EDGE_FAILPOINT(C07);  // crash matrix: READING_CLAIMING, epoch not yet set
		slot_mark_reading(slot, handle->role_epoch);
		EDGE_FAILPOINT(C08);  // crash matrix: READING, before the copy

		// Linearization recheck (design §12.2): the slot must still be the current
		// latest at this instant.
		const uint64_t claimed_ticket = make_ticket(slot->sample_sequence, slot_index);
		if (shared_load_acquire(&header->latest_ticket) != claimed_ticket) {
			slot_release_read(slot);
			continue;
		}

		// Freeze all metadata BEFORE release (§12.2: no slot access after release).
		const uint32_t slot_payload_size = slot->payload_size;
		const uint64_t sample_sequence = slot->sample_sequence;
		const uint64_t publish_boot_ns = slot->publish_boot_ns;
		const uint64_t expected_checksum = slot->payload_checksum;
		auto* payload =
		        reinterpret_cast<std::byte*>(slot) + static_cast<uint64_t>(kSlotHeaderSize);
		if (slot_payload_size != handle->payload_size || slot_payload_size > encoded_cap) {
			slot_release_read(slot);
			return make_error(ErrorCode::kPayloadCorrupt, "Consumer::try_read_latest",
			                  "payload size mismatch");
		}
		std::memcpy(encoded_out, payload, static_cast<size_t>(slot_payload_size));
		const uint64_t receive_boot_ns = boottime_now_ns();
		slot_release_read(slot);
		EDGE_FAILPOINT(C09);  // crash matrix: released, epoch cleared, claim finished

		if (receive_boot_ns == 0) {
			return make_error(ErrorCode::kClockAnomaly, "Consumer::try_read_latest",
			                  "monotonic clock unavailable");
		}

		const bool checksum_ok =
		        fnv1a64(encoded_out, static_cast<size_t>(slot_payload_size)) ==
		        expected_checksum;

		ReadSnapshot snap;
		snap.encoded_size = slot_payload_size;
		snap.checksum_ok = checksum_ok;
		snap.sample_sequence = sample_sequence;
		snap.publish_boot_ns = publish_boot_ns;
		snap.receive_boot_ns = receive_boot_ns;
		snap.generation = handle->generation;
		pack_nonce(*handle, &snap.instance_nonce);
		snap.missed_samples =
		        handle->first_sample_in_generation
		                ? 0
		                : saturated_gap(handle->last_sequence, sample_sequence);
		handle->last_sequence = sample_sequence;
		handle->first_sample_in_generation = false;
		shared_fetch_add_relaxed(&header->read_count, uint64_t{1});
		return Result<ReadSnapshot>(std::move(snap));
	}

	return make_error(ErrorCode::kReadContention, "Consumer::try_read_latest",
	                  "retry bound exceeded");
}

namespace {
// §15.5 wait-timeout classification: producer liveness decides whether the
// timeout means "alive but idle" (DataStale), "gone" (ProducerOffline), or
// "cannot verify" (RecoveryBlocked — fail closed, never guess). v0.2 §34 adds
// the heartbeat branch: alive + heartbeat enabled + no fresh publish + no fresh
// heartbeat -> ProducerStalled (an observation only; never a takeover grant).
Result<ReadSnapshot> classify_wait_timeout(const std::shared_ptr<ConsumerHandle>&,
                                           ChannelHeaderAbi* header) noexcept {
	const uint32_t pstate = shared_load_acquire(&header->producer_state);
	const bool actively_owned = pstate == static_cast<uint32_t>(EndpointState::kOnline) ||
	                            pstate == static_cast<uint32_t>(EndpointState::kStopping) ||
	                            pstate == static_cast<uint32_t>(EndpointState::kFault);
	if (!actively_owned) {
		// OFFLINE (clean shutdown) or never registered: the producer is gone.
		return make_error(ErrorCode::kProducerOffline, "Consumer::wait_latest",
		                  "producer offline on wait timeout");
	}
	auto pid_res = identity_snapshot_read(&header->producer);
	if (!pid_res) {
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::wait_latest",
		                  "producer identity unreadable");
	}
	const ProcessIdentityAbi& pid_abi = pid_res.value();
	if (pid_abi.pid == 0) {
		return make_error(ErrorCode::kRecoveryBlocked, "Consumer::wait_latest",
		                  "producer identity unknown");
	}
	switch (probe_liveness(pid_abi.pid, pid_abi.proc_start_ticks)) {
		case Liveness::kAlive: {
			// v0.2 §34.3 heartbeat classification, in order (read-only paths):
			const uint64_t interval =
			        shared_load_acquire(&header->producer_heartbeat_interval_ns);
			const uint64_t now = boottime_now_ns();
			const uint64_t last_publish =
			        shared_load_acquire(&header->last_publish_boot_ns);
			const uint64_t last_beat = shared_load_acquire(&header->heartbeat_boot_ns);
			// 1. Heartbeat disabled: v0.1 semantics unchanged.
			// 2. Data still advancing: normal DataStale.
			// 3. Heartbeat enabled and stale beyond 3x the interval: stalled.
			// 4. Otherwise: within tolerance, plain DataStale.
			if (interval != 0 && now != 0 &&
			    (last_publish == 0 || now - last_publish > interval) &&
			    (last_beat == 0 || now - last_beat > interval * kHeartbeatStallFactor)) {
				return make_error(ErrorCode::kProducerStalled, "Consumer::wait_latest",
				                  "producer alive, heartbeat stale");
			}
			return make_error(ErrorCode::kDataStale, "Consumer::wait_latest",
			                  "producer alive but no new sample");
		}
		case Liveness::kExited:
			return make_error(ErrorCode::kProducerOffline, "Consumer::wait_latest",
			                  "producer exited");
		case Liveness::kPidReused:
		case Liveness::kUnverifiable:
			return make_error(ErrorCode::kRecoveryBlocked, "Consumer::wait_latest",
			                  "producer identity unverifiable");
	}
	return make_error(ErrorCode::kRecoveryBlocked, "Consumer::wait_latest",
	                  "producer identity unverifiable");
}
}  // namespace

Result<ReadSnapshot> consumer_wait_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                               std::byte* encoded_out, uint32_t encoded_cap,
                                               uint64_t timeout_ns) noexcept {
	// deadline == 0 is the clock-failure sentinel (monotonic_now_ns == 0).
	const uint64_t deadline = monotonic_deadline_ns(timeout_ns);
	if (deadline == 0) {
		return make_error(ErrorCode::kClockAnomaly, "Consumer::wait_latest",
		                  "monotonic clock unavailable");
	}

	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);

	for (;;) {
		const auto read = consumer_try_read_latest_impl(handle, encoded_out, encoded_cap);
		if (read) return read;  // a sample, or a real error already returned
		const ErrorCode ec = read.error().code;
		if (ec != ErrorCode::kNoNewSample && ec != ErrorCode::kReadContention) {
			// PayloadCorrupt / DecodeFailed / StaleHandle / ClockAnomaly / system
			// errors pass through unchanged (§14.2) — never swallowed as a timeout.
			return read.error();
		}

		const uint32_t expected = shared_load_acquire(&header->notify_epoch);
		// Recheck the ticket AFTER loading the epoch: a publish between the failed
		// read and this load must not be lost (§14.2 — the epoch can only move
		// forward, so loading it first closes the lost-wakeup window).
		const uint64_t ticket = shared_load_acquire(&header->latest_ticket);
		if (ticket != 0 && ticket_sequence(ticket) > handle->last_sequence) {
			continue;
		}

		const uint64_t remaining = remaining_time_ns(deadline);
		if (remaining == 0) {
			return classify_wait_timeout(handle, header);
		}
		struct timespec ts {};
		ts.tv_sec = static_cast<time_t>(remaining / 1000000000ull);
		ts.tv_nsec = static_cast<long>(remaining % 1000000000ull);
		const int rc = futex_wait(&header->notify_epoch, expected, &ts);
		if (rc == 0) continue;  // woken: epoch changed or spurious -> re-read
		if (rc < 0 && (errno == EAGAIN || errno == EINTR)) {
			continue;  // epoch changed before arming, or signal: loop, deadline intact
		}
		if (rc < 0 && errno == ETIMEDOUT) {
			return classify_wait_timeout(handle, header);
		}
		if (rc < 0) {
			return make_error(ErrorCode::kSystemError, "Consumer::wait_latest",
			                  "futex_wait failed");
		}
	}
}

Result<ChannelStatus> consumer_status_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept {
	if (handle->transport == Transport::kPosixShm) {
		const std::string shm_name = channel_shm_name(handle->channel_name);
		auto re = shm_open_existing(shm_name);
		if (!re) {
			return make_error(ErrorCode::kStaleHandle, "Consumer::status",
			                  "channel name gone");
		}
		uint64_t dev = 0, ino = 0;
		auto fst = shm_fstat_and_capture(re.value(), &dev, &ino, nullptr);
		if (!fst) return fst.error();
		if (dev != handle->shm.dev || ino != handle->shm.ino) {
			return make_error(ErrorCode::kStaleHandle, "Consumer::status",
			                  "instance replaced under name");
		}
	}
	// fd mode (v0.2 §33.6): the mapping IS the one-shot identity.
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	return read_channel_status(base);
}

void consumer_shutdown_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept {
	// Best-effort clean shutdown (design §15.2): only mark OFFLINE if the header
	// still carries our consumer identity.
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	auto cid_res = identity_snapshot_read(&header->consumer);
	if (!cid_res || cid_res.value().role_epoch != handle->role_epoch) {
		return;  // not ours (anymore) — leave the header untouched
	}
	shared_store_release(&header->consumer_state,
	                     static_cast<uint32_t>(EndpointState::kOffline));
}

}  // namespace edge_runtime::detail
