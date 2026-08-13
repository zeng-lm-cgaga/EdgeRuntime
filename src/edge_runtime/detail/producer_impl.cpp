#include "edge_runtime/detail/producer_impl.hpp"

#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

#include "edge_runtime/detail/channel_abi.hpp"
#include "edge_runtime/detail/checksum.hpp"
#include "edge_runtime/detail/clock.hpp"
#include "edge_runtime/detail/failpoint.hpp"
#include "edge_runtime/detail/fd_broker.hpp"
#include "edge_runtime/detail/futex.hpp"
#include "edge_runtime/detail/random.hpp"
#include "edge_runtime/detail/slot_protocol.hpp"

namespace edge_runtime::detail {

namespace {

// Restores the pre-transaction journal record on failure, so a failed
// create/remove never leaves the journal permanently non-idle (which would
// block later creates until the ER4 recovery engine reconciles it).
struct JournalGuard {
	ControlLock* lock = nullptr;
	ControlJournalV1 reset_to{};
	bool armed = false;

	~JournalGuard() {
		if (armed && lock != nullptr) {
			(void)lock->write_journal(reset_to);
		}
	}
};

// Unlinks only the object this call created (never a pre-existing one).
struct CreatedObjectGuard {
	std::string name;
	bool armed = false;
	uint64_t dev = 0;
	uint64_t ino = 0;

	~CreatedObjectGuard() {
		if (armed) {
			(void)shm_unlink_checked(name, dev, ino);
		}
	}
};

// Shared stale-guard block for publish() and heartbeat() (design §15.6/§34):
// the instance must still be READY, the frozen generation/nonce must still
// match, the producer role must still belong to this handle, and the producer
// must still be online. Returns the header on success.
Result<ChannelHeaderAbi*> verify_handle_ownership(const ProducerHandle& handle,
                                                  const char* operation) noexcept {
	auto* base = static_cast<std::byte*>(handle.shm.mapping.get());
	auto* boot = reinterpret_cast<BootstrapHeaderAbi*>(base);
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	if (shared_load_acquire(&boot->init_state) != static_cast<uint32_t>(InitState::kReady) ||
	    shared_load_acquire(&header->init_state) != static_cast<uint32_t>(InitState::kReady)) {
		return make_error(ErrorCode::kStaleHandle, operation, "instance not ready");
	}
	if (header->generation != handle.generation ||
	    header->instance_nonce_hi != handle.instance_nonce_hi ||
	    header->instance_nonce_lo != handle.instance_nonce_lo) {
		return make_error(ErrorCode::kStaleHandle, operation, "instance replaced");
	}
	auto pid_res = identity_snapshot_read(&header->producer);
	if (!pid_res || pid_res.value().role_epoch != handle.role_epoch) {
		return make_error(ErrorCode::kStaleHandle, operation, "producer role changed");
	}
	if (shared_load_acquire(&header->producer_state) !=
	    static_cast<uint32_t>(EndpointState::kOnline)) {
		return make_error(ErrorCode::kProducerOffline, operation, "producer offline");
	}
	return Result<ChannelHeaderAbi*>(header);
}

bool payload_and_schema_valid(const ChannelOptions& options, const SchemaDescriptor& schema,
                              uint32_t payload_size) {
	if (options.name.empty() || options.name.size() > kMaxChannelNameLen ||
	    !validate_channel_name(options.name.c_str(), options.name.size())) {
		return false;
	}
	if (payload_size == 0 || payload_size > kMaxPayloadSize) return false;
	bool fingerprint_set = false;
	for (const std::byte b : schema.fingerprint) {
		if (b != std::byte{0}) {
			fingerprint_set = true;
			break;
		}
	}
	return fingerprint_set;
}

// ---- ER4: reconcile a journal left in a mid-transaction state by a dead
// creator (design §9.1/§9.3/§15.3, crash matrix C01/C02/C13). Called under the
// control lock. Only the creator's PROVEN death lets us intervene, and even
// then only when the object matches what the journal claims it created — every
// ambiguity fails closed so a stale recovery never unlinks someone else's
// object (name/inode ABA). Returns the reconciled Idle record (already written
// back to the lock file) for the create to continue from.
Result<ControlJournalV1> reconcile_stale_journal(ControlLock& lock, const ControlJournalV1& journal,
                                                 const std::string& shm_name) {
	const auto state = static_cast<JournalState>(journal.state);
	if (state == JournalState::kIdle) return journal;

	const Liveness creator_liv =
	        probe_liveness(journal.creator.pid, journal.creator.proc_start_ticks);
	if (creator_liv == Liveness::kAlive || creator_liv == Liveness::kUnverifiable) {
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
		                  "prior transaction owned or unverifiable");
	}
	// kExited / kPidReused: the recorded creator process is gone.

	// The aborted attempt committed nothing, so its intended generation/nonce are
	// discarded; the next completed create numbers from the last completed
	// record (design §9.4). The READY sub-case below overrides with the actually
	// committed instance.
	ControlJournalV1 reset = journal;
	reset.state = static_cast<uint32_t>(JournalState::kIdle);
	reset.target_dev = 0;
	reset.target_ino = 0;
	reset.new_generation = journal.old_generation;
	reset.new_nonce_hi = journal.old_nonce_hi;
	reset.new_nonce_lo = journal.old_nonce_lo;

	switch (state) {
		case JournalState::kCreatingPreObject: {
			auto existing = shm_open_existing(shm_name);
			if (!existing) {
				if (existing.error().code != ErrorCode::kNotFound)
					return existing.error();
				break;  // nothing was ever created: stale-but-harmless journal
			}
			uint64_t dev = 0, ino = 0, size = 0;
			auto fst = shm_fstat_and_capture(existing.value(), &dev, &ino, &size);
			if (!fst) return fst.error();
			// Narrow condition (§9.1): owner/mode already validated by fstat; a
			// size-0 object can only be the crashed creator's just-created (never
			// truncated) object. Anything non-empty might be someone else's — refuse.
			if (size != 0) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "object under name after preobject crash");
			}
			auto un = shm_unlink_checked(shm_name, dev, ino);
			if (!un) return un.error();
			break;
		}
		case JournalState::kCreatingObject:
		case JournalState::kReplacing: {
			auto existing = shm_open_existing(shm_name);
			if (!existing) {
				if (existing.error().code != ErrorCode::kNotFound)
					return existing.error();
				break;  // creator already removed it
			}
			uint64_t dev = 0, ino = 0, size = 0;
			auto fst = shm_fstat_and_capture(existing.value(), &dev, &ino, &size);
			if (!fst) return fst.error();
			if (dev != journal.target_dev || ino != journal.target_ino) {
				return make_error(ErrorCode::kNameRaceDetected, "Producer::create",
				                  "object inode differs from journal target");
			}
			// The object must be bound to this journal: parseable bootstrap whose
			// creator nonce matches the journal's intended nonce.
			BootstrapHeaderAbi boot{};
			auto rboot = pread_full(existing.value().get(), &boot, sizeof(boot), 0);
			if (!rboot) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "bootstrap unreadable after object crash");
			}
			auto vboot = validate_bootstrap_parse(boot, size);
			if (!vboot) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "bootstrap corrupt after object crash");
			}
			if (boot.creator_nonce_hi != journal.new_nonce_hi ||
			    boot.creator_nonce_lo != journal.new_nonce_lo) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "creator nonce does not bind to journal");
			}
			if (boot.init_state == static_cast<uint32_t>(InitState::kReady)) {
				// The creator committed READY but died before the IDLE journal: a
				// valid dead instance. Do NOT unlink it here — the normal
				// dead-producer replacement path below re-reads it and advances
				// generation+1. Reset the journal to the actually-committed
				// instance so the generations agree.
				ChannelHeaderAbi header{};
				auto rh = pread_full(existing.value().get(), &header,
				                     sizeof(header), kChannelHeaderOffset);
				if (!rh) {
					return make_error(ErrorCode::kRecoveryBlocked,
					                  "Producer::create",
					                  "committed header unreadable");
				}
				reset.new_generation = header.generation;
				reset.new_nonce_hi = header.instance_nonce_hi;
				reset.new_nonce_lo = header.instance_nonce_lo;
				break;
			}
			auto un = shm_unlink_checked(shm_name, dev, ino);
			if (!un) return un.error();
			break;
		}
		case JournalState::kRemoving: {
			auto existing = shm_open_existing(shm_name);
			if (!existing) {
				if (existing.error().code != ErrorCode::kNotFound)
					return existing.error();
				break;  // removal completed
			}
			uint64_t dev = 0, ino = 0;
			auto fst = shm_fstat_and_capture(existing.value(), &dev, &ino, nullptr);
			if (!fst) return fst.error();
			if (dev != journal.target_dev || ino != journal.target_ino) {
				return make_error(ErrorCode::kNameRaceDetected, "Producer::create",
				                  "object inode differs from removal target");
			}
			// The removal did not complete. Reset the journal; the normal inspect
			// path below decides (dead instance -> replace, corrupt -> fail closed).
			break;
		}
		default:
			break;
	}

	auto wr = lock.write_journal(reset);
	if (!wr) return wr.error();
	return Result<ControlJournalV1>(reset);
}

}  // namespace

Result<std::shared_ptr<ProducerHandle>> producer_create_impl(const ChannelOptions& options,
                                                             const SchemaDescriptor& schema,
                                                             uint32_t payload_size) {
	if (!payload_and_schema_valid(options, schema, payload_size)) {
		return make_error(ErrorCode::kInvalidOptions, "Producer::create",
		                  "name/schema/payload invalid");
	}
	if (options.transport != Transport::kPosixShm &&
	    options.transport != Transport::kMemfdFdPass) {
		return make_error(ErrorCode::kInvalidOptions, "Producer::create",
		                  "unknown transport");
	}
	const bool fd_mode = options.transport == Transport::kMemfdFdPass;
	const std::string shm_name = channel_shm_name(options.name);
	const std::string lock_path = channel_lock_path(options.name);
	const std::string socket_path = channel_socket_path(options.name);
	const uint32_t name_hash = channel_name_hash(options.name.c_str(), options.name.size());

	// ---- control lock + journal ---------------------------------------------
	auto lock_res = ControlLock::acquire(lock_path);
	if (!lock_res) return lock_res.error();
	ControlLock lock = std::move(lock_res.value());

	auto jr = lock.read_journal();
	if (!jr) return jr.error();
	ControlJournalV1 journal = jr.value();
	if (journal.channel_hash != 0 && journal.channel_hash != name_hash) {
		return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
		                  "journal channel mismatch");
	}
	if (journal.state != static_cast<uint32_t>(JournalState::kIdle)) {
		// ER4: a previous create/remove died mid-transaction. Only its proven-dead
		// creator's own half-transaction is reconciled (narrow conditions); every
		// ambiguity fails closed and asks for manual intervention.
		auto rec = reconcile_stale_journal(lock, journal, shm_name);
		if (!rec) return rec.error();
		journal = rec.value();
	}

	uint64_t generation = 0;
	if (!next_generation_from_journal(journal, &generation)) {
		return make_error(ErrorCode::kSequenceExhausted, "Producer::create",
		                  "generation exhausted");
	}

	// ---- cross-transport guard (v0.2, design §7.3) -----------------------------
	// The journal's last completed record pins the channel's transport. A
	// different transport is only allowed once the old owner is proven dead:
	// the old object is unreachable to the other transport (fd mode has no
	// name), so a live owner cannot be detected by the other transport's
	// inspect path — fail closed instead of risking a split brain.
	if (journal.transport != static_cast<uint32_t>(options.transport) &&
	    journal.creator.pid != 0) {
		const Liveness prev = probe_liveness(journal.creator.pid,
		                                     journal.creator.proc_start_ticks);
		if (prev == Liveness::kAlive) {
			return make_error(ErrorCode::kAlreadyOwned, "Producer::create",
			                  "previous owner alive under other transport");
		}
		if (prev == Liveness::kUnverifiable) {
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "cannot verify previous owner transport");
		}
		// Previous owner dead: transport switch allowed, generation continues.
	}

	// ---- inspect existing object --------------------------------------------
	// POSIX mode: the named object. fd mode: also run this check — a live POSIX
	// instance under the same name must block the fd-mode create (design §7.3);
	// when absent, the fd broker socket probe below decides.
	auto existing = shm_open_existing(shm_name);
	if (existing) {
		uint64_t edev = 0, eino = 0, esize = 0;
		auto fst = shm_fstat_and_capture(existing.value(), &edev, &eino, &esize);
		if (!fst) return fst.error();
		if (esize < sizeof(BootstrapHeaderAbi)) {
			return make_error(ErrorCode::kInitializationIncomplete, "Producer::create",
			                  "existing object below bootstrap size");
		}
		auto emap = mmap_region(existing.value(), esize);
		if (!emap) return emap.error();
		auto* ebase = static_cast<std::byte*>(emap.value().get());
		auto* eboot = reinterpret_cast<BootstrapHeaderAbi*>(ebase);
		auto vboot = validate_bootstrap_parse(*eboot, esize);
		if (!vboot) {
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "existing bootstrap corrupt; use edge_shm_ctl");
		}
		if (shared_load_acquire(&eboot->init_state) !=
		    static_cast<uint32_t>(InitState::kReady)) {
			return make_error(ErrorCode::kInitializationIncomplete, "Producer::create",
			                  "existing instance not ready");
		}
		auto* eheader = reinterpret_cast<ChannelHeaderAbi*>(ebase + kChannelHeaderOffset);
		if (std::memcmp(eheader->magic, kChannelHeaderMagic,
		                sizeof(kChannelHeaderMagic) - 1) != 0 ||
		    eheader->abi_major != kAbiMajor) {
			return make_error(ErrorCode::kCorruptHeader, "Producer::create",
			                  "existing header invalid");
		}
		auto pid_res = identity_snapshot_read(&eheader->producer);
		if (!pid_res) {
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "old producer identity unreadable");
		}
		const ProcessIdentityAbi& opid = pid_res.value();
		const uint32_t pstate = shared_load_acquire(&eheader->producer_state);
		const bool actively_owned =
		        pstate == static_cast<uint32_t>(EndpointState::kOnline) ||
		        pstate == static_cast<uint32_t>(EndpointState::kStopping) ||
		        pstate == static_cast<uint32_t>(EndpointState::kFault);
		if (opid.pid != 0) {
			const Liveness liv = probe_liveness(opid.pid, opid.proc_start_ticks);
			if (liv == Liveness::kAlive && actively_owned) {
				return make_error(ErrorCode::kAlreadyOwned, "Producer::create",
				                  "producer alive");
			}
			if (liv == Liveness::kUnverifiable && actively_owned) {
				return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
				                  "cannot verify old producer");
			}
			// Alive but OFFLINE: clean shutdown (design §15.2) — the old instance no
			// longer owns the channel, so the new one replaces it.
		}
		// Old producer is dead/exited or cleanly shut down: verified replacement
		// (design §15.3). A completed journal that disagrees with the segment's
		// generation means someone replaced the instance under us — never proceed.
		if (journal.new_generation != 0 && journal.new_generation != eheader->generation) {
			return make_error(ErrorCode::kRecoveryBlocked, "Producer::create",
			                  "journal and segment generations disagree");
		}
		if (eheader->generation == UINT64_MAX) {
			return make_error(ErrorCode::kSequenceExhausted, "Producer::create",
			                  "generation wrap");
		}
		generation = eheader->generation + 1;
		EDGE_FAILPOINT(C10);  // crash matrix: recovery in progress, lock held
		auto un = shm_unlink_checked(shm_name, edev, eino);
		if (!un) return un.error();
	} else if (existing.error().code != ErrorCode::kNotFound) {
		return existing.error();  // EACCES etc. — do not treat as absent
	}

	// ---- journal CREATING_PREOBJECT ------------------------------------------
	const ProcessIdentity self = current_process_identity();
	uint8_t nonce_bytes[16]{};
	if (!random_bytes(nonce_bytes, sizeof(nonce_bytes))) {
		return make_error(ErrorCode::kSystemError, "Producer::create", "getrandom");
	}
	uint64_t nonce_hi = 0;
	uint64_t nonce_lo = 0;
	std::memcpy(&nonce_hi, nonce_bytes, 8);
	std::memcpy(&nonce_lo, nonce_bytes + 8, 8);

	JournalGuard journal_guard;
	journal_guard.lock = &lock;
	journal_guard.reset_to = journal;

	auto j0 = make_control_journal(options.name, JournalState::kCreatingPreObject,
	                               journal.old_generation, generation, journal.old_nonce_hi,
	                               journal.old_nonce_lo, nonce_hi, nonce_lo, self);
	j0.transport = static_cast<uint32_t>(options.transport);
	auto wj0 = lock.write_journal(j0);
	if (!wj0) return wj0.error();
	journal_guard.armed = true;

	// ---- create the object ----------------------------------------------------
	UniqueFd fd;
	uint64_t cdev = 0;
	uint64_t cino = 0;
	CreatedObjectGuard created;
	if (fd_mode) {
		// v0.2 fd-pass (design §33.3): anonymous memfd; the object dies with
		// this fd. No global name, no unlink cleanup, no inode-ABA surface.
		auto mfd = memfd_create_object(options.name);
		if (!mfd) return mfd.error();
		fd = std::move(mfd.value());
		EDGE_FAILPOINT(C16);  // crash matrix: memfd created, journal PREOBJECT
		auto cst = memfd_fstat_and_capture(fd, &cdev, &cino, nullptr);
		if (!cst) return cst.error();
	} else {
		auto fd_res = shm_open_create(shm_name);
		if (!fd_res) {
			if (fd_res.error().code == ErrorCode::kAlreadyOwned) {
				return make_error(ErrorCode::kNameRaceDetected, "Producer::create",
				                  "object appeared under name");
			}
			return fd_res.error();
		}
		fd = std::move(fd_res.value());
		EDGE_FAILPOINT(C01);  // crash matrix: created, journal still PREOBJECT
		created.name = shm_name;
		auto cst = shm_fstat_and_capture(fd, &cdev, &cino, nullptr);
		if (!cst) return cst.error();
		created.dev = cdev;
		created.ino = cino;
		created.armed = true;
	}

	auto j1 = make_control_journal(options.name, JournalState::kCreatingObject,
	                               journal.old_generation, generation, journal.old_nonce_hi,
	                               journal.old_nonce_lo, nonce_hi, nonce_lo, self);
	j1.transport = static_cast<uint32_t>(options.transport);
	j1.target_dev = cdev;
	j1.target_ino = cino;
	auto wj1 = lock.write_journal(j1);
	if (!wj1) return wj1.error();

	// ---- bootstrap INITIALIZING ------------------------------------------------
	// v0.2 heartbeat (design §34): interval > 0 enables heartbeat and bumps
	// abi_minor to 1 (both headers, same value); disabled stays minor 0 and the
	// heartbeat fields stay zeroed — byte-identical to a v0.1 segment.
	const uint64_t heartbeat_interval_ns =
	        static_cast<uint64_t>(options.heartbeat_interval.count() > 0
	                                     ? options.heartbeat_interval.count()
	                                     : 0);
	const uint16_t abi_minor =
	        heartbeat_interval_ns > 0 ? static_cast<uint16_t>(kAbiMinorMax) : kAbiMinor;
	BootstrapHeaderAbi boot{};
	std::memcpy(boot.magic, kBootstrapMagic, sizeof(kBootstrapMagic) - 1);
	boot.abi_major = kAbiMajor;
	boot.abi_minor = abi_minor;
	boot.header_size = sizeof(BootstrapHeaderAbi);
	uint64_t mapping = 0;
	if (!mapping_size_for_payload(payload_size, &mapping)) {
		return make_error(ErrorCode::kInvalidOptions, "Producer::create",
		                  "mapping size overflow");
	}
	boot.expected_mapping_size = mapping;
	boot.creator_nonce_hi = nonce_hi;
	boot.creator_nonce_lo = nonce_lo;
	boot.creator_pid = self.pid;
	boot.creator_proc_start_ticks = self.proc_start_ticks;
	boot.creator_boot_id_hash_hi = self.boot_id_hash_hi;
	boot.creator_boot_id_hash_lo = self.boot_id_hash_lo;
	boot.init_state = static_cast<uint32_t>(InitState::kInitializing);
	boot.bootstrap_checksum = bootstrap_checksum_of(boot);
	auto wboot = pwrite_full(fd.get(), &boot, sizeof(boot), 0);
	if (!wboot) return wboot.error();
	EDGE_FAILPOINT(C02);  // crash matrix: bootstrap INITIALIZING, not yet READY

	// ---- ftruncate exact mapping + mmap ----------------------------------------
	auto tr = shm_truncate(fd, mapping);
	if (!tr) return tr.error();
	auto mm = mmap_region(fd, mapping);
	if (!mm) return mm.error();
	MappedRegion region = std::move(mm.value());
	auto* base = static_cast<std::byte*>(region.get());

	// ---- initialize header + slots (never overwrite the bootstrap) ---------------
	std::memset(base + kChannelHeaderOffset, 0, mapping - kChannelHeaderOffset);

	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	std::memcpy(header->magic, kChannelHeaderMagic, sizeof(kChannelHeaderMagic) - 1);
	header->abi_major = kAbiMajor;
	header->abi_minor = abi_minor;
	header->header_size = sizeof(ChannelHeaderAbi);
	header->endian_marker = kEndianMarker;
	header->slot_count = kSlotCount;
	header->payload_size = payload_size;
	header->max_payload_size = kMaxPayloadSize;
	header->schema_version = schema.version;
	std::memcpy(header->schema_fingerprint, schema.fingerprint.data(), 32);
	header->mapping_size = mapping;
	header->generation = generation;
	header->instance_nonce_hi = nonce_hi;
	header->instance_nonce_lo = nonce_lo;
	header->producer_heartbeat_interval_ns = heartbeat_interval_ns;  // v0.2 §34

	ProcessIdentityAbi pid_abi{};
	pid_abi.pid = self.pid;
	pid_abi.proc_start_ticks = self.proc_start_ticks;
	pid_abi.boot_id_hash_hi = self.boot_id_hash_hi;
	pid_abi.boot_id_hash_lo = self.boot_id_hash_lo;
	auto pidw = identity_snapshot_write(&header->producer, pid_abi);
	if (!pidw) return pidw.error();
	const uint64_t role_epoch = pidw.value();

	shared_store_relaxed(&header->init_state, static_cast<uint32_t>(InitState::kInitializing));
	shared_store_relaxed(&header->producer_state,
	                     static_cast<uint32_t>(EndpointState::kOnline));
	// latest_ticket / notify_epoch / consumer_state / counters stay zeroed.

	// ---- fd mode: bind the broker socket BEFORE the READY commit ----------------
	// (v0.2, design §33.5). The socket is the only reachability point of a
	// nameless object; a bind failure after READY would be unrollable and leave
	// a ghost instance. Still under the control lock, so the probe-before-unlink
	// discipline in fd_broker_bind cannot race a concurrent create.
	UniqueFd listen_fd;
	bool socket_bound = false;
	if (fd_mode) {
		auto b = fd_broker_bind(socket_path);
		if (!b) return b.error();
		listen_fd = std::move(b.value());
		socket_bound = true;
		EDGE_FAILPOINT(C17);  // crash matrix: socket bound, READY not committed
	}

	// ---- commit ----------------------------------------------------------------
	shared_store_release(&header->init_state, static_cast<uint32_t>(InitState::kReady));
	auto* boot_map = reinterpret_cast<BootstrapHeaderAbi*>(base);
	shared_store_release(&boot_map->init_state, static_cast<uint32_t>(InitState::kReady));

	// ---- journal IDLE ------------------------------------------------------------
	auto jdone = make_control_journal(options.name, JournalState::kIdle, journal.old_generation,
	                                  generation, journal.old_nonce_hi, journal.old_nonce_lo,
	                                  nonce_hi, nonce_lo, self);
	jdone.transport = static_cast<uint32_t>(options.transport);
	jdone.target_dev = cdev;
	jdone.target_ino = cino;
	auto wjdone = lock.write_journal(jdone);
	if (!wjdone) {
		if (socket_bound) (void)::unlink(socket_path.c_str());
		return wjdone.error();
	}

	journal_guard.armed = false;
	created.armed = false;

	auto handle = std::make_shared<ProducerHandle>();
	handle->shm.name = options.name;
	handle->shm.fd = std::move(fd);
	handle->shm.mapping = std::move(region);
	handle->shm.dev = cdev;
	handle->shm.ino = cino;
	handle->shm.size = mapping;
	handle->channel_name = options.name;
	handle->generation = generation;
	handle->role_epoch = role_epoch;
	handle->instance_nonce_hi = nonce_hi;
	handle->instance_nonce_lo = nonce_lo;
	handle->schema_fingerprint = schema.fingerprint;
	handle->schema_version = schema.version;
	handle->payload_size = payload_size;
	handle->self = self;
	handle->transport = options.transport;
	handle->channel_hash = name_hash;
	handle->heartbeat_interval_ns = heartbeat_interval_ns;
	if (fd_mode) {
		handle->socket_path = socket_path;
		handle->listen_fd = std::move(listen_fd);
	}

	// ---- fd mode: start the serving thread (design §18.1/§33.4) ------------------
	// The thread reads only handle-owned state (mapping, channel_hash, frozen
	// fingerprint, serve_stop), so it outlives this function safely.
	if (fd_mode) {
		const int listen = handle->listen_fd.get();
		const int shm_fd = handle->shm.fd.get();
		std::byte* serve_base = static_cast<std::byte*>(handle->shm.mapping.get());
		try {
			handle->server_thread = std::thread(
			        fd_broker_serve_loop, listen, shm_fd, serve_base,
			        &handle->channel_hash, &handle->schema_fingerprint,
			        &handle->serve_stop);
		} catch (...) {
			(void)::unlink(socket_path.c_str());  // no server: never leave a live path
			return make_error(ErrorCode::kSystemError, "Producer::create",
			                  "serving thread start failed");
		}
	}
	return Result<std::shared_ptr<ProducerHandle>>(std::move(handle));
}

Result<PublishInfo> producer_publish_impl(const std::shared_ptr<ProducerHandle>& handle,
                                          const std::byte* encoded,
                                          uint32_t encoded_size) noexcept {
	// The encoded size was frozen at create; any drift is an internal bug that
	// must never reach shared memory.
	if (encoded_size != handle->payload_size) {
		return make_error(ErrorCode::kPayloadEncodeFailed, "Producer::publish",
		                  "codec size drift");
	}

	// Cheap mapping-internal stale checks (design §15.6): instance must still be
	// READY, the frozen generation/nonce must still match, and the producer role
	// must still belong to this handle.
	auto v = verify_handle_ownership(*handle, "Producer::publish");
	if (!v) return v.error();
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = v.value();

	const uint64_t current = shared_load_acquire(&header->latest_ticket);
	uint64_t next_sequence = 0;
	if (!checked_next_sequence(current, &next_sequence)) {
		return make_error(ErrorCode::kSequenceExhausted, "Producer::publish",
		                  "sequence exhausted");
	}
	const uint64_t publish_boot_ns = boottime_now_ns();
	if (publish_boot_ns == 0) {
		return make_error(ErrorCode::kClockAnomaly, "Producer::publish",
		                  "boottime unavailable");
	}
	const uint64_t checksum = fnv1a64(encoded, static_cast<size_t>(encoded_size));

	uint64_t stride = 0;
	if (!round_up_to_multiple_u64(kSlotHeaderSize + handle->payload_size, 64, &stride)) {
		return make_error(ErrorCode::kInvalidOptions, "Producer::publish",
		                  "stride overflow");
	}
	const uint32_t current_slot = current == 0 ? kInvalidSlot : ticket_slot(current);

	// Claim a writable slot that is not the current latest (design §11.1).
	SlotHeaderAbi* chosen = nullptr;
	uint32_t chosen_index = 0;
	for (uint32_t i = 0; i < kSlotCount; ++i) {
		if (i == current_slot) continue;
		uint64_t offset = 0;
		if (!slot_byte_offset(i, stride, &offset)) continue;
		auto* slot = reinterpret_cast<SlotHeaderAbi*>(base + offset);
		const uint32_t observed = shared_load_relaxed(&slot->state);
		if (observed != static_cast<uint32_t>(SlotState::kFree) &&
		    observed != static_cast<uint32_t>(SlotState::kPublished)) {
			continue;
		}
		if (slot_claim_writable(slot, observed)) {
			chosen = slot;
			chosen_index = i;
			break;
		}
	}
	if (chosen == nullptr) {
		return make_error(ErrorCode::kNoWritableSlot, "Producer::publish",
		                  "all slots busy");
	}
	EDGE_FAILPOINT(C03);  // crash matrix: slot claimed WRITING, before any data

	// Write slot metadata + payload (relaxed; visibility via the publish release
	// and the ticket release).
	shared_store_relaxed(&chosen->payload_size, encoded_size);
	shared_store_relaxed(&chosen->sample_sequence, next_sequence);
	shared_store_relaxed(&chosen->publish_boot_ns, publish_boot_ns);
	auto* payload =
	        reinterpret_cast<std::byte*>(chosen) + static_cast<uint64_t>(kSlotHeaderSize);
	std::memcpy(payload, encoded, static_cast<size_t>(encoded_size));
	EDGE_FAILPOINT(C04);  // crash matrix: payload copied, slot still WRITING
	shared_store_relaxed(&chosen->payload_checksum, checksum);

	// No failure path remains below here (§11.3: once WRITING, we only ever
	// reach PUBLISHED or leave the process in WRITING for recovery).
	slot_publish(chosen);
	EDGE_FAILPOINT(C05);  // crash matrix: slot PUBLISHED, ticket not yet committed

	// Commit the ticket in the same release order as the slot publish so a
	// consumer that acquire-loads the ticket observes the PUBLISHED slot.
	shared_store_release(&header->latest_ticket, make_ticket(next_sequence, chosen_index));
	EDGE_FAILPOINT(C06);  // crash matrix: ticket committed, wake not yet sent
	shared_fetch_add_relaxed(&header->publish_count, uint64_t{1});
	shared_store_relaxed(&header->last_publish_boot_ns, publish_boot_ns);
	shared_fetch_add_relaxed(&header->notify_epoch, uint32_t{1});
	(void)futex_wake(&header->notify_epoch, 1);

	return Result<PublishInfo>(PublishInfo{handle->generation, next_sequence, publish_boot_ns});
}

uint64_t producer_generation_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	return handle ? handle->generation : 0;
}

Result<ChannelStatus> producer_status_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	if (handle->transport == Transport::kPosixShm) {
		// Slow check (design §15.6): the name must still resolve to our frozen inode.
		const std::string shm_name = channel_shm_name(handle->channel_name);
		auto re = shm_open_existing(shm_name);
		if (!re) {
			return make_error(ErrorCode::kStaleHandle, "Producer::status",
			                  "channel name gone");
		}
		uint64_t dev = 0, ino = 0;
		auto fst = shm_fstat_and_capture(re.value(), &dev, &ino, nullptr);
		if (!fst) return fst.error();
		if (dev != handle->shm.dev || ino != handle->shm.ino) {
			return make_error(ErrorCode::kStaleHandle, "Producer::status",
			                  "instance replaced under name");
		}
	}
	// fd mode (v0.2 §33.6): the mapping IS the one-shot identity — a name-based
	// reopen would always be ENOENT and falsely report StaleHandle.
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	return read_channel_status(base);
}

void producer_shutdown_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	// Best-effort clean shutdown (design §15.2): only mark OFFLINE if the header
	// still carries our producer identity (no replacement happened under us).
	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	auto pid_res = identity_snapshot_read(&header->producer);
	if (!pid_res || pid_res.value().role_epoch != handle->role_epoch) {
		return;  // not ours (anymore) — leave the header untouched
	}
	shared_store_release(&header->producer_state,
	                     static_cast<uint32_t>(EndpointState::kOffline));

	if (handle->transport == Transport::kMemfdFdPass && !handle->socket_unlinked) {
		// v0.2 §33.5: clean shutdown releases the channel by removing the broker
		// socket (the fd-mode reachability point), so a successor can bind
		// directly — the equivalent of v0.1's OFFLINE -> replace. Best-effort and
		// under the control lock: only unlink if the journal still records OUR
		// instance, so a late shutdown can never remove a successor's socket
		// (socket name-ABA guard, mirror of §9.4).
		handle->serve_stop.store(true, std::memory_order_relaxed);
		if (handle->listen_fd.get() >= 0) {
			(void)::shutdown(handle->listen_fd.get(), SHUT_RDWR);
		}
		auto lock_res = ControlLock::acquire(channel_lock_path(handle->channel_name));
		if (lock_res) {
			auto jr = lock_res.value().read_journal();
			if (jr && jr.value().new_generation == handle->generation &&
			    jr.value().new_nonce_hi == handle->instance_nonce_hi &&
			    jr.value().new_nonce_lo == handle->instance_nonce_lo) {
				(void)::unlink(handle->socket_path.c_str());
				handle->socket_unlinked = true;
			}
		}
	}
}

Result<void> producer_remove_if_owner_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	const bool fd_mode = handle->transport == Transport::kMemfdFdPass;
	const std::string shm_name = channel_shm_name(handle->channel_name);
	const std::string lock_path = channel_lock_path(handle->channel_name);

	auto lock_res = ControlLock::acquire(lock_path);
	if (!lock_res) return lock_res.error();
	ControlLock lock = std::move(lock_res.value());

	auto jr = lock.read_journal();
	if (!jr) return jr.error();
	const ControlJournalV1 journal = jr.value();

	uint64_t dev = 0, ino = 0;
	if (!fd_mode) {
		auto re = shm_open_existing(shm_name);
		if (!re) return re.error();
		auto fst = shm_fstat_and_capture(re.value(), &dev, &ino, nullptr);
		if (!fst) return fst.error();
		if (dev != handle->shm.dev || ino != handle->shm.ino) {
			return make_error(ErrorCode::kNameRaceDetected, "Producer::remove_if_owner",
			                  "inode changed");
		}
	}

	auto* base = static_cast<std::byte*>(handle->shm.mapping.get());
	auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);
	if (header->generation != handle->generation ||
	    header->instance_nonce_hi != handle->instance_nonce_hi ||
	    header->instance_nonce_lo != handle->instance_nonce_lo) {
		return make_error(ErrorCode::kStaleHandle, "Producer::remove_if_owner",
		                  "instance changed");
	}
	auto pid_res = identity_snapshot_read(&header->producer);
	if (!pid_res) return pid_res.error();
	if (pid_res.value().role_epoch != handle->role_epoch) {
		return make_error(ErrorCode::kStaleHandle, "Producer::remove_if_owner",
		                  "producer role changed");
	}

	// fd mode: stop serving first so no new fd can be handed out mid-removal.
	if (fd_mode) {
		handle->serve_stop.store(true, std::memory_order_relaxed);
		if (handle->listen_fd.get() >= 0) {
			(void)::shutdown(handle->listen_fd.get(), SHUT_RDWR);
		}
	}

	JournalGuard journal_guard;
	journal_guard.lock = &lock;
	journal_guard.reset_to = journal;

	auto jr2 = make_control_journal(handle->channel_name, JournalState::kRemoving,
	                                handle->generation, 0, handle->instance_nonce_hi,
	                                handle->instance_nonce_lo, 0, 0, handle->self);
	jr2.transport = static_cast<uint32_t>(handle->transport);
	jr2.target_dev = dev;
	jr2.target_ino = ino;
	auto wj = lock.write_journal(jr2);
	if (!wj) return wj.error();
	journal_guard.armed = true;

	if (fd_mode) {
		// v0.2 §33.5: the object dies with its fds; removal = unlink our own
		// socket under the lock (no probe needed: it is ours by journal match).
		if (!handle->socket_unlinked) {
			(void)::unlink(handle->socket_path.c_str());
			handle->socket_unlinked = true;
		}
	} else {
		auto un = shm_unlink_checked(shm_name, dev, ino);
		if (!un) return un.error();
	}

	// Completed removal: keep the removed instance's gen/nonce for audit.
	auto jdone = make_control_journal(
	        handle->channel_name, JournalState::kIdle, handle->generation, handle->generation,
	        handle->instance_nonce_hi, handle->instance_nonce_lo, handle->instance_nonce_hi,
	        handle->instance_nonce_lo, handle->self);
	jdone.transport = static_cast<uint32_t>(handle->transport);
	jdone.target_dev = dev;
	jdone.target_ino = ino;
	auto wjd = lock.write_journal(jdone);
	if (!wjd) return wjd.error();
	journal_guard.armed = false;
	return Result<void>::ok();
}

Result<void> producer_heartbeat_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept {
	// v0.2 optional heartbeat (design §34): an explicit application declaration
	// of making-progress. Same stale guards as publish; disabled heartbeat is a
	// validated no-op (the interval was frozen at create).
	if (handle->heartbeat_interval_ns == 0) {
		return Result<void>::ok();
	}
	auto v = verify_handle_ownership(*handle, "Producer::heartbeat");
	if (!v) return v.error();
	auto* header = v.value();

	const uint64_t now = boottime_now_ns();
	if (now == 0) {
		return make_error(ErrorCode::kClockAnomaly, "Producer::heartbeat",
		                  "boottime unavailable");
	}
	shared_store_release(&header->heartbeat_boot_ns, now);
	EDGE_FAILPOINT(C18);  // crash matrix: heartbeat written, producer now frozen
	return Result<void>::ok();
}

}  // namespace edge_runtime::detail
