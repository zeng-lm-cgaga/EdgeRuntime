// edge_shm_ctl: diagnostic / forensic CLI for shared-memory channels
// (design §22). Two subcommands:
//
//   edge_shm_ctl inspect <name>
//       Read-only dump of an instance. Prints stable one-line records the
//       crash-matrix driver parses into evidence files. Never dumps payload.
//
//   edge_shm_ctl remove <name> [--expect-generation N] [--expect-nonce HEX32]
//       Verified removal: takes the control lock, re-binds the object against
//       its journal / explicit expectations (inode/generation/nonce), refuses
//       while the producer is provably alive, then unlinks inode-checked and
//       journals the outcome.
//
// Exit codes: 0 success, 1 inspect-only "not present", 2 refused/failed.

#include <sys/stat.h>

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

#include "edge_runtime/detail/channel_abi.hpp"
#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/control_lock.hpp"
#include "edge_runtime/detail/process_identity.hpp"
#include "edge_runtime/detail/shared_atomic.hpp"
#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/detail/slot_protocol.hpp"
#include "edge_runtime/error.hpp"
#include "tool_common.hpp"

namespace {

using edge_runtime::ErrorCode;
using edge_runtime::to_string;
namespace detail = edge_runtime::detail;

const char* init_state_name(uint32_t v) {
	switch (static_cast<detail::InitState>(v)) {
		case detail::InitState::kEmpty:
			return "empty";
		case detail::InitState::kInitializing:
			return "initializing";
		case detail::InitState::kReady:
			return "ready";
		case detail::InitState::kAborted:
			return "aborted";
	}
	return "unknown";
}

const char* endpoint_state_name(uint32_t v) {
	switch (static_cast<detail::EndpointState>(v)) {
		case detail::EndpointState::kOffline:
			return "offline";
		case detail::EndpointState::kOnline:
			return "online";
		case detail::EndpointState::kStopping:
			return "stopping";
		case detail::EndpointState::kFault:
			return "fault";
	}
	return "unknown";
}

const char* slot_state_name(uint32_t v) {
	switch (static_cast<detail::SlotState>(v)) {
		case detail::SlotState::kFree:
			return "free";
		case detail::SlotState::kWriting:
			return "writing";
		case detail::SlotState::kPublished:
			return "published";
		case detail::SlotState::kReadingClaiming:
			return "reading_claiming";
		case detail::SlotState::kReading:
			return "reading";
	}
	return "unknown";
}

const char* journal_state_name(uint32_t v) {
	switch (static_cast<detail::JournalState>(v)) {
		case detail::JournalState::kIdle:
			return "idle";
		case detail::JournalState::kCreatingPreObject:
			return "creating_pre_object";
		case detail::JournalState::kCreatingObject:
			return "creating_object";
		case detail::JournalState::kReplacing:
			return "replacing";
		case detail::JournalState::kRemoving:
			return "removing";
	}
	return "unknown";
}

const char* liveness_name(detail::Liveness v) {
	switch (v) {
		case detail::Liveness::kAlive:
			return "alive";
		case detail::Liveness::kExited:
			return "exited";
		case detail::Liveness::kPidReused:
			return "pid_reused";
		case detail::Liveness::kUnverifiable:
			return "unverifiable";
	}
	return "unknown";
}

void print_identity_line(const char* tag,
                         const edge_runtime::Result<detail::ProcessIdentityAbi>& id,
                         uint32_t endpoint_state) {
	if (!id) {
		std::printf("%s snapshot_error code=%s\n", tag, to_string(id.error().code));
		return;
	}
	const detail::ProcessIdentityAbi& p = id.value();
	if (p.pid == 0) {
		std::printf("%s pid=0 epoch=%" PRIu64 " state=%s liveness=unset\n", tag,
		            p.role_epoch, endpoint_state_name(endpoint_state));
		return;
	}
	const detail::Liveness lv = detail::probe_liveness(p.pid, p.proc_start_ticks);
	std::printf("%s pid=%" PRIu64 " start=%" PRIu64 " boot=%016" PRIx64 "%016" PRIx64
	            " epoch=%" PRIu64 " state=%s liveness=%s\n",
	            tag, p.pid, p.proc_start_ticks, p.boot_id_hash_hi, p.boot_id_hash_lo,
	            p.role_epoch, endpoint_state_name(endpoint_state), liveness_name(lv));
}

// ---- inspect --------------------------------------------------------------

int cmd_inspect(const std::string& channel_name) {
	const std::string shm_name = detail::channel_shm_name(channel_name);
	const std::string lock_path = detail::channel_lock_path(channel_name);
	std::printf("OBJECT name=%s\n", shm_name.c_str());

	// Control lock + journal first: a consistent cross-check for the header dump
	// (the journal is the binding record of the instance we are inspecting).
	auto lock_res = detail::ControlLock::acquire(lock_path);
	if (!lock_res) {
		std::printf("JOURNAL lock_failed code=%s errno=%d op=%s\n",
		            to_string(lock_res.error().code), lock_res.error().errno_value,
		            lock_res.error().operation);
		return 1;
	}
	auto lock = std::move(lock_res.value());
	auto jr = lock.read_journal();
	if (jr) {
		const detail::ControlJournalV1& j = jr.value();
		std::printf("JOURNAL state=%s target_ino=%" PRIu64 " gen_old=%" PRIu64
		            " gen_new=%" PRIu64 " nonce_old=%016" PRIx64 "%016" PRIx64
		            " nonce_new=%016" PRIx64 "%016" PRIx64 " creator_pid=%" PRIu64 "\n",
		            journal_state_name(j.state), j.target_ino, j.old_generation,
		            j.new_generation, j.old_nonce_hi, j.old_nonce_lo, j.new_nonce_hi,
		            j.new_nonce_lo, j.creator.pid);
	} else {
		std::printf("JOURNAL read_failed code=%s\n", to_string(jr.error().code));
	}

	auto fd_res = detail::shm_open_existing(shm_name);
	if (!fd_res) {
		if (fd_res.error().code == ErrorCode::kNotFound) {
			std::printf("STATUS not_found\n");
			return 1;
		}
		std::printf("STATUS open_failed code=%s errno=%d op=%s\n",
		            to_string(fd_res.error().code), fd_res.error().errno_value,
		            fd_res.error().operation);
		return 1;
	}
	auto fd = std::move(fd_res.value());

	struct stat st {};
	if (::fstat(fd.get(), &st) == 0) {
		std::printf("OBJECT inode=%" PRIu64 " dev=%" PRIu64 " size=%" PRIu64
		            " mode=%04o uid=%u\n",
		            static_cast<uint64_t>(st.st_ino), static_cast<uint64_t>(st.st_dev),
		            static_cast<uint64_t>(st.st_size),
		            static_cast<unsigned>(st.st_mode & 07777u),
		            static_cast<unsigned>(st.st_uid));
	}

	uint64_t dev = 0, ino = 0, size = 0;
	auto fst = detail::shm_fstat_and_capture(fd, &dev, &ino, &size);
	if (!fst) {
		std::printf("STATUS fstat_failed code=%s\n", to_string(fst.error().code));
		return 1;
	}

	detail::BootstrapHeaderAbi boot{};
	auto rboot = detail::pread_full(fd.get(), &boot, sizeof(boot), 0);
	if (!rboot) {
		std::printf("BOOT unreadable size=%" PRIu64 "\n", size);
		return 0;
	}
	auto vboot = detail::validate_bootstrap_parse(boot, size);
	if (!vboot) {
		std::printf("BOOT invalid code=%s\n", to_string(vboot.error().code));
		return 0;
	}
	std::printf("BOOT abi=%u.%u map=%" PRIu64 " init=%s nonce=%016" PRIx64 "%016" PRIx64
	            " pid=%" PRIu64 " start=%" PRIu64 "\n",
	            boot.abi_major, boot.abi_minor, boot.expected_mapping_size,
	            init_state_name(boot.init_state), boot.creator_nonce_hi, boot.creator_nonce_lo,
	            boot.creator_pid, boot.creator_proc_start_ticks);

	// A partial object (creator killed mid-create, design §9.1) has no full
	// header yet; the bootstrap is all we can say about it.
	if (size < detail::kChannelHeaderOffset + sizeof(detail::ChannelHeaderAbi)) {
		std::printf("HEADER absent (partial object)\n");
		return 0;
	}

	auto mm = detail::mmap_region(fd, size);
	if (!mm) {
		std::printf("STATUS mmap_failed code=%s errno=%d\n", to_string(mm.error().code),
		            mm.error().errno_value);
		return 1;
	}
	auto* base = static_cast<std::byte*>(mm.value().get());
	auto* h = reinterpret_cast<detail::ChannelHeaderAbi*>(base + detail::kChannelHeaderOffset);

	if (std::memcmp(h->magic, detail::kChannelHeaderMagic, 8) != 0) {
		std::printf("HEADER magic_mismatch\n");
		return 0;
	}

	const uint32_t payload_size = h->payload_size;
	std::printf(
	        "HEADER abi=%u.%u endian=%s slot_count=%u payload=%u max=%u "
	        "map=%" PRIu64 " gen=%" PRIu64 " nonce=%016" PRIx64 "%016" PRIx64
	        " schema_v=%u fp=%016" PRIx64 "\n",
	        h->abi_major, h->abi_minor,
	        h->endian_marker == detail::kEndianMarker ? "ok" : "wrong", h->slot_count,
	        payload_size, h->max_payload_size, h->mapping_size, h->generation,
	        h->instance_nonce_hi, h->instance_nonce_lo, h->schema_version,
	        static_cast<uint64_t>(h->schema_fingerprint[0]) << 56 |
	                static_cast<uint64_t>(h->schema_fingerprint[1]) << 48 |
	                static_cast<uint64_t>(h->schema_fingerprint[2]) << 40 |
	                static_cast<uint64_t>(h->schema_fingerprint[3]) << 32 |
	                static_cast<uint64_t>(h->schema_fingerprint[4]) << 24 |
	                static_cast<uint64_t>(h->schema_fingerprint[5]) << 16 |
	                static_cast<uint64_t>(h->schema_fingerprint[6]) << 8 |
	                static_cast<uint64_t>(h->schema_fingerprint[7]));

	const uint32_t init_state = detail::shared_load_acquire(&h->init_state);
	const uint32_t pstate = detail::shared_load_acquire(&h->producer_state);
	const uint32_t cstate = detail::shared_load_acquire(&h->consumer_state);
	const uint64_t ticket = detail::shared_load_acquire(&h->latest_ticket);
	const uint64_t publish_count = detail::shared_load_acquire(&h->publish_count);
	const uint64_t read_count = detail::shared_load_acquire(&h->read_count);
	const uint64_t last_pub = detail::shared_load_acquire(&h->last_publish_boot_ns);
	const uint64_t notify = detail::shared_load_acquire(&h->notify_epoch);

	std::printf("STATE init=%s producer=%s consumer=%s notify=%" PRIu64 "\n",
	            init_state_name(init_state), endpoint_state_name(pstate),
	            endpoint_state_name(cstate), notify);
	std::printf("COUNTERS publish=%" PRIu64 " read=%" PRIu64 " last_pub_ns=%" PRIu64 "\n",
	            publish_count, read_count, last_pub);
	std::printf("TICKET seq=%" PRIu64 " slot=%u\n", detail::ticket_sequence(ticket),
	            detail::ticket_slot(ticket));

	print_identity_line("PRODUCER", detail::identity_snapshot_read(&h->producer), pstate);
	print_identity_line("CONSUMER", detail::identity_snapshot_read(&h->consumer), cstate);

	// Slot dump (metadata only; payload is never read).
	if (payload_size > 0 && payload_size <= detail::kMaxPayloadSize) {
		uint64_t slot_stride = 0;
		if (detail::round_up_to_multiple_u64(detail::kSlotHeaderSize + payload_size, 64,
		                                     &slot_stride)) {
			for (uint32_t i = 0; i < detail::kSlotCount; ++i) {
				uint64_t off = 0;
				if (!detail::slot_byte_offset(i, slot_stride, &off)) break;
				if (off + detail::kSlotHeaderSize > size) {
					std::printf("SLOT i=%u truncated\n", i);
					break;
				}
				const auto* s =
				        reinterpret_cast<const detail::SlotHeaderAbi*>(base + off);
				std::printf("SLOT i=%u state=%s seq=%" PRIu64 " publish_ns=%" PRIu64
				            " reader_epoch=%" PRIu64 "\n",
				            i,
				            slot_state_name(detail::shared_load_relaxed(&s->state)),
				            detail::shared_load_relaxed(&s->sample_sequence),
				            detail::shared_load_relaxed(&s->publish_boot_ns),
				            detail::shared_load_relaxed(&s->reader_role_epoch));
			}
		}
	}
	return 0;
}

// ---- remove ---------------------------------------------------------------

// Parses exactly 16 hex chars at s (the first half of a 32-char nonce is not
// NUL-terminated, so no length check — the caller bounds it). False on any
// non-hex digit.
bool parse_u64_hex16(const char* s, uint64_t* out) {
	uint64_t v = 0;
	for (int i = 0; i < 16; ++i) {
		const char c = s[i];
		unsigned digit = 0;
		if (c >= '0' && c <= '9') {
			digit = static_cast<unsigned>(c - '0');
		} else if (c >= 'a' && c <= 'f') {
			digit = static_cast<unsigned>(c - 'a' + 10);
		} else if (c >= 'A' && c <= 'F') {
			digit = static_cast<unsigned>(c - 'A' + 10);
		} else {
			return false;
		}
		v = (v << 4) | digit;
	}
	*out = v;
	return true;
}

// --expect-nonce accepts 32 hex chars (128-bit nonce): hi = first 16 chars.
bool parse_nonce_hex(const char* hex, uint64_t* hi, uint64_t* lo) {
	if (hex == nullptr || std::strlen(hex) != 32) return false;
	return parse_u64_hex16(hex, hi) && parse_u64_hex16(hex + 16, lo);
}

int cmd_remove(const std::string& channel_name, bool has_expect_gen, uint64_t expect_gen,
               bool has_expect_nonce, uint64_t expect_hi, uint64_t expect_lo) {
	const std::string shm_name = detail::channel_shm_name(channel_name);
	const std::string lock_path = detail::channel_lock_path(channel_name);
	const uint32_t name_hash =
	        detail::channel_name_hash(channel_name.c_str(), channel_name.size());

	auto lock_res = detail::ControlLock::acquire(lock_path);
	if (!lock_res) {
		std::printf("REMOVE lock_failed code=%s errno=%d op=%s\n",
		            to_string(lock_res.error().code), lock_res.error().errno_value,
		            lock_res.error().operation);
		return 2;
	}
	auto lock = std::move(lock_res.value());

	auto jr = lock.read_journal();
	if (!jr) {
		std::printf("REMOVE journal_read_failed code=%s\n", to_string(jr.error().code));
		return 2;
	}
	const detail::ControlJournalV1 journal = jr.value();

	if (journal.channel_hash != 0 && journal.channel_hash != name_hash) {
		std::printf("REFUSE journal_channel_mismatch\n");
		return 2;
	}

	auto fd_res = detail::shm_open_existing(shm_name);
	if (!fd_res) {
		if (fd_res.error().code == ErrorCode::kNotFound) {
			std::printf("REMOVED already_absent\n");
			return 0;
		}
		std::printf("OPEN_FAIL code=%s errno=%d op=%s\n", to_string(fd_res.error().code),
		            fd_res.error().errno_value, fd_res.error().operation);
		return 2;
	}
	auto fd = std::move(fd_res.value());

	uint64_t dev = 0, ino = 0, size = 0;
	auto fst = detail::shm_fstat_and_capture(fd, &dev, &ino, &size);
	if (!fst) {
		std::printf("REMOVE fstat_failed code=%s\n", to_string(fst.error().code));
		return 2;
	}

	// The journal's in-flight target must be the object we are about to unlink.
	if (journal.state != static_cast<uint32_t>(detail::JournalState::kIdle) &&
	    journal.target_ino != 0 && journal.target_ino != ino) {
		std::printf("REFUSE inode_mismatch journal_target=%" PRIu64 " obj=%" PRIu64 "\n",
		            journal.target_ino, ino);
		return 2;
	}

	// Recover the object's binding identity.
	detail::BootstrapHeaderAbi boot{};
	auto rboot = detail::pread_full(fd.get(), &boot, sizeof(boot), 0);
	const bool boot_ok = rboot && detail::validate_bootstrap_parse(boot, size);

	bool ready = false;
	uint64_t obj_gen = 0, obj_nonce_hi = 0, obj_nonce_lo = 0;
	uint64_t prod_pid = 0, prod_start = 0;
	bool has_prod_id = false;
	bool bound = false;

	if (boot_ok &&
	    static_cast<detail::InitState>(boot.init_state) == detail::InitState::kReady &&
	    size >= detail::kChannelHeaderOffset + sizeof(detail::ChannelHeaderAbi)) {
		auto mm = detail::mmap_region(fd, size);
		if (!mm) {
			std::printf("REMOVE mmap_failed code=%s errno=%d\n",
			            to_string(mm.error().code), mm.error().errno_value);
			return 2;
		}
		auto* base = static_cast<std::byte*>(mm.value().get());
		auto* h = reinterpret_cast<detail::ChannelHeaderAbi*>(base +
		                                                      detail::kChannelHeaderOffset);
		if (std::memcmp(h->magic, detail::kChannelHeaderMagic, 8) != 0) {
			std::printf("REFUSE header_magic_mismatch\n");
			return 2;
		}
		ready = true;
		obj_gen = detail::shared_load_acquire(&h->generation);
		obj_nonce_hi = detail::shared_load_acquire(&h->instance_nonce_hi);
		obj_nonce_lo = detail::shared_load_acquire(&h->instance_nonce_lo);
		auto pid = detail::identity_snapshot_read(&h->producer);
		if (pid) {
			has_prod_id = true;
			prod_pid = pid.value().pid;
			prod_start = pid.value().proc_start_ticks;
		}
		if (has_expect_gen && obj_gen != expect_gen) {
			std::printf("REFUSE generation_mismatch want=%" PRIu64 " got=%" PRIu64 "\n",
			            expect_gen, obj_gen);
			return 2;
		}
		bound = true;
	} else if (boot_ok) {
		// Partial object (design §9.1): no generation yet; the bootstrap creator
		// nonce is the authoritative binding. --expect-nonce is required.
		std::printf("REMOVE partial_object size=%" PRIu64 " init=%s\n", size,
		            init_state_name(boot.init_state));
		if (journal.state != static_cast<uint32_t>(detail::JournalState::kIdle) &&
		    journal.new_nonce_hi != 0 &&
		    (journal.new_nonce_hi != boot.creator_nonce_hi ||
		     journal.new_nonce_lo != boot.creator_nonce_lo)) {
			std::printf("REFUSE journal_nonce_mismatch\n");
			return 2;
		}
		obj_nonce_hi = boot.creator_nonce_hi;
		obj_nonce_lo = boot.creator_nonce_lo;
		bound = true;
	} else if (size == 0) {
		// Narrow-window pre-object (design §9.1): the creator died right after
		// shm_open, before the bootstrap was written. The only identity binding is
		// the journal's in-flight new_nonce; failing that, an explicit
		// --expect-nonce the operator is willing to vouch for.
		std::printf("REMOVE preobject size=0\n");
		if (journal.new_nonce_hi != 0 || journal.new_nonce_lo != 0) {
			obj_nonce_hi = journal.new_nonce_hi;
			obj_nonce_lo = journal.new_nonce_lo;
			bound = true;
		} else if (has_expect_nonce) {
			obj_nonce_hi = expect_hi;
			obj_nonce_lo = expect_lo;
			bound = true;
		} else {
			std::printf(
			        "REFUSE preobject_no_binding "
			        "(journal has no nonce; pass --expect-nonce to confirm removal)\n");
			return 2;
		}
	} else {
		std::printf("REFUSE object_corrupt\n");
		return 2;
	}

	if (!bound) {
		std::printf("REFUSE no_identity_binding\n");
		return 2;
	}
	if (has_expect_nonce && (obj_nonce_hi != expect_hi || obj_nonce_lo != expect_lo)) {
		std::printf("REFUSE nonce_mismatch\n");
		return 2;
	}

	// The Idle journal records the completed instance identity; a channel that
	// was replaced since must not be silently removed.
	if (ready && journal.state == static_cast<uint32_t>(detail::JournalState::kIdle) &&
	    journal.new_generation != 0 &&
	    (journal.new_generation != obj_gen || journal.new_nonce_hi != obj_nonce_hi ||
	     journal.new_nonce_lo != obj_nonce_lo)) {
		std::printf("REFUSE journal_instance_mismatch\n");
		return 2;
	}

	// Refuse while the producer is provably alive (design §22.2); a dead /
	// pid-reused producer is safe to replace.
	if (ready) {
		if (has_prod_id && prod_pid != 0) {
			const detail::Liveness lv = detail::probe_liveness(prod_pid, prod_start);
			if (lv == detail::Liveness::kAlive) {
				std::printf("REFUSE producer_alive pid=%" PRIu64 "\n", prod_pid);
				return 2;
			}
			if (lv == detail::Liveness::kUnverifiable) {
				std::printf("REFUSE producer_unverifiable\n");
				return 2;
			}
			if (lv == detail::Liveness::kPidReused) {
				std::printf("PRODUCER pid_reused_proceed\n");
			}
		}
	}

	// Verified removal transaction (design §9.4): REMOVING journal -> inode
	// checked unlink -> Idle journal recording the removed instance for audit.
	auto remove_rec = detail::make_control_journal(
	        channel_name, detail::JournalState::kRemoving, obj_gen, 0, obj_nonce_hi,
	        obj_nonce_lo, 0, 0, detail::current_process_identity());
	remove_rec.target_dev = dev;
	remove_rec.target_ino = ino;
	auto wr = lock.write_journal(remove_rec);
	if (!wr) {
		std::printf("REMOVE journal_write_failed code=%s\n", to_string(wr.error().code));
		return 2;
	}

	auto un = detail::shm_unlink_checked(shm_name, dev, ino);
	if (!un) {
		auto idle = detail::make_control_journal(channel_name, detail::JournalState::kIdle,
		                                         0, 0, 0, 0, 0, 0,
		                                         detail::current_process_identity());
		(void)lock.write_journal(idle);
		std::printf("REMOVE unlink_failed code=%s\n", to_string(un.error().code));
		return 2;
	}

	auto done = detail::make_control_journal(channel_name, detail::JournalState::kIdle, obj_gen,
	                                         obj_gen, obj_nonce_hi, obj_nonce_lo, obj_nonce_hi,
	                                         obj_nonce_lo, detail::current_process_identity());
	done.target_dev = dev;
	done.target_ino = ino;
	(void)lock.write_journal(done);

	std::printf("REMOVED name=%s inode=%" PRIu64 " generation=%" PRIu64 " nonce=%016" PRIx64
	            "%016" PRIx64 "\n",
	            shm_name.c_str(), ino, obj_gen, obj_nonce_hi, obj_nonce_lo);
	return 0;
}

}  // namespace

int main(int argc, char** argv) {
	if (argc < 3) {
		std::fprintf(stderr,
		             "usage: edge_shm_ctl inspect <name>\n"
		             "       edge_shm_ctl remove  <name> "
		             "[--expect-generation N] [--expect-nonce HEX32]\n");
		return 2;
	}
	const std::string sub = argv[1];
	const std::string name = argv[2];

	if (sub == "inspect") {
		return cmd_inspect(name);
	}
	if (sub == "remove") {
		const char* gen = edge_tool::arg_value(argc, argv, "--expect-generation");
		uint64_t expect_gen = 0;
		const bool has_gen =
		        gen != nullptr && (std::sscanf(gen, "%" SCNu64, &expect_gen) == 1);
		if (gen != nullptr && !has_gen) {
			std::fprintf(stderr, "bad --expect-generation: %s\n", gen);
			return 2;
		}
		const char* nonce = edge_tool::arg_value(argc, argv, "--expect-nonce");
		uint64_t expect_hi = 0, expect_lo = 0;
		const bool has_nonce = parse_nonce_hex(nonce, &expect_hi, &expect_lo);
		if (nonce != nullptr && !has_nonce) {
			std::fprintf(stderr,
			             "bad --expect-nonce (need 32 hex chars, 128-bit): %s\n",
			             nonce);
			return 2;
		}
		return cmd_remove(name, has_gen, expect_gen, has_nonce, expect_hi, expect_lo);
	}

	std::fprintf(stderr, "unknown subcommand: %s\n", sub.c_str());
	return 2;
}
