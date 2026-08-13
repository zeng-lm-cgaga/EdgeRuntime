#ifndef EDGE_RUNTIME_DETAIL_PROCESS_IDENTITY_HPP
#define EDGE_RUNTIME_DETAIL_PROCESS_IDENTITY_HPP

#include <cstdint>

#include "edge_runtime/result.hpp"

namespace edge_runtime::detail {

// Cross-process process identity (design §7.2): pid + /proc starttime +
// boot-id hash. Used only for recovery disambiguation, never as security
// authentication.
struct ProcessIdentity {
	uint64_t pid = 0;
	uint64_t proc_start_ticks = 0;
	uint64_t boot_id_hash_hi = 0;
	uint64_t boot_id_hash_lo = 0;
};

enum class Liveness : uint8_t {
	kAlive = 0,         // pidfd live and starttime matches
	kExited = 1,        // pidfd readable or pidfd_open ESRCH
	kPidReused = 2,     // process alive but starttime differs
	kUnverifiable = 3,  // fail closed
};

// Parses field 22 (starttime) of /proc/<pid>/stat (U06). Returns an error on
// ESRCH/malformed input; handles comm containing ')' and spaces.
Result<uint64_t> proc_stat_starttime(int pid) noexcept;

// FNV-1a based 128-bit hash of /proc/sys/kernel/random/boot_id.
void current_boot_id_hash(uint64_t* hi, uint64_t* lo) noexcept;

ProcessIdentity current_process_identity() noexcept;

// pidfd one-shot probe cross-checked with /proc starttime (design §7.2).
// Contradictions fail closed as kUnverifiable (C12).
Liveness probe_liveness(uint64_t pid, uint64_t expected_start_ticks) noexcept;

bool identity_matches_current(const ProcessIdentity& id) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_PROCESS_IDENTITY_HPP
