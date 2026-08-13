#ifndef EDGE_RUNTIME_DETAIL_FAILPOINT_HPP
#define EDGE_RUNTIME_DETAIL_FAILPOINT_HPP

// Compile-time named failpoints (design §20.3, §22.3). Each EDGE_FAILPOINT(id)
// emits a readonly record into the link-section registry
// (`edg_failpoints`, delimited by the linker-provided __start_/__stop_
// symbols), so the toolchain can enumerate every wired failpoint with zero
// runtime registration. A hit consults the process environment once (lazily,
// on the first hit) and then only pays a branch:
//
//   EDGE_FAILPOINT=<id>       activate one failpoint by id
//   EDGE_FAILPOINT=*          activate every failpoint
//   EDGE_FAILPOINT_COUNT=N    fire on the N-th hit (default 1 = first hit)
//   EDGE_FAILPOINT_MODE=stop  SIGSTOP the process (crash-matrix default)
//   EDGE_FAILPOINT_MODE=crash _exit(134) deterministically
//   EDGE_FAILPOINT_MODE=log   print a marker and continue (wiring smoke test)
//
// When EDGERUNTIME_ENABLE_FAILPOINTS is not defined (Release), the macro
// compiles to nothing: production binaries carry no failpoint code, records,
// or environment reads.

#include <cstddef>

namespace edge_runtime::detail {

struct FailpointRecord {
	const char* id;  // never null; points at a string literal
};

}  // namespace edge_runtime::detail

#if EDGERUNTIME_ENABLE_FAILPOINTS

#include <cstdint>

namespace edge_runtime::detail {

// Linker-provided bounds of the `edg_failpoints` section. Only defined when at
// least one failpoint record exists in the link (guaranteed here: the library
// itself wires C01-C10), so failpoint-enabled binaries always have them.
extern "C" {
extern const FailpointRecord __start_edg_failpoints[];
extern const FailpointRecord __stop_edg_failpoints[];
}

// Check activation for `fp` and act (SIGSTOP / _exit / log). noexcept: a
// failpoint must never perturb a valid data path.
void failpoint_trigger(const FailpointRecord* fp) noexcept;

// Returns the array of every registered failpoint id (link-section scan) and
// its length. Used by edge_shm_crash_child --list and env validation.
size_t failpoint_list(const char* const** ids_out) noexcept;

}  // namespace edge_runtime::detail

#define EDGE_FAILPOINT(id)                                                          \
	do {                                                                        \
		static const ::edge_runtime::detail::FailpointRecord fp_record_##id \
		        __attribute__((section("edg_failpoints"), used)) = {#id};   \
		::edge_runtime::detail::failpoint_trigger(&fp_record_##id);         \
	} while (0)

#else  // EDGERUNTIME_ENABLE_FAILPOINTS

#define EDGE_FAILPOINT(id) \
	do {               \
	} while (0)

#endif  // EDGERUNTIME_ENABLE_FAILPOINTS

#endif  // EDGE_RUNTIME_DETAIL_FAILPOINT_HPP
