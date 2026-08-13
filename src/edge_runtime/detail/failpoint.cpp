#include "edge_runtime/detail/failpoint.hpp"

#if EDGERUNTIME_ENABLE_FAILPOINTS

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <vector>

namespace edge_runtime::detail {

extern "C" const FailpointRecord __start_edg_failpoints[];
extern "C" const FailpointRecord __stop_edg_failpoints[];

namespace {

enum class Mode : uint8_t { kStop, kCrash, kLog };

struct Activation {
	bool enabled = false;
	bool wildcard = false;  // EDGE_FAILPOINT=*
	const char* single = nullptr;
	uint64_t count = 1;  // fire on the COUNT-th hit (1-based)
	Mode mode = Mode::kStop;
};

Activation read_activation() noexcept {
	Activation a;
	const char* fp = std::getenv("EDGE_FAILPOINT");
	if (fp == nullptr || fp[0] == '\0') return a;  // disabled: failpoints inert
	a.enabled = true;
	a.wildcard = std::strcmp(fp, "*") == 0;
	a.single = fp;
	const char* mode = std::getenv("EDGE_FAILPOINT_MODE");
	if (mode != nullptr && std::strcmp(mode, "crash") == 0) {
		a.mode = Mode::kCrash;
	} else if (mode != nullptr && std::strcmp(mode, "log") == 0) {
		a.mode = Mode::kLog;
	}
	const char* count = std::getenv("EDGE_FAILPOINT_COUNT");
	if (count != nullptr && count[0] != '\0') {
		char* end = nullptr;
		const unsigned long long v = std::strtoull(count, &end, 10);
		if (end != count && v > 0) a.count = v;
	}
	return a;
}

// Lazy one-time activation read (C++11 magic static: thread-safe). Every hit
// after the first pays one load + branch when disabled.
Activation& activation() noexcept {
	static Activation act = read_activation();
	return act;
}

// Per-record hit counters so EDGE_FAILPOINT_COUNT=N fires on the N-th hit of a
// specific failpoint (crash-matrix: let a producer publish N-1 samples cleanly,
// then crash mid-publish).
struct HitTable {
	std::mutex mu;
	std::map<const FailpointRecord*, uint64_t> hits;
};

HitTable& hit_table() noexcept {
	static HitTable table;
	return table;
}

}  // namespace

void failpoint_trigger(const FailpointRecord* fp) noexcept {
	const Activation& act = activation();
	if (!act.enabled) return;
	const bool match = act.wildcard || std::strcmp(act.single, fp->id) == 0;
	if (!match) return;

	HitTable& table = hit_table();
	uint64_t nth = 1;
	{
		std::lock_guard<std::mutex> lock(table.mu);
		nth = ++table.hits[fp];
	}
	if (nth != act.count) return;  // not the configured hit yet

	// Flush stdout/stderr first so the crash matrix's captured output includes
	// everything the child emitted up to the kill point.
	std::fflush(stdout);
	std::fflush(stderr);
	switch (act.mode) {
		case Mode::kStop:
			::raise(SIGSTOP);  // parent confirms the crash state, then SIGKILLs
			break;
		case Mode::kCrash:
			::_exit(134);  // deterministic "SIGABRT" exit code; no core dump
			break;
		case Mode::kLog:
			std::fprintf(stderr, "FAILPOINT hit id=%s nth=%llu\n", fp->id,
			             static_cast<unsigned long long>(nth));
			std::fflush(stderr);
			break;
	}
}

size_t failpoint_list(const char* const** ids_out) noexcept {
	static const std::vector<const char*>* ids = [] {
		auto* v = new std::vector<const char*>();
		for (const FailpointRecord* p = __start_edg_failpoints; p != __stop_edg_failpoints;
		     ++p) {
			v->push_back(p->id);
		}
		return v;
	}();
	if (ids_out != nullptr) *ids_out = ids->data();
	return ids->size();
}

}  // namespace edge_runtime::detail

#endif  // EDGERUNTIME_ENABLE_FAILPOINTS
