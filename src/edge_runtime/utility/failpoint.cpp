#include "edge_runtime/utility/failpoint.hpp"

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
	bool wildcard = false;
	const char* single = nullptr;
	uint64_t count = 1;
	Mode mode = Mode::kStop;
};

Activation read_activation() noexcept {
	Activation a;
	const char* fp = std::getenv("EDGE_FAILPOINT");
	if (fp == nullptr || fp[0] == '\0') return a;
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

Activation& activation() noexcept {
	static Activation act = read_activation();
	return act;
}

struct HitTable {
	std::mutex mu;
	std::map<const FailpointRecord*, uint64_t> hits;
};

HitTable& hit_table() noexcept {
	static HitTable table;
	return table;
}

}

// 故障点只在首次命中时读取环境变量，命中后按次数执行停止、退出或记录。
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
	if (nth != act.count) return;

	std::fflush(stdout);
	std::fflush(stderr);
	switch (act.mode) {
		case Mode::kStop:
			::raise(SIGSTOP);
			break;
		case Mode::kCrash:
			::_exit(134);
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

}

#endif
