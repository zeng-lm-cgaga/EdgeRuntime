#ifndef EDGE_RUNTIME_TOOL_COMMON_HPP
#define EDGE_RUNTIME_TOOL_COMMON_HPP

// 工具共用参数解析、时钟、CPU 时间和 Schema 构造，输出格式由驱动程序消费。
#include <sys/resource.h>
#include <time.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "edge_runtime/common/schema.hpp"

namespace edge_tool {

inline int64_t monotonic_ms_now() {
	struct timespec ts {};
	::clock_gettime(CLOCK_MONOTONIC, &ts);
	return static_cast<int64_t>(ts.tv_sec) * 1000 + static_cast<int64_t>(ts.tv_nsec / 1000000);
}

inline uint64_t monotonic_raw_now_ns() {
	struct timespec ts {};
	::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
	return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

inline uint64_t cpu_us_now() {
	struct rusage ru {};
	::getrusage(RUSAGE_SELF, &ru);
	return static_cast<uint64_t>(ru.ru_utime.tv_sec) * 1000000ull +
	       static_cast<uint64_t>(ru.ru_utime.tv_usec) +
	       static_cast<uint64_t>(ru.ru_stime.tv_sec) * 1000000ull +
	       static_cast<uint64_t>(ru.ru_stime.tv_usec);
}

inline uint64_t monotonic_deadline_ns(uint64_t delta_ns) noexcept {
	struct timespec ts {};
	::clock_gettime(CLOCK_MONOTONIC, &ts);
	const uint64_t now = static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
	                     static_cast<uint64_t>(ts.tv_nsec);
	return now + delta_ns;
}

class RatePacer {
       public:
	explicit RatePacer(uint64_t hz) noexcept : enabled_(hz > 0) {
		period_ns_ = enabled_ ? 1000000000ull / hz : 0;
	}
	void start() noexcept { next_ = monotonic_deadline_ns(period_ns_); }
	void wait_until_next() noexcept {
		if (!enabled_) return;
		for (;;) {
			struct timespec ts {};
			ts.tv_sec = static_cast<time_t>(next_ / 1000000000ull);
			ts.tv_nsec = static_cast<long>(next_ % 1000000000ull);
			const int rc =
			        ::clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, nullptr);
			if (rc == 0 || rc == EINTR) break;
			if (rc == EINVAL) {

				while (monotonic_deadline_ns(0) < next_) {
				}
				break;
			}
			break;
		}
		next_ += period_ns_;
	}

       private:
	bool enabled_ = false;
	uint64_t period_ns_ = 0;
	uint64_t next_ = 0;
};

inline const char* arg_value(int argc, char** argv, const char* name) {
	for (int i = 1; i + 1 < argc; ++i) {
		if (std::strcmp(argv[i], name) == 0) return argv[i + 1];
	}
	return nullptr;
}

inline bool arg_flag(int argc, char** argv, const char* name) {
	for (int i = 1; i < argc; ++i) {
		if (std::strcmp(argv[i], name) == 0) return true;
	}
	return false;
}

inline uint64_t arg_u64(int argc, char** argv, const char* name, uint64_t fallback) {
	const char* v = arg_value(argc, argv, name);
	if (v == nullptr) return fallback;
	char* end = nullptr;
	const unsigned long long parsed = std::strtoull(v, &end, 10);
	if (end == v || *end != '\0') {
		std::fprintf(stderr, "bad numeric value for %s: %s\n", name, v);
		std::exit(2);
	}
	return static_cast<uint64_t>(parsed);
}

inline std::array<std::byte, 32> parse_fingerprint_hex(const char* hex) {
	std::array<std::byte, 32> fp{};
	if (hex == nullptr || std::strlen(hex) != 64) {
		std::fprintf(stderr, "missing/invalid --schema-hex (need 64 hex chars)\n");
		std::exit(2);
	}
	for (size_t i = 0; i < 32; ++i) {
		unsigned int byte = 0;
		char two[3] = {hex[2 * i], hex[2 * i + 1], '\0'};
		if (std::sscanf(two, "%2x", &byte) != 1) {
			std::fprintf(stderr, "bad --schema-hex at byte %zu\n", i);
			std::exit(2);
		}
		fp[i] = static_cast<std::byte>(byte);
	}
	return fp;
}

inline edge_runtime::SchemaDescriptor schema_from_args(int argc, char** argv,
                                                       const char* debug_name) {
	const char* hex = arg_value(argc, argv, "--schema-hex");
	const uint64_t version = arg_u64(argc, argv, "--schema-version", 1);
	if (version > 0xFFFFFFFFull) {
		std::fprintf(stderr, "--schema-version out of range\n");
		std::exit(2);
	}
	return edge_runtime::SchemaDescriptor{parse_fingerprint_hex(hex),
	                                      static_cast<uint32_t>(version), debug_name};
}

}

#endif
