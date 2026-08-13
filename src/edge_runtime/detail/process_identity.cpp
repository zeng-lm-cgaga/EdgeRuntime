#include "edge_runtime/detail/process_identity.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace edge_runtime::detail {

namespace {

constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;

void fnv_hash_bytes(const uint8_t* data, size_t size, uint64_t* h0, uint64_t* h1) {
	uint64_t a = h0 != nullptr ? *h0 : kFnvOffset;
	uint64_t b = h1 != nullptr ? *h1 : (kFnvOffset ^ 0x9E3779B97F4A7C15ULL);
	for (size_t i = 0; i < size; ++i) {
		const uint8_t byte = data[i];
		a = (a ^ byte) * kFnvPrime;
		b = (b ^ static_cast<uint8_t>(byte + 1u)) * kFnvPrime;
	}
	if (h0 != nullptr) *h0 = a;
	if (h1 != nullptr) *h1 = b;
}

}  // namespace

Result<uint64_t> proc_stat_starttime(int pid) noexcept {
	char path[64];
	const int path_len = std::snprintf(path, sizeof(path), "/proc/%d/stat", pid);
	if (path_len < 0 || path_len >= static_cast<int>(sizeof(path))) {
		return make_error(ErrorCode::kSystemError, "proc_stat_starttime", "path too long");
	}

	char buf[4096];
	const int fd = ::open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0) {
		return make_error(classify_errno(errno), "proc_stat_starttime",
		                  std::strerror(errno));
	}
	const ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
	::close(fd);
	if (n <= 0) {
		return make_error(ErrorCode::kRecoveryBlocked, "proc_stat_starttime",
		                  "empty or unreadable stat");
	}
	buf[static_cast<size_t>(n)] = '\0';

	// comm is delimited by the LAST ')' in the line; fields after it are
	// state(3), ppid(4), ... starttime(22).
	char* close_paren = std::strrchr(buf, ')');
	if (close_paren == nullptr) {
		return make_error(ErrorCode::kRecoveryBlocked, "proc_stat_starttime",
		                  "malformed stat (no close paren)");
	}
	char* p = close_paren + 1;
	if (*p == ' ') ++p;
	// Skip fields 3..21 (19 fields) to reach field 22 (starttime).
	for (int i = 0; i < 19; ++i) {
		p = std::strchr(p, ' ');
		if (p == nullptr) {
			return make_error(ErrorCode::kRecoveryBlocked, "proc_stat_starttime",
			                  "malformed stat (too short)");
		}
		++p;
	}
	char* end = nullptr;
	const unsigned long long value = std::strtoull(p, &end, 10);
	if (end == p) {
		return make_error(ErrorCode::kRecoveryBlocked, "proc_stat_starttime",
		                  "malformed stat (starttime)");
	}
	return Result<uint64_t>(static_cast<uint64_t>(value));
}

void current_boot_id_hash(uint64_t* hi, uint64_t* lo) noexcept {
	const int fd = ::open("/proc/sys/kernel/random/boot_id", O_RDONLY | O_CLOEXEC);
	char buf[64]{};
	ssize_t n = -1;
	if (fd >= 0) {
		n = ::read(fd, buf, sizeof(buf) - 1);
		::close(fd);
	}
	const size_t len = n > 0 ? static_cast<size_t>(n) : 0;
	const auto* bytes = reinterpret_cast<const uint8_t*>(buf);
	uint64_t h0 = kFnvOffset;
	uint64_t h1 = kFnvOffset ^ 0x9E3779B97F4A7C15ULL;
	fnv_hash_bytes(bytes, len, &h0, &h1);
	if (hi != nullptr) *hi = h0;
	if (lo != nullptr) *lo = h1;
}

ProcessIdentity current_process_identity() noexcept {
	ProcessIdentity id;
	id.pid = static_cast<uint64_t>(getpid());
	auto start = proc_stat_starttime(getpid());
	if (start) id.proc_start_ticks = start.value();
	current_boot_id_hash(&id.boot_id_hash_hi, &id.boot_id_hash_lo);
	return id;
}

Liveness probe_liveness(uint64_t pid, uint64_t expected_start_ticks) noexcept {
	if (pid == 0 || pid > static_cast<uint64_t>(INT32_MAX)) {
		return Liveness::kExited;  // no owner registered / implausible pid
	}
	const int pidfd = static_cast<int>(::syscall(SYS_pidfd_open, static_cast<pid_t>(pid), 0));
	if (pidfd < 0) {
		if (errno == ESRCH) return Liveness::kExited;
		return Liveness::kUnverifiable;  // EPERM / hidepid: fail closed
	}
	struct pollfd pfd {};
	pfd.fd = pidfd;
	pfd.events = POLLIN;
	const int rc = ::poll(&pfd, 1, 0);
	::close(pidfd);
	if (rc > 0) return Liveness::kExited;  // pidfd readable => process exited
	if (rc < 0 && errno != EINTR) return Liveness::kUnverifiable;

	// Process is alive; cross-check starttime for PID reuse (design §7.2).
	auto start = proc_stat_starttime(static_cast<int>(pid));
	if (!start) return Liveness::kUnverifiable;  // /proc unreadable => fail closed
	if (start.value() == expected_start_ticks) return Liveness::kAlive;
	return Liveness::kPidReused;
}

bool identity_matches_current(const ProcessIdentity& id) noexcept {
	if (id.pid != static_cast<uint64_t>(getpid())) return false;
	auto start = proc_stat_starttime(getpid());
	if (!start) return false;
	if (start.value() != id.proc_start_ticks) return false;
	uint64_t hi = 0;
	uint64_t lo = 0;
	current_boot_id_hash(&hi, &lo);
	return hi == id.boot_id_hash_hi && lo == id.boot_id_hash_lo;
}

}  // namespace edge_runtime::detail
