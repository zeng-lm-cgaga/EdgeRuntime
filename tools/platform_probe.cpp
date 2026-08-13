#include <fcntl.h>
#include <gnu/libc-version.h>
#include <linux/futex.h>
#include <sys/mman.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>

namespace {

static_assert(sizeof(void*) == 8, "EdgeRuntime v0.1 requires a 64-bit process");
static_assert(__atomic_always_lock_free(sizeof(std::uint32_t), nullptr),
              "EdgeRuntime v0.1 requires lock-free 32-bit machine atomics");
static_assert(__atomic_always_lock_free(sizeof(std::uint64_t), nullptr),
              "EdgeRuntime v0.1 requires lock-free 64-bit machine atomics");

class ShmCleanup final {
       public:
	explicit ShmCleanup(std::string name) : name_(std::move(name)) {}
	~ShmCleanup() { shm_unlink(name_.c_str()); }

	ShmCleanup(const ShmCleanup&) = delete;
	ShmCleanup& operator=(const ShmCleanup&) = delete;

       private:
	std::string name_;
};

bool report(const char* capability, const bool ok, const std::string& detail) {
	std::cout << (ok ? "PASS" : "FAIL") << " " << capability << " " << detail << '\n';
	return ok;
}

bool check_kernel() {
	utsname info{};
	if (uname(&info) != 0) {
		return report("kernel", false, std::strerror(errno));
	}

	int major = 0;
	int minor = 0;
	const bool parsed = std::sscanf(info.release, "%d.%d", &major, &minor) == 2;
	const bool supported = parsed && (major > 5 || (major == 5 && minor >= 15));
	return report("kernel", supported, info.release);
}

bool check_endian() {
	const std::uint32_t marker = 0x01020304U;
	const auto* bytes = reinterpret_cast<const unsigned char*>(&marker);
	return report("little_endian", bytes[0] == 0x04U, bytes[0] == 0x04U ? "yes" : "no");
}

bool check_glibc() {
	int major = 0;
	int minor = 0;
	const char* version = gnu_get_libc_version();
	const bool parsed = std::sscanf(version, "%d.%d", &major, &minor) == 2;
	const bool supported = parsed && (major > 2 || (major == 2 && minor >= 35));
	return report("glibc", supported, version);
}

bool check_clock(const clockid_t id, const char* name) {
	timespec value{};
	if (clock_gettime(id, &value) != 0) {
		return report(name, false, std::strerror(errno));
	}
	return report(name, true, "available");
}

bool check_getrandom() {
	std::uint8_t nonce[16]{};
	const ssize_t bytes = getrandom(nonce, sizeof(nonce), 0);
	return report("getrandom", bytes == static_cast<ssize_t>(sizeof(nonce)),
	              std::string("bytes=") + std::to_string(bytes));
}

bool check_pidfd() {
#ifdef SYS_pidfd_open
	const int fd = static_cast<int>(syscall(SYS_pidfd_open, getpid(), 0));
	if (fd < 0) {
		return report("pidfd_open", false, std::strerror(errno));
	}
	close(fd);
	return report("pidfd_open", true, "current-process");
#else
	return report("pidfd_open", false, "SYS_pidfd_open unavailable");
#endif
}

bool check_futex() {
#ifdef SYS_futex
	alignas(4) std::uint32_t futex_word = 0;
	const long woken = syscall(SYS_futex, &futex_word, FUTEX_WAKE, 1, nullptr, nullptr, 0);
	return report("futex_shared_wake", woken == 0,
	              std::string("woken=") + std::to_string(woken));
#else
	return report("futex_shared_wake", false, "SYS_futex unavailable");
#endif
}

bool check_shm_lifecycle() {
	std::uint64_t random_suffix = 0;
	if (getrandom(&random_suffix, sizeof(random_suffix), 0) !=
	    static_cast<ssize_t>(sizeof(random_suffix))) {
		return report("shm_lifecycle", false, "getrandom failed");
	}

	const std::string name = "/edgeruntime.probe." + std::to_string(getuid()) + "." +
	                         std::to_string(getpid()) + "." + std::to_string(random_suffix);
	ShmCleanup cleanup(name);
	const int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
	if (fd < 0) {
		return report("shm_lifecycle", false,
		              std::string("shm_open: ") + std::strerror(errno));
	}

	constexpr std::size_t kMappingSize = 4096;
	if (ftruncate(fd, static_cast<off_t>(kMappingSize)) != 0) {
		const std::string detail = std::string("ftruncate: ") + std::strerror(errno);
		close(fd);
		return report("shm_lifecycle", false, detail);
	}

	void* mapping = mmap(nullptr, kMappingSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (mapping == MAP_FAILED) {
		const std::string detail = std::string("mmap: ") + std::strerror(errno);
		close(fd);
		return report("shm_lifecycle", false, detail);
	}

	auto* bytes = static_cast<std::uint8_t*>(mapping);
	bytes[0] = 0x5AU;
	const bool mapped = bytes[0] == 0x5AU;
	const bool unmapped = munmap(mapping, kMappingSize) == 0;
	const bool closed = close(fd) == 0;
	const bool unlinked = shm_unlink(name.c_str()) == 0;
	return report("shm_lifecycle", mapped && unmapped && closed && unlinked,
	              "create-truncate-map-unmap-close-unlink");
}

}  // namespace

int main() {
#if !defined(__linux__) || !defined(__x86_64__) || !defined(__GNUC__)
	std::cerr << "FAIL compile_contract requires Linux x86-64 and GCC-compatible atomics\n";
	return 1;
#endif

	bool ok = true;
	ok &= check_glibc();
	ok &= check_kernel();
	ok &= check_endian();
	ok &= report("atomic_u32_lock_free", true, "always");
	ok &= report("atomic_u64_lock_free", true, "always");
	ok &= check_clock(CLOCK_BOOTTIME, "clock_boottime");
	ok &= check_clock(CLOCK_MONOTONIC, "clock_monotonic");
	ok &= check_clock(CLOCK_MONOTONIC_RAW, "clock_monotonic_raw");
	ok &= check_getrandom();
	ok &= check_pidfd();
	ok &= check_futex();
	ok &= check_shm_lifecycle();
	return ok ? 0 : 1;
}
