#include "edge_runtime/transport/shm_object.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>

#include <cerrno>
#include <cstdint>
#include <cstring>

namespace edge_runtime::detail {

namespace {
constexpr mode_t kExpectedShmMode = S_IRUSR | S_IWUSR;
constexpr int kMaxInterruptedAttempts = 8;

int bounded_fstat(int fd, struct stat* status) noexcept {
	int result = -1;
	for (int attempt = 0; attempt < kMaxInterruptedAttempts; ++attempt) {
		result = ::fstat(fd, status);
		if (result == 0 || errno != EINTR) break;
	}
	return result;
}
}

// 创建后先保持空对象，调用方完成截断、写头和 READY 提交后才对消费者可见。
Result<UniqueFd> shm_open_create(const std::string& posix_name) {
	const int fd = ::shm_open(posix_name.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
	                          kExpectedShmMode);
	if (fd < 0) {
		const int e = errno;
		return make_errno_error(e, "shm_open_create", std::strerror(e));
	}
	return Result<UniqueFd>(UniqueFd(fd));
}

Result<UniqueFd> shm_open_existing(const std::string& posix_name) {
	const int fd = ::shm_open(posix_name.c_str(), O_RDWR | O_CLOEXEC, 0);
	if (fd < 0) {
		const int e = errno;
		return make_errno_error(e, "shm_open_existing", std::strerror(e));
	}
	return Result<UniqueFd>(UniqueFd(fd));
}

Result<void> shm_truncate(const UniqueFd& fd, uint64_t size) {
	if (size > static_cast<uint64_t>(INT64_MAX)) {
		return make_error(ErrorCode::kInvalidOptions, "shm_truncate", "size exceeds off_t");
	}
	if (::ftruncate(fd.get(), static_cast<off_t>(size)) != 0) {
		const int e = errno;
		return make_errno_error(e, "shm_truncate", std::strerror(e));
	}
	return Result<void>::ok();
}

Result<MappedRegion> mmap_region(const UniqueFd& fd, uint64_t size) {
	if (size > static_cast<uint64_t>(SIZE_MAX)) {
		return make_error(ErrorCode::kInvalidOptions, "mmap_region", "size exceeds size_t");
	}
	void* addr = ::mmap(nullptr, static_cast<size_t>(size), PROT_READ | PROT_WRITE, MAP_SHARED,
	                    fd.get(), 0);
	if (addr == MAP_FAILED) {
		const int e = errno;
		return make_errno_error(e, "mmap_region", std::strerror(e));
	}
	return Result<MappedRegion>(MappedRegion(addr, static_cast<size_t>(size)));
}

Result<MappedRegion> mmap_region_readonly(const UniqueFd& fd, uint64_t size) {
	if (size > static_cast<uint64_t>(SIZE_MAX)) {
		return make_error(ErrorCode::kInvalidOptions, "mmap_region_readonly",
		                  "size exceeds size_t");
	}
	void* addr = ::mmap(nullptr, static_cast<size_t>(size), PROT_READ, MAP_SHARED, fd.get(), 0);
	if (addr == MAP_FAILED) {
		const int e = errno;
		return make_errno_error(e, "mmap_region_readonly", std::strerror(e));
	}
	return Result<MappedRegion>(MappedRegion(addr, static_cast<size_t>(size)));
}

Result<void> shm_fstat_and_capture(const UniqueFd& fd, uint64_t* dev, uint64_t* ino,
                                   uint64_t* size) {
	struct stat st {};
	if (bounded_fstat(fd.get(), &st) != 0) {
		const int e = errno;
		return make_errno_error(e, "shm_fstat_and_capture", std::strerror(e));
	}
	if (!S_ISREG(st.st_mode)) {
		return make_error(ErrorCode::kCorruptHeader, "shm_fstat_and_capture",
		                  "not a regular file");
	}
	if (st.st_uid != geteuid()) {
		return make_error(ErrorCode::kPermissionDenied, "shm_fstat_and_capture",
		                  "owner uid mismatch");
	}
	if ((st.st_mode & 0777) != kExpectedShmMode) {
		return make_error(ErrorCode::kPermissionDenied, "shm_fstat_and_capture",
		                  "unexpected mode");
	}
	if (dev != nullptr) *dev = static_cast<uint64_t>(st.st_dev);
	if (ino != nullptr) *ino = static_cast<uint64_t>(st.st_ino);
	if (size != nullptr) *size = static_cast<uint64_t>(st.st_size);
	return Result<void>::ok();
}

Result<void> shm_capture_created_identity(const UniqueFd& fd, uint64_t* dev, uint64_t* ino) {
	struct statx status {};
	int result = -1;
	for (int attempt = 0; attempt < kMaxInterruptedAttempts; ++attempt) {
		result = ::statx(fd.get(), "", AT_EMPTY_PATH | AT_STATX_SYNC_AS_STAT,
		                 STATX_BASIC_STATS, &status);
		if (result == 0 || errno != EINTR) break;
	}
	if (result != 0) {
		return make_errno_error(ErrorCode::kRecoveryBlocked, errno,
		                        "shm_capture_created_identity",
		                        "created object retained; identity unavailable");
	}
	constexpr unsigned int kRequired = STATX_TYPE | STATX_MODE | STATX_UID | STATX_INO;
	if ((status.stx_mask & kRequired) != kRequired || !S_ISREG(status.stx_mode) ||
	    status.stx_uid != geteuid() || (status.stx_mode & 0777) != kExpectedShmMode ||
	    status.stx_ino == 0) {
		return make_error(ErrorCode::kRecoveryBlocked, "shm_capture_created_identity",
		                  "created object retained; invalid identity");
	}
	*dev = static_cast<uint64_t>(makedev(status.stx_dev_major, status.stx_dev_minor));
	*ino = status.stx_ino;
	return Result<void>::ok();
}

Result<void> memfd_fstat_and_capture(const UniqueFd& fd, uint64_t* dev, uint64_t* ino,
                                     uint64_t* size) {
	struct stat st {};
	if (bounded_fstat(fd.get(), &st) != 0) {
		const int e = errno;
		return make_errno_error(e, "memfd_fstat_and_capture", std::strerror(e));
	}
	if (!S_ISREG(st.st_mode)) {
		return make_error(ErrorCode::kCorruptHeader, "memfd_fstat_and_capture",
		                  "not a regular file");
	}
	if (st.st_uid != geteuid()) {
		return make_error(ErrorCode::kPermissionDenied, "memfd_fstat_and_capture",
		                  "owner uid mismatch");
	}

	const mode_t forbidden = static_cast<mode_t>(S_IWGRP | S_IXGRP | S_IWOTH | S_IXOTH);
	if ((st.st_mode & forbidden) != 0) {
		return make_error(ErrorCode::kPermissionDenied, "memfd_fstat_and_capture",
		                  "unexpected mode");
	}
	if (dev != nullptr) *dev = static_cast<uint64_t>(st.st_dev);
	if (ino != nullptr) *ino = static_cast<uint64_t>(st.st_ino);
	if (size != nullptr) *size = static_cast<uint64_t>(st.st_size);
	return Result<void>::ok();
}

Result<void> shm_unlink_checked(const std::string& posix_name, uint64_t expected_dev,
                                uint64_t expected_ino) {

	auto re = shm_open_existing(posix_name);
	if (!re) return re.error();
	uint64_t dev = 0;
	uint64_t ino = 0;
	uint64_t size = 0;
	auto fst = shm_fstat_and_capture(re.value(), &dev, &ino, &size);
	if (!fst) {
		auto captured = shm_capture_created_identity(re.value(), &dev, &ino);
		if (!captured) return captured.error();
	}
	if (dev != expected_dev || ino != expected_ino) {
		return make_error(ErrorCode::kNameRaceDetected, "shm_unlink_checked",
		                  "inode changed before unlink");
	}
	if (::shm_unlink(posix_name.c_str()) != 0) {
		const int e = errno;
		return make_errno_error(e, "shm_unlink_checked", std::strerror(e));
	}

	const int probe = ::shm_open(posix_name.c_str(), O_RDWR | O_CLOEXEC, 0);
	if (probe >= 0) {
		::close(probe);
		return make_error(ErrorCode::kNameRaceDetected, "shm_unlink_checked",
		                  "new object appeared after unlink");
	}
	if (errno != ENOENT) {
		const int e = errno;
		return make_errno_error(e, "shm_unlink_checked", std::strerror(e));
	}
	return Result<void>::ok();
}

}
