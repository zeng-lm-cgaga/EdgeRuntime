#include "edge_runtime/detail/shm_object.hpp"

#include <fcntl.h>
#include <sys/stat.h>

#include <cerrno>
#include <cstdint>
#include <cstring>

namespace edge_runtime::detail {

namespace {
constexpr mode_t kExpectedShmMode = S_IRUSR | S_IWUSR;  // 0600
}  // namespace

Result<UniqueFd> shm_open_create(const std::string& posix_name) {
	const int fd = ::shm_open(posix_name.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
	                          kExpectedShmMode);
	if (fd < 0) {
		return make_error(classify_errno(errno), "shm_open_create", std::strerror(errno));
	}
	return Result<UniqueFd>(UniqueFd(fd));
}

Result<UniqueFd> shm_open_existing(const std::string& posix_name) {
	const int fd = ::shm_open(posix_name.c_str(), O_RDWR | O_CLOEXEC, 0);
	if (fd < 0) {
		return make_error(classify_errno(errno), "shm_open_existing", std::strerror(errno));
	}
	return Result<UniqueFd>(UniqueFd(fd));
}

Result<void> shm_truncate(const UniqueFd& fd, uint64_t size) {
	if (size > static_cast<uint64_t>(INT64_MAX)) {
		return make_error(ErrorCode::kInvalidOptions, "shm_truncate", "size exceeds off_t");
	}
	if (::ftruncate(fd.get(), static_cast<off_t>(size)) != 0) {
		return make_error(classify_errno(errno), "shm_truncate", std::strerror(errno));
	}
	return Result<void>::ok();
}

Result<MappedRegion> mmap_region(const UniqueFd& fd, uint64_t size) {
	void* addr = ::mmap(nullptr, static_cast<size_t>(size), PROT_READ | PROT_WRITE, MAP_SHARED,
	                    fd.get(), 0);
	if (addr == MAP_FAILED) {
		return make_error(classify_errno(errno), "mmap_region", std::strerror(errno));
	}
	return Result<MappedRegion>(MappedRegion(addr, static_cast<size_t>(size)));
}

Result<void> shm_fstat_and_capture(const UniqueFd& fd, uint64_t* dev, uint64_t* ino,
                                   uint64_t* size) {
	struct stat st {};
	if (::fstat(fd.get(), &st) != 0) {
		return make_error(classify_errno(errno), "shm_fstat_and_capture",
		                  std::strerror(errno));
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

Result<void> shm_unlink_checked(const std::string& posix_name, uint64_t expected_dev,
                                uint64_t expected_ino) {
	// Reopen and revalidate immediately before unlink (design §9.4 step 3).
	auto re = shm_open_existing(posix_name);
	if (!re) return re.error();
	uint64_t dev = 0;
	uint64_t ino = 0;
	uint64_t size = 0;
	auto fst = shm_fstat_and_capture(re.value(), &dev, &ino, &size);
	if (!fst) return fst.error();
	if (dev != expected_dev || ino != expected_ino) {
		return make_error(ErrorCode::kNameRaceDetected, "shm_unlink_checked",
		                  "inode changed before unlink");
	}
	if (::shm_unlink(posix_name.c_str()) != 0) {
		return make_error(classify_errno(errno), "shm_unlink_checked",
		                  std::strerror(errno));
	}
	// The name must now be ENOENT; a new object already present is a race.
	const int probe = ::shm_open(posix_name.c_str(), O_RDWR | O_CLOEXEC, 0);
	if (probe >= 0) {
		::close(probe);
		return make_error(ErrorCode::kNameRaceDetected, "shm_unlink_checked",
		                  "new object appeared after unlink");
	}
	if (errno != ENOENT) {
		return make_error(classify_errno(errno), "shm_unlink_checked",
		                  std::strerror(errno));
	}
	return Result<void>::ok();
}

}  // namespace edge_runtime::detail
