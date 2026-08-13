#ifndef EDGE_RUNTIME_DETAIL_SHM_OBJECT_HPP
#define EDGE_RUNTIME_DETAIL_SHM_OBJECT_HPP

#include <sys/mman.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "edge_runtime/result.hpp"

namespace edge_runtime::detail {

class UniqueFd {
       public:
	UniqueFd() noexcept = default;
	explicit UniqueFd(int fd) noexcept : fd_(fd) {}
	~UniqueFd() { reset(); }

	UniqueFd(const UniqueFd&) = delete;
	UniqueFd& operator=(const UniqueFd&) = delete;
	UniqueFd(UniqueFd&& other) noexcept : fd_(other.release()) {}
	UniqueFd& operator=(UniqueFd&& other) noexcept {
		if (this != &other) reset(other.release());
		return *this;
	}

	int get() const noexcept { return fd_; }
	int release() noexcept {
		const int fd = fd_;
		fd_ = -1;
		return fd;
	}
	void reset(int fd = -1) noexcept {
		if (fd_ >= 0) ::close(fd_);
		fd_ = fd;
	}

       private:
	int fd_ = -1;
};

class MappedRegion {
       public:
	MappedRegion() noexcept = default;
	MappedRegion(void* addr, size_t size) noexcept : addr_(addr), size_(size) {}
	~MappedRegion() { reset(); }

	MappedRegion(const MappedRegion&) = delete;
	MappedRegion& operator=(const MappedRegion&) = delete;
	MappedRegion(MappedRegion&& other) noexcept : addr_(other.addr_), size_(other.size_) {
		other.addr_ = nullptr;
		other.size_ = 0;
	}
	MappedRegion& operator=(MappedRegion&& other) noexcept {
		if (this != &other) {
			reset();
			addr_ = other.addr_;
			size_ = other.size_;
			other.addr_ = nullptr;
			other.size_ = 0;
		}
		return *this;
	}

	void* get() const noexcept { return addr_; }
	size_t size() const noexcept { return size_; }

	void reset() noexcept {
		if (addr_ != nullptr && addr_ != MAP_FAILED) ::munmap(addr_, size_);
		addr_ = nullptr;
		size_ = 0;
	}

       private:
	void* addr_ = nullptr;
	size_t size_ = 0;
};

// Owns one POSIX shared-memory object: name, fd, mapping and its
// dev/inode/size identity (design §7.3/§9.4).
struct ShmObject {
	std::string name;
	UniqueFd fd;
	MappedRegion mapping;
	uint64_t dev = 0;
	uint64_t ino = 0;
	uint64_t size = 0;
};

// Create a new named shm object (O_CREAT|O_EXCL, 0600). The object is created
// EMPTY (size 0): the caller truncates/extends it as its transaction proceeds.
// This preserves the design §9.1 narrow recovery window — a creator killed
// right after shm_open leaves a provably-nothing object (size 0) that a
// takeover can unlink, whereas any truncated object could be someone else's.
Result<UniqueFd> shm_open_create(const std::string& posix_name);

// Open an existing named shm object (O_RDWR|O_CLOEXEC).
Result<UniqueFd> shm_open_existing(const std::string& posix_name);

Result<void> shm_truncate(const UniqueFd& fd, uint64_t size);

Result<MappedRegion> mmap_region(const UniqueFd& fd, uint64_t size);

// Read-only mapping for a fd granted with read-only access (v0.2 §33.4: ctl
// inspect via the broker). PROT_READ only; writing through it would SIGSEGV.
Result<MappedRegion> mmap_region_readonly(const UniqueFd& fd, uint64_t size);

// fstat validation: owner uid == euid, mode 0600, regular file; captures
// st_dev/st_ino/st_size. Fails closed on any mismatch.
Result<void> shm_fstat_and_capture(const UniqueFd& fd, uint64_t* dev, uint64_t* ino,
                                   uint64_t* size);

// Inode-checked unlink (design §9.4): reopen the name, revalidate dev/ino,
// shm_unlink, then confirm the name is ENOENT. A new object under the same
// name is a race -> NameRaceDetected, never touched.
Result<void> shm_unlink_checked(const std::string& posix_name, uint64_t expected_dev,
                                uint64_t expected_ino);

// fstat validation for a memfd-backed object (v0.2, design §33.3). memfd fstat
// mode is typically 0700, so the exact-0600 rule of shm_fstat_and_capture does
// not apply; instead: regular file, owner uid == euid, and no group/other
// write/execute bits. Captures st_dev/st_ino/st_size.
Result<void> memfd_fstat_and_capture(const UniqueFd& fd, uint64_t* dev, uint64_t* ino,
                                     uint64_t* size);

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_SHM_OBJECT_HPP
