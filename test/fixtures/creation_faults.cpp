#include "creation_faults.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <utility>

extern "C" int __real_fstat(int descriptor, struct stat* status);
extern "C" int __real_shm_open(const char* name, int flags, mode_t mode);
extern "C" int __real_statx(int descriptor, const char* path, int flags, unsigned int mask,
                            struct statx* status);

namespace edge_test {
namespace {

thread_local CreationFaults* active_faults = nullptr;

CreationObjectEvidence inspect_fd(int descriptor) noexcept {
	CreationObjectEvidence result;
	struct stat status {};
	if (descriptor < 0 || __real_fstat(descriptor, &status) != 0) {
		result.error = descriptor < 0 ? EBADF : errno;
		return result;
	}
	result.exists = true;
	result.dev = static_cast<uint64_t>(status.st_dev);
	result.ino = static_cast<uint64_t>(status.st_ino);
	result.size = static_cast<uint64_t>(status.st_size);
	result.links = static_cast<uint64_t>(status.st_nlink);
	return result;
}

bool unlink_owned(const std::string& name, int owned_descriptor) noexcept {
	const auto owned = inspect_fd(owned_descriptor);
	if (!owned.exists) return false;
	const int named_descriptor = __real_shm_open(name.c_str(), O_RDONLY | O_CLOEXEC, 0);
	if (named_descriptor < 0) return errno == ENOENT;
	const auto named = inspect_fd(named_descriptor);
	::close(named_descriptor);
	if (!named.exists || named.dev != owned.dev || named.ino != owned.ino) {
		errno = ESTALE;
		return false;
	}
	return ::shm_unlink(name.c_str()) == 0;
}

}  // namespace

std::string unique_creation_name() {
	static std::atomic<uint64_t> sequence{0};
	struct timespec now {};
	::clock_gettime(CLOCK_MONOTONIC, &now);
	return "creation_fault_" + std::to_string(::getpid()) + "_" + std::to_string(now.tv_nsec) +
	       "_" + std::to_string(sequence.fetch_add(1));
}

CreationFaults::CreationFaults(std::string posix_name, CreationFaultPlan plan)
    : posix_name_(std::move(posix_name)), plan_(plan) {
	if (active_faults != nullptr) {
		evidence_.fixture_error = EBUSY;
		return;
	}
	active_faults = this;
}

CreationFaults::~CreationFaults() {
	disarm();
	if (!cleanup_fixture_owned()) {
		std::fprintf(stderr, "fixture refused cleanup: %s dev=%llu ino=%llu errno=%d\n",
		             posix_name_.c_str(), static_cast<unsigned long long>(created_dev_),
		             static_cast<unsigned long long>(created_ino_), errno);
	}
	if (created_pin_ >= 0) ::close(created_pin_);
	if (sentinel_pin_ >= 0) ::close(sentinel_pin_);
}

void CreationFaults::disarm() noexcept {
	if (active_faults == this) active_faults = nullptr;
}

const CreationFaultEvidence& CreationFaults::evidence() const noexcept { return evidence_; }

CreationObjectEvidence CreationFaults::created_object() const noexcept {
	return inspect_fd(created_pin_);
}

CreationObjectEvidence CreationFaults::sentinel_object() const noexcept {
	return inspect_fd(sentinel_pin_);
}

CreationObjectEvidence CreationFaults::named_object() const noexcept {
	const int descriptor = __real_shm_open(posix_name_.c_str(), O_RDONLY | O_CLOEXEC, 0);
	if (descriptor < 0) {
		CreationObjectEvidence result;
		result.error = errno;
		return result;
	}
	const auto result = inspect_fd(descriptor);
	::close(descriptor);
	return result;
}

bool CreationFaults::sentinel_contents_match() const noexcept {
	const int descriptor = __real_shm_open(posix_name_.c_str(), O_RDONLY | O_CLOEXEC, 0);
	if (descriptor < 0) return false;
	const auto named = inspect_fd(descriptor);
	const auto sentinel = sentinel_object();
	char contents[sizeof(kCreationSentinelContents)]{};
	const ssize_t count = ::pread(descriptor, contents, sizeof(contents), 0);
	::close(descriptor);
	return named.exists && sentinel.exists && named.dev == sentinel.dev &&
	       named.ino == sentinel.ino && named.size == sizeof(kCreationSentinelContents) &&
	       count == static_cast<ssize_t>(sizeof(contents)) &&
	       std::memcmp(contents, kCreationSentinelContents, sizeof(contents)) == 0;
}

bool CreationFaults::original_fd_closed() const noexcept {
	return created_fd_ >= 0 && !matches_created_fd(created_fd_);
}

bool CreationFaults::cleanup_fixture_owned() noexcept {
	if (sentinel_pin_ >= 0) return unlink_owned(posix_name_, sentinel_pin_);
	if (created_pin_ >= 0) return unlink_owned(posix_name_, created_pin_);
	return true;
}

bool CreationFaults::matches_created_fd(int descriptor) const noexcept {
	if (descriptor != created_fd_ || created_pin_ < 0) return false;
	const auto actual = inspect_fd(descriptor);
	return actual.exists && actual.dev == created_dev_ && actual.ino == created_ino_;
}

void CreationFaults::observe_shm_open(const char* name, int flags, int descriptor) noexcept {
	if (descriptor < 0 || name == nullptr || posix_name_ != name ||
	    (flags & (O_CREAT | O_EXCL)) != (O_CREAT | O_EXCL)) {
		return;
	}
	++evidence_.exclusive_opens;
	if (created_pin_ >= 0) {
		evidence_.fixture_error = EEXIST;
		return;
	}
	created_fd_ = descriptor;
	created_pin_ = ::fcntl(descriptor, F_DUPFD_CLOEXEC, 128);
	const auto created = created_object();
	if (!created.exists) {
		evidence_.fixture_error = created.error;
		return;
	}
	created_dev_ = created.dev;
	created_ino_ = created.ino;
	struct statx status {};
	evidence_.statx_available =
	        __real_statx(descriptor, "", AT_EMPTY_PATH, STATX_BASIC_STATS, &status) == 0 &&
	        (status.stx_mask & STATX_INO) != 0 && status.stx_ino == created.ino;
}

bool CreationFaults::install_sentinel() noexcept {
	if (!unlink_owned(posix_name_, created_pin_)) {
		evidence_.fixture_error = errno;
		return false;
	}
	sentinel_pin_ =
	        __real_shm_open(posix_name_.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
	if (sentinel_pin_ < 0 ||
	    ::ftruncate(sentinel_pin_, static_cast<off_t>(sizeof(kCreationSentinelContents))) !=
	            0 ||
	    ::pwrite(sentinel_pin_, kCreationSentinelContents, sizeof(kCreationSentinelContents),
	             0) != static_cast<ssize_t>(sizeof(kCreationSentinelContents))) {
		evidence_.fixture_error = errno == 0 ? EIO : errno;
		return false;
	}
	return true;
}

bool CreationFaults::inject_fstat(int descriptor) noexcept {
	if (!matches_created_fd(descriptor)) {
		++evidence_.other_fstat_calls;
		return false;
	}
	++evidence_.target_fstat_calls;
	if (evidence_.target_fstat_calls > kCreationFaultCallBudget) {
		evidence_.budget_exceeded = true;
		return false;
	}
	if (plan_.fault == CreationFault::kOnceEintr && evidence_.fstat_fault_hits != 0) {
		return false;
	}
	if (plan_.replace_with_sentinel && evidence_.fstat_fault_hits == 0) {
		(void)install_sentinel();
	}
	++evidence_.fstat_fault_hits;
	errno = plan_.fault == CreationFault::kPersistentEio ? EIO : EINTR;
	return true;
}

bool CreationFaults::inject_statx(int descriptor, const char* path, int flags) noexcept {
	if (!matches_created_fd(descriptor) || path == nullptr || path[0] != '\0' ||
	    (flags & AT_EMPTY_PATH) == 0) {
		return false;
	}
	++evidence_.target_statx_calls;
	if (evidence_.target_statx_calls > kCreationFaultCallBudget) {
		evidence_.budget_exceeded = true;
		return false;
	}
	if (!plan_.deny_statx) return false;
	++evidence_.statx_fault_hits;
	errno = ENOSYS;
	return true;
}

}  // namespace edge_test

extern "C" int __wrap_shm_open(const char* name, int flags, mode_t mode) {
	const int descriptor = __real_shm_open(name, flags, mode);
	const int saved_errno = errno;
	if (edge_test::active_faults != nullptr) {
		edge_test::active_faults->observe_shm_open(name, flags, descriptor);
	}
	errno = saved_errno;
	return descriptor;
}

extern "C" int __wrap_fstat(int descriptor, struct stat* status) {
	if (edge_test::active_faults != nullptr &&
	    edge_test::active_faults->inject_fstat(descriptor)) {
		return -1;
	}
	return __real_fstat(descriptor, status);
}

extern "C" int __wrap_statx(int descriptor, const char* path, int flags, unsigned int mask,
                            struct statx* status) {
	if (edge_test::active_faults != nullptr &&
	    edge_test::active_faults->inject_statx(descriptor, path, flags)) {
		return -1;
	}
	return __real_statx(descriptor, path, flags, mask, status);
}
