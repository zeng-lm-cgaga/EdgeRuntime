#ifndef EDGE_TEST_CREATION_FAULTS_HPP
#define EDGE_TEST_CREATION_FAULTS_HPP

#include <sys/stat.h>

#include <cstdint>
#include <string>

namespace edge_test {

enum class CreationFault { kOnceEintr, kPersistentEintr, kPersistentEio };

struct CreationFaultPlan {
	CreationFault fault = CreationFault::kOnceEintr;
	bool deny_statx = false;
	bool replace_with_sentinel = false;
};

struct CreationObjectEvidence {
	bool exists = false;
	int error = 0;
	uint64_t dev = 0;
	uint64_t ino = 0;
	uint64_t size = 0;
	uint64_t links = 0;
};

struct CreationFaultEvidence {
	uint32_t exclusive_opens = 0;
	uint32_t target_fstat_calls = 0;
	uint32_t fstat_fault_hits = 0;
	uint32_t target_statx_calls = 0;
	uint32_t statx_fault_hits = 0;
	uint32_t other_fstat_calls = 0;
	int fixture_error = 0;
	bool budget_exceeded = false;
	bool statx_available = false;
};

inline constexpr uint32_t kCreationFaultCallBudget = 64;
inline constexpr char kCreationSentinelContents[] = "external-creation-sentinel-do-not-remove";

std::string unique_creation_name();

class CreationFaults {
       public:
	CreationFaults(std::string posix_name, CreationFaultPlan plan);
	~CreationFaults();
	CreationFaults(const CreationFaults&) = delete;
	CreationFaults& operator=(const CreationFaults&) = delete;

	void disarm() noexcept;
	const CreationFaultEvidence& evidence() const noexcept;
	CreationObjectEvidence created_object() const noexcept;
	CreationObjectEvidence sentinel_object() const noexcept;
	CreationObjectEvidence named_object() const noexcept;
	bool sentinel_contents_match() const noexcept;
	bool original_fd_closed() const noexcept;
	bool cleanup_fixture_owned() noexcept;

	void observe_shm_open(const char* name, int flags, int descriptor) noexcept;
	bool inject_fstat(int descriptor) noexcept;
	bool inject_statx(int descriptor, const char* path, int flags) noexcept;

       private:
	bool matches_created_fd(int descriptor) const noexcept;
	bool install_sentinel() noexcept;

	std::string posix_name_;
	CreationFaultPlan plan_;
	CreationFaultEvidence evidence_;
	int created_fd_ = -1;
	int created_pin_ = -1;
	int sentinel_pin_ = -1;
	uint64_t created_dev_ = 0;
	uint64_t created_ino_ = 0;
};

}  // namespace edge_test

#endif
