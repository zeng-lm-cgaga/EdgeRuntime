#ifndef EDGE_RUNTIME_DETAIL_CONTROL_LOCK_HPP
#define EDGE_RUNTIME_DETAIL_CONTROL_LOCK_HPP

#include <string>

#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/common/result.hpp"

namespace edge_runtime::detail {

// 控制日志记录创建、替换和删除事务，崩溃后用于确认实例归属与恢复边界。
enum class JournalState : uint32_t {
	kIdle = 0,
	kCreatingPreObject = 1,
	kCreatingObject = 2,
	kReplacing = 3,
	kRemoving = 4,
};

inline constexpr char kJournalMagic[] = "EDGJRN1";
inline constexpr uint32_t kJournalVersion = 1;
inline constexpr size_t kJournalRecordSize = 256;

struct alignas(64) ControlJournalV1 {
	char magic[8];
	uint32_t version;
	uint32_t channel_hash;
	uint32_t state;
	uint32_t reserved0;
	uint64_t target_dev;
	uint64_t target_ino;
	uint64_t old_generation;
	uint64_t new_generation;
	uint64_t old_nonce_hi;
	uint64_t old_nonce_lo;
	uint64_t new_nonce_hi;
	uint64_t new_nonce_lo;
	ProcessIdentityAbi creator;
	uint64_t record_checksum;

	uint32_t transport;
	uint8_t reserved[52];
};
static_assert(sizeof(ControlJournalV1) == kJournalRecordSize, "journal record size");
static_assert(offsetof(ControlJournalV1, channel_hash) == 12, "journal channel_hash");
static_assert(offsetof(ControlJournalV1, target_dev) == 24, "journal target_dev");
static_assert(offsetof(ControlJournalV1, target_ino) == 32, "journal target_ino");
static_assert(offsetof(ControlJournalV1, creator) == 128, "journal creator");
static_assert(offsetof(ControlJournalV1, record_checksum) == 192, "journal checksum");
static_assert(offsetof(ControlJournalV1, transport) == 200, "journal transport");

uint64_t journal_checksum(const ControlJournalV1& journal) noexcept;

uint32_t channel_name_hash(const char* name, size_t len) noexcept;

class ControlLock {
       public:
	// 锁的生命周期覆盖一次完整控制面事务，不能跨线程转移使用。
	static Result<ControlLock> acquire(const std::string& lock_path);

	~ControlLock() { release(); }
	ControlLock(const ControlLock&) = delete;
	ControlLock& operator=(const ControlLock&) = delete;
	ControlLock(ControlLock&& other) noexcept : fd_(other.fd_.release()) {}
	ControlLock& operator=(ControlLock&& other) noexcept {
		if (this != &other) {
			release();
			fd_ = UniqueFd(other.fd_.release());
		}
		return *this;
	}

	Result<ControlJournalV1> read_journal() noexcept;

	Result<void> write_journal(const ControlJournalV1& journal) noexcept;

	void release() noexcept;

       private:
	ControlLock() = default;
	UniqueFd fd_;
};

}  // 命名空间 edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CONTROL_LOCK_HPP
