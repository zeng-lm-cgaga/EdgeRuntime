#ifndef EDGE_RUNTIME_DETAIL_CONTROL_LOCK_HPP
#define EDGE_RUNTIME_DETAIL_CONTROL_LOCK_HPP

#include <string>

#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/result.hpp"

namespace edge_runtime::detail {

enum class JournalState : uint32_t {
	kIdle = 0,
	kCreatingPreObject = 1,
	kCreatingObject = 2,
	kReplacing = 3,
	kRemoving = 4,
};

inline constexpr char kJournalMagic[] = "EDGJRN1";  // 7 chars, NUL not copied
inline constexpr uint32_t kJournalVersion = 1;
inline constexpr size_t kJournalRecordSize = 256;

// Fixed-width ControlJournalV1 record (design §7.3), serialized into the lock
// file with pwrite and guarded by flock. Never on the publish/read hot path.
// Value-initialize before filling so padding bytes are deterministic.
struct alignas(64) ControlJournalV1 {
	char magic[8];               // 0
	uint32_t version;            // 8
	uint32_t channel_hash;       // 12
	uint32_t state;              // 16  (JournalState)
	uint32_t reserved0;          // 20
	uint64_t target_dev;         // 24
	uint64_t target_ino;         // 32
	uint64_t old_generation;     // 40
	uint64_t new_generation;     // 48
	uint64_t old_nonce_hi;       // 56
	uint64_t old_nonce_lo;       // 64
	uint64_t new_nonce_hi;       // 72
	uint64_t new_nonce_lo;       // 80
	ProcessIdentityAbi creator;  // 128 (aligned to 64)
	uint64_t record_checksum;    // 192
	uint8_t reserved[56];        // 200
};
static_assert(sizeof(ControlJournalV1) == kJournalRecordSize, "journal record size");
static_assert(offsetof(ControlJournalV1, channel_hash) == 12, "journal channel_hash");
static_assert(offsetof(ControlJournalV1, target_dev) == 24, "journal target_dev");
static_assert(offsetof(ControlJournalV1, target_ino) == 32, "journal target_ino");
static_assert(offsetof(ControlJournalV1, creator) == 128, "journal creator");
static_assert(offsetof(ControlJournalV1, record_checksum) == 192, "journal checksum");

// Checksum over the whole record with the checksum field zeroed.
uint64_t journal_checksum(const ControlJournalV1& journal) noexcept;

// FNV-1a 32-bit hash of the channel name, used to bind a journal to a name.
uint32_t channel_name_hash(const char* name, size_t len) noexcept;

// An flock(LOCK_EX) guard over the control-lock file, plus the journal record.
class ControlLock {
       public:
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

	// Reads and validates the journal record; an empty/short lock file returns
	// a zeroed record with state kIdle.
	Result<ControlJournalV1> read_journal() noexcept;

	// Writes the record with a fresh checksum and fdatasyncs.
	Result<void> write_journal(const ControlJournalV1& journal) noexcept;

	void release() noexcept;

       private:
	ControlLock() = default;
	UniqueFd fd_;
};

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CONTROL_LOCK_HPP
