#ifndef EDGE_RUNTIME_ERROR_HPP
#define EDGE_RUNTIME_ERROR_HPP

#include <cstdint>

namespace edge_runtime {

// Frozen ErrorCode values (design §17). The order and underlying values are
// part of the stable contract; tests U08 and I05–I09 assert them. Do not
// reorder or renumber.
enum class ErrorCode : uint32_t {
	kInvalidName = 0,
	kInvalidOptions,
	kNotFound,
	kPermissionDenied,
	kAlreadyOwned,
	kConsumerAlreadyOwned,
	kInitializationIncomplete,
	kCorruptHeader,
	kAbiMismatch,
	kSchemaMismatch,
	kUnsupportedPlatform,
	kNoNewSample,
	kNoWritableSlot,
	kReadContention,
	kTimeout,
	kDataStale,
	kProducerOffline,
	kRecoveryBlocked,
	kStaleHandle,
	kPayloadCorrupt,
	kPayloadEncodeFailed,
	kPayloadDecodeFailed,
	kCorruptSlot,
	kNameRaceDetected,
	kClockAnomaly,
	kSequenceExhausted,
	kConcurrentHandleUse,
	kSystemError,
};

// Fixed-size error payload. The hot path must not allocate or throw (design
// §11, §16.3), so context is a fixed buffer, never std::string.
struct Error {
	ErrorCode code{ErrorCode::kSystemError};
	int errno_value{0};
	const char* operation{nullptr};
	char context[64]{};

	constexpr Error() = default;
	Error(ErrorCode c, const char* op, const char* ctx = nullptr);
};

const char* to_string(ErrorCode code) noexcept;

// errno -> stable ErrorCode (U08). The mapping must not drift.
ErrorCode classify_errno(int errno_value) noexcept;

Error make_error(ErrorCode code, const char* operation, const char* context = nullptr) noexcept;

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_ERROR_HPP
