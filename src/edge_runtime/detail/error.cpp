#include "edge_runtime/error.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>

namespace edge_runtime {

namespace {

constexpr const char* kErrorNames[] = {
        "InvalidName",               // 0
        "InvalidOptions",            // 1
        "NotFound",                  // 2
        "PermissionDenied",          // 3
        "AlreadyOwned",              // 4
        "ConsumerAlreadyOwned",      // 5
        "InitializationIncomplete",  // 6
        "CorruptHeader",             // 7
        "AbiMismatch",               // 8
        "SchemaMismatch",            // 9
        "UnsupportedPlatform",       // 10
        "NoNewSample",               // 11
        "NoWritableSlot",            // 12
        "ReadContention",            // 13
        "Timeout",                   // 14
        "DataStale",                 // 15
        "ProducerOffline",           // 16
        "RecoveryBlocked",           // 17
        "StaleHandle",               // 18
        "PayloadCorrupt",            // 19
        "PayloadEncodeFailed",       // 20
        "PayloadDecodeFailed",       // 21
        "CorruptSlot",               // 22
        "NameRaceDetected",          // 23
        "ClockAnomaly",              // 24
        "SequenceExhausted",         // 25
        "ConcurrentHandleUse",       // 26
        "SystemError",               // 27
        "ProducerStalled",           // 28  (v0.2)
        "TransportFailed",           // 29  (v0.2)
        "SupervisionExhausted",      // 30  (v0.3)
};
static_assert(sizeof(kErrorNames) / sizeof(kErrorNames[0]) ==
                      static_cast<size_t>(ErrorCode::kSupervisionExhausted) + 1u,
              "error name table must match the frozen ErrorCode enum");

}  // namespace

Error::Error(ErrorCode c, const char* op, const char* ctx) : code(c), operation(op) {
	if (ctx != nullptr) {
		std::snprintf(context, sizeof(context), "%s", ctx);
	}
}

Error::Error(ErrorCode c, int saved_errno, const char* op, const char* ctx)
    : code(c), errno_value(saved_errno), operation(op) {
	if (ctx != nullptr) {
		std::snprintf(context, sizeof(context), "%s", ctx);
	}
}

const char* to_string(ErrorCode code) noexcept {
	const uint32_t index = static_cast<uint32_t>(code);
	if (index > static_cast<uint32_t>(ErrorCode::kSupervisionExhausted)) return "UnknownError";
	return kErrorNames[index];
}

ErrorCode classify_errno(int errno_value) noexcept {
	switch (errno_value) {
		case ENOENT:
			return ErrorCode::kNotFound;
		case EEXIST:
			return ErrorCode::kAlreadyOwned;
		case EACCES:
		case EPERM:
			return ErrorCode::kPermissionDenied;
		default:
			return ErrorCode::kSystemError;
	}
}

Error make_error(ErrorCode code, const char* operation, const char* context) noexcept {
	return Error(code, operation, context);
}

Error make_errno_error(int saved_errno, const char* operation, const char* context) noexcept {
	return Error(classify_errno(saved_errno), saved_errno, operation, context);
}

Error make_errno_error(ErrorCode code, int saved_errno, const char* operation,
                       const char* context) noexcept {
	return Error(code, saved_errno, operation, context);
}

}  // namespace edge_runtime
