// U08: frozen ErrorCode table, errno classification, fixed-context Error.

#include "edge_runtime/error.hpp"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>

namespace {

using edge_runtime::classify_errno;
using edge_runtime::Error;
using edge_runtime::ErrorCode;
using edge_runtime::to_string;

TEST(ErrorModel, EveryCodeHasStableName) {  // U08
	EXPECT_STREQ(to_string(ErrorCode::kInvalidName), "InvalidName");
	EXPECT_STREQ(to_string(ErrorCode::kSystemError), "SystemError");
	// v0.2 additions (design §17/§34) append after kSystemError.
	EXPECT_STREQ(to_string(ErrorCode::kProducerStalled), "ProducerStalled");
	EXPECT_STREQ(to_string(ErrorCode::kTransportFailed), "TransportFailed");
	// v0.3 addition (design §35).
	EXPECT_STREQ(to_string(ErrorCode::kSupervisionExhausted), "SupervisionExhausted");
	// no code maps to the sentinel; the table must cover the full range
	for (uint32_t i = 0; i <= static_cast<uint32_t>(ErrorCode::kSupervisionExhausted); ++i) {
		const char* name = to_string(static_cast<ErrorCode>(i));
		EXPECT_NE(name, nullptr);
		EXPECT_STRNE(name, "UnknownError");
	}
	// out-of-range input degrades to the sentinel, never to garbage
	EXPECT_STREQ(to_string(static_cast<ErrorCode>(0xFFFFu)), "UnknownError");
}

TEST(ErrorModel, ErrnoClassification) {  // U08
	EXPECT_EQ(classify_errno(ENOENT), ErrorCode::kNotFound);
	EXPECT_EQ(classify_errno(EEXIST), ErrorCode::kAlreadyOwned);
	EXPECT_EQ(classify_errno(EACCES), ErrorCode::kPermissionDenied);
	EXPECT_EQ(classify_errno(EPERM), ErrorCode::kPermissionDenied);
	EXPECT_EQ(classify_errno(EBADF), ErrorCode::kSystemError);
	EXPECT_EQ(classify_errno(0), ErrorCode::kSystemError);
}

TEST(ErrorModel, FixedContextNoAlloc) {  // U08
	Error e(ErrorCode::kPermissionDenied, "test_op", "the context");
	EXPECT_EQ(e.code, ErrorCode::kPermissionDenied);
	EXPECT_STREQ(e.operation, "test_op");
	EXPECT_STREQ(e.context, "the context");
	EXPECT_EQ(e.errno_value, 0);
}

TEST(ErrorModel, ContextTruncation) {  // U08
	std::string long_ctx(300, 'x');
	Error e(ErrorCode::kSystemError, "op2", long_ctx.c_str());
	const size_t len = std::strlen(e.context);
	EXPECT_GT(len, 0u);
	EXPECT_LT(len, 64u);  // fixed buffer, NUL-terminated, never overflows
	EXPECT_EQ(e.context[len], '\0');
}

TEST(ErrorModel, NullContext) {  // U08
	Error e(ErrorCode::kNotFound, "op3");
	EXPECT_STREQ(e.context, "");
	EXPECT_STREQ(e.operation, "op3");
}

}  // namespace
