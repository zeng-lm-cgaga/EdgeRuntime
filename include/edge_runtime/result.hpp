#ifndef EDGE_RUNTIME_RESULT_HPP
#define EDGE_RUNTIME_RESULT_HPP

#include <optional>
#include <utility>

#include "edge_runtime/error.hpp"

namespace edge_runtime {

// Non-throwing result wrapper (design §16.3). The value path never throws,
// never allocates beyond T's own construction, and supports move-only T
// (Producer/Consumer handles are move-only).
template <typename T>
class Result {
       public:
	Result(T value) : value_(std::move(value)) {}        // NOLINT
	Result(const Error& error) : error_(error) {}        // NOLINT
	Result(Error&& error) : error_(std::move(error)) {}  // NOLINT
	Result(ErrorCode code, const char* operation, const char* context = nullptr)
	    : error_(make_error(code, operation, context)) {}

	Result(const Result&) = default;
	Result& operator=(const Result&) = default;
	Result(Result&&) noexcept = default;
	Result& operator=(Result&&) noexcept = default;

	bool has_value() const noexcept { return value_.has_value(); }
	explicit operator bool() const noexcept { return value_.has_value(); }

	T& value() & { return *value_; }
	const T& value() const& { return *value_; }
	T&& value() && { return std::move(*value_); }

	const Error& error() const noexcept { return error_; }
	Error& error() noexcept { return error_; }

       private:
	std::optional<T> value_;
	Error error_;
};

template <>
class Result<void> {
       public:
	Result() = default;
	Result(const Error& error) : error_(error), failed_(true) {}        // NOLINT
	Result(Error&& error) : error_(std::move(error)), failed_(true) {}  // NOLINT
	Result(ErrorCode code, const char* operation, const char* context = nullptr)
	    : error_(make_error(code, operation, context)), failed_(true) {}

	Result(const Result&) = default;
	Result& operator=(const Result&) = default;
	Result(Result&&) noexcept = default;
	Result& operator=(Result&&) noexcept = default;

	static Result ok() { return Result(); }

	bool has_value() const noexcept { return !failed_; }
	explicit operator bool() const noexcept { return !failed_; }

	const Error& error() const noexcept { return error_; }
	Error& error() noexcept { return error_; }

       private:
	Error error_;
	bool failed_ = false;
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_RESULT_HPP
