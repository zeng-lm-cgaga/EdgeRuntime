#ifndef EDGE_RUNTIME_RESULT_HPP
#define EDGE_RUNTIME_RESULT_HPP

#include <optional>
#include <utility>

#include "edge_runtime/error.hpp"

namespace edge_runtime {

// 非异常结果包装器；成功值和固定大小 Error 二选一，支持不可复制的句柄类型。
template <typename T>
class Result {
       public:
	Result(T value) : value_(std::move(value)) {}
	Result(const Error& error) : error_(error) {}
	Result(Error&& error) : error_(std::move(error)) {}
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
	Result(const Error& error) : error_(error), failed_(true) {}
	Result(Error&& error) : error_(std::move(error)), failed_(true) {}
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

}  // 命名空间 edge_runtime

#endif  // EDGE_RUNTIME_RESULT_HPP
