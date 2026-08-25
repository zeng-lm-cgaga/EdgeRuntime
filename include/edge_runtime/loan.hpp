#ifndef EDGE_RUNTIME_LOAN_HPP
#define EDGE_RUNTIME_LOAN_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "edge_runtime/result.hpp"
#include "edge_runtime/sample.hpp"

namespace edge_runtime {

class WriteLoan;
class ReadLoan;

namespace detail {

struct ProducerHandle;
struct ConsumerHandle;

Result<WriteLoan> producer_loan_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;
Result<PublishInfo> producer_commit_loan_impl(WriteLoan* loan) noexcept;
void producer_abort_loan_impl(WriteLoan* loan) noexcept;

Result<ReadLoan> consumer_try_loan_latest_impl(
        const std::shared_ptr<ConsumerHandle>& handle) noexcept;
Result<ReadLoan> consumer_wait_loan_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                               uint64_t timeout_ns) noexcept;
void consumer_release_loan_impl(ReadLoan* loan) noexcept;

}  // 命名空间 detail

// 可写槽的独占视图。调用方直接编码到 data()，commit() 发布；未提交时析构会退回 FREE。
class WriteLoan {
       public:
	WriteLoan(const WriteLoan&) = delete;
	WriteLoan& operator=(const WriteLoan&) = delete;
	WriteLoan(WriteLoan&& other) noexcept;
	WriteLoan& operator=(WriteLoan&& other) noexcept;
	~WriteLoan();

	std::byte* data() noexcept { return data_; }
	const std::byte* data() const noexcept { return data_; }
	size_t size() const noexcept { return static_cast<size_t>(size_); }
	bool active() const noexcept { return active_; }

	Result<PublishInfo> commit() noexcept;
	void abort() noexcept;

       private:
	friend Result<WriteLoan> detail::producer_loan_impl(
	        const std::shared_ptr<detail::ProducerHandle>& handle) noexcept;
	friend Result<PublishInfo> detail::producer_commit_loan_impl(WriteLoan* loan) noexcept;
	friend void detail::producer_abort_loan_impl(WriteLoan* loan) noexcept;

	WriteLoan(std::shared_ptr<detail::ProducerHandle> handle, void* slot, std::byte* data,
	          uint32_t size, uint32_t slot_index, uint64_t sequence) noexcept;

	std::shared_ptr<detail::ProducerHandle> handle_;
	void* slot_{nullptr};
	std::byte* data_{nullptr};
	uint32_t size_{0};
	uint32_t slot_index_{0};
	uint64_t sequence_{0};
	bool active_{false};
};

// 可读槽的只读视图。返回前已完成校验和检查，直到 release() 或析构前数据保持稳定。
class ReadLoan {
       public:
	ReadLoan(const ReadLoan&) = delete;
	ReadLoan& operator=(const ReadLoan&) = delete;
	ReadLoan(ReadLoan&& other) noexcept;
	ReadLoan& operator=(ReadLoan&& other) noexcept;
	~ReadLoan();

	const std::byte* data() const noexcept { return data_; }
	size_t size() const noexcept { return static_cast<size_t>(size_); }
	bool active() const noexcept { return active_; }

	uint64_t generation() const noexcept { return generation_; }
	const std::array<std::byte, 16>& instance_nonce() const noexcept { return instance_nonce_; }
	uint64_t sequence() const noexcept { return sequence_; }
	uint64_t publish_boot_ns() const noexcept { return publish_boot_ns_; }
	uint64_t receive_boot_ns() const noexcept { return receive_boot_ns_; }
	uint64_t missed_samples() const noexcept { return missed_samples_; }

	void release() noexcept;

       private:
	friend Result<ReadLoan> detail::consumer_try_loan_latest_impl(
	        const std::shared_ptr<detail::ConsumerHandle>& handle) noexcept;
	friend Result<ReadLoan> detail::consumer_wait_loan_latest_impl(
	        const std::shared_ptr<detail::ConsumerHandle>& handle, uint64_t timeout_ns) noexcept;
	friend void detail::consumer_release_loan_impl(ReadLoan* loan) noexcept;

	ReadLoan(std::shared_ptr<detail::ConsumerHandle> handle, void* slot, const std::byte* data,
	         uint32_t size, uint64_t generation, std::array<std::byte, 16> instance_nonce,
	         uint64_t sequence, uint64_t publish_boot_ns, uint64_t receive_boot_ns,
	         uint64_t missed_samples) noexcept;

	std::shared_ptr<detail::ConsumerHandle> handle_;
	void* slot_{nullptr};
	const std::byte* data_{nullptr};
	uint32_t size_{0};
	uint64_t generation_{0};
	std::array<std::byte, 16> instance_nonce_{};
	uint64_t sequence_{0};
	uint64_t publish_boot_ns_{0};
	uint64_t receive_boot_ns_{0};
	uint64_t missed_samples_{0};
	bool active_{false};
};

}  // 命名空间 edge_runtime

#endif  // EDGE_RUNTIME_LOAN_HPP
