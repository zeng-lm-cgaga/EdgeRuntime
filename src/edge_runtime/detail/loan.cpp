#include "edge_runtime/loan.hpp"

#include <utility>

namespace edge_runtime {

WriteLoan::WriteLoan(std::shared_ptr<detail::ProducerHandle> handle, void* slot, std::byte* data,
                     uint32_t size, uint32_t slot_index, uint64_t sequence) noexcept
    : handle_(std::move(handle)),
      slot_(slot),
      data_(data),
      size_(size),
      slot_index_(slot_index),
      sequence_(sequence),
      active_(true) {}

WriteLoan::WriteLoan(WriteLoan&& other) noexcept
    : handle_(std::move(other.handle_)),
      slot_(std::exchange(other.slot_, nullptr)),
      data_(std::exchange(other.data_, nullptr)),
      size_(std::exchange(other.size_, 0)),
      slot_index_(std::exchange(other.slot_index_, 0)),
      sequence_(std::exchange(other.sequence_, 0)),
      active_(std::exchange(other.active_, false)) {}

WriteLoan& WriteLoan::operator=(WriteLoan&& other) noexcept {
	if (this == &other) return *this;
	abort();
	handle_ = std::move(other.handle_);
	slot_ = std::exchange(other.slot_, nullptr);
	data_ = std::exchange(other.data_, nullptr);
	size_ = std::exchange(other.size_, 0);
	slot_index_ = std::exchange(other.slot_index_, 0);
	sequence_ = std::exchange(other.sequence_, 0);
	active_ = std::exchange(other.active_, false);
	return *this;
}

WriteLoan::~WriteLoan() { abort(); }

Result<PublishInfo> WriteLoan::commit() noexcept {
	return detail::producer_commit_loan_impl(this);
}

void WriteLoan::abort() noexcept { detail::producer_abort_loan_impl(this); }

ReadLoan::ReadLoan(std::shared_ptr<detail::ConsumerHandle> handle, void* slot,
                   const std::byte* data, uint32_t size, uint64_t generation,
                   std::array<std::byte, 16> instance_nonce, uint64_t sequence,
                   uint64_t publish_boot_ns, uint64_t receive_boot_ns,
                   uint64_t missed_samples) noexcept
    : handle_(std::move(handle)),
      slot_(slot),
      data_(data),
      size_(size),
      generation_(generation),
      instance_nonce_(instance_nonce),
      sequence_(sequence),
      publish_boot_ns_(publish_boot_ns),
      receive_boot_ns_(receive_boot_ns),
      missed_samples_(missed_samples),
      active_(true) {}

ReadLoan::ReadLoan(ReadLoan&& other) noexcept
    : handle_(std::move(other.handle_)),
      slot_(std::exchange(other.slot_, nullptr)),
      data_(std::exchange(other.data_, nullptr)),
      size_(std::exchange(other.size_, 0)),
      generation_(std::exchange(other.generation_, 0)),
      instance_nonce_(other.instance_nonce_),
      sequence_(std::exchange(other.sequence_, 0)),
      publish_boot_ns_(std::exchange(other.publish_boot_ns_, 0)),
      receive_boot_ns_(std::exchange(other.receive_boot_ns_, 0)),
      missed_samples_(std::exchange(other.missed_samples_, 0)),
      active_(std::exchange(other.active_, false)) {
	other.instance_nonce_.fill(std::byte{0});
}

ReadLoan& ReadLoan::operator=(ReadLoan&& other) noexcept {
	if (this == &other) return *this;
	release();
	handle_ = std::move(other.handle_);
	slot_ = std::exchange(other.slot_, nullptr);
	data_ = std::exchange(other.data_, nullptr);
	size_ = std::exchange(other.size_, 0);
	generation_ = std::exchange(other.generation_, 0);
	instance_nonce_ = other.instance_nonce_;
	other.instance_nonce_.fill(std::byte{0});
	sequence_ = std::exchange(other.sequence_, 0);
	publish_boot_ns_ = std::exchange(other.publish_boot_ns_, 0);
	receive_boot_ns_ = std::exchange(other.receive_boot_ns_, 0);
	missed_samples_ = std::exchange(other.missed_samples_, 0);
	active_ = std::exchange(other.active_, false);
	return *this;
}

ReadLoan::~ReadLoan() { release(); }

void ReadLoan::release() noexcept { detail::consumer_release_loan_impl(this); }

}  // namespace edge_runtime
