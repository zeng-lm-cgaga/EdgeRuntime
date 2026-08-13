#ifndef EDGE_RUNTIME_PRODUCER_HPP
#define EDGE_RUNTIME_PRODUCER_HPP

#include <cstdint>
#include <memory>
#include <utility>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/error.hpp"
#include "edge_runtime/result.hpp"
#include "edge_runtime/sample.hpp"
#include "edge_runtime/schema.hpp"

namespace edge_runtime::detail {

struct ProducerHandle;
Result<std::shared_ptr<ProducerHandle>> producer_create_impl(const ChannelOptions& options,
                                                             const SchemaDescriptor& schema,
                                                             uint32_t payload_size);
Result<PublishInfo> producer_publish_impl(const std::shared_ptr<ProducerHandle>& handle,
                                          const std::byte* encoded, uint32_t encoded_size) noexcept;
Result<ChannelStatus> producer_status_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;
uint64_t producer_generation_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;
void producer_shutdown_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;
Result<void> producer_remove_if_owner_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;
Result<void> producer_heartbeat_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

}  // namespace edge_runtime::detail

namespace edge_runtime {

// SPSC producer handle (design §9.1, §16.2). Move-only; the underlying fd and
// MAP_SHARED mapping are released on destruction. publish() is ER2.
template <typename T>
class Producer {
       public:
	using value_type = T;

	// Full §9.1 create sequence: validated control-lock transaction, inode-checked
	// replacement of a dead predecessor, commit at bootstrap READY.
	static Result<Producer> create(const ChannelOptions& options,
	                               const SchemaDescriptor& schema) {
		static_assert(kSupportedPayload<T>,
		              "T must satisfy the PayloadCodec<T> contract (schema.hpp)");
		auto h = detail::producer_create_impl(options, schema,
		                                      PayloadCodec<T>::kEncodedSize);
		if (!h) return h.error();
		return Producer(std::move(h.value()));
	}

	// Publish a new latest sample (design §11). The codec encode runs here (the
	// impl never touches T); failures leave shared memory untouched.
	Result<PublishInfo> publish(const T& value) noexcept {
		typename PayloadCodec<T>::EncodedBuffer encoded{};
		const uint32_t size = PayloadCodec<T>::kEncodedSize;
		if (!PayloadCodec<T>::encode(value, encoded.data(), encoded.size())) {
			return make_error(ErrorCode::kPayloadEncodeFailed, "Producer::publish",
			                  "encode failed");
		}
		return detail::producer_publish_impl(handle_, encoded.data(), size);
	}

	// Slow-check diagnostic: revalidates the name still resolves to this
	// instance's inode, then snapshots the channel state.
	Result<ChannelStatus> status() const noexcept {
		return detail::producer_status_impl(handle_);
	}

	// Frozen generation of the owned instance (design §15.6). Advances by one on
	// each verified replacement; used by tests/CLI to prove generation+1.
	uint64_t generation() const noexcept { return detail::producer_generation_impl(handle_); }

	// Explicit verified removal of the owned instance (design §9.4). Never called
	// implicitly from the destructor.
	Result<void> remove_if_owner() noexcept {
		return detail::producer_remove_if_owner_impl(handle_);
	}

	// v0.2 optional heartbeat (design §34): declare "the application is still
	// making progress" between publishes. No-op data-wise when the channel was
	// created with heartbeat disabled; otherwise stores BOOTTIME into
	// heartbeat_boot_ns under the same stale guards as publish().
	Result<void> heartbeat() noexcept {
		return detail::producer_heartbeat_impl(handle_);
	}

	Producer(const Producer&) = delete;
	Producer& operator=(const Producer&) = delete;
	Producer(Producer&&) noexcept = default;
	Producer& operator=(Producer&&) noexcept = default;

	// Best-effort clean shutdown (design §15.2): marks the shared producer_state
	// OFFLINE so a same-process recreate is not mistaken for a live owner.
	~Producer() {
		if (handle_) detail::producer_shutdown_impl(handle_);
	}

       private:
	explicit Producer(std::shared_ptr<detail::ProducerHandle> h) : handle_(std::move(h)) {}

	std::shared_ptr<detail::ProducerHandle> handle_;
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_PRODUCER_HPP
