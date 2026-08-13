#ifndef EDGE_RUNTIME_CONSUMER_HPP
#define EDGE_RUNTIME_CONSUMER_HPP

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/error.hpp"
#include "edge_runtime/result.hpp"
#include "edge_runtime/sample.hpp"
#include "edge_runtime/schema.hpp"

namespace edge_runtime::detail {

struct ConsumerHandle;

// Metadata the read impl returns alongside the copied payload bytes (the impl
// never touches T and never decodes; §12.2). Complete here because the inline
// template method below copies these fields into Sample<T>.
struct ReadSnapshot {
	uint32_t encoded_size{0};  // valid bytes written to encoded_out
	bool checksum_ok{true};
	uint64_t sample_sequence{0};
	uint64_t publish_boot_ns{0};
	uint64_t receive_boot_ns{0};
	uint64_t generation{0};
	std::array<std::byte, 16> instance_nonce{};
	uint64_t missed_samples{0};
};

Result<std::shared_ptr<ConsumerHandle>> consumer_open_impl(const ChannelOptions& options,
                                                           const SchemaDescriptor& schema,
                                                           uint32_t payload_size);
Result<ReadSnapshot> consumer_try_read_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                                   std::byte* encoded_out,
                                                   uint32_t encoded_cap) noexcept;
Result<ReadSnapshot> consumer_wait_latest_impl(const std::shared_ptr<ConsumerHandle>& handle,
                                               std::byte* encoded_out, uint32_t encoded_cap,
                                               uint64_t timeout_ns) noexcept;
Result<ChannelStatus> consumer_status_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept;
void consumer_shutdown_impl(const std::shared_ptr<ConsumerHandle>& handle) noexcept;

}  // namespace edge_runtime::detail

namespace edge_runtime {

// SPSC consumer handle (design §9.2, §16.2). Move-only; the underlying fd and
// MAP_SHARED mapping are released on destruction. Read-path methods arrive
// with ER2/ER3; reconnect() with ER4.
template <typename T>
class Consumer {
       public:
	using value_type = T;

	// Full §9.2 open sequence: bootstrap/header validation, control-lock
	// revalidation, consumer identity registration (rejects a live consumer).
	static Result<Consumer> open(const ChannelOptions& options,
	                             const SchemaDescriptor& schema) {
		static_assert(kSupportedPayload<T>,
		              "T must satisfy the PayloadCodec<T> contract (schema.hpp)");
		auto h = detail::consumer_open_impl(options, schema, PayloadCodec<T>::kEncodedSize);
		if (!h) return h.error();
		return Consumer(std::move(h.value()));
	}

	// Latest-value read (design §12). The impl freezes the slot, copies the
	// encoded payload, validates the checksum, then releases the slot; decode
	// into T runs here over the local copy only (§12.2).
	Result<Sample<T>> try_read_latest() noexcept {
		typename PayloadCodec<T>::EncodedBuffer encoded{};
		auto snap = detail::consumer_try_read_latest_impl(
		        handle_, encoded.data(), static_cast<uint32_t>(encoded.size()));
		if (!snap) return snap.error();
		if (!snap.value().checksum_ok) {
			return make_error(ErrorCode::kPayloadCorrupt, "Consumer::try_read_latest",
			                  "checksum mismatch");
		}
		Sample<T> out;
		if (!PayloadCodec<T>::decode(encoded.data(), snap.value().encoded_size,
		                             &out.value)) {
			return make_error(ErrorCode::kPayloadDecodeFailed,
			                  "Consumer::try_read_latest", "decode failed");
		}
		out.generation = snap.value().generation;
		out.instance_nonce = snap.value().instance_nonce;
		out.sequence = snap.value().sample_sequence;
		out.publish_boot_ns = snap.value().publish_boot_ns;
		out.receive_boot_ns = snap.value().receive_boot_ns;
		out.missed_samples = snap.value().missed_samples;
		return Result<Sample<T>>(std::move(out));
	}

	// Blocking read with an absolute MONOTONIC deadline (design §14.2, ER3).
	// Timeout is classified by producer liveness (§15.5): alive-but-idle ->
	// kDataStale, offline/dead -> kProducerOffline, unverifiable -> kRecoveryBlocked.
	// EAGAIN/EINTR/spurious wakeups loop without resetting the deadline. A zero
	// timeout degrades to one bounded probe + immediate classification.
	Result<Sample<T>> wait_latest(std::chrono::nanoseconds timeout) noexcept {
		static_assert(kSupportedPayload<T>,
		              "T must satisfy the PayloadCodec<T> contract (schema.hpp)");
		typename PayloadCodec<T>::EncodedBuffer encoded{};
		const int64_t count = timeout.count();
		const uint64_t timeout_ns = count > 0 ? static_cast<uint64_t>(count) : 0;
		auto snap = detail::consumer_wait_latest_impl(
		        handle_, encoded.data(), static_cast<uint32_t>(encoded.size()), timeout_ns);
		if (!snap) return snap.error();
		if (!snap.value().checksum_ok) {
			return make_error(ErrorCode::kPayloadCorrupt, "Consumer::wait_latest",
			                  "checksum mismatch");
		}
		Sample<T> out;
		if (!PayloadCodec<T>::decode(encoded.data(), snap.value().encoded_size,
		                             &out.value)) {
			return make_error(ErrorCode::kPayloadDecodeFailed, "Consumer::wait_latest",
			                  "decode failed");
		}
		out.generation = snap.value().generation;
		out.instance_nonce = snap.value().instance_nonce;
		out.sequence = snap.value().sample_sequence;
		out.publish_boot_ns = snap.value().publish_boot_ns;
		out.receive_boot_ns = snap.value().receive_boot_ns;
		out.missed_samples = snap.value().missed_samples;
		return Result<Sample<T>>(std::move(out));
	}

	// Reopen against a replaced instance (ER4).
	Result<ReconnectInfo> reconnect() noexcept;

	// Slow-check diagnostic (design §15.6).
	Result<ChannelStatus> status() const noexcept {
		return detail::consumer_status_impl(handle_);
	}

	Consumer(const Consumer&) = delete;
	Consumer& operator=(const Consumer&) = delete;
	Consumer(Consumer&&) noexcept = default;
	Consumer& operator=(Consumer&&) noexcept = default;

	// Best-effort clean shutdown (design §15.2): marks consumer_state OFFLINE so
	// a later open in the same process is not rejected as an active owner.
	~Consumer() {
		if (handle_) detail::consumer_shutdown_impl(handle_);
	}

       private:
	explicit Consumer(std::shared_ptr<detail::ConsumerHandle> h) : handle_(std::move(h)) {}

	std::shared_ptr<detail::ConsumerHandle> handle_;
};

}  // namespace edge_runtime

#endif  // EDGE_RUNTIME_CONSUMER_HPP
