#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <type_traits>

#include "edge_runtime/consumer.hpp"
#include "edge_runtime/mpmc_queue.hpp"
#include "edge_runtime/producer.hpp"
#include "edge_runtime/schema.hpp"
#include "edge_runtime/shared_buffer_pool.hpp"
#include "edge_runtime/supervisor.hpp"
#include "self_ipcs/buffer/shared_buffer_pool.hpp"
#include "self_ipcs/channel/channel_options.hpp"
#include "self_ipcs/channel/consumer.hpp"
#include "self_ipcs/channel/loan.hpp"
#include "self_ipcs/channel/producer.hpp"
#include "self_ipcs/channel/sample.hpp"
#include "self_ipcs/common/error.hpp"
#include "self_ipcs/common/result.hpp"
#include "self_ipcs/common/schema.hpp"
#include "self_ipcs/process/supervisor.hpp"
#include "self_ipcs/queue/mpmc_queue.hpp"

namespace compat_demo {

struct NewPayload {
	uint32_t value{0};
};

struct LegacyPayload {
	uint32_t value{0};
};

bool encode_value(uint32_t value, std::byte* output, size_t capacity) noexcept {
	if (output == nullptr || capacity < sizeof(value)) return false;
	std::memcpy(output, &value, sizeof(value));
	return true;
}

bool decode_value(const std::byte* input, size_t size, uint32_t* value) noexcept {
	if (input == nullptr || value == nullptr || size != sizeof(*value)) return false;
	std::memcpy(value, input, sizeof(*value));
	return true;
}

}  // namespace compat_demo

template <>
struct self_ipcs::PayloadCodec<compat_demo::NewPayload> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 4;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;

	static bool encode(const compat_demo::NewPayload& value, std::byte* output,
	                   size_t capacity) noexcept {
		return compat_demo::encode_value(value.value, output, capacity);
	}

	static bool decode(const std::byte* input, size_t size,
	                   compat_demo::NewPayload* value) noexcept {
		return value != nullptr && compat_demo::decode_value(input, size, &value->value);
	}
};

template <>
struct edge_runtime::PayloadCodec<compat_demo::LegacyPayload> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 4;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;

	static bool encode(const compat_demo::LegacyPayload& value, std::byte* output,
	                   size_t capacity) noexcept {
		return compat_demo::encode_value(value.value, output, capacity);
	}

	static bool decode(const std::byte* input, size_t size,
	                   compat_demo::LegacyPayload* value) noexcept {
		return value != nullptr && compat_demo::decode_value(input, size, &value->value);
	}
};

static_assert(self_ipcs::kSupportedPayload<compat_demo::NewPayload>);
static_assert(edge_runtime::kSupportedPayload<compat_demo::NewPayload>);
static_assert(self_ipcs::kSupportedPayload<compat_demo::LegacyPayload>);
static_assert(edge_runtime::kSupportedPayload<compat_demo::LegacyPayload>);
static_assert(std::is_same_v<self_ipcs::PayloadCodec<compat_demo::NewPayload>,
                             edge_runtime::PayloadCodec<compat_demo::NewPayload>>);
static_assert(std::is_same_v<self_ipcs::PayloadCodec<compat_demo::LegacyPayload>,
                             edge_runtime::PayloadCodec<compat_demo::LegacyPayload>>);
static_assert(std::is_same_v<self_ipcs::SchemaDescriptor, edge_runtime::SchemaDescriptor>);
static_assert(std::is_same_v<self_ipcs::Result<compat_demo::NewPayload>,
                             edge_runtime::Result<compat_demo::NewPayload>>);
static_assert(std::is_same_v<self_ipcs::Producer<compat_demo::NewPayload>,
                             edge_runtime::Producer<compat_demo::NewPayload>>);
static_assert(std::is_same_v<self_ipcs::Consumer<compat_demo::LegacyPayload>,
                             edge_runtime::Consumer<compat_demo::LegacyPayload>>);
static_assert(std::is_same_v<self_ipcs::MpmcQueue<uint64_t>, edge_runtime::MpmcQueue<uint64_t>>);
static_assert(std::is_same_v<self_ipcs::SharedBufferPool, edge_runtime::SharedBufferPool>);
static_assert(std::is_same_v<self_ipcs::ProducerSupervisor, edge_runtime::ProducerSupervisor>);

namespace compat_demo {

template <typename Payload>
bool roundtrip(uint32_t value) {
	typename self_ipcs::PayloadCodec<Payload>::EncodedBuffer buffer{};
	if (!edge_runtime::PayloadCodec<Payload>::encode(Payload{value}, buffer.data(),
	                                                 buffer.size())) {
		return false;
	}
	Payload decoded{};
	if (!self_ipcs::PayloadCodec<Payload>::decode(buffer.data(), buffer.size(), &decoded) ||
	    decoded.value != value) {
		return false;
	}
	return !self_ipcs::PayloadCodec<Payload>::encode(Payload{value}, buffer.data(), 0) &&
	       !edge_runtime::PayloadCodec<Payload>::decode(buffer.data(), 0, &decoded);
}

}  // namespace compat_demo

int main() {
	if (!compat_demo::roundtrip<compat_demo::NewPayload>(42) ||
	    !compat_demo::roundtrip<compat_demo::LegacyPayload>(84)) {
		std::fprintf(stderr, "mixed qualified PayloadCodec roundtrip failed\n");
		return 1;
	}
	if (self_ipcs::to_string(edge_runtime::ErrorCode::kTimeout) == nullptr ||
	    edge_runtime::to_string(self_ipcs::ErrorCode::kQueueEmpty) == nullptr) {
		return 1;
	}
	std::printf(
	        "OK installed mixed SelfIPCs/EdgeRuntime public API and qualified PayloadCodec\n");
	return 0;
}
