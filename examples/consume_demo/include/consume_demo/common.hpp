#ifndef ER_CONSUME_DEMO_COMMON_HPP
#define ER_CONSUME_DEMO_COMMON_HPP

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

#include "edge_runtime/common/schema.hpp"

namespace consume_demo {

struct Point {
	int32_t x{0};
	int32_t y{0};
};

struct QueueMessage {
	uint64_t sequence{0};
	uint32_t value{0};
	uint32_t reserved{0};
};

static_assert(std::is_trivially_copyable_v<QueueMessage>, "queue message must be trivial");

struct ChildProcess {
	int event_read{-1};
	int command_write{-1};
	int pid{-1};
	int exit_code{-1};
};

std::string executable_path();
std::string unique_name(const char* prefix);

bool write_token(int fd, char token);
bool read_token(int fd, char expected, std::chrono::milliseconds timeout);
bool wait_child(ChildProcess* child, std::chrono::milliseconds timeout);
void stop_child(ChildProcess* child);
void close_child(ChildProcess* child);

ChildProcess spawn_child(const std::string& mode, const std::vector<std::string>& args,
						const std::vector<int>& inherited_fds,
						const std::vector<int>& keep_fds);

edge_runtime::SchemaDescriptor point_schema();
edge_runtime::SchemaDescriptor queue_schema();
edge_runtime::SchemaDescriptor buffer_schema();

bool encode_point(const Point& point, std::byte* output, size_t capacity) noexcept;
bool decode_point(const std::byte* input, size_t size, Point* point) noexcept;

}  // namespace consume_demo

template <>
struct edge_runtime::PayloadCodec<consume_demo::Point> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 8;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;

	static bool encode(const consume_demo::Point& value, std::byte* output,
	                  size_t capacity) noexcept {
		return consume_demo::encode_point(value, output, capacity);
	}

	static bool decode(const std::byte* input, size_t size, consume_demo::Point* value) noexcept {
		return consume_demo::decode_point(input, size, value);
	}
};

#endif  // ER_CONSUME_DEMO_COMMON_HPP
