// consume_demo: the minimal downstream consumer for the INSTALLED EdgeRuntime
// package. It mirrors what an application does: include only public headers
// (detail/ is deliberately NOT installed), supply its own PayloadCodec<T>, and
// run one create -> publish -> open -> try_read_latest round trip.
//
// Compile-and-run is validated by the release-audit CI gate and the local
// scripts/ci.sh (evidence: er8_install_consume.txt).

#include <array>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include "edge_runtime/consumer.hpp"
#include "edge_runtime/producer.hpp"
#include "edge_runtime/sample.hpp"
#include "edge_runtime/schema.hpp"

namespace {

// The library never touches T; the caller's codec defines the canonical bytes
// (design §16.1). This demo uses a host-endian struct, exactly what a real
// same-machine application would do.
struct Point {
	int32_t x;
	int32_t y;
};

}  // namespace

template <>
struct edge_runtime::PayloadCodec<Point> {
	static constexpr bool kDefined = true;
	static constexpr uint32_t kEncodedSize = 8;
	using EncodedBuffer = std::array<std::byte, kEncodedSize>;
	static bool encode(const Point& v, std::byte* out, size_t cap) noexcept {
		if (cap < kEncodedSize) return false;
		const int32_t x = v.x;
		const int32_t y = v.y;
		std::memcpy(out, &x, 4);
		std::memcpy(out + 4, &y, 4);
		return true;
	}
	static bool decode(const std::byte* in, size_t size, Point* v) noexcept {
		if (size < kEncodedSize) return false;
		std::memcpy(&v->x, in, 4);
		std::memcpy(&v->y, in + 4, 4);
		return true;
	}
};

int main() {
	constexpr char kName[] = "er_consume_demo_ch";
	edge_runtime::ChannelOptions opts;
	opts.name = kName;
	edge_runtime::SchemaDescriptor schema;
	schema.fingerprint.fill(std::byte{0xAB});
	schema.version = 1;
	schema.debug_name = "er_consume_demo";

	auto p = edge_runtime::Producer<Point>::create(opts, schema);
	if (!p) {
		std::fprintf(stderr, "create failed\n");
		return 1;
	}
	auto c = edge_runtime::Consumer<Point>::open(opts, schema);
	if (!c) {
		std::fprintf(stderr, "open failed\n");
		return 1;
	}

	auto pub = p.value().publish(Point{3, 4});
	if (!pub) {
		std::fprintf(stderr, "publish failed\n");
		return 1;
	}
	auto r = c.value().try_read_latest();
	if (!r) {
		std::fprintf(stderr, "read failed\n");
		return 1;
	}
	const Point& got = r.value().value;
	const uint64_t seq = r.value().sequence;
	std::printf("OK x=%d y=%d seq=%" PRIu64 "\n", got.x, got.y, seq);
	return (got.x == 3 && got.y == 4) ? 0 : 2;
}
