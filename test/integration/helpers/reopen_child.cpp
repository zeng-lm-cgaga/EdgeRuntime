// Cross-exec reopen helper (ER1 I01): a SEPARATE binary that re-opens the
// channel by name and verifies the frozen identity (generation, instance
// nonce, ABI, schema, endpoint states) that the driver producer created. It
// proves a true reopen: this image's mmap is fresh, not inherited.
//
// usage: reopen_child <name> <schema_version> <fingerprint_hex64> <gen>
//                      <nonce_hi> <nonce_lo>
// prints "OPEN_RESULT ok=1|0 ..." to stdout, exit 0 on success.

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "edge_runtime/consumer.hpp"
#include "edge_runtime/schema.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {

std::array<std::byte, 32> parse_fingerprint(const char* hex) {
	std::array<std::byte, 32> fp{};
	if (std::strlen(hex) != 64) {
		std::fprintf(stderr, "bad fingerprint length\n");
		std::exit(2);
	}
	for (size_t i = 0; i < 32; ++i) {
		unsigned int byte = 0;
		char two[3] = {hex[2 * i], hex[2 * i + 1], '\0'};
		if (std::sscanf(two, "%2x", &byte) != 1) {
			std::fprintf(stderr, "bad fingerprint hex\n");
			std::exit(2);
		}
		fp[i] = static_cast<std::byte>(byte);
	}
	return fp;
}

}  // namespace

int main(int argc, char** argv) {
	if (argc != 7) {
		std::fprintf(stderr,
		             "usage: reopen_child <name> <schema_version> <fp_hex64> "
		             "<gen> <nonce_hi> <nonce_lo>\n");
		return 2;
	}
	const std::string name = argv[1];
	const uint32_t schema_version = static_cast<uint32_t>(std::strtoul(argv[2], nullptr, 10));
	const auto fingerprint = parse_fingerprint(argv[3]);
	const uint64_t gen = std::strtoull(argv[4], nullptr, 10);
	const uint64_t nonce_hi = std::strtoull(argv[5], nullptr, 10);
	const uint64_t nonce_lo = std::strtoull(argv[6], nullptr, 10);

	const edge_runtime::SchemaDescriptor schema{fingerprint, schema_version,
	                                            kTestPayloadV1Name};
	edge_runtime::ChannelOptions opts;
	opts.name = name;

	auto cons = edge_runtime::Consumer<TestPayloadV1>::open(opts, schema);
	if (!cons) {
		std::fprintf(stderr, "OPEN_FAIL code=%s ctx=%s\n",
		             edge_runtime::to_string(cons.error().code), cons.error().context);
		return 1;
	}
	auto st = cons.value().status();
	if (!st) {
		std::fprintf(stderr, "STATUS_FAIL code=%s\n",
		             edge_runtime::to_string(st.error().code));
		return 1;
	}
	const edge_runtime::ChannelStatus& s = st.value();
	const bool ok = s.ready && s.abi_major == 1 && s.abi_minor == 0 && s.slot_count == 3 &&
	                s.payload_size == edge_runtime::PayloadCodec<TestPayloadV1>::kEncodedSize &&
	                s.generation == gen && s.instance_nonce_hi == nonce_hi &&
	                s.instance_nonce_lo == nonce_lo && s.producer_alive && s.consumer_alive;
	std::printf("OPEN_RESULT ok=%d gen=%llu nonce_hi=%llx nonce_lo=%llx\n", ok ? 1 : 0,
	            static_cast<unsigned long long>(s.generation),
	            static_cast<unsigned long long>(s.instance_nonce_hi),
	            static_cast<unsigned long long>(s.instance_nonce_lo));
	return ok ? 0 : 1;
}
