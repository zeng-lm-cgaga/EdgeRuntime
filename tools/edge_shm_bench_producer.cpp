// edge_shm_bench_producer: the ShmChannel producer side of the ER7 benchmark
// (design §21). Publishes BenchPayloadV1<N> samples with a CLOCK_MONOTONIC_RAW
// publish_ns stamped into the payload immediately before publish(), paced by
// --rate (absolute schedule, no burst catch-up) or at max throughput, until
// --samples or --max-time-ms is reached. Emits the stable markers the bench
// driver parses:
//
//   READY generation=<n>
//   PUBLISH_FAIL code=<name>
//   DONE published=<n> cpu_us=<n>
//
// Like every benchmark helper it is a SEPARATE binary exec'd by the driver
// (§18.3) — never an in-process thread.

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "bench_payload.hpp"
#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/error.hpp"
#include "edge_runtime/producer.hpp"
#include "edge_runtime/result.hpp"
#include "edge_runtime/sample.hpp"
#include "edge_runtime/schema.hpp"
#include "tool_common.hpp"

namespace {

using edge_tool::arg_flag;
using edge_tool::arg_u64;
using edge_tool::arg_value;
using edge_tool::cpu_us_now;
using edge_tool::monotonic_raw_now_ns;

struct Args {
	std::string name;
	uint32_t payload = 64;
	uint64_t samples = 1000000;    // stop at this many publishes
	uint64_t max_time_ms = 60000;  // ... or this wall cap
	uint64_t rate_hz = 0;          // 0 = max throughput
	bool checksum = true;
};

template <size_t N>
int run_producer_n(const Args& a, const edge_runtime::SchemaDescriptor& schema) {
	using ProducerT = edge_runtime::Producer<bench::BenchPayloadV1<N>>;
	edge_runtime::ChannelOptions opts;
	opts.name = a.name;
	opts.enable_payload_checksum = a.checksum;

	auto p = ProducerT::create(opts, schema);
	if (!p) {
		const auto& e = p.error();
		std::printf("CREATE_FAIL code=%s errno=%d op=%s ctx=%s\n",
		            edge_runtime::to_string(e.code), e.errno_value, e.operation, e.context);
		std::fflush(stdout);
		return 2;
	}
	std::printf("READY generation=%" PRIu64 "\n", p.value().generation());
	std::fflush(stdout);

	const int64_t deadline_ms = a.max_time_ms > 0 ? edge_tool::monotonic_ms_now() +
	                                                        static_cast<int64_t>(a.max_time_ms)
	                                              : 0;
	const uint64_t start_cpu = cpu_us_now();

	edge_tool::RatePacer pacer(a.rate_hz);
	pacer.start();
	uint64_t published = 0;
	for (;;) {
		if (published > 0) pacer.wait_until_next();  // first sample is immediate
		const uint64_t publish_ns = monotonic_raw_now_ns();
		bench::BenchPayloadV1<N> v{};
		v.counter = published;  // sample_sequence starts at 1 -> seq - 1
		v.publish_ns = publish_ns;
		auto r = p.value().publish(v);
		if (!r) {
			std::printf("PUBLISH_FAIL code=%s published=%" PRIu64 "\n",
			            edge_runtime::to_string(r.error().code), published);
			std::fflush(stdout);
			return 3;
		}
		++published;
		if (a.samples > 0 && published >= a.samples) break;
		if (deadline_ms != 0 && edge_tool::monotonic_ms_now() >= deadline_ms) break;
	}

	const uint64_t end_cpu = cpu_us_now();
	std::printf("DONE published=%" PRIu64 " cpu_us=%" PRIu64 "\n", published,
	            end_cpu - start_cpu);
	std::fflush(stdout);
	return 0;
}

}  // namespace

int main(int argc, char** argv) {
	const char* name = edge_tool::arg_value(argc, argv, "--name");
	if (name == nullptr || *name == '\0') {
		std::fprintf(stderr,
		             "usage: edge_shm_bench_producer --name <name> --payload <bytes> "
		             "[--samples N] [--max-time-ms T] [--rate hz] [--checksum 0|1]\n");
		return 2;
	}
	Args a;
	a.name = name;
	a.payload = static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--payload", 64));
	a.samples = edge_tool::arg_u64(argc, argv, "--samples", 1000000);
	a.max_time_ms = edge_tool::arg_u64(argc, argv, "--max-time-ms", 60000);
	a.rate_hz = edge_tool::arg_u64(argc, argv, "--rate", 0);
	a.checksum = edge_tool::arg_u64(argc, argv, "--checksum", 1) != 0;
	if (edge_tool::arg_flag(argc, argv, "--checksum-off")) a.checksum = false;

	const edge_runtime::SchemaDescriptor schema{bench::bench_fingerprint(a.payload), 1,
	                                            "BenchPayloadV1"};

	switch (a.payload) {
		case 64:
			return run_producer_n<64>(a, schema);
		case 256:
			return run_producer_n<256>(a, schema);
		case 1024:
			return run_producer_n<1024>(a, schema);
		case 4096:
			return run_producer_n<4096>(a, schema);
		case 16384:
			return run_producer_n<16384>(a, schema);
		case 65536:
			return run_producer_n<65536>(a, schema);
		default:
			std::fprintf(
			        stderr,
			        "unsupported --payload %u (need 64|256|1024|4096|16384|65536)\n",
			        a.payload);
			return 2;
	}
}
