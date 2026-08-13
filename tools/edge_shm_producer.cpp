// edge_shm_producer: demo / integration / crash-matrix workhorse for the
// producer side (design §9.1 + §11). Publishes a running counter as
// TestPayloadV1 (or V2 with --schema testpayloadv2) into a channel and emits
// stable one-line markers that test drivers parse:
//
//   READY                         channel created, publishing starts
//   DONE published=<n> last_seq=<k>
//   CREATE_FAIL code=<name>
//   PUBLISH_FAIL code=<name> seq=<n>
//
// `--count 0` (default) publishes forever until SIGTERM/SIGINT, which triggers
// the clean-shutdown destructor (§15.2).

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/error.hpp"
#include "edge_runtime/producer.hpp"
#include "edge_runtime/result.hpp"
#include "test_payload.hpp"
#include "tool_common.hpp"

namespace {

std::atomic<bool> g_stop{false};

void on_signal(int) { g_stop.store(true, std::memory_order_relaxed); }

struct Args {
	std::string name;
	uint64_t count = 0;           // 0 = publish forever
	uint64_t interval_us = 0;     // 0 = as fast as possible
	uint64_t seq_start = 0;       // counter of the first sample
	uint64_t sleep_first_us = 0;  // stay alive+idle this long before publishing (ER3 I13/I14)
	bool no_publish = false;      // create channel, publish nothing, wait for signal (ER3 I14)
	bool checksum = true;
	bool use_v2 = false;
	edge_runtime::Transport transport{edge_runtime::Transport::kPosixShm};
	uint64_t heartbeat_interval_us = 0;  // v0.2: 0 = heartbeat disabled
	bool heartbeat_only = false;         // v0.2: heartbeat loop, never publish (C18)
};

// Publish one sample; returns the published sequence or 0 on failure.
template <typename ProducerT>
uint64_t publish_one(ProducerT& producer, uint64_t counter) {
	using ValueT = typename ProducerT::value_type;
	ValueT v;
	if constexpr (std::is_same_v<ValueT, TestPayloadV1>) {
		v.magic = 0x5A000001u;
		v.counter = counter;
		v.flags = 0;
	} else {
		v.magic = 0x5A000002u;
		v.value = static_cast<uint32_t>(counter & 0xFFFFFFFFu);
	}
	auto res = producer.publish(v);
	return res ? res.value().sequence : 0;
}

template <typename ProducerT>
int run_publish(const Args& a, const edge_runtime::SchemaDescriptor& schema) {
	edge_runtime::ChannelOptions opts;
	opts.name = a.name;
	opts.enable_payload_checksum = a.checksum;
	opts.transport = a.transport;
	opts.heartbeat_interval = std::chrono::microseconds(a.heartbeat_interval_us);

	auto producer = ProducerT::create(opts, schema);
	if (!producer) {
		const auto& e = producer.error();
		std::printf("CREATE_FAIL code=%s errno=%d op=%s ctx=%s\n",
		            edge_runtime::to_string(e.code), e.errno_value, e.operation, e.context);
		std::fflush(stdout);
		return 2;
	}
	std::printf("READY\n");
	std::printf("GENERATION %" PRIu64 "\n", producer.value().generation());
	std::fflush(stdout);

	uint64_t published = 0;
	uint64_t counter = a.seq_start;
	uint64_t last_seq = 0;

	// ER3 I13/I14: the producer stays alive but publishes nothing for the window,
	// so a consumer's wait_latest times out with the producer ONLINE+alive
	// (-> DataStale) rather than being woken by a sample.
	if (a.sleep_first_us > 0) {
		const uint64_t step = std::min<uint64_t>(a.sleep_first_us, 250000);
		uint64_t slept = 0;
		while (slept < a.sleep_first_us && !g_stop.load(std::memory_order_relaxed)) {
			std::this_thread::sleep_for(std::chrono::microseconds(step));
			slept += step;
		}
	}

	if (a.no_publish) {
		// Create-only mode (I14): hold the channel open and publish nothing. Only a
		// signal ends this, so the clean-shutdown destructor (§15.2) marks
		// producer_state OFFLINE and the consumer's wait classifies ProducerOffline.
		while (!g_stop.load(std::memory_order_relaxed)) {
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
	} else if (a.heartbeat_only) {
		// v0.2 C18: heartbeat loop with no publishes. The application-level loop
		// is "healthy" while this runs; SIGSTOP at the C18 failpoint freezes it.
		const uint64_t step = std::min<uint64_t>(a.heartbeat_interval_us, 50000);
		while (!g_stop.load(std::memory_order_relaxed)) {
			(void)producer.value().heartbeat();
			std::this_thread::sleep_for(std::chrono::microseconds(step));
		}
	} else {
		while (true) {
			const uint64_t seq = publish_one(producer.value(), counter);
			if (seq == 0) {
				std::printf("PUBLISH_FAIL seq=%" PRIu64 "\n",
				            static_cast<uint64_t>(last_seq));
				std::fflush(stdout);
				return 3;
			}
			++published;
			last_seq = seq;

			if (a.count > 0 && published >= a.count) break;
			if (a.count == 0 && g_stop.load(std::memory_order_relaxed)) break;
			if (a.interval_us > 0) {
				std::this_thread::sleep_for(
				        std::chrono::microseconds(a.interval_us));
			}
			++counter;
		}
	}  // else: normal publish loop

	std::printf("DONE published=%" PRIu64 " last_seq=%" PRIu64 "\n", published, last_seq);
	std::fflush(stdout);
	return 0;
}

}  // namespace

int main(int argc, char** argv) {
	Args a;
	const char* name = edge_tool::arg_value(argc, argv, "--name");
	a.name = name ? name : "";
	a.count = edge_tool::arg_u64(argc, argv, "--count", 0);
	a.interval_us = edge_tool::arg_u64(argc, argv, "--interval-us", 0);
	a.seq_start = edge_tool::arg_u64(argc, argv, "--seq-start", 0);
	a.checksum = edge_tool::arg_u64(argc, argv, "--checksum", 1) != 0;
	a.sleep_first_us = edge_tool::arg_u64(argc, argv, "--sleep-first-us", 0);
	a.no_publish = edge_tool::arg_flag(argc, argv, "--no-publish") ||
	               edge_tool::arg_u64(argc, argv, "--no-publish", 0) != 0;
	a.heartbeat_interval_us = edge_tool::arg_u64(argc, argv, "--heartbeat-interval-us", 0);
	a.heartbeat_only = edge_tool::arg_flag(argc, argv, "--heartbeat-only") ||
	                   edge_tool::arg_u64(argc, argv, "--heartbeat-only", 0) != 0;
	{
		const char* transport_arg = edge_tool::arg_value(argc, argv, "--transport");
		if (transport_arg != nullptr && std::string(transport_arg) == "fd") {
			a.transport = edge_runtime::Transport::kMemfdFdPass;
		}
	}

	if (a.name.empty()) {
		std::fprintf(stderr,
		             "usage: edge_shm_producer --name <name> "
		             "--schema-hex <64hex> [--schema-version N] "
		             "[--schema testpayloadv1|testpayloadv2] "
		             "[--count N] [--interval-us N] [--seq-start N] "
		             "[--sleep-first-us N] [--no-publish 0|1] "
		             "[--checksum 0|1] [--transport fd|posix] "
		             "[--heartbeat-interval-us N] [--heartbeat-only 0|1]\n");
		return 2;
	}

	const char* schema_name = edge_tool::arg_value(argc, argv, "--schema");
	if (schema_name != nullptr) {
		a.use_v2 = std::string(schema_name) == "testpayloadv2";
	}

	edge_runtime::SchemaDescriptor schema;
	if (edge_tool::arg_value(argc, argv, "--schema-hex") != nullptr) {
		schema = edge_tool::schema_from_args(
		        argc, argv, a.use_v2 ? kTestPayloadV2Name : kTestPayloadV1Name);
	} else {
		schema = a.use_v2 ? TestPayloadV2Schema() : TestPayloadV1Schema();
	}

	std::signal(SIGTERM, on_signal);
	std::signal(SIGINT, on_signal);

	if (a.use_v2) {
		return run_publish<edge_runtime::Producer<TestPayloadV2>>(a, schema);
	}
	return run_publish<edge_runtime::Producer<TestPayloadV1>>(a, schema);
}
