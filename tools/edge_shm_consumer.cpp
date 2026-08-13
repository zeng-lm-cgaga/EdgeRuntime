// edge_shm_consumer: demo / integration / crash-matrix workhorse for the
// consumer side (design §9.2 + §12). Opens a channel, reads latest samples and
// verifies the I02/I17 pattern contract (counter == sequence - 1 when
// --seq-start 0), tracking torn reads and missed-sample gaps. Emits stable
// one-line markers that test drivers parse:
//
//   READY                           open succeeded, reading starts
//   SUMMARY reads=<n> torn=<n> missed_total=<n> last_seq=<n> max_gap=<n>
//           waits=<n> timed_out=<n> last_error=<name>
//   OPEN_FAIL code=<name>
//   READ_ERROR code=<name> seq=<n>
//
// --open-retry-ms retries NotFound so a consumer can start before the producer
// (ER2 I04). --use-wait-ms N switches from polling to wait_latest(N ms) (ER3):
// a timeout then ends with timed_out=1 and last_error=DataStale|ProducerOffline|
// RecoveryBlocked, classified from producer liveness (§15.5). Exit code 4
// signals a torn read, 3 a read error, 2 an open error.

#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/consumer.hpp"
#include "edge_runtime/error.hpp"
#include "edge_runtime/result.hpp"
#include "edge_runtime/sample.hpp"
#include "test_payload.hpp"
#include "tool_common.hpp"

namespace {

using edge_tool::monotonic_ms_now;

struct Args {
	std::string name;
	uint64_t reads = 0;                // 0 = until expect-last-seq / timeout
	uint64_t read_interval_ms = 0;     // 0 = spin
	uint64_t expect_last_seq = 0;      // 0 = disabled
	uint64_t open_retry_ms = 10000;    // 0 = fail fast on NotFound
	uint64_t read_timeout_ms = 30000;  // 0 = infinite
	uint64_t seq_start = 0;            // pattern offset (counter = seq-1+start)
	uint64_t use_wait_ms = 0;          // >0: wait_latest(ms) instead of polling (ER3)
	bool checksum = true;
	bool use_v2 = false;
};

template <typename T>
int run_consume(const Args& a, const edge_runtime::SchemaDescriptor& schema) {
	using ConsumerT = edge_runtime::Consumer<T>;
	edge_runtime::ChannelOptions opts;
	opts.name = a.name;
	opts.enable_payload_checksum = a.checksum;

	// ---- open with retry while the producer is absent or mid-create (I04) ------
	// NotFound: no channel yet. InitializationIncomplete: the bootstrap is
	// present but the producer has not committed the READY header yet. CorruptHeader
	// on the bootstrap parse: the consumer read the segment in the tiny window
	// between ftruncate and the bootstrap write (magic not yet present) — also
	// "producer not ready", retry it. Retrying all three is what lets a consumer
	// reliably start before (or while) its producer creates.
	auto open_one = [&]() { return ConsumerT::open(opts, schema); };
	auto consumer = open_one();
	if (!consumer &&
	    (consumer.error().code == edge_runtime::ErrorCode::kNotFound ||
	     consumer.error().code == edge_runtime::ErrorCode::kInitializationIncomplete ||
	     consumer.error().code == edge_runtime::ErrorCode::kCorruptHeader) &&
	    a.open_retry_ms > 0) {
		const int64_t deadline_ms =
		        monotonic_ms_now() + static_cast<int64_t>(a.open_retry_ms);
		while (!consumer && monotonic_ms_now() < deadline_ms) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			consumer = open_one();
		}
	}
	if (!consumer) {
		const auto& e = consumer.error();
		std::printf("OPEN_FAIL code=%s errno=%d op=%s ctx=%s\n",
		            edge_runtime::to_string(e.code), e.errno_value, e.operation, e.context);
		std::fflush(stdout);
		return 2;
	}
	std::printf("READY\n");
	std::fflush(stdout);

	// ---- read loop --------------------------------------------------------------
	const int64_t deadline_ms =
	        a.read_timeout_ms > 0 ? monotonic_ms_now() + static_cast<int64_t>(a.read_timeout_ms)
	                              : 0;
	uint64_t reads = 0;
	uint64_t torn = 0;
	uint64_t missed_total = 0;
	uint64_t last_seq = 0;
	uint64_t max_gap = 0;
	uint64_t waits = 0;      // wait_latest calls that returned no sample
	uint64_t timed_out = 0;  // 1 when a wait ended in a timeout classification
	const char* last_error = "none";

	while (true) {
		auto snap = a.use_wait_ms > 0
		                    ? consumer.value().wait_latest(std::chrono::milliseconds(
		                              static_cast<int64_t>(a.use_wait_ms)))
		                    : consumer.value().try_read_latest();
		if (snap) {
			edge_runtime::Sample<T> s = std::move(snap.value());
			last_seq = s.sequence;

			const uint64_t expected_counter =
			        a.seq_start == 0 ? s.sequence - 1 : s.sequence - 1 - a.seq_start;
			bool ok = true;
			if constexpr (std::is_same_v<T, TestPayloadV1>) {
				if (s.value.magic != 0x5A000001u) ok = false;
				if (s.value.counter != expected_counter) ok = false;
				if (s.value.flags != 0) ok = false;
			} else {
				if (s.value.magic != 0x5A000002u) ok = false;
			}
			if (!ok) ++torn;

			if (s.missed_samples > max_gap) max_gap = s.missed_samples;
			missed_total += s.missed_samples;
			++reads;

			if (a.expect_last_seq > 0 && s.sequence >= a.expect_last_seq) break;
			if (a.reads > 0 && reads >= a.reads) break;
			continue;
		}

		const edge_runtime::ErrorCode ec = snap.error().code;
		if (a.use_wait_ms > 0) {
			++waits;
			// A wait_latest call only returns a classified timeout or a real error
			// (NoNewSample/ReadContention are consumed inside the wait loop). The
			// timeout classifications are a clean, informative end: the producer
			// liveness decided the outcome (alive-but-idle -> DataStale, offline/dead
			// -> ProducerOffline, unverifiable -> RecoveryBlocked).
			if (ec == edge_runtime::ErrorCode::kDataStale ||
			    ec == edge_runtime::ErrorCode::kProducerOffline ||
			    ec == edge_runtime::ErrorCode::kRecoveryBlocked) {
				timed_out = 1;
				last_error = edge_runtime::to_string(ec);
				break;
			}
			if (ec == edge_runtime::ErrorCode::kNoNewSample ||
			    ec == edge_runtime::ErrorCode::kReadContention) {
				if (deadline_ms != 0 && monotonic_ms_now() >= deadline_ms) break;
				continue;
			}
			std::printf("READ_ERROR code=%s seq=%" PRIu64 "\n",
			            edge_runtime::to_string(ec), static_cast<uint64_t>(last_seq));
			std::fflush(stdout);
			return 3;
		}
		if (ec == edge_runtime::ErrorCode::kNoNewSample ||
		    ec == edge_runtime::ErrorCode::kReadContention) {
			// nothing new yet: spin or pace, then re-check deadline
			if (a.read_interval_ms > 0) {
				std::this_thread::sleep_for(
				        std::chrono::milliseconds(a.read_interval_ms));
			}
			if (deadline_ms != 0 && monotonic_ms_now() >= deadline_ms) break;
			continue;
		}
		// a real read error: report and fail
		std::printf("READ_ERROR code=%s seq=%" PRIu64 "\n", edge_runtime::to_string(ec),
		            static_cast<uint64_t>(last_seq));
		std::fflush(stdout);
		return 3;
	}

	std::printf("SUMMARY reads=%" PRIu64 " torn=%" PRIu64 " missed_total=%" PRIu64
	            " last_seq=%" PRIu64 " max_gap=%" PRIu64 " waits=%" PRIu64 " timed_out=%" PRIu64
	            " last_error=%s\n",
	            reads, torn, missed_total, last_seq, max_gap, waits, timed_out, last_error);
	std::fflush(stdout);
	return torn > 0 ? 4 : 0;
}

}  // namespace

namespace {
// The I13 driver sends SIGUSR1 to interrupt a blocked wait_latest. The handler
// does nothing but give the kernel a reason to return EINTR from futex_wait; the
// wait loop then re-waits with the ORIGINAL deadline (design §14.2/§15.7).
void on_sigusr1(int) {}
}  // namespace

int main(int argc, char** argv) {
	// No SA_RESTART: the wait loop must observe EINTR itself.
	struct sigaction sa {};
	sa.sa_handler = on_sigusr1;
	::sigemptyset(&sa.sa_mask);
	::sigaction(SIGUSR1, &sa, nullptr);

	Args a;
	const char* name = edge_tool::arg_value(argc, argv, "--name");
	a.name = name ? name : "";
	a.reads = edge_tool::arg_u64(argc, argv, "--reads", 0);
	a.read_interval_ms = edge_tool::arg_u64(argc, argv, "--read-interval-ms", 0);
	a.expect_last_seq = edge_tool::arg_u64(argc, argv, "--expect-last-seq", 0);
	a.open_retry_ms = edge_tool::arg_u64(argc, argv, "--open-retry-ms", 10000);
	a.read_timeout_ms = edge_tool::arg_u64(argc, argv, "--read-timeout-ms", 30000);
	a.seq_start = edge_tool::arg_u64(argc, argv, "--seq-start", 0);
	a.use_wait_ms = edge_tool::arg_u64(argc, argv, "--use-wait-ms", 0);
	a.checksum = edge_tool::arg_u64(argc, argv, "--checksum", 1) != 0;

	if (a.name.empty()) {
		std::fprintf(stderr,
		             "usage: edge_shm_consumer --name <name> "
		             "--schema-hex <64hex> [--schema-version N] "
		             "[--schema testpayloadv1|testpayloadv2] "
		             "[--reads N] [--expect-last-seq N] "
		             "[--read-interval-ms N] [--read-timeout-ms N] "
		             "[--open-retry-ms N] [--seq-start N] "
		             "[--use-wait-ms N] [--checksum 0|1]\n");
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

	if (a.use_v2) {
		return run_consume<TestPayloadV2>(a, schema);
	}
	return run_consume<TestPayloadV1>(a, schema);
}
