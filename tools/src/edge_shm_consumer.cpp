// 消费者工具读取最新样本，校验序号、载荷模式和丢样本数量，并输出稳定诊断标记。
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>

#include "edge_runtime/channel/channel_options.hpp"
#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/common/result.hpp"
#include "edge_runtime/channel/sample.hpp"
#include "test_payload.hpp"
#include "edge_runtime_tool/tool_common.hpp"

namespace {

using edge_tool::monotonic_ms_now;

struct Args {
	std::string name;
	uint64_t reads = 0;
	uint64_t read_interval_ms = 0;
	uint64_t expect_last_seq = 0;
	uint64_t open_retry_ms = 10000;
	uint64_t read_timeout_ms = 30000;
	uint64_t seq_start = 0;
	uint64_t use_wait_ms = 0;
	bool checksum = true;
	bool use_v2 = false;
	bool loaned = false;
	edge_runtime::Transport transport{edge_runtime::Transport::kPosixShm};
};

template <typename T>
int run_consume(const Args& a, const edge_runtime::SchemaDescriptor& schema) {
	using ConsumerT = edge_runtime::Consumer<T>;
	edge_runtime::ChannelOptions opts;
	opts.name = a.name;
	opts.enable_payload_checksum = a.checksum;
	opts.transport = a.transport;

	auto open_one = [&]() { return ConsumerT::open(opts, schema); };
	auto consumer = open_one();
	if (!consumer &&
	    (consumer.error().code == edge_runtime::ErrorCode::kNotFound ||
	     consumer.error().code == edge_runtime::ErrorCode::kInitializationIncomplete ||
	     consumer.error().code == edge_runtime::ErrorCode::kCorruptHeader ||

	     consumer.error().code == edge_runtime::ErrorCode::kProducerOffline) &&
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

	const int64_t deadline_ms =
	        a.read_timeout_ms > 0 ? monotonic_ms_now() + static_cast<int64_t>(a.read_timeout_ms)
	                              : 0;
	uint64_t reads = 0;
	uint64_t torn = 0;
	uint64_t missed_total = 0;
	uint64_t last_seq = 0;
	uint64_t max_gap = 0;
	uint64_t waits = 0;
	uint64_t timed_out = 0;
	const char* last_error = "none";
	auto read_one = [&]() -> edge_runtime::Result<edge_runtime::Sample<T>> {
		if (!a.loaned) {
			return a.use_wait_ms > 0
			               ? consumer.value().wait_latest(std::chrono::milliseconds(
			                         static_cast<int64_t>(a.use_wait_ms)))
			               : consumer.value().try_read_latest();
		}
		auto borrowed = a.use_wait_ms > 0
		                        ? consumer.value().wait_loan_latest(std::chrono::milliseconds(
		                                  static_cast<int64_t>(a.use_wait_ms)))
		                        : consumer.value().try_loan_latest();
		if (!borrowed) return borrowed.error();
		edge_runtime::Sample<T> sample;
		if (!edge_runtime::PayloadCodec<T>::decode(borrowed.value().data(),
		                                           borrowed.value().size(), &sample.value)) {
			return edge_runtime::make_error(edge_runtime::ErrorCode::kPayloadDecodeFailed,
			                                "edge_shm_consumer", "loan decode failed");
		}
		sample.generation = borrowed.value().generation();
		sample.instance_nonce = borrowed.value().instance_nonce();
		sample.sequence = borrowed.value().sequence();
		sample.publish_boot_ns = borrowed.value().publish_boot_ns();
		sample.receive_boot_ns = borrowed.value().receive_boot_ns();
		sample.missed_samples = borrowed.value().missed_samples();
		return sample;
	};

	while (true) {
		auto snap = read_one();
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

			if (ec == edge_runtime::ErrorCode::kDataStale ||
			    ec == edge_runtime::ErrorCode::kProducerOffline ||
			    ec == edge_runtime::ErrorCode::kRecoveryBlocked ||
			    ec == edge_runtime::ErrorCode::kProducerStalled) {
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

			if (a.read_interval_ms > 0) {
				std::this_thread::sleep_for(
				        std::chrono::milliseconds(a.read_interval_ms));
			}
			if (deadline_ms != 0 && monotonic_ms_now() >= deadline_ms) break;
			continue;
		}

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

}

namespace {

void on_sigusr1(int) {}
}

int main(int argc, char** argv) {

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
	a.loaned = edge_tool::arg_flag(argc, argv, "--loaned") ||
	           edge_tool::arg_u64(argc, argv, "--loaned", 0) != 0;
	a.checksum = edge_tool::arg_u64(argc, argv, "--checksum", 1) != 0;
	{
		const char* transport_arg = edge_tool::arg_value(argc, argv, "--transport");
		if (transport_arg != nullptr && std::string(transport_arg) == "fd") {
			a.transport = edge_runtime::Transport::kMemfdFdPass;
		}
	}

	if (a.name.empty()) {
		std::fprintf(stderr,
		             "usage: edge_shm_consumer --name <name> "
		             "--schema-hex <64hex> [--schema-version N] "
		             "[--schema testpayloadv1|testpayloadv2] "
		             "[--reads N] [--expect-last-seq N] "
		             "[--read-interval-ms N] [--read-timeout-ms N] "
		             "[--open-retry-ms N] [--seq-start N] "
		             "[--use-wait-ms N] [--checksum 0|1] [--transport fd|posix] "
		             "[--loaned 0|1]\n");
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
