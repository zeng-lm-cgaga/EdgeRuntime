#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "edge_runtime/consumer.hpp"
#include "edge_runtime/error.hpp"
#include "test_payload.hpp"

int main(int argc, char** argv) {
	if (argc != 4) {
		std::fprintf(stderr, "usage: loan_child <name> <expected_counter> <posix|fd>\n");
		return 2;
	}
	edge_runtime::ChannelOptions options;
	options.name = argv[1];
	options.transport = std::string(argv[3]) == "fd" ? edge_runtime::Transport::kMemfdFdPass
	                                                 : edge_runtime::Transport::kPosixShm;
	const uint64_t expected = std::strtoull(argv[2], nullptr, 10);
	auto consumer = edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	if (!consumer) {
		std::fprintf(stderr, "open failed: %s\n", edge_runtime::to_string(consumer.error().code));
		return 1;
	}
	auto loan = consumer.value().wait_loan_latest(std::chrono::seconds(2));
	if (!loan) {
		std::fprintf(stderr, "loan failed: %s\n", edge_runtime::to_string(loan.error().code));
		return 1;
	}
	TestPayloadV1 decoded;
	const bool decoded_ok = edge_runtime::PayloadCodec<TestPayloadV1>::decode(
	        loan.value().data(), loan.value().size(), &decoded);
	const bool ok = decoded_ok && decoded.counter == expected && loan.value().sequence() == 1;
	std::printf("LOAN_RESULT ok=%d counter=%llu seq=%llu\n", ok ? 1 : 0,
	            static_cast<unsigned long long>(decoded.counter),
	            static_cast<unsigned long long>(loan.value().sequence()));
	return ok ? 0 : 1;
}
