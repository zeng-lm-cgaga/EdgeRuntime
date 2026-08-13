// ER2 integration driver: pattern/publish-read tests (I02–I08, I17). Every
// cross-process case fork+execs the SEPARATE edge_shm_producer / edge_shm_consumer
// tool binaries (design §18.3); the tools publish a running counter and verify
// the pattern contract counter == sequence-1 on every read, so torn reads are
// observable as a nonzero torn count in the consumer's SUMMARY line.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "edge_runtime/consumer.hpp"
#include "edge_runtime/error.hpp"
#include "edge_runtime/producer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {

using edge_runtime::ChannelOptions;
using edge_runtime::Consumer;
using edge_runtime::ErrorCode;
using edge_runtime::Producer;

std::string g_producer_tool;
std::string g_consumer_tool;

std::vector<std::string> producer_args(const std::string& name) {
	return {g_producer_tool, "--name", name, "--schema", "testpayloadv1"};
}

std::vector<std::string> consumer_args(const std::string& name) {
	return {g_consumer_tool, "--name", name, "--schema", "testpayloadv1"};
}

bool out_contains(const std::string& out, const char* needle) {
	return out.find(needle) != std::string::npos;
}

// Parse `SUMMARY reads=<n> torn=<n> missed_total=<n> ...` fields.
uint64_t summary_field(const std::string& out, const char* field) {
	const std::string needle = std::string(field) + "=";
	const size_t pos = out.find(needle);
	if (pos == std::string::npos) return 0;
	const size_t val_start = pos + needle.size();
	const size_t val_end = out.find_first_of(" \n\r", val_start);
	const std::string val = out.substr(
	        val_start, val_end == std::string::npos ? std::string::npos : val_end - val_start);
	return std::strtoull(val.c_str(), nullptr, 10);
}

TEST(Pattern, MillionNoTorn) {  // I02
	const std::string name = edge_test::unique_channel_name("i02");
	auto pa = producer_args(name);
	pa.push_back("--count");
	pa.push_back("1000000");
	auto ca = consumer_args(name);
	ca.push_back("--expect-last-seq");
	ca.push_back("1000000");
	ca.push_back("--read-timeout-ms");
	ca.push_back("240000");
	ca.push_back("--open-retry-ms");
	ca.push_back("20000");

	edge_test::SpawnedChild prod, cons;
	ASSERT_TRUE(prod.spawn(pa));
	ASSERT_TRUE(cons.spawn(ca));
	std::string prod_out, cons_out;
	ASSERT_TRUE(cons.wait(250000, &cons_out)) << "consumer hung: " << cons_out;
	ASSERT_TRUE(prod.wait(250000, &prod_out)) << "producer hung: " << prod_out;
	ASSERT_EQ(prod.exit_code(), 0) << prod_out;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	// every read checksum-verified and pattern-checked -> zero torn
	EXPECT_EQ(summary_field(cons_out, "torn"), 0u) << cons_out;
	EXPECT_GT(summary_field(cons_out, "reads"), 0u) << cons_out;
	EXPECT_TRUE(out_contains(prod_out, "DONE published=1000000")) << prod_out;
}

TEST(Pattern, GapObservable) {  // I03
	const std::string name = edge_test::unique_channel_name("i03");
	// consumer first (it must open via NotFound retry), then a producer that
	// publishes 200 samples at 1ms while the consumer reads every 15ms -> the
	// per-read missed_samples gap must be observable.
	auto ca = consumer_args(name);
	ca.push_back("--reads");
	ca.push_back("12");
	ca.push_back("--read-interval-ms");
	ca.push_back("15");
	ca.push_back("--read-timeout-ms");
	ca.push_back("8000");
	ca.push_back("--open-retry-ms");
	ca.push_back("5000");
	edge_test::SpawnedChild cons;
	ASSERT_TRUE(cons.spawn(ca));
	std::this_thread::sleep_for(std::chrono::milliseconds(400));

	auto pa = producer_args(name);
	pa.push_back("--count");
	pa.push_back("200");
	pa.push_back("--interval-us");
	pa.push_back("1000");
	edge_test::SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));

	std::string cons_out, prod_out;
	ASSERT_TRUE(cons.wait(20000, &cons_out)) << "consumer hung: " << cons_out;
	ASSERT_TRUE(prod.wait(20000, &prod_out)) << "producer hung: " << prod_out;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	ASSERT_EQ(prod.exit_code(), 0) << prod_out;
	EXPECT_EQ(summary_field(cons_out, "torn"), 0u) << cons_out;
	EXPECT_GT(summary_field(cons_out, "missed_total"), 0u) << cons_out;
}

TEST(Pattern, ConsumerFirst) {  // I04
	const std::string name = edge_test::unique_channel_name("i04");
	// consumer starts before the producer exists and must wait for it (open retry)
	auto ca = consumer_args(name);
	ca.push_back("--expect-last-seq");
	ca.push_back("100");
	ca.push_back("--read-timeout-ms");
	ca.push_back("60000");
	ca.push_back("--open-retry-ms");
	ca.push_back("20000");
	edge_test::SpawnedChild cons;
	ASSERT_TRUE(cons.spawn(ca));
	std::this_thread::sleep_for(std::chrono::milliseconds(600));

	auto pa = producer_args(name);
	pa.push_back("--count");
	pa.push_back("100");
	edge_test::SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));

	std::string cons_out, prod_out;
	ASSERT_TRUE(cons.wait(70000, &cons_out)) << "consumer hung: " << cons_out;
	ASSERT_TRUE(prod.wait(30000, &prod_out)) << "producer hung: " << prod_out;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	ASSERT_EQ(prod.exit_code(), 0) << prod_out;
	EXPECT_EQ(summary_field(cons_out, "torn"), 0u) << cons_out;
	EXPECT_GE(summary_field(cons_out, "reads"), 1u) << cons_out;
	EXPECT_GE(summary_field(cons_out, "last_seq"), 100u) << cons_out;
}

TEST(Pattern, DuplicateProducerRejected) {  // I05
	const std::string name = edge_test::unique_channel_name("i05");
	// producer A stays alive (count 0 = infinite); a second producer must be
	// rejected as AlreadyOwned, not allowed to replace the live owner.
	auto aa = producer_args(name);
	aa.push_back("--count");
	aa.push_back("0");
	aa.push_back("--interval-us");
	aa.push_back("1000000");
	edge_test::SpawnedChild prod_a;
	ASSERT_TRUE(prod_a.spawn(aa));
	std::this_thread::sleep_for(std::chrono::milliseconds(400));

	auto ba = producer_args(name);
	ba.push_back("--count");
	ba.push_back("1");
	edge_test::SpawnedChild prod_b;
	ASSERT_TRUE(prod_b.spawn(ba));
	std::string b_out;
	ASSERT_TRUE(prod_b.wait(30000, &b_out)) << "second producer hung: " << b_out;
	EXPECT_EQ(prod_b.exit_code(), 2) << b_out;
	EXPECT_TRUE(out_contains(b_out, "CREATE_FAIL code=AlreadyOwned")) << b_out;

	prod_a.kill(SIGTERM);
	std::string a_out;
	prod_a.wait(10000, &a_out);
}

TEST(Pattern, DuplicateConsumerRejected) {  // I06
	const std::string name = edge_test::unique_channel_name("i06");
	auto pa = producer_args(name);
	pa.push_back("--count");
	pa.push_back("0");
	pa.push_back("--interval-us");
	pa.push_back("1000000");
	edge_test::SpawnedChild prod;
	ASSERT_TRUE(prod.spawn(pa));
	std::this_thread::sleep_for(std::chrono::milliseconds(400));

	// consumer A stays alive; a second consumer must be rejected.
	auto aa = consumer_args(name);
	aa.push_back("--reads");
	aa.push_back("0");
	aa.push_back("--read-interval-ms");
	aa.push_back("1000");
	aa.push_back("--read-timeout-ms");
	aa.push_back("0");
	edge_test::SpawnedChild cons_a;
	ASSERT_TRUE(cons_a.spawn(aa));
	std::this_thread::sleep_for(std::chrono::milliseconds(400));

	auto ba = consumer_args(name);
	ba.push_back("--reads");
	ba.push_back("1");
	edge_test::SpawnedChild cons_b;
	ASSERT_TRUE(cons_b.spawn(ba));
	std::string b_out;
	ASSERT_TRUE(cons_b.wait(30000, &b_out)) << "second consumer hung: " << b_out;
	EXPECT_EQ(cons_b.exit_code(), 2) << b_out;
	EXPECT_TRUE(out_contains(b_out, "OPEN_FAIL code=ConsumerAlreadyOwned")) << b_out;

	cons_a.kill(SIGTERM);
	prod.kill(SIGTERM);
	std::string tmp;
	cons_a.wait(10000, &tmp);
	prod.wait(10000, &tmp);
}

TEST(Pattern, SchemaMismatchRejected) {  // I07
	const std::string name = edge_test::unique_channel_name("i07");
	ChannelOptions opts;
	opts.name = name;
	auto prod = Producer<TestPayloadV1>::create(opts, TestPayloadV1Schema());
	ASSERT_TRUE(prod);

	// same payload size (16), but a different fingerprint -> kSchemaMismatch
	auto c = Consumer<TestPayloadV1>::open(opts, TestPayloadV2Schema());
	ASSERT_FALSE(c);
	EXPECT_EQ(c.error().code, ErrorCode::kSchemaMismatch);

	ASSERT_TRUE(prod.value().remove_if_owner());
}

TEST(Pattern, PayloadSizeMismatchRejected) {  // I08
	const std::string name = edge_test::unique_channel_name("i08");
	ChannelOptions opts;
	opts.name = name;
	auto prod = Producer<TestPayloadV1>::create(opts, TestPayloadV1Schema());
	ASSERT_TRUE(prod);

	// matching fingerprint, but consumer<TestPayloadV2> expects 8-byte payloads
	// against a 16-byte channel -> kSchemaMismatch
	auto c = Consumer<TestPayloadV2>::open(opts, TestPayloadV2Schema());
	ASSERT_FALSE(c);
	EXPECT_EQ(c.error().code, ErrorCode::kSchemaMismatch);

	ASSERT_TRUE(prod.value().remove_if_owner());
}

TEST(Pattern, CodecCrossExec) {  // I17
	// producer and consumer run in separate binaries; the consumer's pattern
	// check (counter == sequence-1) proves the codec bytes are identical across
	// exec.
	const std::string name = edge_test::unique_channel_name("i17");
	auto pa = producer_args(name);
	pa.push_back("--count");
	pa.push_back("7");
	pa.push_back("--interval-us");
	pa.push_back("1000");
	auto ca = consumer_args(name);
	ca.push_back("--expect-last-seq");
	ca.push_back("7");
	ca.push_back("--read-timeout-ms");
	ca.push_back("30000");
	ca.push_back("--open-retry-ms");
	ca.push_back("20000");

	edge_test::SpawnedChild prod, cons;
	ASSERT_TRUE(prod.spawn(pa));
	ASSERT_TRUE(cons.spawn(ca));
	std::string prod_out, cons_out;
	ASSERT_TRUE(cons.wait(40000, &cons_out)) << "consumer hung: " << cons_out;
	ASSERT_TRUE(prod.wait(40000, &prod_out)) << "producer hung: " << prod_out;
	ASSERT_EQ(prod.exit_code(), 0) << prod_out;
	ASSERT_EQ(cons.exit_code(), 0) << cons_out;
	EXPECT_EQ(summary_field(cons_out, "torn"), 0u) << cons_out;
	EXPECT_GE(summary_field(cons_out, "reads"), 1u) << cons_out;
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc < 3) {
		std::fprintf(stderr,
		             "usage: pattern_test <edge_shm_producer> <edge_shm_consumer>\n");
		return 2;
	}
	g_producer_tool = argv[1];
	g_consumer_tool = argv[2];
	return RUN_ALL_TESTS();
}
