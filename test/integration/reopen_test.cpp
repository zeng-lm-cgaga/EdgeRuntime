// ER1 I01: minimal cross-exec reopen. The driver creates a producer, fork+execs
// a SEPARATE binary (reopen_child) that re-opens the channel by name and
// verifies the frozen identity. After the child exits, the driver confirms the
// child's consumer identity is still recorded in the header (dead) and that
// clean shutdown lets a same-process recreate replace the instance.

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <optional>
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
using edge_runtime::SchemaDescriptor;
using edge_runtime::Transport;

std::string g_helper;

std::string fingerprint_hex(const std::array<std::byte, 32>& fp) {
	std::string out;
	out.reserve(64);
	for (const std::byte b : fp) {
		char two[4];
		std::snprintf(two, sizeof(two), "%02x",
		              static_cast<unsigned int>(std::to_integer<unsigned char>(b)));
		out += two;
	}
	return out;
}

TEST(Reopen, CrossExecReopen) {  // I01
	const std::string name = edge_test::unique_channel_name("reopen");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();

	auto prod = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_TRUE(prod) << edge_runtime::to_string(prod.error().code) << " "
	                  << prod.error().context;
	auto st = prod.value().status();
	ASSERT_TRUE(st);

	std::vector<std::string> argv;
	argv.push_back(g_helper);
	argv.push_back(name);
	argv.push_back(std::to_string(schema.version));
	argv.push_back(fingerprint_hex(schema.fingerprint));
	argv.push_back(std::to_string(st.value().generation));
	argv.push_back(std::to_string(st.value().instance_nonce_hi));
	argv.push_back(std::to_string(st.value().instance_nonce_lo));

	const auto r = edge_test::run_child_capture(argv, 10000);
	ASSERT_FALSE(r.timed_out) << "child hung";
	ASSERT_EQ(r.exit_code, 0) << "child stderr above; stdout: " << r.stdout_text;
	EXPECT_NE(r.stdout_text.find("OPEN_RESULT ok=1"), std::string::npos)
	        << "stdout: " << r.stdout_text;

	// The child's consumer identity must be recorded in the header (now dead).
	auto st2 = prod.value().status();
	ASSERT_TRUE(st2);
	EXPECT_EQ(st2.value().consumer_pid, static_cast<uint64_t>(r.child_pid));
	EXPECT_FALSE(st2.value().consumer_alive);

	// Cleanup: verified removal exercises §9.4 and leaves no stray shm object.
	auto rm = prod.value().remove_if_owner();
	ASSERT_TRUE(rm) << edge_runtime::to_string(rm.error().code);
}

TEST(Reopen, SameProcessRecreateReplacesInstance) {
	const std::string name = edge_test::unique_channel_name("recreate");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();

	uint64_t gen1 = 0;
	uint64_t nonce1_hi = 0;
	uint64_t nonce1_lo = 0;
	{
		auto p1 = Producer<TestPayloadV1>::create(opts, schema);
		ASSERT_TRUE(p1);
		auto s1 = p1.value().status();
		ASSERT_TRUE(s1);
		gen1 = s1.value().generation;
		nonce1_hi = s1.value().instance_nonce_hi;
		nonce1_lo = s1.value().instance_nonce_lo;
		// p1 clean shutdown (destructor) marks producer_state OFFLINE.
	}

	// Same process, same name: the old instance is OFFLINE (not a live owner),
	// so create replaces it with generation+1 and a fresh nonce.
	auto p2 = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_TRUE(p2) << edge_runtime::to_string(p2.error().code) << " " << p2.error().context;
	auto s2 = p2.value().status();
	ASSERT_TRUE(s2);
	EXPECT_EQ(s2.value().generation, gen1 + 1);
	EXPECT_NE(s2.value().instance_nonce_hi, nonce1_hi);
	EXPECT_NE(s2.value().instance_nonce_lo, nonce1_lo);

	// An active producer (p2 alive, ONLINE) must reject a second create.
	auto dup = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_FALSE(dup);
	EXPECT_EQ(dup.error().code, ErrorCode::kAlreadyOwned);

	ASSERT_TRUE(p2.value().remove_if_owner());
}

TEST(Reopen, ConsumerOwnershipAndCleanShutdown) {
	const std::string name = edge_test::unique_channel_name("consumer_owner");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();

	auto prod = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_TRUE(prod);

	{
		auto c1 = edge_runtime::Consumer<TestPayloadV1>::open(opts, schema);
		ASSERT_TRUE(c1) << edge_runtime::to_string(c1.error().code) << " "
		                << c1.error().context;

		// A live consumer owns the channel: second open is rejected.
		auto c2 = edge_runtime::Consumer<TestPayloadV1>::open(opts, schema);
		ASSERT_FALSE(c2);
		EXPECT_EQ(c2.error().code, ErrorCode::kConsumerAlreadyOwned);

		// c1 destructor marks consumer_state OFFLINE.
	}

	// Cleanly-shutdown consumer is reclaimable in the same process.
	auto c3 = edge_runtime::Consumer<TestPayloadV1>::open(opts, schema);
	ASSERT_TRUE(c3) << edge_runtime::to_string(c3.error().code) << " " << c3.error().context;

	ASSERT_TRUE(prod.value().remove_if_owner());
}

void verify_reconnect_to_replaced_instance(Transport transport) {
	const std::string name = edge_test::unique_channel_name("consumer_reconnect");
	ChannelOptions opts;
	opts.name = name;
	opts.transport = transport;
	opts.reconnect_timeout = std::chrono::milliseconds(2000);
	const SchemaDescriptor schema = TestPayloadV1Schema();

	std::optional<Consumer<TestPayloadV1>> consumer;
	uint64_t old_generation = 0;
	std::array<std::byte, 16> old_nonce{};
	{
		auto producer = Producer<TestPayloadV1>::create(opts, schema);
		ASSERT_TRUE(producer) << edge_runtime::to_string(producer.error().code);
		auto first_publish = producer.value().publish(TestPayloadV1{0x5A000001u, 11, 0});
		ASSERT_TRUE(first_publish);
		old_generation = first_publish.value().generation;

		auto opened = Consumer<TestPayloadV1>::open(opts, schema);
		ASSERT_TRUE(opened) << edge_runtime::to_string(opened.error().code);
		consumer.emplace(std::move(opened.value()));
		auto first = consumer->try_read_latest();
		ASSERT_TRUE(first);
		old_nonce = first.value().instance_nonce;
		EXPECT_EQ(first.value().sequence, 1u);

		// Reconnect is only for a replacement. Failure against the current live
		// instance must leave the existing handle usable.
		auto premature = consumer->reconnect();
		ASSERT_FALSE(premature);
		EXPECT_EQ(premature.error().code, ErrorCode::kConsumerAlreadyOwned);
		ASSERT_TRUE(producer.value().publish(TestPayloadV1{0x5A000001u, 12, 0}));
		auto second = consumer->try_read_latest();
		ASSERT_TRUE(second);
		EXPECT_EQ(second.value().value.counter, 12u);
		// producer clean shutdown leaves the consumer holding the old mapping.
	}

	auto successor = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_TRUE(successor) << edge_runtime::to_string(successor.error().code) << " "
	                       << successor.error().context;
	auto successor_publish = successor.value().publish(TestPayloadV1{0x5A000001u, 21, 0});
	ASSERT_TRUE(successor_publish);
	ASSERT_EQ(successor_publish.value().generation, old_generation + 1);
	ASSERT_EQ(successor_publish.value().sequence, 1u);

	auto reconnected = consumer->reconnect();
	ASSERT_TRUE(reconnected) << edge_runtime::to_string(reconnected.error().code) << " "
	                         << reconnected.error().context;
	EXPECT_EQ(reconnected.value().old_generation, old_generation);
	EXPECT_EQ(reconnected.value().new_generation, old_generation + 1);
	EXPECT_EQ(reconnected.value().old_instance_nonce, old_nonce);
	EXPECT_NE(reconnected.value().new_instance_nonce, old_nonce);
	EXPECT_FALSE(reconnected.value().schema_changed);

	auto sample = consumer->try_read_latest();
	ASSERT_TRUE(sample) << edge_runtime::to_string(sample.error().code);
	EXPECT_EQ(sample.value().value.counter, 21u);
	EXPECT_EQ(sample.value().generation, old_generation + 1);
	EXPECT_EQ(sample.value().sequence, 1u);
	EXPECT_EQ(sample.value().missed_samples, 0u);
	consumer.reset();
	EXPECT_TRUE(successor.value().remove_if_owner());
}

TEST(Reopen, ConsumerReconnectsToPosixReplacement) {
	verify_reconnect_to_replaced_instance(Transport::kPosixShm);
}

TEST(Reopen, ConsumerReconnectsToMemfdReplacement) {
	verify_reconnect_to_replaced_instance(Transport::kMemfdFdPass);
}

TEST(Reopen, ReconnectRejectsConcurrentWaitWithoutInvalidatingIt) {
	const std::string name = edge_test::unique_channel_name("reconnect_wait");
	ChannelOptions opts;
	opts.name = name;
	const SchemaDescriptor schema = TestPayloadV1Schema();
	auto producer = Producer<TestPayloadV1>::create(opts, schema);
	ASSERT_TRUE(producer);
	auto opened = Consumer<TestPayloadV1>::open(opts, schema);
	ASSERT_TRUE(opened);
	std::optional<Consumer<TestPayloadV1>> consumer(std::move(opened.value()));

	std::atomic<bool> wait_started{false};
	std::optional<edge_runtime::Result<edge_runtime::Sample<TestPayloadV1>>> waited;
	std::thread waiter([&] {
		wait_started.store(true, std::memory_order_release);
		waited.emplace(consumer->wait_latest(std::chrono::seconds(2)));
	});
	while (!wait_started.load(std::memory_order_acquire)) std::this_thread::yield();
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	auto reconnect = consumer->reconnect();
	EXPECT_FALSE(reconnect);
	EXPECT_EQ(reconnect.error().code, ErrorCode::kConcurrentHandleUse);
	EXPECT_TRUE(producer.value().publish(TestPayloadV1{0x5A000001u, 31, 0}));
	waiter.join();
	ASSERT_TRUE(waited.has_value());
	ASSERT_TRUE(*waited) << edge_runtime::to_string(waited->error().code);
	EXPECT_EQ(waited->value().value.counter, 31u);

	consumer.reset();
	EXPECT_TRUE(producer.value().remove_if_owner());
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc < 2) {
		std::fprintf(stderr, "usage: reopen_test <reopen_child_binary>\n");
		return 2;
	}
	g_helper = argv[1];
	return RUN_ALL_TESTS();
}
