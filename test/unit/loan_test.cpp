#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "edge_runtime/consumer.hpp"
#include "edge_runtime/error.hpp"
#include "edge_runtime/producer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {

using edge_runtime::ChannelOptions;
using edge_runtime::Consumer;
using edge_runtime::ErrorCode;
using edge_runtime::PayloadCodec;
using edge_runtime::Producer;
using edge_runtime::ReadLoan;
using edge_runtime::WriteLoan;

ChannelOptions options_for(const char* tag) {
	ChannelOptions options;
	options.name = edge_test::unique_channel_name(tag);
	return options;
}

TEST(Loan, CommitAbortMoveAndSameHandleGuard) {
	const ChannelOptions options = options_for("loan_unit");
	auto producer = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto opened = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(opened);
	std::optional<Consumer<TestPayloadV1>> consumer;
	consumer.emplace(std::move(opened.value()));

	auto borrowed = producer.value().loan();
	ASSERT_TRUE(borrowed);
	ASSERT_TRUE(PayloadCodec<TestPayloadV1>::encode(
	        TestPayloadV1{0x5A000001u, 41, 1}, borrowed.value().data(), borrowed.value().size()));
	auto producer_busy = producer.value().status();
	ASSERT_FALSE(producer_busy);
	EXPECT_EQ(producer_busy.error().code, ErrorCode::kConcurrentHandleUse);

	WriteLoan moved(std::move(borrowed.value()));
	EXPECT_FALSE(borrowed.value().active());
	auto published = moved.commit();
	ASSERT_TRUE(published);
	EXPECT_EQ(published.value().sequence, 1u);
	auto committed_twice = moved.commit();
	ASSERT_FALSE(committed_twice);
	EXPECT_EQ(committed_twice.error().code, ErrorCode::kInvalidOptions);

	auto read = consumer.value().try_loan_latest();
	ASSERT_TRUE(read);
	TestPayloadV1 decoded;
	ASSERT_TRUE(PayloadCodec<TestPayloadV1>::decode(read.value().data(), read.value().size(),
	                                               &decoded));
	EXPECT_EQ(decoded.counter, 41u);
	EXPECT_EQ(read.value().sequence(), 1u);
	auto consumer_busy = consumer.value().status();
	ASSERT_FALSE(consumer_busy);
	EXPECT_EQ(consumer_busy.error().code, ErrorCode::kConcurrentHandleUse);

	ReadLoan moved_read(std::move(read.value()));
	EXPECT_FALSE(read.value().active());
	moved_read.release();
	EXPECT_FALSE(moved_read.active());
	EXPECT_TRUE(consumer.value().status());

	{
		auto aborted = producer.value().loan();
		ASSERT_TRUE(aborted);
		ASSERT_TRUE(PayloadCodec<TestPayloadV1>::encode(
		        TestPayloadV1{0x5A000001u, 42, 0}, aborted.value().data(),
		        aborted.value().size()));
	}
	auto no_sample = consumer.value().try_loan_latest();
	ASSERT_FALSE(no_sample);
	EXPECT_EQ(no_sample.error().code, ErrorCode::kNoNewSample);

	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST(Loan, HeldReadSlotCannotBeOverwritten) {
	const ChannelOptions options = options_for("loan_stable");
	auto producer = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto opened = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(opened);
	std::optional<Consumer<TestPayloadV1>> consumer;
	consumer.emplace(std::move(opened.value()));
	ASSERT_TRUE(producer.value().publish(TestPayloadV1{0x5A000001u, 1, 0}));

	auto held = consumer.value().try_loan_latest();
	ASSERT_TRUE(held);
	const std::byte* const held_address = held.value().data();
	for (uint64_t counter = 2; counter <= 100; ++counter) {
		ASSERT_TRUE(producer.value().publish(TestPayloadV1{0x5A000001u, counter, 0}));
		TestPayloadV1 decoded;
		ASSERT_TRUE(PayloadCodec<TestPayloadV1>::decode(held_address, held.value().size(), &decoded));
		ASSERT_EQ(decoded.counter, 1u);
	}
	held.value().release();
	auto latest = consumer.value().try_loan_latest();
	ASSERT_TRUE(latest);
	TestPayloadV1 decoded;
	ASSERT_TRUE(PayloadCodec<TestPayloadV1>::decode(latest.value().data(), latest.value().size(),
	                                               &decoded));
	EXPECT_EQ(decoded.counter, 100u);
	latest.value().release();
	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST(Loan, ProducerShutdownWaitsForOutstandingWriteLoan) {
	const ChannelOptions options = options_for("loan_parent_write");
	std::optional<Producer<TestPayloadV1>> producer;
	auto created = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(created);
	producer.emplace(std::move(created.value()));
	auto opened = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(opened);
	std::optional<Consumer<TestPayloadV1>> consumer;
	consumer.emplace(std::move(opened.value()));

	auto write = producer->loan();
	ASSERT_TRUE(write);
	ASSERT_TRUE(PayloadCodec<TestPayloadV1>::encode(
	        TestPayloadV1{0x5A000001u, 77, 0}, write.value().data(), write.value().size()));
	producer.reset();
	auto published = write.value().commit();
	ASSERT_TRUE(published);
	auto read = consumer->try_read_latest();
	ASSERT_TRUE(read);
	EXPECT_EQ(read.value().value.counter, 77u);
	auto status = consumer->status();
	ASSERT_TRUE(status);
	EXPECT_EQ(status.value().producer_state, 0u);

	consumer.reset();
	auto cleanup = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(cleanup);
	EXPECT_TRUE(cleanup.value().remove_if_owner());
}

TEST(Loan, ConsumerShutdownWaitsForOutstandingReadLoan) {
	const ChannelOptions options = options_for("loan_parent_read");
	auto producer = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	ASSERT_TRUE(producer.value().publish(TestPayloadV1{0x5A000001u, 88, 0}));

	std::optional<Consumer<TestPayloadV1>> consumer;
	auto opened = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(opened);
	consumer.emplace(std::move(opened.value()));
	auto read = consumer->try_loan_latest();
	ASSERT_TRUE(read);
	consumer.reset();

	auto while_borrowed = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	ASSERT_FALSE(while_borrowed);
	EXPECT_EQ(while_borrowed.error().code, ErrorCode::kConsumerAlreadyOwned);
	read.value().release();
	auto after_release = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(after_release);
	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST(Loan, WaitLoanUsesExistingTimeoutClassification) {
	const ChannelOptions options = options_for("loan_wait");
	auto producer = Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto consumer = Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(consumer);
	auto timeout = consumer.value().wait_loan_latest(std::chrono::milliseconds(1));
	ASSERT_FALSE(timeout);
	EXPECT_EQ(timeout.error().code, ErrorCode::kDataStale);
	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST(Loan, HandleMoveAssignmentRetiresPreviousEndpoint) {
	const ChannelOptions first_options = options_for("loan_move_first");
	const ChannelOptions second_options = options_for("loan_move_second");
	auto first = Producer<TestPayloadV1>::create(first_options, TestPayloadV1Schema());
	auto second = Producer<TestPayloadV1>::create(second_options, TestPayloadV1Schema());
	ASSERT_TRUE(first);
	ASSERT_TRUE(second);
	first.value() = std::move(second.value());
	auto first_successor =
	        Producer<TestPayloadV1>::create(first_options, TestPayloadV1Schema());
	ASSERT_TRUE(first_successor);
	EXPECT_TRUE(first_successor.value().remove_if_owner());
	EXPECT_TRUE(first.value().remove_if_owner());

	const ChannelOptions third_options = options_for("loan_move_consumer_first");
	const ChannelOptions fourth_options = options_for("loan_move_consumer_second");
	auto third = Producer<TestPayloadV1>::create(third_options, TestPayloadV1Schema());
	auto fourth = Producer<TestPayloadV1>::create(fourth_options, TestPayloadV1Schema());
	ASSERT_TRUE(third);
	ASSERT_TRUE(fourth);
	auto third_consumer = Consumer<TestPayloadV1>::open(third_options, TestPayloadV1Schema());
	auto fourth_consumer = Consumer<TestPayloadV1>::open(fourth_options, TestPayloadV1Schema());
	ASSERT_TRUE(third_consumer);
	ASSERT_TRUE(fourth_consumer);
	third_consumer.value() = std::move(fourth_consumer.value());
	auto third_successor = Consumer<TestPayloadV1>::open(third_options, TestPayloadV1Schema());
	ASSERT_TRUE(third_successor);
	EXPECT_TRUE(third.value().remove_if_owner());
	EXPECT_TRUE(fourth.value().remove_if_owner());
}

}
