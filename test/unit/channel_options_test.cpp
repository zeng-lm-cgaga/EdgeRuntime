#include <gtest/gtest.h>

#include <chrono>

#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {

TEST(ChannelOptions, DefaultContinuesToValidatePayload) {
	edge_runtime::ChannelOptions options;
	options.name = edge_test::unique_channel_name("options_default");
	auto created =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(created);
	auto opened = edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	ASSERT_TRUE(opened);
	ASSERT_TRUE(created.value().publish(TestPayloadV1{}));
	EXPECT_TRUE(opened.value().try_read_latest());
	EXPECT_TRUE(created.value().remove_if_owner());
}

TEST(ChannelOptions, ProducerRejectsChecksumDisable) {
	edge_runtime::ChannelOptions options;
	options.name = edge_test::unique_channel_name("options_no_checksum");
	options.enable_payload_checksum = false;
	auto created =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	EXPECT_FALSE(created);
	if (created) {
		EXPECT_TRUE(created.value().remove_if_owner());
	} else {
		EXPECT_EQ(created.error().code, edge_runtime::ErrorCode::kInvalidOptions);
	}
}

TEST(ChannelOptions, ConsumerRejectsChecksumDisable) {
	edge_runtime::ChannelOptions options;
	options.name = edge_test::unique_channel_name("options_consumer_checksum");
	auto created =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(created);
	options.enable_payload_checksum = false;
	auto opened = edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	EXPECT_FALSE(opened);
	if (!opened) {
		EXPECT_EQ(opened.error().code, edge_runtime::ErrorCode::kInvalidOptions);
	}
	EXPECT_TRUE(created.value().remove_if_owner());
}

TEST(ChannelOptions, NondefaultLegacyStaleTimeoutIsExplicitlyRejected) {
	for (const auto timeout : {std::chrono::milliseconds(-1), std::chrono::milliseconds(0),
	                           std::chrono::milliseconds(500)}) {
		edge_runtime::ChannelOptions options;
		options.name = edge_test::unique_channel_name("options_legacy_stale");
		options.stale_timeout = timeout;
		auto created = edge_runtime::Producer<TestPayloadV1>::create(options,
		                                                             TestPayloadV1Schema());
		EXPECT_FALSE(created);
		if (created) {
			EXPECT_TRUE(created.value().remove_if_owner());
		} else {
			EXPECT_EQ(created.error().code, edge_runtime::ErrorCode::kInvalidOptions);
		}
	}
}

TEST(ChannelOptions, ConsumerRejectsNondefaultLegacyTimeoutBeforeRegistration) {
	edge_runtime::ChannelOptions options;
	options.name = edge_test::unique_channel_name("options_consumer_stale");
	auto created =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(created);
	options.stale_timeout = std::chrono::milliseconds(500);
	auto opened = edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	EXPECT_FALSE(opened);
	if (!opened) {
		EXPECT_EQ(opened.error().code, edge_runtime::ErrorCode::kInvalidOptions);
	}
	EXPECT_TRUE(created.value().remove_if_owner());
}

}  // namespace
