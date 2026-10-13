#include <gtest/gtest.h>
#include <unistd.h>

#include <string>

#include "broker_observer_fixture.hpp"
#include "edge_runtime/channel/channel_layout.hpp"

namespace {

using namespace edge_test::broker_observer;
using namespace edge_runtime::detail;

TEST(ChannelObserverReady, BootstrapAndHeaderMustBothBeReady) {
	for (const auto& states : {std::pair{"1", "2"}, std::pair{"2", "1"}, std::pair{"1", "1"}}) {
		SCOPED_TRACE(std::string(states.first) + "/" + states.second);
		const std::string name = unique_name();
		OwnedPath owned("/dev/shm" + channel_shm_name(name));
		Child image;
		ASSERT_TRUE(image.start(
		        {peer_binary(), "image", name, states.first, states.second, "none"}));
		ASSERT_EQ(image.line(), "READY");
		ASSERT_TRUE(owned.capture());
		Child observer;
		ASSERT_TRUE(observer.start({peer_binary(), "observer", name, "posix", "0"}));
		ASSERT_EQ(observer.line(), "READY");
		const std::string result = observer.line();
		EXPECT_EQ(result.find("RESULT Timeout "), 0u) << result;
		EXPECT_TRUE(observer.wait());
		ASSERT_TRUE(image.command());
		EXPECT_TRUE(image.wait());
		EXPECT_EQ(image.exit_code(), 0);
	}
}

TEST(ChannelObserverReady, ReadyViewRequiresMatchingInstanceAndMapping) {
	for (const std::string mutation : {"none", "nonce", "zero_nonce", "size", "tiny"}) {
		SCOPED_TRACE(mutation);
		const std::string name = unique_name();
		OwnedPath owned("/dev/shm" + channel_shm_name(name));
		Child image;
		ASSERT_TRUE(image.start({peer_binary(), "image", name, "2", "2", mutation}));
		ASSERT_EQ(image.line(), "READY");
		ASSERT_TRUE(owned.capture());
		Child observer;
		ASSERT_TRUE(observer.start({peer_binary(), "observer", name, "posix", "0"}));
		ASSERT_EQ(observer.line(), "READY");
		const std::string result = observer.line();
		if (mutation == "none")
			EXPECT_EQ(result.find("RESULT Ok "), 0u) << result;
		else
			EXPECT_NE(result.find("RESULT CorruptHeader "), std::string::npos)
			        << result;
		EXPECT_TRUE(observer.wait());
		ASSERT_TRUE(image.command());
		EXPECT_TRUE(image.wait());
	}
}

TEST(ChannelObserverReady, BrokerReplyMustMatchMappedInstance) {
	for (const std::string behavior : {"legal", "generation", "nonce", "size", "abi"}) {
		SCOPED_TRACE(behavior);
		const std::string name = unique_name();
		Child server;
		ASSERT_TRUE(
		        server.start({peer_binary(), "fake", channel_socket_path(name), behavior}));
		ASSERT_EQ(server.line(), "READY");
		Child observer;
		ASSERT_TRUE(observer.start({peer_binary(), "observer", name, "fd", "120"}));
		ASSERT_EQ(observer.line(), "READY");
		const std::string result = observer.line(350);
		EXPECT_EQ(
		        result.find(behavior == "legal" ? "RESULT Ok " : "RESULT TransportFailed "),
		        0u)
		        << result;
		EXPECT_TRUE(observer.wait());
		ASSERT_TRUE(server.command());
		EXPECT_TRUE(server.wait());
	}
}

TEST(ChannelObserverReady, BrokerOpenSharesObserverDeadlineIncludingZero) {
	for (const std::string behavior : {"stall", "boot_not_ready", "header_not_ready"}) {
		for (const std::string budget : {"0", "120"}) {
			SCOPED_TRACE(budget);
			const std::string name = unique_name();
			Child server;
			ASSERT_TRUE(server.start(
			        {peer_binary(), "fake", channel_socket_path(name), behavior}));
			ASSERT_EQ(server.line(), "READY");
			Child observer;
			ASSERT_TRUE(
			        observer.start({peer_binary(), "observer", name, "fd", budget}));
			ASSERT_EQ(observer.line(), "READY");
			const std::string result = observer.line(350);
			EXPECT_EQ(result.find("RESULT Timeout "), 0u) << result;
			EXPECT_TRUE(observer.wait(50));
			ASSERT_TRUE(server.command());
			EXPECT_TRUE(server.wait());
		}
	}
}

TEST(ChannelObserverReady, PreReadyRetryUsesOriginalDeadlineAndCanObservePublication) {
	for (const bool publish_ready : {false, true}) {
		const std::string name = unique_name();
		OwnedPath owned("/dev/shm" + channel_shm_name(name));
		Child image;
		ASSERT_TRUE(image.start({peer_binary(), "image", name, "1", "1", "none"}));
		ASSERT_EQ(image.line(), "READY");
		ASSERT_TRUE(owned.capture());
		Child observer;
		ASSERT_TRUE(observer.start({peer_binary(), "observer", name, "posix", "120"}));
		ASSERT_EQ(observer.line(), "READY");
		if (publish_ready) {
			::usleep(40'000);
			ASSERT_TRUE(image.command('r'));
			ASSERT_EQ(image.line(), "PROMOTED");
		}
		ClientReport report;
		ASSERT_TRUE(parse_report(observer.line(350), &report));
		EXPECT_EQ(report.code, publish_ready ? "Ok" : "Timeout");
		EXPECT_EQ(report.fd_delta, 0);
		EXPECT_LT(report.duration_ms, 220u);
		if (!publish_ready) {
			EXPECT_GE(report.duration_ms, kClientBudgetMs - 5);
		}
		EXPECT_TRUE(observer.wait());
		ASSERT_TRUE(image.command());
		EXPECT_TRUE(image.wait());
		EXPECT_EQ(image.exit_code(), 0);
	}
}

}
