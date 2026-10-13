#include <fcntl.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <string>

#include "broker_observer_fixture.hpp"
#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/sync/clock.hpp"
#include "edge_runtime/transport/fd_broker.hpp"

namespace {

using namespace edge_test::broker_observer;
using namespace edge_runtime::detail;

TEST(FdBrokerDeadline, OneBytePeerCannotStarveLegalConsumerAndExit) {
	const std::string name = unique_name();
	OwnedPath lock(channel_lock_path(name));
	Child producer;
	ASSERT_TRUE(producer.start({peer_binary(), "producer", name}));
	ASSERT_EQ(producer.line(), "READY");
	ASSERT_TRUE(lock.capture());
	auto partial = connect_peer(channel_socket_path(name));
	ASSERT_GE(partial.get(), 0);
	const char byte = 'E';
	ASSERT_EQ(::send(partial.get(), &byte, 1, MSG_NOSIGNAL), 1);
	Child consumer;
	ASSERT_TRUE(consumer.start({peer_binary(), "consumer", name, "300"}));
	ASSERT_EQ(consumer.line(), "READY");
	const std::string result = consumer.line();
	EXPECT_EQ(result.find("RESULT Ok "), 0u) << result;
	EXPECT_TRUE(consumer.wait());
	ASSERT_TRUE(producer.command());
	EXPECT_EQ(producer.line(), "STOPPED");
	EXPECT_TRUE(producer.wait());
	EXPECT_EQ(producer.exit_code(), 0);
}

TEST(FdBrokerDeadline, PartialRequestAndStopAreBounded) {
	const std::string name = unique_name();
	OwnedPath lock(channel_lock_path(name));
	Child producer;
	ASSERT_TRUE(producer.start({peer_binary(), "producer", name}));
	ASSERT_EQ(producer.line(), "READY");
	ASSERT_TRUE(lock.capture());
	auto partial = connect_peer(channel_socket_path(name));
	ASSERT_GE(partial.get(), 0);
	FdBrokerRequestAbi request{};
	std::memcpy(request.magic, kFdBrokerRequestMagic, sizeof(kFdBrokerRequestMagic) - 1);
	ASSERT_EQ(::send(partial.get(), &request, sizeof(request) - 1, MSG_NOSIGNAL),
	          static_cast<ssize_t>(sizeof(request) - 1));
	::usleep(20'000);
	const uint64_t start = monotonic_now_ns();
	ASSERT_TRUE(producer.command());
	EXPECT_EQ(producer.line(300), "STOPPED");
	EXPECT_LT(elapsed_ms(start), 300u);
	EXPECT_TRUE(producer.wait());
	EXPECT_EQ(producer.exit_code(), 0);
}

TEST(FdBrokerDeadline, SlowShortWrongAndMissingFdRepliesShareTotalBudget) {
	for (const std::string behavior :
	     {"stall", "slow", "short", "wrong_magic", "wrong_checksum", "no_fd", "not_ready"}) {
		SCOPED_TRACE(behavior);
		const std::string path = "/tmp/" + unique_name() + ".sock";
		Child server;
		ASSERT_TRUE(server.start({peer_binary(), "fake", path, behavior}));
		ASSERT_EQ(server.line(), "READY");
		Child client;
		ASSERT_TRUE(client.start({peer_binary(), "request", path, "120", "rw"}));
		ASSERT_EQ(client.line(), "READY");
		const uint64_t start = monotonic_now_ns();
		const std::string result = client.line(350);
		ClientReport report;
		ASSERT_TRUE(parse_report(result, &report)) << result;
		EXPECT_EQ(report.code, "ProducerOffline");
		EXPECT_GE(report.duration_ms, kClientBudgetMs - 5);
		EXPECT_LT(report.duration_ms, kClientBudgetMs + 100);
		EXPECT_EQ(report.fd_delta, 0);
		EXPECT_LT(elapsed_ms(start), 350u);
		EXPECT_TRUE(client.wait(50));
		ASSERT_TRUE(server.command());
		EXPECT_TRUE(server.wait());
	}
}

TEST(FdBrokerDeadline, RepeatedEintrCannotResetTotalDeadline) {
	const std::string path = "/tmp/" + unique_name() + ".sock";
	Child server;
	ASSERT_TRUE(server.start({peer_binary(), "fake", path, "stall"}));
	ASSERT_EQ(server.line(), "READY");
	Child client;
	ASSERT_TRUE(client.start({peer_binary(), "request", path, "120", "rw"}));
	ASSERT_EQ(client.line(), "READY");
	const uint64_t start = monotonic_now_ns();
	for (int attempt = 0; attempt < 100; ++attempt) {
		(void)::kill(client.pid(), SIGUSR1);
		::usleep(2'000);
	}
	const std::string result = client.line(100);
	ClientReport report;
	ASSERT_TRUE(parse_report(result, &report)) << result;
	EXPECT_EQ(report.code, "ProducerOffline");
	EXPECT_GT(report.signals, 10);
	EXPECT_GE(report.duration_ms, kClientBudgetMs - 5);
	EXPECT_LT(report.duration_ms, kClientBudgetMs + 100);
	EXPECT_EQ(report.fd_delta, 0);
	EXPECT_LT(elapsed_ms(start), 350u);
	EXPECT_TRUE(client.wait(50));
	ASSERT_TRUE(server.command());
	EXPECT_TRUE(server.wait());
}

TEST(FdBrokerDeadline, MultipleAndTruncatedRightsAreClosedBeforeRetry) {
	for (const std::string behavior : {"multiple", "truncated", "short", "wrong_checksum"}) {
		SCOPED_TRACE(behavior);
		const std::string path = "/tmp/" + unique_name() + ".sock";
		Child server;
		ASSERT_TRUE(server.start({peer_binary(), "fake", path, behavior}));
		ASSERT_EQ(server.line(), "READY");
		Child client;
		ASSERT_TRUE(client.start({peer_binary(), "request", path, "120", "ro"}));
		ASSERT_EQ(client.line(), "READY");
		const std::string result = client.line(350);
		ClientReport report;
		ASSERT_TRUE(parse_report(result, &report)) << result;
		EXPECT_EQ(report.fd_delta, 0);
		if (behavior == "multiple" || behavior == "truncated") {
			EXPECT_EQ(report.code, "Ok");
			EXPECT_EQ(report.access, O_RDONLY);
			EXPECT_EQ(report.cloexec, FD_CLOEXEC);
		} else
			EXPECT_EQ(report.code, "ProducerOffline");
		EXPECT_TRUE(client.wait());
		ASSERT_TRUE(server.command());
		EXPECT_TRUE(server.wait());
	}
}

TEST(FdBrokerDeadline, ZeroBudgetIsOneBoundedExchangeAndLegalRightsArePreserved) {
	for (const std::string behavior : {"legal", "stall", "wrong_magic", "multiple"}) {
		SCOPED_TRACE(behavior);
		const std::string path = "/tmp/" + unique_name() + ".sock";
		Child server;
		ASSERT_TRUE(server.start({peer_binary(), "fake", path, behavior}));
		ASSERT_EQ(server.line(), "READY");
		Child client;
		ASSERT_TRUE(client.start({peer_binary(), "request", path, "0", "ro"}));
		ASSERT_EQ(client.line(), "READY");
		const std::string result = client.line(150);
		ClientReport report;
		ASSERT_TRUE(parse_report(result, &report)) << result;
		EXPECT_EQ(report.code, behavior == "legal" ? "Ok" : "ProducerOffline");
		EXPECT_LT(report.duration_ms, kFdBrokerSingleAttemptTimeoutMs + 50);
		EXPECT_EQ(report.fd_delta, 0);
		EXPECT_TRUE(client.wait(50));
		ASSERT_TRUE(server.command());
		EXPECT_TRUE(server.wait());
	}
}

TEST(FdBrokerDeadline, BacklogConnectAndMissingSocketUseOneTotalBudget) {
	for (const std::string budget : {"0", "120"}) {
		const std::string path = "/tmp/" + unique_name() + ".sock";
		Child server;
		ASSERT_TRUE(server.start({peer_binary(), "backlog", path}));
		ASSERT_EQ(server.line(), "READY");
		Child client;
		ASSERT_TRUE(client.start({peer_binary(), "request", path, budget, "rw"}));
		ASSERT_EQ(client.line(), "READY");
		ClientReport report;
		ASSERT_TRUE(parse_report(client.line(350), &report));
		EXPECT_EQ(report.code, "ProducerOffline");
		EXPECT_EQ(report.fd_delta, 0);
		EXPECT_LT(report.duration_ms, budget == "0" ? 70u : 220u);
		EXPECT_TRUE(client.wait());
		ASSERT_TRUE(server.command());
		EXPECT_TRUE(server.wait());
	}
	Child absent;
	ASSERT_TRUE(absent.start(
	        {peer_binary(), "request", "/tmp/" + unique_name() + ".sock", "120", "rw"}));
	ASSERT_EQ(absent.line(), "READY");
	ClientReport report;
	ASSERT_TRUE(parse_report(absent.line(350), &report));
	EXPECT_EQ(report.code, "ProducerOffline");
	EXPECT_GE(report.duration_ms, kClientBudgetMs - 5);
	EXPECT_LT(report.duration_ms, kClientBudgetMs + 100);
	EXPECT_TRUE(absent.wait());
}

TEST(FdBrokerDeadline, LegalWritableRightsAndRejectionsRetainTheirContract) {
	for (const std::string behavior : {"legal", "mismatch", "refused"}) {
		const std::string path = "/tmp/" + unique_name() + ".sock";
		Child server;
		ASSERT_TRUE(server.start({peer_binary(), "fake", path, behavior}));
		ASSERT_EQ(server.line(), "READY");
		Child client;
		ASSERT_TRUE(client.start({peer_binary(), "request", path, "120", "rw"}));
		ASSERT_EQ(client.line(), "READY");
		ClientReport report;
		ASSERT_TRUE(parse_report(client.line(), &report));
		EXPECT_EQ(report.code, behavior == "legal"      ? "Ok"
		                       : behavior == "mismatch" ? "SchemaMismatch"
		                                                : "PermissionDenied");
		EXPECT_EQ(report.fd_delta, 0);
		if (behavior == "legal") {
			EXPECT_EQ(report.access, O_RDWR);
			EXPECT_EQ(report.cloexec, FD_CLOEXEC);
		}
		EXPECT_TRUE(client.wait());
		ASSERT_TRUE(server.command());
		EXPECT_TRUE(server.wait());
	}
}

TEST(FdBrokerDeadline, ConnectSendAndRecvmsgEintrRemainInsideOriginalDeadline) {
	for (const std::string operation : {"connect", "send", "recvmsg"}) {
		SCOPED_TRACE(operation);
		const std::string path = "/tmp/" + unique_name() + ".sock";
		Child server;
		ASSERT_TRUE(server.start({peer_binary(), "fake", path, "legal"}));
		ASSERT_EQ(server.line(), "READY");
		Child client;
		ASSERT_TRUE(client.start({peer_binary(), "request", path, "120", "rw", operation}));
		ASSERT_EQ(client.line(), "READY");
		ClientReport report;
		ASSERT_TRUE(parse_report(client.line(350), &report));
		EXPECT_EQ(report.code, "ProducerOffline");
		EXPECT_GT(report.syscall_interruptions, 5u);
		EXPECT_GE(report.duration_ms, kClientBudgetMs - 5);
		EXPECT_LT(report.duration_ms, kClientBudgetMs + 100);
		EXPECT_EQ(report.fd_delta, 0);
		EXPECT_TRUE(client.wait());
		ASSERT_TRUE(server.command());
		EXPECT_TRUE(server.wait());
	}
}

TEST(FdBrokerDeadline, ServerReplyEintrDoesNotBlockStopAndJoin) {
	const std::string name = unique_name();
	OwnedPath lock(channel_lock_path(name));
	Child producer;
	ASSERT_TRUE(producer.start({peer_binary(), "producer", name, "sendmsg"}));
	ASSERT_EQ(producer.line(), "READY");
	ASSERT_TRUE(lock.capture());
	Child consumer;
	ASSERT_TRUE(consumer.start({peer_binary(), "consumer", name, "120"}));
	ASSERT_EQ(consumer.line(), "READY");
	::usleep(20'000);
	const uint64_t start = monotonic_now_ns();
	ASSERT_TRUE(producer.command());
	EXPECT_EQ(producer.line(300), "STOPPED");
	EXPECT_TRUE(producer.wait(50));
	EXPECT_LT(elapsed_ms(start), 300u);
	EXPECT_EQ(producer.exit_code(), 0);
	EXPECT_EQ(consumer.line(300).find("RESULT ProducerOffline "), 0u);
	EXPECT_TRUE(consumer.wait());
}

}
