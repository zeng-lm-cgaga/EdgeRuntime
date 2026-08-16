#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "edge_runtime/producer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {

std::string g_helper;

void run_cross_exec_loan(edge_runtime::Transport transport, const char* tag) {
	edge_runtime::ChannelOptions options;
	options.name = edge_test::unique_channel_name(tag);
	options.transport = transport;
	auto producer =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	auto loan = producer.value().loan();
	ASSERT_TRUE(loan);
	ASSERT_TRUE(edge_runtime::PayloadCodec<TestPayloadV1>::encode(
	        TestPayloadV1{0x5A000001u, 2026, 0}, loan.value().data(), loan.value().size()));
	ASSERT_TRUE(loan.value().commit());

	const std::vector<std::string> argv = {
	        g_helper, options.name, "2026",
	        transport == edge_runtime::Transport::kMemfdFdPass ? "fd" : "posix"};
	const edge_test::ChildResult child = edge_test::run_child_capture(argv, 10000);
	ASSERT_FALSE(child.timed_out);
	ASSERT_EQ(child.exit_code, 0) << child.stdout_text;
	EXPECT_NE(child.stdout_text.find("LOAN_RESULT ok=1 counter=2026 seq=1"),
	          std::string::npos);
	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST(LoanCrossExec, PosixShm) {
	run_cross_exec_loan(edge_runtime::Transport::kPosixShm, "loan_xproc_posix");
}

TEST(LoanCrossExec, MemfdFdPass) {
	run_cross_exec_loan(edge_runtime::Transport::kMemfdFdPass, "loan_xproc_fd");
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc < 2) {
		std::fprintf(stderr, "usage: loan_test <loan_child_binary>\n");
		return 2;
	}
	g_helper = argv[1];
	return RUN_ALL_TESTS();
}
