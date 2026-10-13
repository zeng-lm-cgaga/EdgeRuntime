#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "test_util.hpp"

namespace {

std::vector<std::string> tool_paths;

TEST(ChannelOptionsCli, DisablingOrInvalidChecksumIsRejectedBeforeIpc) {
	for (const auto& tool : tool_paths) {
		for (const auto& value : {"0", "2", "invalid"}) {
			const std::string name = edge_test::unique_channel_name("checksum_cli");
			const auto result = edge_test::run_child_capture(
			        {tool, "--name", name, "--csv", "/dev/null", "--checksum", value},
			        2000);
			EXPECT_FALSE(result.timed_out) << tool;
			EXPECT_EQ(result.exit_code, 2) << tool << " --checksum " << value;
		}
	}
}

TEST(ChannelOptionsCli, LegacyChecksumOffIsExplicitlyRejected) {
	for (const auto& tool : tool_paths) {
		const auto result = edge_test::run_child_capture(
		        {tool, "--name", edge_test::unique_channel_name("checksum_off"), "--csv",
		         "/dev/null", "--checksum-off"},
		        2000);
		EXPECT_FALSE(result.timed_out) << tool;
		EXPECT_EQ(result.exit_code, 2) << tool;
	}
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc != 5) return 2;
	for (int index = 1; index < argc; ++index) tool_paths.emplace_back(argv[index]);
	return RUN_ALL_TESTS();
}
