#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/producer_impl.hpp"
#include "edge_runtime/error.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {

TEST(ProducerConcurrency, RejectsOverlapAndPreservesUniqueSequences) {
	edge_runtime::ChannelOptions options;
	options.name = edge_test::unique_channel_name("producer_overlap");
	const auto schema = TestPayloadV1Schema();
	auto created = edge_runtime::detail::producer_create_impl(
	        options, schema, edge_runtime::detail::kMaxPayloadSize);
	ASSERT_TRUE(created) << edge_runtime::to_string(created.error().code);
	const auto handle = created.value();
	std::vector<std::byte> encoded(edge_runtime::detail::kMaxPayloadSize, std::byte{0x5a});

	handle->operation_in_use.store(true, std::memory_order_release);
	auto rejected = edge_runtime::detail::producer_publish_impl(
	        handle, encoded.data(), static_cast<uint32_t>(encoded.size()));
	ASSERT_FALSE(rejected);
	EXPECT_EQ(rejected.error().code, edge_runtime::ErrorCode::kConcurrentHandleUse);
	handle->operation_in_use.store(false, std::memory_order_release);

	constexpr int kAttemptsPerThread = 4000;
	std::atomic<int> ready{0};
	std::atomic<bool> start{false};
	std::atomic<uint64_t> rejected_overlap{0};
	std::atomic<uint64_t> unexpected_errors{0};
	std::vector<uint64_t> sequences[2];

	auto publish_loop = [&](int index) {
		sequences[index].reserve(kAttemptsPerThread);
		ready.fetch_add(1, std::memory_order_release);
		while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
		for (int i = 0; i < kAttemptsPerThread; ++i) {
			auto result = edge_runtime::detail::producer_publish_impl(
			        handle, encoded.data(), static_cast<uint32_t>(encoded.size()));
			if (result) {
				sequences[index].push_back(result.value().sequence);
			} else if (result.error().code == edge_runtime::ErrorCode::kConcurrentHandleUse) {
				rejected_overlap.fetch_add(1, std::memory_order_relaxed);
			} else {
				unexpected_errors.fetch_add(1, std::memory_order_relaxed);
			}
		}
	};

	std::thread first(publish_loop, 0);
	std::thread second(publish_loop, 1);
	while (ready.load(std::memory_order_acquire) != 2) std::this_thread::yield();
	start.store(true, std::memory_order_release);
	first.join();
	second.join();

	EXPECT_GT(rejected_overlap.load(std::memory_order_relaxed), 0u);
	EXPECT_EQ(unexpected_errors.load(std::memory_order_relaxed), 0u);
	std::vector<uint64_t> combined;
	combined.reserve(sequences[0].size() + sequences[1].size());
	combined.insert(combined.end(), sequences[0].begin(), sequences[0].end());
	combined.insert(combined.end(), sequences[1].begin(), sequences[1].end());
	ASSERT_FALSE(combined.empty());
	std::sort(combined.begin(), combined.end());
	for (size_t i = 0; i < combined.size(); ++i) {
		EXPECT_EQ(combined[i], static_cast<uint64_t>(i + 1));
	}

	EXPECT_TRUE(edge_runtime::detail::producer_remove_if_owner_impl(handle));
}

}  // namespace
