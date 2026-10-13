#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>

#include "creation_faults.hpp"
#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/channel/control_lock.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "mpmc_payload.hpp"
#include "test_payload.hpp"

namespace {

using edge_runtime::ErrorCode;
using edge_runtime::Result;
using edge_runtime::detail::ControlJournalV1;
using edge_runtime::detail::JournalState;
using edge_test::CreationFault;
using edge_test::CreationFaultPlan;
using edge_test::CreationFaults;

enum class CreationKind { kSpsc, kMpmc, kPool };

const char* kind_name(CreationKind kind) {
	switch (kind) {
		case CreationKind::kSpsc:
			return "SPSC";
		case CreationKind::kMpmc:
			return "MPMC";
		case CreationKind::kPool:
			return "Pool";
	}
	return "Unknown";
}

class CreationFailureTest : public ::testing::TestWithParam<CreationKind> {
       protected:
	void SetUp() override {
		name_ = edge_test::unique_creation_name();
		const std::string prefix = GetParam() == CreationKind::kMpmc ? "/edgeruntime.mpmc."
		                           : GetParam() == CreationKind::kPool
		                                   ? "/edgeruntime.bufferpool."
		                                   : "/edgeruntime.";
		posix_name_ = prefix + std::to_string(::getuid()) + "." + name_;
	}

	Result<void> create_and_remove() {
		if (GetParam() == CreationKind::kSpsc) {
			edge_runtime::ChannelOptions options;
			options.name = name_;
			auto created = edge_runtime::Producer<TestPayloadV1>::create(
			        options, TestPayloadV1Schema());
			if (!created) return created.error();
			return created.value().remove_if_owner();
		}
		if (GetParam() == CreationKind::kMpmc) {
			edge_runtime::MpmcQueueOptions options;
			options.name = name_;
			options.capacity = 4;
			auto created = edge_runtime::MpmcQueue<MpmcTestPayload>::create(
			        options, MpmcTestSchema());
			if (!created) return created.error();
			return created.value().remove_if_creator();
		}
		edge_runtime::SharedBufferPoolOptions options;
		options.name = name_;
		options.block_size = 4096;
		options.block_count = 2;
		options.schema = MpmcTestSchema();
		auto created = edge_runtime::SharedBufferPool::create(options);
		if (!created) return created.error();
		return created.value().remove_if_creator();
	}

	void assert_injection(const CreationFaults& faults, bool once) {
		const auto& hits = faults.evidence();
		EXPECT_EQ(hits.fixture_error, 0);
		EXPECT_EQ(hits.exclusive_opens, 1u);
		EXPECT_FALSE(hits.budget_exceeded);
		EXPECT_GE(hits.target_fstat_calls, 1u);
		EXPECT_LE(hits.target_fstat_calls, edge_test::kCreationFaultCallBudget);
		if (once) {
			EXPECT_EQ(hits.fstat_fault_hits, 1u);
		} else {
			EXPECT_EQ(hits.fstat_fault_hits, hits.target_fstat_calls);
		}
		EXPECT_TRUE(faults.original_fd_closed());
		const auto created = faults.created_object();
		EXPECT_TRUE(created.exists);
		EXPECT_NE(created.ino, 0u);
		std::cout << "fault-evidence name=" << posix_name_ << " dev=" << created.dev
		          << " ino=" << created.ino << " links=" << created.links
		          << " exclusive_opens=" << hits.exclusive_opens
		          << " fstat_calls=" << hits.target_fstat_calls
		          << " fstat_hits=" << hits.fstat_fault_hits
		          << " statx_calls=" << hits.target_statx_calls
		          << " statx_hits=" << hits.statx_fault_hits
		          << " other_fstat_calls=" << hits.other_fstat_calls << '\n';
	}

	bool read_journal(ControlJournalV1* journal) {
		const std::string path = edge_runtime::detail::channel_lock_path(name_);
		const int descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
		if (descriptor < 0) return false;
		const ssize_t count = ::pread(descriptor, journal, sizeof(*journal), 0);
		::close(descriptor);
		return count == static_cast<ssize_t>(sizeof(*journal));
	}

	void persistent_failure(CreationFault fault, bool deny_statx) {
		CreationFaults faults(posix_name_, CreationFaultPlan{fault, deny_statx, false});
		const auto result = create_and_remove();
		faults.disarm();
		assert_injection(faults, false);
		EXPECT_EQ(faults.evidence().fstat_fault_hits,
		          fault == CreationFault::kPersistentEintr ? 8u : 1u);
		ASSERT_FALSE(result) << "creation must not succeed after persistent fstat failures";
		const auto named = faults.named_object();
		const auto created = faults.created_object();
		if (!deny_statx) {
			ASSERT_TRUE(faults.evidence().statx_available)
			        << "host lacks the statx cleanup fallback; run "
			           "unavailable-identity cases";
			EXPECT_EQ(result.error().errno_value,
			          fault == CreationFault::kPersistentEintr ? EINTR : EIO);
			EXPECT_FALSE(named.exists) << "failed creation left a named inode";
			EXPECT_EQ(named.error, ENOENT);
			EXPECT_EQ(created.links, 0u);
			const auto recreated = create_and_remove();
			EXPECT_TRUE(recreated) << "legitimate same-name recreate failed: "
			                       << edge_runtime::to_string(recreated.error().code);
			return;
		}
		EXPECT_GE(faults.evidence().statx_fault_hits, 1u);
		EXPECT_EQ(faults.evidence().statx_fault_hits, faults.evidence().target_statx_calls);
		EXPECT_EQ(result.error().code, ErrorCode::kRecoveryBlocked);
		ASSERT_TRUE(named.exists) << "unverifiable creation must retain recovery evidence";
		EXPECT_EQ(named.dev, created.dev);
		EXPECT_EQ(named.ino, created.ino);
		EXPECT_EQ(named.size, 0u);
		EXPECT_EQ(created.links, 1u);
		if (GetParam() == CreationKind::kSpsc) {
			ControlJournalV1 before{};
			ASSERT_TRUE(read_journal(&before));
			EXPECT_EQ(before.state,
			          static_cast<uint32_t>(JournalState::kCreatingObject));
			EXPECT_EQ(before.target_dev, 0u);
			EXPECT_EQ(before.target_ino, 0u);
			EXPECT_EQ(before.creator.pid, static_cast<uint64_t>(::getpid()));
			EXPECT_NE(before.creator.proc_start_ticks, 0u);
			EXPECT_NE(before.new_nonce_hi | before.new_nonce_lo, 0u);
			EXPECT_EQ(before.record_checksum,
			          edge_runtime::detail::journal_checksum(before));
			std::cout << "journal-evidence name=" << name_ << " state=" << before.state
			          << " creator=" << before.creator.pid
			          << " start=" << before.creator.proc_start_ticks
			          << " target_dev=" << before.target_dev
			          << " target_ino=" << before.target_ino
			          << " generation=" << before.new_generation
			          << " nonce_hi=" << before.new_nonce_hi
			          << " nonce_lo=" << before.new_nonce_lo
			          << " checksum=" << before.record_checksum << '\n';
			const auto retry = create_and_remove();
			ASSERT_FALSE(retry);
			EXPECT_EQ(retry.error().code, ErrorCode::kRecoveryBlocked);
			ControlJournalV1 after{};
			ASSERT_TRUE(read_journal(&after));
			EXPECT_EQ(std::memcmp(&before, &after, sizeof(before)), 0);
			EXPECT_EQ(faults.named_object().ino, created.ino);
		} else {
			const auto retry = create_and_remove();
			EXPECT_FALSE(retry);
			EXPECT_EQ(faults.named_object().ino, created.ino);
			ASSERT_TRUE(faults.cleanup_fixture_owned());
			const auto recreated = create_and_remove();
			EXPECT_TRUE(recreated) << edge_runtime::to_string(recreated.error().code);
		}
	}

	std::string name_;
	std::string posix_name_;
};

TEST_P(CreationFailureTest, FirstFstatEintrRetriesAndCreates) {
	CreationFaults faults(posix_name_,
	                      CreationFaultPlan{CreationFault::kOnceEintr, false, false});
	const auto result = create_and_remove();
	faults.disarm();
	assert_injection(faults, true);
	EXPECT_GE(faults.evidence().target_fstat_calls, 2u);
	EXPECT_TRUE(result) << edge_runtime::to_string(result.error().code);
	EXPECT_FALSE(faults.named_object().exists);
	EXPECT_EQ(faults.created_object().links, 0u);
	const auto recreated = create_and_remove();
	EXPECT_TRUE(recreated) << edge_runtime::to_string(recreated.error().code);
}

TEST_P(CreationFailureTest, PersistentEintrCleansAndAllowsLegitimateRecreate) {
	persistent_failure(CreationFault::kPersistentEintr, false);
}

TEST_P(CreationFailureTest, PersistentEioCleansAndPreservesOriginalError) {
	persistent_failure(CreationFault::kPersistentEio, false);
}

TEST_P(CreationFailureTest, PersistentEintrUnavailableIdentityRetainsEvidence) {
	persistent_failure(CreationFault::kPersistentEintr, true);
}

TEST_P(CreationFailureTest, PersistentEioUnavailableIdentityRetainsEvidence) {
	persistent_failure(CreationFault::kPersistentEio, true);
}

TEST_P(CreationFailureTest, SameNameExternalSentinelSurvivesFailedCreation) {
	CreationFaults faults(posix_name_,
	                      CreationFaultPlan{CreationFault::kPersistentEio, false, true});
	const auto result = create_and_remove();
	faults.disarm();
	assert_injection(faults, false);
	ASSERT_FALSE(result);
	const auto sentinel = faults.sentinel_object();
	const auto named = faults.named_object();
	const auto created = faults.created_object();
	ASSERT_TRUE(sentinel.exists);
	ASSERT_TRUE(named.exists) << "failure cleanup deleted an external same-name inode";
	EXPECT_NE(sentinel.ino, created.ino);
	EXPECT_EQ(named.dev, sentinel.dev);
	EXPECT_EQ(named.ino, sentinel.ino);
	EXPECT_TRUE(faults.sentinel_contents_match());
	EXPECT_EQ(created.links, 0u);
	EXPECT_EQ(result.error().code, ErrorCode::kNameRaceDetected);
	ControlJournalV1 before{};
	if (GetParam() == CreationKind::kSpsc) {
		ASSERT_TRUE(read_journal(&before));
		EXPECT_EQ(before.state, static_cast<uint32_t>(JournalState::kCreatingObject));
		EXPECT_EQ(before.target_dev, created.dev);
		EXPECT_EQ(before.target_ino, created.ino);
		EXPECT_NE(before.target_ino, sentinel.ino);
	}
	const auto retry_with_sentinel = create_and_remove();
	EXPECT_FALSE(retry_with_sentinel);
	EXPECT_TRUE(faults.sentinel_contents_match());
	if (GetParam() == CreationKind::kSpsc) {
		ControlJournalV1 after{};
		ASSERT_TRUE(read_journal(&after));
		EXPECT_EQ(std::memcmp(&before, &after, sizeof(before)), 0);
	}
	ASSERT_TRUE(faults.cleanup_fixture_owned());
	const auto recreated = create_and_remove();
	if (GetParam() == CreationKind::kSpsc && !recreated &&
	    recreated.error().code == ErrorCode::kRecoveryBlocked) {
		ControlJournalV1 journal{};
		ASSERT_TRUE(read_journal(&journal));
		EXPECT_NE(journal.state, static_cast<uint32_t>(JournalState::kIdle));
		std::cout << "limitation: live SPSC creator journal remains blocked; dead-owner "
		             "recovery "
		             "requires a separate fork/exec fixture\n";
	} else {
		EXPECT_TRUE(recreated) << edge_runtime::to_string(recreated.error().code);
	}
}

INSTANTIATE_TEST_SUITE_P(AllCreationApis, CreationFailureTest,
                         ::testing::Values(CreationKind::kSpsc, CreationKind::kMpmc,
                                           CreationKind::kPool),
                         [](const ::testing::TestParamInfo<CreationKind>& parameter) {
	                         return kind_name(parameter.param);
                         });

}  // namespace
