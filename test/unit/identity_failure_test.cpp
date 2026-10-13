#include <fcntl.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdarg>
#include <cstring>

#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/channel/channel_layout.hpp"
#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/consumer_impl.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "edge_runtime/channel/producer_impl.hpp"
#include "edge_runtime/channel/slot_protocol.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {

int stat_failures = 0;
int boot_failures = 0;
int poll_failures = 0;
int fault_hits = 0;
int stat_read_failures = 0;
int boot_read_failures = 0;
int stat_descriptor = -1;
int boot_descriptor = -1;
int read_error = EIO;
bool malformed_boot = false;
bool truncated_stat = false;

class IdentityFailureTest : public ::testing::Test {
       protected:
	void TearDown() override {
		stat_failures = 0;
		boot_failures = 0;
		poll_failures = 0;
		fault_hits = 0;
		stat_read_failures = 0;
		boot_read_failures = 0;
		stat_descriptor = -1;
		boot_descriptor = -1;
		read_error = EIO;
		malformed_boot = false;
		truncated_stat = false;
	}
};

bool fail_next(int* remaining) {
	if (*remaining == 0) return false;
	if (*remaining > 0) --*remaining;
	++fault_hits;
	return true;
}

edge_runtime::ChannelOptions options_for(const char* tag) {
	edge_runtime::ChannelOptions options;
	options.name = edge_test::unique_channel_name(tag);
	return options;
}

TEST_F(IdentityFailureTest, StatReadFailureInvalidatesIdentity) {
	stat_failures = 1;
	const auto identity = edge_runtime::detail::current_process_identity();
	EXPECT_EQ(fault_hits, 1);
	EXPECT_EQ(identity.proc_start_ticks, 0u);
	EXPECT_FALSE(edge_runtime::detail::identity_matches_current(identity));
	EXPECT_EQ(edge_runtime::detail::probe_liveness(identity.pid, identity.proc_start_ticks),
	          edge_runtime::detail::Liveness::kUnverifiable);
}

TEST_F(IdentityFailureTest, BootReadFailureDoesNotHashEmptyInput) {
	uint64_t hi = 1;
	uint64_t lo = 1;
	boot_failures = 1;
	(void)edge_runtime::detail::current_boot_id_hash(&hi, &lo);
	EXPECT_EQ(fault_hits, 1);
	EXPECT_EQ(hi, 0u);
	EXPECT_EQ(lo, 0u);
}

TEST_F(IdentityFailureTest, ActualStatReadErrorPropagatesBeforeRegistration) {
	stat_read_failures = 1;
	const auto identity = edge_runtime::detail::read_current_process_identity();
	ASSERT_FALSE(identity);
	EXPECT_EQ(identity.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	EXPECT_EQ(identity.error().errno_value, EIO);
	EXPECT_EQ(fault_hits, 1);
}

TEST_F(IdentityFailureTest, TruncatedStatStarttimeFailsClosed) {
	truncated_stat = true;
	const auto result = edge_runtime::detail::read_current_process_identity();
	EXPECT_FALSE(result);
	EXPECT_EQ(fault_hits, 1);
	if (!result) {
		EXPECT_EQ(result.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	}
}

TEST_F(IdentityFailureTest, ActualBootReadErrorInvalidatesHash) {
	boot_read_failures = 1;
	uint64_t hi = 1;
	uint64_t lo = 1;
	const auto result = edge_runtime::detail::current_boot_id_hash(&hi, &lo);
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().errno_value, EIO);
	EXPECT_EQ(fault_hits, 1);
	EXPECT_EQ(hi, 0u);
	EXPECT_EQ(lo, 0u);
}

TEST_F(IdentityFailureTest, OneBootReadInterruptRetriesWithoutChangingIdentity) {
	const auto expected = edge_runtime::detail::current_process_identity();
	boot_read_failures = 1;
	read_error = EINTR;
	const auto result = edge_runtime::detail::read_current_process_identity();
	ASSERT_TRUE(result);
	EXPECT_EQ(fault_hits, 1);
	EXPECT_EQ(result.value().boot_id_hash_hi, expected.boot_id_hash_hi);
	EXPECT_EQ(result.value().boot_id_hash_lo, expected.boot_id_hash_lo);
}

TEST_F(IdentityFailureTest, PersistentStatReadInterruptsAreBounded) {
	stat_read_failures = -1;
	read_error = EINTR;
	const auto result = edge_runtime::detail::read_current_process_identity();
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	EXPECT_EQ(result.error().errno_value, EINTR);
	EXPECT_GT(fault_hits, 0);
	EXPECT_LE(fault_hits, 8);
}

TEST_F(IdentityFailureTest, MalformedBootReadDoesNotGenerateValidHash) {
	malformed_boot = true;
	const auto result = edge_runtime::detail::read_current_process_identity();
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	EXPECT_EQ(fault_hits, 1);
}

TEST_F(IdentityFailureTest, ProducerRejectsIdentityReadFailure) {
	const auto options = options_for("identity_create");
	stat_failures = 1;
	auto producer =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	EXPECT_EQ(fault_hits, 1);
	EXPECT_FALSE(producer);
	if (producer) {
		auto replacement = edge_runtime::Producer<TestPayloadV1>::create(
		        options, TestPayloadV1Schema());
		EXPECT_FALSE(replacement);
		if (replacement) {
			EXPECT_TRUE(replacement.value().remove_if_owner());
		} else {
			EXPECT_TRUE(producer.value().remove_if_owner());
		}
	}
}

TEST_F(IdentityFailureTest, ConsumerRejectsIdentityReadFailure) {
	const auto options = options_for("identity_consumer");
	auto producer =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	ASSERT_TRUE(producer.value().publish(TestPayloadV1{}));
	stat_failures = 1;
	auto consumer = edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
	EXPECT_EQ(fault_hits, 1);
	EXPECT_FALSE(consumer);
	if (consumer) {
		auto held = consumer.value().try_loan_latest();
		ASSERT_TRUE(held);
		auto takeover =
		        edge_runtime::Consumer<TestPayloadV1>::open(options, TestPayloadV1Schema());
		EXPECT_FALSE(takeover);
	}
	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST_F(IdentityFailureTest, LiveProducerWithInvalidStoredIdentityIsNotReplaced) {
	const auto options = options_for("invalid_live_producer");
	auto created =
	        edge_runtime::detail::producer_create_impl(options, TestPayloadV1Schema(), 16);
	ASSERT_TRUE(created);
	auto* base = static_cast<std::byte*>(created.value()->shm.mapping.get());
	auto* header = reinterpret_cast<edge_runtime::detail::ChannelHeaderAbi*>(
	        base + edge_runtime::detail::kChannelHeaderOffset);
	header->producer.proc_start_ticks = 0;
	auto replacement =
	        edge_runtime::detail::producer_create_impl(options, TestPayloadV1Schema(), 16);
	EXPECT_FALSE(replacement);
	if (replacement) {
		EXPECT_TRUE(
		        edge_runtime::detail::producer_remove_if_owner_impl(replacement.value()));
	} else {
		EXPECT_EQ(replacement.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
		EXPECT_TRUE(edge_runtime::detail::producer_remove_if_owner_impl(created.value()));
	}
}

TEST_F(IdentityFailureTest, LiveConsumerReadingSlotWithInvalidIdentityIsNotReclaimed) {
	const auto options = options_for("invalid_live_consumer");
	auto producer =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(producer);
	ASSERT_TRUE(producer.value().publish(TestPayloadV1{}));
	auto opened = edge_runtime::detail::consumer_open_impl(options, TestPayloadV1Schema(), 16);
	ASSERT_TRUE(opened);
	auto held = edge_runtime::detail::consumer_try_loan_latest_impl(opened.value());
	ASSERT_TRUE(held);
	auto* base = static_cast<std::byte*>(opened.value()->shm.mapping.get());
	auto* header = reinterpret_cast<edge_runtime::detail::ChannelHeaderAbi*>(
	        base + edge_runtime::detail::kChannelHeaderOffset);
	header->consumer.proc_start_ticks = 0;
	auto takeover =
	        edge_runtime::detail::consumer_open_impl(options, TestPayloadV1Schema(), 16);
	ASSERT_FALSE(takeover);
	EXPECT_EQ(takeover.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	uint32_t reading = 0;
	uint64_t stride = 0;
	ASSERT_TRUE(edge_runtime::detail::round_up_to_multiple_u64(
	        edge_runtime::detail::kSlotHeaderSize + 16, 64, &stride));
	for (uint32_t index = 0; index < edge_runtime::detail::kSlotCount; ++index) {
		uint64_t offset = 0;
		ASSERT_TRUE(edge_runtime::detail::slot_byte_offset(index, stride, &offset));
		auto* slot = reinterpret_cast<edge_runtime::detail::SlotHeaderAbi*>(base + offset);
		if (slot->state == static_cast<uint32_t>(edge_runtime::detail::SlotState::kReading))
			++reading;
	}
	EXPECT_EQ(reading, 1u);
	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST_F(IdentityFailureTest, BootFailureRejectsProducerAndAllowsLaterCreate) {
	const auto options = options_for("identity_boot_create");
	boot_failures = 1;
	auto rejected =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_FALSE(rejected);
	EXPECT_EQ(fault_hits, 1);
	EXPECT_EQ(rejected.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	auto created =
	        edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(created);
	EXPECT_TRUE(created.value().remove_if_owner());
}

TEST_F(IdentityFailureTest, QueueAndPoolRejectInvalidHandleIdentity) {
	edge_runtime::MpmcQueueOptions queue_options;
	queue_options.name = edge_test::unique_channel_name("identity_queue");
	queue_options.capacity = 4;
	stat_failures = 1;
	auto queue = edge_runtime::MpmcQueue<TestPayloadV1>::create(queue_options,
	                                                            TestPayloadV1Schema());
	ASSERT_FALSE(queue);
	EXPECT_EQ(queue.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	auto valid_queue = edge_runtime::MpmcQueue<TestPayloadV1>::create(queue_options,
	                                                                  TestPayloadV1Schema());
	ASSERT_TRUE(valid_queue);
	stat_failures = 1;
	auto opened_queue =
	        edge_runtime::MpmcQueue<TestPayloadV1>::open(queue_options, TestPayloadV1Schema());
	ASSERT_FALSE(opened_queue);
	EXPECT_EQ(opened_queue.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	EXPECT_TRUE(valid_queue.value().remove_if_creator());
	edge_runtime::SharedBufferPoolOptions pool_options;
	pool_options.name = edge_test::unique_channel_name("identity_pool");
	pool_options.block_size = 4096;
	pool_options.block_count = 2;
	pool_options.schema = TestPayloadV1Schema();
	boot_failures = 1;
	auto pool = edge_runtime::SharedBufferPool::create(pool_options);
	ASSERT_FALSE(pool);
	EXPECT_EQ(pool.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	auto valid_pool = edge_runtime::SharedBufferPool::create(pool_options);
	ASSERT_TRUE(valid_pool);
	boot_failures = 1;
	auto opened_pool = edge_runtime::SharedBufferPool::open(pool_options);
	ASSERT_FALSE(opened_pool);
	EXPECT_EQ(opened_pool.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	EXPECT_TRUE(valid_pool.value().remove_if_creator());
	EXPECT_EQ(fault_hits, 4);
}

TEST_F(IdentityFailureTest, InterruptedZombiePollDoesNotReportAlive) {
	const pid_t child = ::fork();
	ASSERT_GE(child, 0);
	if (child == 0) {
		::execl("/bin/true", "true", static_cast<char*>(nullptr));
		::_exit(127);
	}
	siginfo_t info{};
	ASSERT_EQ(::waitid(P_PID, static_cast<id_t>(child), &info, WEXITED | WNOWAIT), 0);
	const auto start = edge_runtime::detail::proc_stat_starttime(child);
	ASSERT_TRUE(start);
	poll_failures = 1;
	const auto liveness =
	        edge_runtime::detail::probe_liveness(static_cast<uint64_t>(child), start.value());
	EXPECT_EQ(fault_hits, 1);
	EXPECT_EQ(liveness, edge_runtime::detail::Liveness::kExited);
	int status = 0;
	EXPECT_EQ(::waitpid(child, &status, 0), child);
}

TEST_F(IdentityFailureTest, PersistentPollInterruptionsAreBoundedAndClosed) {
	const auto self = edge_runtime::detail::current_process_identity();
	poll_failures = -1;
	EXPECT_EQ(edge_runtime::detail::probe_liveness(self.pid, self.proc_start_ticks),
	          edge_runtime::detail::Liveness::kUnverifiable);
	EXPECT_GT(fault_hits, 0);
	EXPECT_LE(fault_hits, 16);
}

}  // namespace

extern "C" int __real_open(const char* path, int flags, ...);
extern "C" int __real_poll(struct pollfd* descriptors, nfds_t count, int timeout);
extern "C" ssize_t __real_read(int descriptor, void* buffer, size_t count);

extern "C" int __wrap_open(const char* path, int flags, ...) {
	const bool proc_stat =
	        std::strncmp(path, "/proc/", 6) == 0 && std::strstr(path, "/stat") != nullptr;
	const bool boot_id = std::strcmp(path, "/proc/sys/kernel/random/boot_id") == 0;
	if ((proc_stat && fail_next(&stat_failures)) || (boot_id && fail_next(&boot_failures))) {
		errno = EACCES;
		return -1;
	}
	if ((flags & O_CREAT) != 0) {
		va_list arguments;
		va_start(arguments, flags);
		const mode_t mode = static_cast<mode_t>(va_arg(arguments, int));
		va_end(arguments);
		return __real_open(path, flags, mode);
	}
	const int descriptor = __real_open(path, flags);
	if (descriptor >= 0 && proc_stat) {
		stat_descriptor = descriptor;
		if (boot_descriptor == descriptor) boot_descriptor = -1;
	}
	if (descriptor >= 0 && boot_id) {
		boot_descriptor = descriptor;
		if (stat_descriptor == descriptor) stat_descriptor = -1;
	}
	return descriptor;
}

extern "C" ssize_t __wrap_read(int descriptor, void* buffer, size_t count) {
	if ((descriptor == stat_descriptor && fail_next(&stat_read_failures)) ||
	    (descriptor == boot_descriptor && fail_next(&boot_read_failures))) {
		errno = read_error;
		return -1;
	}
	if (descriptor == boot_descriptor && malformed_boot && count >= 36) {
		++fault_hits;
		std::memset(buffer, 'x', 36);
		return 36;
	}
	const ssize_t actual = __real_read(descriptor, buffer, count);
	if (descriptor == stat_descriptor && truncated_stat && actual > 0) {
		const std::string contents(static_cast<const char*>(buffer),
		                           static_cast<size_t>(actual));
		size_t position = contents.rfind(')');
		if (position != std::string::npos) {
			position += 2;
			for (int field = 0; field < 19 && position != std::string::npos; ++field) {
				const size_t space = contents.find(' ', position);
				position = space == std::string::npos ? space : space + 1;
			}
			if (position != std::string::npos && position + 1 < contents.size()) {
				++fault_hits;
				return static_cast<ssize_t>(position + 1);
			}
		}
	}
	return actual;
}

extern "C" int __wrap_poll(struct pollfd* descriptors, nfds_t count, int timeout) {
	if (fail_next(&poll_failures)) {
		errno = EINTR;
		return -1;
	}
	return __real_poll(descriptors, count, timeout);
}
