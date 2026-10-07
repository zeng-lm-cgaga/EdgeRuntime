#include <gtest/gtest.h>

#include <csignal>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "edge_runtime/queue/mpmc_layout.hpp"
#include "edge_runtime/transport/shm_object.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime/process/process_identity.hpp"
#include "edge_runtime/sync/owner_snapshot.hpp"
#include "edge_runtime/sync/shared_atomic.hpp"
#include "mpmc_payload.hpp"
#include "test_util.hpp"

namespace {

using Queue = edge_runtime::MpmcQueue<MpmcTestPayload>;
using edge_runtime::ErrorCode;
using edge_runtime::MpmcQueueOptions;

std::string g_helper;

std::vector<std::string> child_args(const std::string& role, const std::string& name,
                                    uint32_t capacity, uint64_t count, uint32_t id = 0) {
	return {g_helper,
	        "--role",
	        role,
	        "--name",
	        name,
	        "--capacity",
	        std::to_string(capacity),
	        "--count",
	        std::to_string(count),
	        "--id",
	        std::to_string(id)};
}

bool contains(const std::string& text, const char* needle) {
	return text.find(needle) != std::string::npos;
}

bool process_is_stopped(pid_t pid) {
	char path[64];
	std::snprintf(path, sizeof(path), "/proc/%ld/stat", static_cast<long>(pid));
	std::FILE* file = std::fopen(path, "r");
	if (file == nullptr) return false;
	char line[512];
	const size_t size = std::fread(line, 1, sizeof(line) - 1, file);
	std::fclose(file);
	if (size == 0) return false;
	line[size] = '\0';
	const char* close_paren = std::strrchr(line, ')');
	if (close_paren == nullptr) return false;
	const char* state = close_paren + 1;
	while (*state == ' ') ++state;
	return *state == 'T';
}

bool wait_stopped(pid_t pid, int timeout_ms) {
	const int64_t deadline = edge_test::monotonic_ms_now() + timeout_ms;
	while (edge_test::monotonic_ms_now() < deadline) {
		if (process_is_stopped(pid)) return true;
		struct timespec delay {};
		delay.tv_nsec = 10 * 1000 * 1000L;
		::nanosleep(&delay, nullptr);
	}
	return process_is_stopped(pid);
}

void kill_and_reap(edge_test::SpawnedChild* child) {
	child->kill(SIGKILL);
	std::string output;
	(void)child->wait(5000, &output);
}

bool continue_and_reap(edge_test::SpawnedChild* child, std::string* output) {
	child->kill(SIGCONT);
	return child->wait(10000, output);
}

bool spawn_with_failpoint(edge_test::SpawnedChild* child, const std::vector<std::string>& args,
		const char* failpoint, unsigned hit = 1) {
	::setenv("EDGE_FAILPOINT", failpoint, 1);
	::setenv("EDGE_FAILPOINT_MODE", "stop", 1);
	const std::string hit_text = std::to_string(hit);
	::setenv("EDGE_FAILPOINT_COUNT", hit_text.c_str(), 1);
	const bool spawned = child->spawn(args);
	::unsetenv("EDGE_FAILPOINT");
	::unsetenv("EDGE_FAILPOINT_MODE");
	::unsetenv("EDGE_FAILPOINT_COUNT");
	return spawned;
}

std::set<std::pair<uint32_t, uint64_t>> parse_items(const std::string& output) {
	std::set<std::pair<uint32_t, uint64_t>> items;
	size_t begin = 0;
	while (begin < output.size()) {
		const size_t end = output.find('\n', begin);
		const std::string line = output.substr(begin, end == std::string::npos
	                                                      ? std::string::npos
	                                                      : end - begin);
		unsigned int producer = 0;
		unsigned long long sequence = 0;
		if (std::sscanf(line.c_str(), "ITEM producer=%u sequence=%llu", &producer, &sequence) ==
		    2) {
			items.emplace(static_cast<uint32_t>(producer), static_cast<uint64_t>(sequence));
		}
		if (end == std::string::npos) break;
		begin = end + 1;
	}
	return items;
}

MpmcQueueOptions options_for(const std::string& name, uint32_t capacity) {
	MpmcQueueOptions options;
	options.name = name;
	options.capacity = capacity;
	return options;
}

template <typename Mutator>
bool with_queue_image(const std::string& name, uint32_t capacity, Mutator mutator) {
	uint64_t mapping_size = 0;
	if (!edge_runtime::detail::mpmc_mapping_size_for(capacity, sizeof(MpmcTestPayload),
	                                                &mapping_size)) {
		return false;
	}
	auto fd = edge_runtime::detail::shm_open_existing(edge_runtime::detail::mpmc_shm_name(name));
	if (!fd) return false;
	auto mapping = edge_runtime::detail::mmap_region(fd.value(), mapping_size);
	if (!mapping) return false;
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	mutator(header, static_cast<std::byte*>(mapping.value().get()));
	mapping.value().reset();
	fd.value().reset();
	return true;
}

bool set_queue_positions(edge_runtime::detail::MpmcQueueHeaderAbi* header, uint64_t enqueue,
                         uint64_t dequeue, uint64_t enqueue_epoch = 0,
                         uint64_t dequeue_epoch = 0) {
	edge_runtime::detail::shared_store_seq_cst(&header->enqueue_position, enqueue);
	edge_runtime::detail::shared_store_seq_cst(&header->dequeue_position, dequeue);
	edge_runtime::detail::shared_store_seq_cst(
	        &header->enqueue_lock,
	        static_cast<uint32_t>(edge_runtime::detail::MpmcLockState::kFree));
	edge_runtime::detail::shared_store_seq_cst(
	        &header->dequeue_lock,
	        static_cast<uint32_t>(edge_runtime::detail::MpmcLockState::kFree));
	edge_runtime::detail::shared_store_seq_cst(&header->enqueue_owner_epoch, enqueue_epoch);
	edge_runtime::detail::shared_store_seq_cst(&header->dequeue_owner_epoch, dequeue_epoch);
	return true;
}

void set_gate_owner(edge_runtime::detail::MpmcQueueHeaderAbi* header, bool enqueue) {
	const auto identity = edge_runtime::detail::current_process_identity();
	const edge_runtime::detail::MpmcOwnerAbi owner{identity.pid, identity.proc_start_ticks,
	                                               identity.boot_id_hash_hi,
	                                               identity.boot_id_hash_lo};
	edge_runtime::detail::store_owner_seq_cst(
		enqueue ? &header->enqueue_owner : &header->dequeue_owner, owner);
}

bool set_slot_image(std::byte* mapping, uint32_t capacity, uint64_t ticket,
                    edge_runtime::detail::MpmcSlotState state, uint64_t sequence,
                    const MpmcTestPayload* payload = nullptr, uint64_t owner_epoch = 0) {
	uint64_t stride = 0;
	if (!edge_runtime::detail::round_up_to_multiple_u64(
	            sizeof(edge_runtime::detail::MpmcSlotHeaderAbi) + sizeof(MpmcTestPayload), 64,
	            &stride)) {
		return false;
	}
	uint64_t offset = 0;
	if (!edge_runtime::detail::mpmc_slot_offset(static_cast<uint32_t>(ticket % capacity), capacity,
	                                            stride, &offset)) {
		return false;
	}
	auto* slot = reinterpret_cast<edge_runtime::detail::MpmcSlotHeaderAbi*>(mapping + offset);
	if (payload != nullptr) {
		std::memcpy(mapping + offset + sizeof(*slot), payload, sizeof(*payload));
	}
	slot->ticket = ticket;
	const auto identity = edge_runtime::detail::current_process_identity();
	const edge_runtime::detail::MpmcOwnerAbi owner{
		owner_epoch == 0 ? 0 : identity.pid,
		owner_epoch == 0 ? 0 : identity.proc_start_ticks,
		owner_epoch == 0 ? 0 : identity.boot_id_hash_hi,
		owner_epoch == 0 ? 0 : identity.boot_id_hash_lo,
	};
	edge_runtime::detail::store_owner_seq_cst(&slot->owner, owner);
	edge_runtime::detail::shared_store_seq_cst(&slot->owner_epoch, owner_epoch);
	edge_runtime::detail::shared_store_seq_cst(&slot->sequence, sequence);
	edge_runtime::detail::shared_store_seq_cst(&slot->state, static_cast<uint32_t>(state));
	return true;
}

TEST(MpmcQueue, CrossProcessSingleProducerConsumer) {
	const std::string name = edge_test::unique_channel_name("mpmc_spsc");
	const auto options = options_for(name, 8);
	auto queue = Queue::create(options, MpmcTestSchema());
	ASSERT_TRUE(queue) << edge_runtime::to_string(queue.error().code) << " "
	                   << queue.error().context;

	edge_test::SpawnedChild consumer;
	edge_test::SpawnedChild producer;
	ASSERT_TRUE(consumer.spawn(child_args("consumer", name, 8, 40)));
	ASSERT_TRUE(producer.spawn(child_args("producer", name, 8, 40, 7)));
	std::string producer_output;
	std::string consumer_output;
	ASSERT_TRUE(producer.wait(30000, &producer_output)) << producer_output;
	ASSERT_TRUE(consumer.wait(30000, &consumer_output)) << consumer_output;
	EXPECT_EQ(producer.exit_code(), 0) << producer_output;
	EXPECT_EQ(consumer.exit_code(), 0) << consumer_output;
	EXPECT_TRUE(contains(producer_output, "PRODUCED producer=7 count=40"));
	EXPECT_TRUE(contains(consumer_output, "CONSUMED count=40"));
	const auto items = parse_items(consumer_output);
	ASSERT_EQ(items.size(), 40u);
	for (uint64_t i = 0; i < 40; ++i) EXPECT_NE(items.find({7, i}), items.end());
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, CrossProcessMultipleProducersAndConsumersDeliverExactlyOnce) {
	const std::string name = edge_test::unique_channel_name("mpmc_mpmc");
	const auto options = options_for(name, 16);
	auto queue = Queue::create(options, MpmcTestSchema());
	ASSERT_TRUE(queue);

	edge_test::SpawnedChild consumer_a;
	edge_test::SpawnedChild consumer_b;
	edge_test::SpawnedChild producer_a;
	edge_test::SpawnedChild producer_b;
	ASSERT_TRUE(consumer_a.spawn(child_args("consumer", name, 16, 100)));
	ASSERT_TRUE(consumer_b.spawn(child_args("consumer", name, 16, 100)));
	ASSERT_TRUE(producer_a.spawn(child_args("producer", name, 16, 100, 1)));
	ASSERT_TRUE(producer_b.spawn(child_args("producer", name, 16, 100, 2)));

	std::string output_a;
	std::string output_b;
	std::string output_c;
	std::string output_d;
	ASSERT_TRUE(producer_a.wait(30000, &output_a)) << output_a;
	ASSERT_TRUE(producer_b.wait(30000, &output_b)) << output_b;
	ASSERT_TRUE(consumer_a.wait(30000, &output_c)) << output_c;
	ASSERT_TRUE(consumer_b.wait(30000, &output_d)) << output_d;
	EXPECT_EQ(producer_a.exit_code(), 0) << output_a;
	EXPECT_EQ(producer_b.exit_code(), 0) << output_b;
	EXPECT_EQ(consumer_a.exit_code(), 0) << output_c;
	EXPECT_EQ(consumer_b.exit_code(), 0) << output_d;

	const auto items_a = parse_items(output_c);
	const auto items_b = parse_items(output_d);
	std::set<std::pair<uint32_t, uint64_t>> all = items_a;
	all.insert(items_b.begin(), items_b.end());
	ASSERT_EQ(all.size(), 200u);
	for (uint32_t producer : {1u, 2u}) {
		for (uint64_t sequence = 0; sequence < 100; ++sequence) {
			EXPECT_NE(all.find({producer, sequence}), all.end());
		}
	}
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, EmptyAndFullAreExplicit) {
	const std::string name = edge_test::unique_channel_name("mpmc_edges");
	const auto options = options_for(name, 2);
	auto queue = Queue::create(options, MpmcTestSchema());
	ASSERT_TRUE(queue);

	auto empty = queue.value().try_pop();
	ASSERT_FALSE(empty);
	EXPECT_EQ(empty.error().code, ErrorCode::kQueueEmpty);
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{0, 3, 0x4D504D43u}));
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{1, 3, 0x4D504D43u}));
	auto full = queue.value().try_push(MpmcTestPayload{2, 3, 0x4D504D43u});
	ASSERT_FALSE(full);
	EXPECT_EQ(full.error().code, ErrorCode::kQueueFull);
	ASSERT_TRUE(queue.value().try_pop());
	ASSERT_TRUE(queue.value().try_pop());
	empty = queue.value().try_pop();
	ASSERT_FALSE(empty);
	EXPECT_EQ(empty.error().code, ErrorCode::kQueueEmpty);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, SchemaAndAbiMismatchAreRejectedCrossProcess) {
	const std::string name = edge_test::unique_channel_name("mpmc_schema");
	const auto options = options_for(name, 4);
	auto queue = Queue::create(options, MpmcTestSchema());
	ASSERT_TRUE(queue);

	auto mismatch = edge_test::run_child_capture(
	        {g_helper, "--role", "consumer", "--name", name, "--capacity", "4", "--count",
	         "1", "--schema-mismatch"},
	        10000);
	ASSERT_FALSE(mismatch.timed_out);
	EXPECT_EQ(mismatch.exit_code, 3) << mismatch.stdout_text;
	EXPECT_TRUE(contains(mismatch.stdout_text, "OPEN_FAIL code=SchemaMismatch"));

	auto fd = edge_runtime::detail::shm_open_existing(
	        edge_runtime::detail::mpmc_shm_name(name));
	ASSERT_TRUE(fd);
	auto mapping = edge_runtime::detail::mmap_region(fd.value(),
	                                                 sizeof(edge_runtime::detail::MpmcQueueHeaderAbi));
	ASSERT_TRUE(mapping);
	// Header validation checks ABI before the checksum, so this deliberately models an old reader.
	auto* header = static_cast<edge_runtime::detail::MpmcQueueHeaderAbi*>(mapping.value().get());
	header->abi_major = 99;
	mapping.value().reset();
	fd.value().reset();
	auto abi = edge_test::run_child_capture(
	        {g_helper, "--role", "consumer", "--name", name, "--capacity", "4", "--count",
	         "1"},
	        10000);
	ASSERT_FALSE(abi.timed_out);
	EXPECT_EQ(abi.exit_code, 3) << abi.stdout_text;
	EXPECT_TRUE(contains(abi.stdout_text, "OPEN_FAIL code=AbiMismatch"));
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, DeadProducerGateFailsClosed) {
	const std::string name = edge_test::unique_channel_name("mpmc_dead_prod");
	const auto options = options_for(name, 4);
	auto queue = Queue::create(options, MpmcTestSchema());
	ASSERT_TRUE(queue);
	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(&victim, child_args("producer", name, 4, 1, 9),
	                                  "MPMC_ENQUEUE_GATE_OWNED"));
	ASSERT_TRUE(wait_stopped(victim.pid(), 5000));
	kill_and_reap(&victim);

	auto result = queue.value().try_push(MpmcTestPayload{0, 1, 0x4D504D43u});
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, ErrorCode::kRecoveryBlocked);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, DeadConsumerSlotFailsClosed) {
	const std::string name = edge_test::unique_channel_name("mpmc_dead_cons");
	const auto options = options_for(name, 4);
	auto queue = Queue::create(options, MpmcTestSchema());
	ASSERT_TRUE(queue);
	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{0, 1, 0x4D504D43u}));
	edge_test::SpawnedChild victim;
	ASSERT_TRUE(spawn_with_failpoint(&victim, child_args("consumer", name, 4, 1),
	                                  "MPMC_DEQUEUE_SLOT_OWNED"));
	ASSERT_TRUE(wait_stopped(victim.pid(), 5000));
	kill_and_reap(&victim);

	auto result = queue.value().try_pop();
	ASSERT_FALSE(result);
	EXPECT_EQ(result.error().code, ErrorCode::kRecoveryBlocked);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, PositionPreflightRetriesAfterHealthyPushPopRace) {
	{
		const std::string name = edge_test::unique_channel_name("mpmc_position_push_race");
		auto queue = Queue::create(options_for(name, 4), MpmcTestSchema());
		ASSERT_TRUE(queue);

		edge_test::SpawnedChild producer;
		ASSERT_TRUE(spawn_with_failpoint(
				&producer, child_args("wait-producer", name, 4, 1, 21),
				"MPMC_POSITION_AFTER_ENQUEUE"));
		ASSERT_TRUE(wait_stopped(producer.pid(), 5000));

		const MpmcTestPayload first{601, 11, 0x4D504D43u};
		ASSERT_TRUE(queue.value().try_push(first));
		auto popped = queue.value().try_pop();
		ASSERT_TRUE(popped) << popped.error().context;
		EXPECT_EQ(popped.value().sequence, first.sequence);
		EXPECT_EQ(popped.value().producer, first.producer);

		std::string output;
		EXPECT_TRUE(continue_and_reap(&producer, &output)) << output;
		EXPECT_EQ(producer.exit_code(), 0) << output;
		EXPECT_TRUE(contains(output, "WAIT_PRODUCED producer=21 sequence=0")) << output;

		auto child_value = queue.value().try_pop();
		EXPECT_TRUE(child_value) << child_value.error().context;
		if (child_value) {
			EXPECT_EQ(child_value.value().sequence, 0u);
			EXPECT_EQ(child_value.value().producer, 21u);
		}
		ASSERT_TRUE(queue.value().remove_if_creator());
	}

	{
		const std::string name = edge_test::unique_channel_name("mpmc_position_pop_race");
		auto queue = Queue::create(options_for(name, 4), MpmcTestSchema());
		ASSERT_TRUE(queue);

		edge_test::SpawnedChild consumer;
		ASSERT_TRUE(spawn_with_failpoint(
				&consumer, child_args("wait-consumer", name, 4, 1),
				"MPMC_POSITION_AFTER_ENQUEUE"));
		ASSERT_TRUE(wait_stopped(consumer.pid(), 5000));

		const MpmcTestPayload first{602, 12, 0x4D504D43u};
		ASSERT_TRUE(queue.value().try_push(first));
		auto popped = queue.value().try_pop();
		ASSERT_TRUE(popped) << popped.error().context;
		EXPECT_EQ(popped.value().sequence, first.sequence);
		EXPECT_EQ(popped.value().producer, first.producer);

		const MpmcTestPayload second{603, 13, 0x4D504D43u};
		ASSERT_TRUE(queue.value().try_push(second));

		std::string output;
		ASSERT_TRUE(continue_and_reap(&consumer, &output)) << output;
		EXPECT_EQ(consumer.exit_code(), 0) << output;
		EXPECT_TRUE(contains(output, "ITEM producer=13 sequence=603")) << output;
		ASSERT_TRUE(queue.value().remove_if_creator());
	}
}

TEST(MpmcQueue, PushAdmissionRetriesAfterConcurrentDrain) {
	constexpr uint64_t kMaxCommitted = UINT64_MAX - 1u;
	const std::string name = edge_test::unique_channel_name("mpmc_admission_drain_race");
	auto queue = Queue::create(options_for(name, 4), MpmcTestSchema());
	ASSERT_TRUE(queue);
	ASSERT_TRUE(with_queue_image(name, 4, [&](auto* header, std::byte*) {
		set_queue_positions(header, 0, 0, 0, kMaxCommitted - 4u);
	}));

	const MpmcTestPayload first{606, 16, 0x4D504D43u};
	ASSERT_TRUE(queue.value().try_push(first));

	edge_test::SpawnedChild producer;
	ASSERT_TRUE(spawn_with_failpoint(
			&producer, child_args("wait-producer", name, 4, 1, 41),
			"MPMC_BEFORE_PUSH_DRAIN_ADMISSION"));
	ASSERT_TRUE(wait_stopped(producer.pid(), 5000));

	auto popped = queue.value().try_pop();
	ASSERT_TRUE(popped) << popped.error().context;
	EXPECT_EQ(popped.value().sequence, first.sequence);
	EXPECT_EQ(popped.value().producer, first.producer);

	std::string producer_output;
	ASSERT_TRUE(continue_and_reap(&producer, &producer_output)) << producer_output;
	EXPECT_EQ(producer.exit_code(), 0) << producer_output;
	EXPECT_TRUE(contains(producer_output, "WAIT_PRODUCED producer=41 sequence=0"))
			<< producer_output;

	edge_test::SpawnedChild consumer;
	ASSERT_TRUE(consumer.spawn(child_args("probe-consumer", name, 4, 1)));
	std::string consumer_output;
	ASSERT_TRUE(consumer.wait(10000, &consumer_output)) << consumer_output;
	EXPECT_EQ(consumer.exit_code(), 0) << consumer_output;
	EXPECT_TRUE(contains(consumer_output, "ITEM producer=41 sequence=0")) << consumer_output;
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, DequeueGateBudgetAdmitsOnlyDrainableMessages) {
	constexpr uint64_t kMaxCommitted = UINT64_MAX - 1u;

	{
		const std::string name = edge_test::unique_channel_name("mpmc_dequeue_budget_last");
		auto queue = Queue::create(options_for(name, 4), MpmcTestSchema());
		ASSERT_TRUE(queue);
		ASSERT_TRUE(with_queue_image(name, 4, [&](auto* header, std::byte*) {
			set_queue_positions(header, 0, 0, 0, kMaxCommitted - 2u);
			set_gate_owner(header, false);
		}));

		const MpmcTestPayload first{701, 31, 0x4D504D43u};
		ASSERT_TRUE(queue.value().try_push(first));
		const MpmcTestPayload second{702, 32, 0x4D504D43u};
		auto rejected = queue.value().try_push(second);
		EXPECT_FALSE(rejected);
		if (!rejected) {
			EXPECT_EQ(rejected.error().code, ErrorCode::kSequenceExhausted);
			auto waited = queue.value().wait_push_until(
					second, std::chrono::steady_clock::now() + std::chrono::milliseconds(80));
			EXPECT_FALSE(waited);
			EXPECT_EQ(waited.error().code, ErrorCode::kSequenceExhausted);
		}
		uint64_t enqueue = 0;
		uint64_t dequeue = 0;
		MpmcTestPayload unused{};
		ASSERT_TRUE(with_queue_image(name, 4, [&](auto* header, std::byte* mapping) {
			enqueue = edge_runtime::detail::shared_load_seq_cst(&header->enqueue_position);
			dequeue = edge_runtime::detail::shared_load_seq_cst(&header->dequeue_position);
			uint64_t offset = 0;
			ASSERT_TRUE(edge_runtime::detail::mpmc_slot_offset(1, 4, 128, &offset));
			auto* slot = reinterpret_cast<edge_runtime::detail::MpmcSlotHeaderAbi*>(mapping + offset);
			std::memcpy(&unused, mapping + offset + sizeof(*slot), sizeof(unused));
		}));
		EXPECT_EQ(enqueue, 1u);
		EXPECT_EQ(dequeue, 0u);
		EXPECT_EQ(unused.sequence, 0u);
		EXPECT_EQ(unused.producer, 0u);
		EXPECT_EQ(unused.magic, 0u);

		edge_test::SpawnedChild consumer;
		ASSERT_TRUE(consumer.spawn(child_args("probe-consumer", name, 4, 1)));
		std::string output;
		ASSERT_TRUE(consumer.wait(10000, &output)) << output;
		EXPECT_EQ(consumer.exit_code(), 0) << output;
		EXPECT_TRUE(contains(output, "ITEM producer=31 sequence=701")) << output;
		ASSERT_TRUE(queue.value().remove_if_creator());
	}

	{
		const std::string name = edge_test::unique_channel_name("mpmc_dequeue_budget_exhausted");
		auto queue = Queue::create(options_for(name, 4), MpmcTestSchema());
		ASSERT_TRUE(queue);
		ASSERT_TRUE(with_queue_image(name, 4, [&](auto* header, std::byte*) {
			set_queue_positions(header, 0, 0, 0, kMaxCommitted);
			set_gate_owner(header, false);
		}));

		const MpmcTestPayload value{703, 33, 0x4D504D43u};
		auto rejected = queue.value().try_push(value);
		EXPECT_FALSE(rejected);
		if (!rejected) {
			EXPECT_EQ(rejected.error().code, ErrorCode::kSequenceExhausted);
			auto waited = queue.value().wait_push_until(
					value, std::chrono::steady_clock::now() + std::chrono::milliseconds(80));
			EXPECT_FALSE(waited);
			EXPECT_EQ(waited.error().code, ErrorCode::kSequenceExhausted);
		}
		auto waited_pop = queue.value().wait_pop_until(
				std::chrono::steady_clock::now() + std::chrono::milliseconds(80));
		ASSERT_FALSE(waited_pop);
		EXPECT_EQ(waited_pop.error().code, ErrorCode::kSequenceExhausted);

		uint64_t enqueue = 0;
		uint64_t dequeue = 0;
		ASSERT_TRUE(with_queue_image(name, 4, [&](auto* header, std::byte*) {
			enqueue = edge_runtime::detail::shared_load_seq_cst(&header->enqueue_position);
			dequeue = edge_runtime::detail::shared_load_seq_cst(&header->dequeue_position);
		}));
		EXPECT_EQ(enqueue, 0u);
		EXPECT_EQ(dequeue, 0u);

		edge_test::SpawnedChild consumer;
		ASSERT_TRUE(consumer.spawn(child_args("probe-consumer", name, 4, 1)));
		std::string output;
		ASSERT_TRUE(consumer.wait(10000, &output)) << output;
		EXPECT_EQ(consumer.exit_code(), 3) << output;
		EXPECT_TRUE(contains(output, "POP_FAIL code=QueueEmpty")) << output;
		ASSERT_TRUE(queue.value().remove_if_creator());
	}

	{
		const std::string name = edge_test::unique_channel_name("mpmc_dequeue_empty_recheck");
		auto queue = Queue::create(options_for(name, 4), MpmcTestSchema());
		ASSERT_TRUE(queue);
		ASSERT_TRUE(with_queue_image(name, 4, [&](auto* header, std::byte*) {
			set_queue_positions(header, 0, 0, 0, kMaxCommitted - 4u);
			set_gate_owner(header, false);
		}));

		const MpmcTestPayload first{704, 34, 0x4D504D43u};
		ASSERT_TRUE(queue.value().try_push(first));
		edge_test::SpawnedChild stale_consumer;
		ASSERT_TRUE(spawn_with_failpoint(
				&stale_consumer, child_args("probe-consumer", name, 4, 1),
				"MPMC_BEFORE_DEQUEUE_GATE_CLAIM"));
		ASSERT_TRUE(wait_stopped(stale_consumer.pid(), 5000));

		auto popped = queue.value().try_pop();
		ASSERT_TRUE(popped) << popped.error().context;
		EXPECT_EQ(popped.value().sequence, first.sequence);

		std::string stale_output;
		EXPECT_TRUE(continue_and_reap(&stale_consumer, &stale_output)) << stale_output;
		EXPECT_EQ(stale_consumer.exit_code(), 3) << stale_output;
		EXPECT_TRUE(contains(stale_output, "POP_FAIL code=QueueEmpty")) << stale_output;

		uint64_t dequeue_epoch = 0;
		ASSERT_TRUE(with_queue_image(name, 4, [&](auto* header, std::byte*) {
			dequeue_epoch = edge_runtime::detail::shared_load_seq_cst(
					&header->dequeue_owner_epoch);
		}));
		EXPECT_EQ(dequeue_epoch, kMaxCommitted - 2u);

		const MpmcTestPayload second{705, 35, 0x4D504D43u};
		ASSERT_TRUE(queue.value().try_push(second));
		edge_test::SpawnedChild consumer;
		ASSERT_TRUE(consumer.spawn(child_args("probe-consumer", name, 4, 1)));
		std::string output;
		ASSERT_TRUE(consumer.wait(10000, &output)) << output;
		EXPECT_EQ(consumer.exit_code(), 0) << output;
		EXPECT_TRUE(contains(output, "ITEM producer=35 sequence=705")) << output;
		ASSERT_TRUE(queue.value().remove_if_creator());
	}
}

TEST(MpmcQueue, PositionSequenceExhaustionCapacityOneDrainsPendingMessage) {
	constexpr uint64_t kMax = UINT64_MAX;
	constexpr uint64_t kLastTicket = kMax - 1u;
	const std::string name = edge_test::unique_channel_name("mpmc_sequence_cap1");
	const auto options = options_for(name, 1);
	auto queue = Queue::create(options, MpmcTestSchema());
	ASSERT_TRUE(queue);

	ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte* mapping) {
		set_queue_positions(header, kLastTicket, kLastTicket);
		const MpmcTestPayload initial{91, 4, 0x4D504D43u};
		EXPECT_TRUE(set_slot_image(mapping, 1, kLastTicket,
		                           edge_runtime::detail::MpmcSlotState::kFree, kLastTicket,
		                           &initial));
	}));

	const MpmcTestPayload initial{91, 4, 0x4D504D43u};
	ASSERT_TRUE(queue.value().try_push(initial));
	const MpmcTestPayload replacement{92, 5, 0x4D504D43u};
	auto full_exhausted = queue.value().try_push(replacement);
	ASSERT_FALSE(full_exhausted);
	EXPECT_EQ(full_exhausted.error().code, ErrorCode::kSequenceExhausted);

	uint64_t enqueue_epoch = 0;
	uint64_t enqueue_position = 0;
	ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte*) {
		enqueue_epoch = edge_runtime::detail::shared_load_seq_cst(&header->enqueue_owner_epoch);
		enqueue_position = edge_runtime::detail::shared_load_seq_cst(&header->enqueue_position);
	}));
	EXPECT_EQ(enqueue_epoch, 2u);
	EXPECT_EQ(enqueue_position, kMax);

	auto popped = queue.value().try_pop();
	ASSERT_TRUE(popped) << popped.error().context;
	EXPECT_EQ(popped.value().sequence, initial.sequence);
	EXPECT_EQ(popped.value().producer, initial.producer);

	auto terminal = queue.value().try_push(replacement);
	ASSERT_FALSE(terminal);
	EXPECT_EQ(terminal.error().code, ErrorCode::kSequenceExhausted);
	auto waited = queue.value().wait_push_until(
		replacement, std::chrono::steady_clock::now() + std::chrono::milliseconds(80));
	ASSERT_FALSE(waited);
	EXPECT_EQ(waited.error().code, ErrorCode::kSequenceExhausted);

	uint64_t final_enqueue = 0;
	uint64_t final_dequeue = 0;
	MpmcTestPayload stored{};
	ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte* mapping) {
		final_enqueue = edge_runtime::detail::shared_load_seq_cst(&header->enqueue_position);
		final_dequeue = edge_runtime::detail::shared_load_seq_cst(&header->dequeue_position);
		uint64_t offset = 0;
		ASSERT_TRUE(edge_runtime::detail::mpmc_slot_offset(0, 1, 128, &offset));
		auto* slot = reinterpret_cast<edge_runtime::detail::MpmcSlotHeaderAbi*>(mapping + offset);
		std::memcpy(&stored, mapping + offset + sizeof(*slot), sizeof(stored));
		EXPECT_EQ(edge_runtime::detail::shared_load_seq_cst(&slot->sequence), kMax);
		EXPECT_EQ(edge_runtime::detail::shared_load_seq_cst(&slot->state),
		          static_cast<uint32_t>(edge_runtime::detail::MpmcSlotState::kFree));
	}));
	EXPECT_EQ(final_enqueue, kMax);
	EXPECT_EQ(final_dequeue, kMax);
	EXPECT_EQ(stored.sequence, initial.sequence);
	EXPECT_EQ(stored.producer, initial.producer);
	EXPECT_EQ(stored.magic, initial.magic);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, PositionSequenceExhaustionCapacityFourAllowsLastDrain) {
	constexpr uint64_t kMax = UINT64_MAX;
	constexpr uint64_t kLastTicket = kMax - 4u;
	const std::string name = edge_test::unique_channel_name("mpmc_sequence_cap4");
	const auto options = options_for(name, 4);
	auto queue = Queue::create(options, MpmcTestSchema());
	ASSERT_TRUE(queue);

	ASSERT_TRUE(with_queue_image(name, 4, [&](auto* header, std::byte* mapping) {
		set_queue_positions(header, kLastTicket, kLastTicket);
		const MpmcTestPayload initial{101, 6, 0x4D504D43u};
		EXPECT_TRUE(set_slot_image(mapping, 4, kLastTicket,
		                           edge_runtime::detail::MpmcSlotState::kFree, kLastTicket,
		                           &initial));
	}));

	const MpmcTestPayload initial{101, 6, 0x4D504D43u};
	ASSERT_TRUE(queue.value().try_push(initial));
	auto popped = queue.value().try_pop();
	ASSERT_TRUE(popped) << popped.error().context;
	EXPECT_EQ(popped.value().sequence, initial.sequence);
	EXPECT_EQ(popped.value().producer, initial.producer);

	const MpmcTestPayload replacement{102, 7, 0x4D504D43u};
	for (int attempt = 0; attempt < 2; ++attempt) {
		auto rejected = queue.value().try_push(replacement);
		ASSERT_FALSE(rejected);
		EXPECT_EQ(rejected.error().code, ErrorCode::kSequenceExhausted);
	}
	auto waited_pop = queue.value().wait_pop_until(
		std::chrono::steady_clock::now() + std::chrono::milliseconds(80));
	ASSERT_FALSE(waited_pop);
	EXPECT_EQ(waited_pop.error().code, ErrorCode::kSequenceExhausted);

	uint64_t enqueue = 0;
	uint64_t dequeue = 0;
	MpmcTestPayload stored{};
	ASSERT_TRUE(with_queue_image(name, 4, [&](auto* header, std::byte* mapping) {
		enqueue = edge_runtime::detail::shared_load_seq_cst(&header->enqueue_position);
		dequeue = edge_runtime::detail::shared_load_seq_cst(&header->dequeue_position);
		uint64_t offset = 0;
		ASSERT_TRUE(edge_runtime::detail::mpmc_slot_offset(
				static_cast<uint32_t>(kLastTicket % 4u), 4, 128, &offset));
		auto* slot = reinterpret_cast<edge_runtime::detail::MpmcSlotHeaderAbi*>(mapping + offset);
		std::memcpy(&stored, mapping + offset + sizeof(*slot), sizeof(stored));
		EXPECT_EQ(edge_runtime::detail::shared_load_seq_cst(&slot->sequence), kMax);
		EXPECT_EQ(edge_runtime::detail::shared_load_seq_cst(&slot->state),
		          static_cast<uint32_t>(edge_runtime::detail::MpmcSlotState::kFree));
	}));
	EXPECT_EQ(enqueue, kLastTicket + 1u);
	EXPECT_EQ(dequeue, kLastTicket + 1u);
	EXPECT_EQ(stored.sequence, initial.sequence);
	EXPECT_EQ(stored.producer, initial.producer);
	EXPECT_EQ(stored.magic, initial.magic);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, LastBoundaryMessageDrainsThroughIndependentExecConsumer) {
	constexpr uint64_t kMax = UINT64_MAX;
	constexpr uint64_t kLastTicket = kMax - 1u;
	const std::string name = edge_test::unique_channel_name("mpmc_sequence_exec");
	auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
	ASSERT_TRUE(queue);
	ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte* mapping) {
		set_queue_positions(header, kLastTicket, kLastTicket);
		EXPECT_TRUE(set_slot_image(mapping, 1, kLastTicket,
		                           edge_runtime::detail::MpmcSlotState::kFree, kLastTicket));
	}));

	ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{303, 8, 0x4D504D43u}));
	edge_test::SpawnedChild consumer;
	ASSERT_TRUE(consumer.spawn(child_args("consumer", name, 1, 1)));
	std::string output;
	ASSERT_TRUE(consumer.wait(10000, &output)) << output;
	EXPECT_EQ(consumer.exit_code(), 0) << output;
	EXPECT_TRUE(contains(output, "ITEM producer=8 sequence=303")) << output;

	auto terminal = queue.value().wait_push_until(
		MpmcTestPayload{304, 9, 0x4D504D43u},
		std::chrono::steady_clock::now() + std::chrono::milliseconds(80));
	ASSERT_FALSE(terminal);
	EXPECT_EQ(terminal.error().code, ErrorCode::kSequenceExhausted);
	ASSERT_TRUE(queue.value().remove_if_creator());
}

TEST(MpmcQueue, StableEmptyAndFullDoNotConsumeGateEpoch) {
	constexpr uint64_t kMaxCommitted = UINT64_MAX - 1u;

	{
		const std::string name = edge_test::unique_channel_name("mpmc_empty_gate_boundary");
		auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
		ASSERT_TRUE(queue);
		ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte*) {
			set_queue_positions(header, 0, 0, 0, kMaxCommitted);
		}));
		auto empty = queue.value().try_pop();
		ASSERT_FALSE(empty);
		EXPECT_EQ(empty.error().code, ErrorCode::kQueueEmpty);
		uint64_t dequeue_epoch = 0;
		ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte*) {
			dequeue_epoch = edge_runtime::detail::shared_load_seq_cst(
					&header->dequeue_owner_epoch);
		}));
		EXPECT_EQ(dequeue_epoch, kMaxCommitted);
		ASSERT_TRUE(queue.value().remove_if_creator());
	}

	{
		const std::string name = edge_test::unique_channel_name("mpmc_full_gate_boundary");
		auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
		ASSERT_TRUE(queue);
		ASSERT_TRUE(queue.value().try_push(MpmcTestPayload{401, 10, 0x4D504D43u}));
		ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte*) {
			edge_runtime::detail::shared_store_seq_cst(&header->enqueue_owner_epoch,
			                                               kMaxCommitted);
		}));
		auto full = queue.value().try_push(MpmcTestPayload{402, 10, 0x4D504D43u});
		ASSERT_FALSE(full);
		EXPECT_EQ(full.error().code, ErrorCode::kQueueFull);
		uint64_t enqueue_epoch = 0;
		ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte*) {
			enqueue_epoch = edge_runtime::detail::shared_load_seq_cst(
					&header->enqueue_owner_epoch);
		}));
		EXPECT_EQ(enqueue_epoch, kMaxCommitted);
		ASSERT_TRUE(queue.value().try_pop());
		ASSERT_TRUE(queue.value().remove_if_creator());
	}

	{
		const std::string name = edge_test::unique_channel_name("mpmc_gate_last_success");
		auto queue = Queue::create(options_for(name, 1), MpmcTestSchema());
		ASSERT_TRUE(queue);
		ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte*) {
			set_queue_positions(header, 0, 0, kMaxCommitted - 2u, kMaxCommitted - 2u);
		}));
		const MpmcTestPayload value{501, 11, 0x4D504D43u};
		ASSERT_TRUE(queue.value().try_push(value));
		auto popped = queue.value().try_pop();
		ASSERT_TRUE(popped) << popped.error().context;
		EXPECT_EQ(popped.value().sequence, value.sequence);
		EXPECT_EQ(popped.value().producer, value.producer);
		uint64_t enqueue_epoch = 0;
		uint64_t dequeue_epoch = 0;
		ASSERT_TRUE(with_queue_image(name, 1, [&](auto* header, std::byte*) {
			enqueue_epoch = edge_runtime::detail::shared_load_seq_cst(
					&header->enqueue_owner_epoch);
			dequeue_epoch = edge_runtime::detail::shared_load_seq_cst(
					&header->dequeue_owner_epoch);
		}));
		EXPECT_EQ(enqueue_epoch, kMaxCommitted);
		EXPECT_EQ(dequeue_epoch, kMaxCommitted);
		ASSERT_TRUE(queue.value().remove_if_creator());
	}
}

}  // namespace

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	if (argc != 2) {
		std::fprintf(stderr, "usage: mpmc_queue_test <mpmc_child_binary>\n");
		return 2;
	}
	g_helper = argv[1];
	return RUN_ALL_TESTS();
}
