

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <string>

#include "edge_runtime/channel/channel_options.hpp"
#include "edge_runtime/channel/channel_abi.hpp"
#include "edge_runtime/channel/control_lock.hpp"
#include "edge_runtime/process/process_identity.hpp"

namespace {

using edge_runtime::detail::channel_lock_path;
using edge_runtime::detail::ControlJournalV1;
using edge_runtime::detail::ControlLock;
using edge_runtime::detail::current_process_identity;
using edge_runtime::detail::journal_checksum;
using edge_runtime::detail::JournalState;
using edge_runtime::detail::make_control_journal;

TEST(ControlJournal, ChecksumDeterministic) {
	edge_runtime::detail::ProcessIdentity creator;
	auto j1 = make_control_journal("abc", JournalState::kCreatingObject, 1, 2, 3, 4, 5, 6,
	                               creator);
	const uint64_t c1 = journal_checksum(j1);

	EXPECT_NE(c1, 0u);

	auto j2 = make_control_journal("abc", JournalState::kCreatingObject, 1, 2, 3, 4, 5, 6,
	                               creator);
	EXPECT_EQ(journal_checksum(j2), c1);

	j2.channel_hash ^= 1u;
	EXPECT_NE(journal_checksum(j2), c1);

	j2 = j1;
	j2.record_checksum = 0xDEADBEEFu;
	EXPECT_EQ(journal_checksum(j2), c1);
}

TEST(ControlJournal, NameHashBinding) {
	EXPECT_NE(edge_runtime::detail::channel_name_hash("alpha", 5),
	          edge_runtime::detail::channel_name_hash("beta", 4));
	EXPECT_EQ(edge_runtime::detail::channel_name_hash("alpha", 5),
	          edge_runtime::detail::channel_name_hash("alpha", 5));
}

TEST(ControlJournal, LockRoundTrip) {
	const std::string name = "ctrl_" + std::to_string(getpid());
	const std::string path = channel_lock_path(name);

	{
		auto lock_res = ControlLock::acquire(path);
		ASSERT_TRUE(lock_res) << edge_runtime::to_string(lock_res.error().code);
		ControlLock lock = std::move(lock_res.value());

		auto idle = lock.read_journal();
		ASSERT_TRUE(idle);
		EXPECT_EQ(idle.value().state, static_cast<uint32_t>(JournalState::kIdle));
		EXPECT_EQ(idle.value().channel_hash, 0u);

		const auto creator = current_process_identity();
		auto rec = make_control_journal(name, JournalState::kCreatingObject, 1, 2, 3, 4, 5,
		                                6, creator);
		rec.target_dev = 99;
		rec.target_ino = 42;
		auto wr = lock.write_journal(rec);
		ASSERT_TRUE(wr);
	}

	{
		auto lock_res = ControlLock::acquire(path);
		ASSERT_TRUE(lock_res);
		ControlLock lock = std::move(lock_res.value());
		auto rd = lock.read_journal();
		ASSERT_TRUE(rd);
		const ControlJournalV1& rec = rd.value();
		EXPECT_EQ(rec.state, static_cast<uint32_t>(JournalState::kCreatingObject));
		EXPECT_EQ(rec.target_ino, 42u);
		EXPECT_EQ(rec.target_dev, 99u);
		EXPECT_EQ(rec.new_generation, 2u);
		EXPECT_EQ(rec.creator.pid, static_cast<uint64_t>(getpid()));

		EXPECT_EQ(journal_checksum(rec), rec.record_checksum);

		EXPECT_EQ(rec.transport, static_cast<uint32_t>(edge_runtime::Transport::kPosixShm));
		EXPECT_EQ(offsetof(ControlJournalV1, transport), 200u);

		edge_runtime::detail::ProcessIdentity creator_id;
		creator_id.pid = rec.creator.pid;
		creator_id.proc_start_ticks = rec.creator.proc_start_ticks;
		creator_id.boot_id_hash_hi = rec.creator.boot_id_hash_hi;
		creator_id.boot_id_hash_lo = rec.creator.boot_id_hash_lo;
		auto back = make_control_journal(name, JournalState::kIdle, 2, 2, 5, 6, 5, 6,
		                                 creator_id);
		ASSERT_TRUE(lock.write_journal(back));
	}
}

TEST(ControlJournal, TransportFieldRoundTrip) {
	const std::string name = "ctrl_tr_" + std::to_string(getpid());
	const std::string path = channel_lock_path(name);

	{
		auto lock_res = ControlLock::acquire(path);
		ASSERT_TRUE(lock_res);
		ControlLock lock = std::move(lock_res.value());
		const auto creator = current_process_identity();
		auto rec = make_control_journal(name, JournalState::kIdle, 0, 1, 0, 0, 7, 8, creator);
		rec.transport = static_cast<uint32_t>(edge_runtime::Transport::kMemfdFdPass);
		ASSERT_TRUE(lock.write_journal(rec));
	}

	{
		auto lock_res = ControlLock::acquire(path);
		ASSERT_TRUE(lock_res);
		ControlLock lock = std::move(lock_res.value());
		auto rd = lock.read_journal();
		ASSERT_TRUE(rd);
		EXPECT_EQ(rd.value().transport,
		          static_cast<uint32_t>(edge_runtime::Transport::kMemfdFdPass));
		EXPECT_EQ(rd.value().new_generation, 1u);

		auto tampered = rd.value();
		tampered.transport = 0;
		EXPECT_NE(journal_checksum(tampered), tampered.record_checksum);
	}
}

TEST(ControlJournal, CorruptRecordRejected) {
	const std::string name = "ctrl_corrupt_" + std::to_string(getpid());
	const std::string path = channel_lock_path(name);

	{
		auto lock_res = ControlLock::acquire(path);
		ASSERT_TRUE(lock_res);
		ControlLock lock = std::move(lock_res.value());
		const auto creator = current_process_identity();
		auto rec = make_control_journal(name, JournalState::kCreatingObject, 1, 2, 3, 4, 5,
		                                6, creator);
		auto wr = lock.write_journal(rec);
		ASSERT_TRUE(wr);
	}

	{
		const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
		ASSERT_GE(fd, 0);
		uint64_t byte = 0;
		ASSERT_EQ(::pread(fd, &byte, sizeof(byte), 80), static_cast<ssize_t>(sizeof(byte)));
		byte ^= 1u;
		ASSERT_EQ(::pwrite(fd, &byte, sizeof(byte), 80),
		          static_cast<ssize_t>(sizeof(byte)));
		::close(fd);
	}

	{
		auto lock_res = ControlLock::acquire(path);
		ASSERT_TRUE(lock_res);
		ControlLock lock = std::move(lock_res.value());
		auto rd = lock.read_journal();
		ASSERT_FALSE(rd);
		EXPECT_EQ(rd.error().code, edge_runtime::ErrorCode::kRecoveryBlocked);
	}
}

}
