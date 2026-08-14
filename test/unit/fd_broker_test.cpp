// v0.2 §33 fd-broker wire records: frozen 64-byte layouts, magic/version
// handling, and checksum discipline (pure functions — no socket involved here;
// the socket behavior is covered by the fd_transport integration driver and
// the crash matrix C14-C17).

#include <gtest/gtest.h>

#include <dirent.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <optional>
#include <thread>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/control_lock.hpp"
#include "edge_runtime/detail/fd_broker.hpp"
#include "edge_runtime/producer.hpp"
#include "test_payload.hpp"
#include "test_util.hpp"

namespace {

using edge_runtime::detail::FdBrokerReplyAbi;
using edge_runtime::detail::FdBrokerRequestAbi;
using edge_runtime::detail::fd_broker_checksum;
using edge_runtime::detail::kFdBrokerFlagReadonly;
using edge_runtime::detail::kFdBrokerReplyMagic;
using edge_runtime::detail::kFdBrokerReplySize;
using edge_runtime::detail::kFdBrokerRequestMagic;
using edge_runtime::detail::kFdBrokerRequestSize;
using edge_runtime::detail::kFdBrokerVersion;

size_t open_fd_count() {
	DIR* dir = ::opendir("/proc/self/fd");
	if (dir == nullptr) return 0;
	size_t count = 0;
	while (const dirent* entry = ::readdir(dir)) {
		if (entry->d_name[0] != '.') ++count;
	}
	::closedir(dir);
	return count;
}

bool send_test_reply_with_fd(int socket_fd, const FdBrokerReplyAbi& reply, int payload_fd) {
	char control[CMSG_SPACE(sizeof(int))]{};
	struct iovec iov {};
	iov.iov_base = const_cast<FdBrokerReplyAbi*>(&reply);
	iov.iov_len = sizeof(reply);
	struct msghdr msg {};
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;
	msg.msg_control = control;
	msg.msg_controllen = sizeof(control);
	auto* cmsg = CMSG_FIRSTHDR(&msg);
	cmsg->cmsg_level = SOL_SOCKET;
	cmsg->cmsg_type = SCM_RIGHTS;
	cmsg->cmsg_len = CMSG_LEN(sizeof(payload_fd));
	std::memcpy(CMSG_DATA(cmsg), &payload_fd, sizeof(payload_fd));
	msg.msg_controllen = cmsg->cmsg_len;
	return ::sendmsg(socket_fd, &msg, MSG_NOSIGNAL) ==
	       static_cast<ssize_t>(sizeof(reply));
}

TEST(FdBroker, FrozenRequestLayout) {
	EXPECT_EQ(kFdBrokerRequestSize, 64u);
	EXPECT_EQ(sizeof(FdBrokerRequestAbi), kFdBrokerRequestSize);
	EXPECT_EQ(offsetof(FdBrokerRequestAbi, version), 8u);
	EXPECT_EQ(offsetof(FdBrokerRequestAbi, flags), 12u);
	EXPECT_EQ(offsetof(FdBrokerRequestAbi, channel_hash), 16u);
	EXPECT_EQ(offsetof(FdBrokerRequestAbi, schema_fingerprint), 24u);
	EXPECT_EQ(offsetof(FdBrokerRequestAbi, checksum), 56u);
	EXPECT_EQ(kFdBrokerFlagReadonly, 1u);
}

TEST(FdBroker, FrozenReplyLayout) {
	EXPECT_EQ(kFdBrokerReplySize, 64u);
	EXPECT_EQ(sizeof(FdBrokerReplyAbi), kFdBrokerReplySize);
	EXPECT_EQ(offsetof(FdBrokerReplyAbi, status), 8u);
	EXPECT_EQ(offsetof(FdBrokerReplyAbi, mapping_size), 16u);
	EXPECT_EQ(offsetof(FdBrokerReplyAbi, generation), 24u);
	EXPECT_EQ(offsetof(FdBrokerReplyAbi, nonce_hi), 32u);
	EXPECT_EQ(offsetof(FdBrokerReplyAbi, nonce_lo), 40u);
	EXPECT_EQ(offsetof(FdBrokerReplyAbi, abi_major), 48u);
	EXPECT_EQ(offsetof(FdBrokerReplyAbi, abi_minor), 50u);
	EXPECT_EQ(offsetof(FdBrokerReplyAbi, checksum), 56u);
}

TEST(FdBroker, ChecksumCoversContent) {
	FdBrokerReplyAbi r{};
	std::memcpy(r.magic, kFdBrokerReplyMagic, sizeof(kFdBrokerReplyMagic) - 1);
	r.status = 0;
	r.mapping_size = 832;
	r.generation = 3;
	r.nonce_hi = 0x11111111;
	r.abi_major = 1;
	const uint64_t c1 = fd_broker_checksum(&r, sizeof(r));
	EXPECT_NE(c1, 0u);
	// any content change changes the checksum
	FdBrokerReplyAbi r2 = r;
	r2.generation = 4;
	EXPECT_NE(fd_broker_checksum(&r2, sizeof(r2)), c1);
	// the checksum field itself is excluded (zeroed before hashing)
	FdBrokerReplyAbi r3 = r;
	r3.checksum = c1;
	EXPECT_EQ(fd_broker_checksum(&r3, sizeof(r3)), c1);
}

TEST(FdBroker, RequestRoundTripShape) {
	FdBrokerRequestAbi q{};
	std::memcpy(q.magic, kFdBrokerRequestMagic, sizeof(kFdBrokerRequestMagic) - 1);
	q.version = kFdBrokerVersion;
	q.flags = kFdBrokerFlagReadonly;
	q.channel_hash = 0xDEADBEEFu;
	for (int i = 0; i < 32; ++i) q.schema_fingerprint[i] = static_cast<uint8_t>(i);
	const uint64_t c = fd_broker_checksum(&q, sizeof(q));
	q.checksum = c;
	EXPECT_EQ(fd_broker_checksum(&q, sizeof(q)), c);
	// bit flip in the fingerprint must break the checksum
	q.schema_fingerprint[0] ^= 1u;
	EXPECT_NE(fd_broker_checksum(&q, sizeof(q)), c);
}

TEST(FdBroker, ChecksumRejectsBadInput) {
	EXPECT_EQ(fd_broker_checksum(nullptr, 64), 0u);
	EXPECT_EQ(fd_broker_checksum(nullptr, 4), 0u);
	uint8_t overlong[128]{};
	EXPECT_EQ(fd_broker_checksum(overlong, sizeof(overlong)), 0u);
}

TEST(FdBroker, SuccessfulRequestsDoNotLeakSenderFds) {
	const std::string name = edge_test::unique_channel_name("fd_no_leak");
	edge_runtime::ChannelOptions options;
	options.name = name;
	options.transport = edge_runtime::Transport::kMemfdFdPass;
	const auto schema = TestPayloadV1Schema();
	auto producer = edge_runtime::Producer<TestPayloadV1>::create(options, schema);
	ASSERT_TRUE(producer) << edge_runtime::to_string(producer.error().code);

	const std::string socket_path = edge_runtime::detail::channel_socket_path(name);
	const uint32_t hash = edge_runtime::detail::channel_name_hash(name.c_str(), name.size());
	const size_t before = open_fd_count();
	ASSERT_GT(before, 0u);
	for (int i = 0; i < 32; ++i) {
		FdBrokerReplyAbi reply{};
		auto granted = edge_runtime::detail::fd_broker_request_fd(
		        socket_path, hash, schema, false, &reply, 1000);
		ASSERT_TRUE(granted) << edge_runtime::to_string(granted.error().code);
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	EXPECT_EQ(open_fd_count(), before);
	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST(FdBroker, SocketPathIs0600EvenWithPermissiveUmask) {
	const mode_t previous_umask = ::umask(0);
	const std::string name = edge_test::unique_channel_name("fd_socket_mode");
	edge_runtime::ChannelOptions options;
	options.name = name;
	options.transport = edge_runtime::Transport::kMemfdFdPass;
	auto producer = edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	::umask(previous_umask);
	ASSERT_TRUE(producer) << edge_runtime::to_string(producer.error().code);

	struct stat st {};
	const std::string path = edge_runtime::detail::channel_socket_path(name);
	ASSERT_EQ(::stat(path.c_str(), &st), 0);
	EXPECT_TRUE(S_ISSOCK(st.st_mode));
	EXPECT_EQ(st.st_mode & 0777, static_cast<mode_t>(0600));
	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST(FdBroker, ReadonlyRequestReturnsReadonlyFd) {
	const std::string name = edge_test::unique_channel_name("fd_readonly");
	edge_runtime::ChannelOptions options;
	options.name = name;
	options.transport = edge_runtime::Transport::kMemfdFdPass;
	const auto schema = TestPayloadV1Schema();
	auto producer = edge_runtime::Producer<TestPayloadV1>::create(options, schema);
	ASSERT_TRUE(producer) << edge_runtime::to_string(producer.error().code);

	FdBrokerReplyAbi reply{};
	auto granted = edge_runtime::detail::fd_broker_request_fd(
	        edge_runtime::detail::channel_socket_path(name),
	        edge_runtime::detail::channel_name_hash(name.c_str(), name.size()), schema, true,
	        &reply, 1000);
	ASSERT_TRUE(granted) << edge_runtime::to_string(granted.error().code);
	const int flags = ::fcntl(granted.value().get(), F_GETFL);
	ASSERT_GE(flags, 0);
	EXPECT_EQ(flags & O_ACCMODE, O_RDONLY);
	EXPECT_TRUE(producer.value().remove_if_owner());
}

TEST(FdBroker, RetryTimeoutOverflowRejectedBeforeConnect) {
	FdBrokerReplyAbi reply{};
	auto granted = edge_runtime::detail::fd_broker_request_fd(
	        "/not/used", 0, TestPayloadV1Schema(), true, &reply, UINT64_MAX);
	ASSERT_FALSE(granted);
	EXPECT_EQ(granted.error().code, edge_runtime::ErrorCode::kInvalidOptions);
}

TEST(FdBroker, MalformedReplyDoesNotLeakReceivedFd) {
	const size_t before = open_fd_count();
	const std::string name = edge_test::unique_channel_name("fd_bad_reply");
	const std::string socket_path = edge_runtime::detail::channel_socket_path(name);
	bool served = false;
	{
		auto lock = edge_runtime::detail::ControlLock::acquire(
		        edge_runtime::detail::channel_lock_path(name));
		ASSERT_TRUE(lock);
		const int listen_raw = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
		ASSERT_GE(listen_raw, 0);
		edge_runtime::detail::UniqueFd listener(listen_raw);
		struct sockaddr_un addr {};
		addr.sun_family = AF_UNIX;
		ASSERT_LT(socket_path.size(), sizeof(addr.sun_path));
		std::memcpy(addr.sun_path, socket_path.c_str(), socket_path.size() + 1);
		(void)::unlink(socket_path.c_str());
		ASSERT_EQ(::bind(listen_raw, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)), 0);
		ASSERT_EQ(::listen(listen_raw, 1), 0);

		const int payload_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
		ASSERT_GE(payload_fd, 0);
		std::thread server([owned_listener = std::move(listener), payload_fd, &served]() mutable {
			const int conn = ::accept4(owned_listener.get(), nullptr, nullptr, SOCK_CLOEXEC);
			if (conn >= 0) {
				FdBrokerRequestAbi request{};
				const ssize_t n = ::recv(conn, &request, sizeof(request), MSG_WAITALL);
				FdBrokerReplyAbi malformed{};  // bad magic/checksum, but a real fd attached
				served = n == static_cast<ssize_t>(sizeof(request)) &&
				         send_test_reply_with_fd(conn, malformed, payload_fd);
				::close(conn);
			}
			owned_listener.reset();
		});

		FdBrokerReplyAbi reply{};
		auto granted = edge_runtime::detail::fd_broker_request_fd(
		        socket_path,
		        edge_runtime::detail::channel_name_hash(name.c_str(), name.size()),
		        TestPayloadV1Schema(), true, &reply, 100);
		EXPECT_FALSE(granted);
		server.join();
		::close(payload_fd);
		(void)::unlink(socket_path.c_str());
	}
	EXPECT_TRUE(served);
	EXPECT_EQ(open_fd_count(), before);
}

TEST(FdBroker, HalfOpenClientDoesNotPinProducerShutdown) {
	const std::string name = edge_test::unique_channel_name("fd_half_open");
	edge_runtime::ChannelOptions options;
	options.name = name;
	options.transport = edge_runtime::Transport::kMemfdFdPass;
	auto created = edge_runtime::Producer<TestPayloadV1>::create(options, TestPayloadV1Schema());
	ASSERT_TRUE(created) << edge_runtime::to_string(created.error().code);

	const int client = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	ASSERT_GE(client, 0);
	struct sockaddr_un addr {};
	addr.sun_family = AF_UNIX;
	const std::string socket_path = edge_runtime::detail::channel_socket_path(name);
	ASSERT_LT(socket_path.size(), sizeof(addr.sun_path));
	std::memcpy(addr.sun_path, socket_path.c_str(), socket_path.size() + 1);
	ASSERT_EQ(::connect(client, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)), 0);
	const uint8_t partial = 0x45;
	ASSERT_EQ(::send(client, &partial, sizeof(partial), MSG_NOSIGNAL),
	          static_cast<ssize_t>(sizeof(partial)));
	std::this_thread::sleep_for(std::chrono::milliseconds(250));

	std::optional<edge_runtime::Producer<TestPayloadV1>> producer(
	        std::move(created.value()));
	std::promise<void> stopped;
	auto stopped_future = stopped.get_future();
	std::thread shutdown_thread([owned = std::move(producer), &stopped]() mutable {
		owned.reset();
		stopped.set_value();
	});

	const auto status = stopped_future.wait_for(std::chrono::seconds(2));
	if (status != std::future_status::ready) {
		::close(client);  // unblock an old implementation so the test can fail cleanly
	}
	shutdown_thread.join();
	if (status == std::future_status::ready) ::close(client);
	EXPECT_EQ(status, std::future_status::ready);
}

}  // namespace
