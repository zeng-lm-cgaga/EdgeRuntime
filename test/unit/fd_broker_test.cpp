// v0.2 §33 fd-broker wire records: frozen 64-byte layouts, magic/version
// handling, and checksum discipline (pure functions — no socket involved here;
// the socket behavior is covered by the fd_transport integration driver and
// the crash matrix C14-C17).

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>

#include "edge_runtime/detail/fd_broker.hpp"

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

}  // namespace
