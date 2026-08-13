#include "edge_runtime/detail/fd_broker.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>

#include <cerrno>
#include <cstdio>
#include <cstring>

#include "edge_runtime/detail/channel_layout.hpp"
#include "edge_runtime/detail/checksum.hpp"
#include "edge_runtime/detail/clock.hpp"
#include "edge_runtime/detail/failpoint.hpp"
#include "edge_runtime/detail/shared_atomic.hpp"

#if defined(__linux__)
#define EDGE_HAVE_MEMFD_CREATE 1
#include <linux/memfd.h>
#include <sys/syscall.h>
#endif

namespace edge_runtime::detail {

namespace {

inline constexpr uint32_t kListenBacklog = 4;
inline constexpr uint64_t kConnectRetryIntervalNs = 10'000'000;  // 10 ms
inline constexpr uint64_t kServePollIntervalNs = 100'000'000;    // 100 ms

uint64_t reply_checksum_of(const FdBrokerReplyAbi& r) noexcept {
	FdBrokerReplyAbi tmp = r;
	tmp.checksum = 0;
	return fnv1a64(reinterpret_cast<const std::byte*>(&tmp), sizeof(tmp));
}

uint64_t request_checksum_of(const FdBrokerRequestAbi& r) noexcept {
	FdBrokerRequestAbi tmp = r;
	tmp.checksum = 0;
	return fnv1a64(reinterpret_cast<const std::byte*>(&tmp), sizeof(tmp));
}

bool request_fingerprint_set(const FdBrokerRequestAbi& r) noexcept {
	for (const uint8_t b : r.schema_fingerprint) {
		if (b != 0) return true;
	}
	return false;
}

// Full recv of exactly `size` bytes (EINTR-safe). Short read / error -> false.
bool recv_full(int fd, void* buf, size_t size) noexcept {
	auto* dst = static_cast<char*>(buf);
	size_t got = 0;
	while (got < size) {
		const ssize_t n = ::recv(fd, dst + got, size - got, 0);
		if (n < 0) {
			if (errno == EINTR) continue;
			return false;
		}
		if (n == 0) return false;  // peer closed mid-record
		got += static_cast<size_t>(n);
	}
	return true;
}

bool send_full(int fd, const void* buf, size_t size) noexcept {
	const auto* src = static_cast<const char*>(buf);
	size_t sent = 0;
	while (sent < size) {
		const ssize_t n = ::send(fd, src + sent, size - sent, MSG_NOSIGNAL);
		if (n < 0) {
			if (errno == EINTR) continue;
			return false;
		}
		sent += static_cast<size_t>(n);
	}
	return true;
}

// Best-effort SO_PEERCRED check: same-UID trust model only (design §33.4).
bool peer_is_same_uid(int fd) noexcept {
	struct ucred cred {};
	socklen_t len = sizeof(cred);
	if (::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) != 0) return false;
	return cred.uid == geteuid();
}

// Best-effort peer liveness probe for a stale socket (design §33.5): connect()
// succeeds only if something is accepting.
bool socket_has_live_peer(const std::string& path) noexcept {
	const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (fd < 0) return false;
	UniqueFd probe(fd);
	struct sockaddr_un addr {};
	addr.sun_family = AF_UNIX;
	if (path.size() >= sizeof(addr.sun_path)) return false;
	std::memcpy(addr.sun_path, path.c_str(), path.size() + 1);
	const int rc = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr),
	                         static_cast<socklen_t>(sizeof(addr)));
	return rc == 0;
}

bool unlink_socket(const std::string& path) noexcept {
	return ::unlink(path.c_str()) == 0 || errno == ENOENT;
}

// Send the reply record plus one fd via SCM_RIGHTS. The whole exchange is
// one-shot: any failure abandons the connection (SCM_RIGHTS is never retried
// on the same connection, design §33.4).
bool send_reply_with_fd(int conn_fd, const FdBrokerReplyAbi& reply, int payload_fd) noexcept {
	char cmsg_buf[CMSG_SPACE(sizeof(int))]{};
	struct iovec iov {};
	iov.iov_base = const_cast<FdBrokerReplyAbi*>(&reply);
	iov.iov_len = sizeof(reply);
	struct msghdr msg {};
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;
	msg.msg_control = cmsg_buf;
	msg.msg_controllen = sizeof(cmsg_buf);
	auto* cmsg = CMSG_FIRSTHDR(&msg);
	cmsg->cmsg_level = SOL_SOCKET;
	cmsg->cmsg_type = SCM_RIGHTS;
	cmsg->cmsg_len = CMSG_LEN(sizeof(int));
	std::memcpy(CMSG_DATA(cmsg), &payload_fd, sizeof(int));
	msg.msg_controllen = cmsg->cmsg_len;
	const ssize_t n = ::sendmsg(conn_fd, &msg, MSG_NOSIGNAL);
	return n == static_cast<ssize_t>(sizeof(reply));
}

// Dup for a read-write hand-out, or reopen read-only through /proc/self/fd for
// a readonly request (dup keeps O_RDWR, design §33.4).
int hand_out_fd(int shm_fd, bool readonly) noexcept {
	if (!readonly) return ::fcntl(shm_fd, F_DUPFD_CLOEXEC, 0);
	char path[64]{};
	std::snprintf(path, sizeof(path), "/proc/self/fd/%d", shm_fd);
	const int ro = ::open(path, O_RDONLY | O_CLOEXEC);
	if (ro >= 0) return ro;
	return ::fcntl(shm_fd, F_DUPFD_CLOEXEC, 0);  // documented fallback
}

}  // namespace

uint64_t fd_broker_checksum(const void* record, size_t size) noexcept {
	// Records are fixed 64 bytes; copy to a local buffer so the trailing
	// checksum field can be zeroed without touching the caller's record.
	if (record == nullptr || size < sizeof(uint64_t) || size > kFdBrokerReplySize) return 0;
	std::byte tmp[kFdBrokerReplySize]{};
	std::memcpy(tmp, record, size);
	uint64_t zero = 0;
	std::memcpy(tmp + size - sizeof(uint64_t), &zero, sizeof(uint64_t));
	return fnv1a64(tmp, size);
}

Result<UniqueFd> memfd_create_object(const std::string& channel_name) {
#ifdef EDGE_HAVE_MEMFD_CREATE
	const std::string tag = "edgeruntime." + channel_name;
	const int fd = static_cast<int>(
	        ::syscall(SYS_memfd_create, tag.c_str(), static_cast<unsigned long>(MFD_CLOEXEC)));
	if (fd < 0) {
		return make_error(classify_errno(errno), "memfd_create_object", std::strerror(errno));
	}
	UniqueFd owned(fd);
	// memfd arrives with umask-derived mode (typically 0755); pin it to 0600 so
	// memfd_fstat_and_capture's group/other check and the §33.3 permission
	// discipline match the named-shm rules.
	if (::fchmod(fd, static_cast<mode_t>(S_IRUSR | S_IWUSR)) != 0) {
		return make_error(classify_errno(errno), "memfd_create_object",
		                  std::strerror(errno));
	}
	return Result<UniqueFd>(std::move(owned));
#else
	(void)channel_name;
	return make_error(ErrorCode::kUnsupportedPlatform, "memfd_create_object",
	                  "memfd_create unavailable");
#endif
}

Result<UniqueFd> fd_broker_bind(const std::string& socket_path) {
	const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
	if (fd < 0) {
		return make_error(classify_errno(errno), "fd_broker_bind", std::strerror(errno));
	}
	UniqueFd listen_fd(fd);
	struct sockaddr_un addr {};
	addr.sun_family = AF_UNIX;
	if (socket_path.size() >= sizeof(addr.sun_path)) {
		return make_error(ErrorCode::kInvalidName, "fd_broker_bind", "socket path too long");
	}
	std::memcpy(addr.sun_path, socket_path.c_str(), socket_path.size() + 1);

	int rc = ::bind(fd, reinterpret_cast<struct sockaddr*>(&addr),
	                static_cast<socklen_t>(sizeof(addr)));
	if (rc != 0 && errno == EADDRINUSE) {
		// EADDRINUSE: probe before unlink (design §33.5, mirror of §9.4). A live
		// peer owns the channel; a dead one left a stale socket we may remove.
		if (socket_has_live_peer(socket_path)) {
			return make_error(ErrorCode::kAlreadyOwned, "fd_broker_bind",
			                  "live broker socket under path");
		}
		if (!unlink_socket(socket_path)) {
			return make_error(classify_errno(errno), "fd_broker_bind", std::strerror(errno));
		}
		rc = ::bind(fd, reinterpret_cast<struct sockaddr*>(&addr),
		            static_cast<socklen_t>(sizeof(addr)));
	}
	if (rc != 0) {
		return make_error(classify_errno(errno), "fd_broker_bind", std::strerror(errno));
	}
	// Socket mode 0600 (design §33.4); bind leaves umask-derived permissions.
	if (::fchmod(fd, static_cast<mode_t>(S_IRUSR | S_IWUSR)) != 0) {
		return make_error(classify_errno(errno), "fd_broker_bind", std::strerror(errno));
	}
	if (::listen(fd, static_cast<int>(kListenBacklog)) != 0) {
		return make_error(classify_errno(errno), "fd_broker_bind", std::strerror(errno));
	}
	return Result<UniqueFd>(std::move(listen_fd));
}

void fd_broker_serve_loop(int listen_fd, int shm_fd, std::byte* base,
                          const uint32_t* channel_hash,
                          const std::array<std::byte, 32>* schema_fingerprint,
                          std::atomic<bool>* stop) noexcept {
	while (!stop->load(std::memory_order_relaxed)) {
		struct pollfd pfd {};
		pfd.fd = listen_fd;
		pfd.events = POLLIN;
		const int prc = ::poll(&pfd, 1, 250);
		if (prc < 0) {
			if (errno == EINTR) continue;
			break;  // EBADF (shutdown+close raced): leave
		}
		if (prc == 0) continue;  // idle tick; re-check stop
		const int conn = ::accept4(listen_fd, nullptr, nullptr, SOCK_CLOEXEC);
		if (conn < 0) {
			if (errno == EINTR || errno == EAGAIN) continue;
			break;  // EBADF after shutdown: leave
		}
		UniqueFd conn_fd(conn);

		if (!peer_is_same_uid(conn)) {
			FdBrokerReplyAbi reply{};
			std::memcpy(reply.magic, kFdBrokerReplyMagic, sizeof(kFdBrokerReplyMagic) - 1);
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kRefused);
			reply.checksum = reply_checksum_of(reply);
			(void)send_full(conn, &reply, sizeof(reply));
			continue;
		}

		FdBrokerRequestAbi req{};
		if (!recv_full(conn, &req, sizeof(req))) continue;
		if (std::memcmp(req.magic, kFdBrokerRequestMagic, sizeof(kFdBrokerRequestMagic) - 1) !=
		    0) {
			continue;  // garbage: drop, never answer an unparseable record
		}
		if (req.version != kFdBrokerVersion || request_checksum_of(req) != req.checksum) {
			continue;
		}

		auto* boot = reinterpret_cast<BootstrapHeaderAbi*>(base);
		auto* header = reinterpret_cast<ChannelHeaderAbi*>(base + kChannelHeaderOffset);

		FdBrokerReplyAbi reply{};
		std::memcpy(reply.magic, kFdBrokerReplyMagic, sizeof(kFdBrokerReplyMagic) - 1);
		reply.status = static_cast<uint32_t>(FdBrokerStatus::kOk);
		reply.mapping_size = boot->expected_mapping_size;
		reply.generation = header->generation;
		reply.nonce_hi = header->instance_nonce_hi;
		reply.nonce_lo = header->instance_nonce_lo;
		reply.abi_major = header->abi_major;
		reply.abi_minor = header->abi_minor;

		if (req.channel_hash != *channel_hash) {
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kChannelMismatch);
		} else if (request_fingerprint_set(req) &&
		           std::memcmp(req.schema_fingerprint, schema_fingerprint->data(), 32) != 0) {
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kChannelMismatch);
		} else if (shared_load_acquire(&boot->init_state) !=
		           static_cast<uint32_t>(InitState::kReady)) {
			reply.status = static_cast<uint32_t>(FdBrokerStatus::kNotReady);
		}

		if (reply.status == static_cast<uint32_t>(FdBrokerStatus::kOk)) {
			const bool readonly = (req.flags & kFdBrokerFlagReadonly) != 0;
			const int out_fd = hand_out_fd(shm_fd, readonly);
			if (out_fd < 0) {
				reply.status = static_cast<uint32_t>(FdBrokerStatus::kSystem);
			} else {
				UniqueFd out_guard(out_fd);
				reply.checksum = reply_checksum_of(reply);
				if (send_reply_with_fd(conn, reply, out_fd)) {
					out_guard.release();  // ownership moved into the socket
					EDGE_FAILPOINT(C14);  // crash matrix: served one request
					continue;
				}
				reply.status = static_cast<uint32_t>(FdBrokerStatus::kSystem);
				reply.checksum = reply_checksum_of(reply);
			}
		}
		reply.checksum = reply_checksum_of(reply);
		(void)send_full(conn, &reply, sizeof(reply));
	}
}

Result<UniqueFd> fd_broker_request_fd(const std::string& socket_path, uint32_t channel_hash,
                                      const SchemaDescriptor& schema, bool readonly,
                                      FdBrokerReplyAbi* reply_out, uint64_t retry_ms) noexcept {
	const uint64_t deadline = monotonic_deadline_ns(retry_ms * 1'000'000ull);
	if (deadline == 0) {
		return make_error(ErrorCode::kClockAnomaly, "fd_broker_request_fd",
		                  "monotonic clock unavailable");
	}

	for (;;) {
		const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
		if (fd < 0) {
			return make_error(ErrorCode::kTransportFailed, "fd_broker_request_fd",
			                  "socket create failed");
		}
		UniqueFd conn_fd(fd);
		struct sockaddr_un addr {};
		addr.sun_family = AF_UNIX;
		if (socket_path.size() >= sizeof(addr.sun_path)) {
			return make_error(ErrorCode::kInvalidName, "fd_broker_request_fd",
			                  "socket path too long");
		}
		std::memcpy(addr.sun_path, socket_path.c_str(), socket_path.size() + 1);
		if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr),
		              static_cast<socklen_t>(sizeof(addr))) != 0) {
			const int e = errno;
			if (remaining_time_ns(deadline) == 0) {
				return make_error(ErrorCode::kProducerOffline, "fd_broker_request_fd",
				                  "broker unreachable (retry budget exhausted)");
			}
			if (e == ECONNREFUSED || e == ENOENT || e == EAGAIN || e == EINTR) {
				const struct timespec ts {0, static_cast<long>(kConnectRetryIntervalNs)};
				(void)::nanosleep(&ts, nullptr);
				continue;
			}
			return make_error(ErrorCode::kTransportFailed, "fd_broker_request_fd",
			                  std::strerror(e));
		}

		FdBrokerRequestAbi req{};
		std::memcpy(req.magic, kFdBrokerRequestMagic, sizeof(kFdBrokerRequestMagic) - 1);
		req.version = kFdBrokerVersion;
		req.flags = readonly ? kFdBrokerFlagReadonly : 0;
		req.channel_hash = channel_hash;
		std::memcpy(req.schema_fingerprint, schema.fingerprint.data(), 32);
		req.checksum = request_checksum_of(req);
		if (!send_full(fd, &req, sizeof(req))) continue;  // fresh connection next round

		// One-shot receive: reply record + optional fd (design §33.4).
		char cmsg_buf[CMSG_SPACE(sizeof(int))]{};
		FdBrokerReplyAbi reply{};
		struct iovec iov {};
		iov.iov_base = &reply;
		iov.iov_len = sizeof(reply);
		struct msghdr msg {};
		msg.msg_iov = &iov;
		msg.msg_iovlen = 1;
		msg.msg_control = cmsg_buf;
		msg.msg_controllen = sizeof(cmsg_buf);
		const ssize_t n = ::recvmsg(fd, &msg, 0);
		if (n != static_cast<ssize_t>(sizeof(reply))) {
			continue;  // truncated/absent reply: abandon this connection
		}
		if (std::memcmp(reply.magic, kFdBrokerReplyMagic, sizeof(kFdBrokerReplyMagic) - 1) != 0 ||
		    reply_checksum_of(reply) != reply.checksum) {
			continue;  // unparseable wire record: never trust it
		}
		const auto status = static_cast<FdBrokerStatus>(reply.status);
		int received_fd = -1;
		for (auto* cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
			if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS &&
			    cmsg->cmsg_len >= CMSG_LEN(sizeof(int))) {
				std::memcpy(&received_fd, CMSG_DATA(cmsg), sizeof(int));
			}
		}
		if (status == FdBrokerStatus::kOk) {
			if (received_fd < 0) continue;  // ok status without fd: protocol violation
			// SCM_RIGHTS does not guarantee CLOEXEC on the received fd.
			const int flags = ::fcntl(received_fd, F_GETFD);
			if (flags < 0 || ::fcntl(received_fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
				::close(received_fd);
				return make_error(ErrorCode::kTransportFailed, "fd_broker_request_fd",
				                  "fcntl on received fd failed");
			}
			if (reply_out != nullptr) *reply_out = reply;
			return Result<UniqueFd>(UniqueFd(received_fd));
		}
		if (received_fd >= 0) ::close(received_fd);
		if (status == FdBrokerStatus::kChannelMismatch) {
			return make_error(ErrorCode::kSchemaMismatch, "fd_broker_request_fd",
			                  "broker rejected channel/schema");
		}
		if (status == FdBrokerStatus::kRefused) {
			return make_error(ErrorCode::kPermissionDenied, "fd_broker_request_fd",
			                  "broker refused peer");
		}
		// kNotReady / kSystem: retry within the remaining budget.
		if (remaining_time_ns(deadline) == 0) {
			return make_error(ErrorCode::kProducerOffline, "fd_broker_request_fd",
			                  "broker not ready (retry budget exhausted)");
		}
		const struct timespec ts {0, static_cast<long>(kConnectRetryIntervalNs)};
		(void)::nanosleep(&ts, nullptr);
	}
}

}  // namespace edge_runtime::detail
