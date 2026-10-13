#include "broker_observer_syscall_fixture.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>

namespace {

const char* interrupted_operation = "none";
std::atomic<uint32_t> interruption_count{0};

bool interrupt(const char* operation) noexcept {
	if (std::strcmp(interrupted_operation, operation) != 0) return false;
	interruption_count.fetch_add(1, std::memory_order_relaxed);
	::usleep(1'000);
	errno = EINTR;
	return true;
}

}

namespace edge_test::broker_observer {

void interrupt_operation(const char* operation) noexcept { interrupted_operation = operation; }

uint32_t syscall_interruption_count() noexcept {
	return interruption_count.load(std::memory_order_relaxed);
}

}

extern "C" {

int __real_connect(int socket, const struct sockaddr* address, socklen_t length);
ssize_t __real_send(int socket, const void* data, size_t size, int flags);
ssize_t __real_recvmsg(int socket, struct msghdr* message, int flags);
ssize_t __real_sendmsg(int socket, const struct msghdr* message, int flags);

int __wrap_connect(int socket, const struct sockaddr* address, socklen_t length) {
	return interrupt("connect") ? -1 : __real_connect(socket, address, length);
}

ssize_t __wrap_send(int socket, const void* data, size_t size, int flags) {
	return interrupt("send") ? -1 : __real_send(socket, data, size, flags);
}

ssize_t __wrap_recvmsg(int socket, struct msghdr* message, int flags) {
	return interrupt("recvmsg") ? -1 : __real_recvmsg(socket, message, flags);
}

ssize_t __wrap_sendmsg(int socket, const struct msghdr* message, int flags) {
	return interrupt("sendmsg") ? -1 : __real_sendmsg(socket, message, flags);
}
}
