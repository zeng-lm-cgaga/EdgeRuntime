// edge_shm_bench_sock: the Unix domain socket baseline for the ER7 benchmark
// (design §21.2 "transport" dimension). One binary, two roles:
//
//   --role producer: bind+listen+accept a SOCK_STREAM socket, then send
//       BenchPayloadV1<N> bytes (publish_ns stamped right before send) paced by
//       --rate — the natural backpressure baseline (every sample delivered,
//       no latest-value semantics, no futex).
//   --role consumer: connect, recv exactly N bytes per sample, stamp a RAW
//       receive_ns immediately, decode+validate, and write the same 7-column
//       rows as the ShmChannel consumer (generation/missed are 0 here) so the
//       bench driver produces a comparable samples.csv.
//
// The consumer closing the connection (its run caps) makes the producer's next
// send hit EPIPE, which ends the producer — so both sides stop together.

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "bench_payload.hpp"
#include "edge_runtime/schema.hpp"
#include "tool_common.hpp"

namespace {

using edge_tool::arg_u64;
using edge_tool::arg_value;
using edge_tool::cpu_us_now;
using edge_tool::monotonic_raw_now_ns;

struct Args {
	std::string role;
	std::string sock_path;
	uint32_t payload = 64;
	uint64_t samples = 1000000;
	uint64_t max_time_ms = 60000;
	uint64_t idle_ms = 1500;
	uint64_t rate_hz = 0;
	uint64_t open_retry_ms = 10000;
	std::string csv_path;
};

template <size_t N>
int run_sock_producer_n(const Args& a) {
	const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		std::fprintf(stderr, "socket: %s\n", std::strerror(errno));
		return 2;
	}
	::unlink(a.sock_path.c_str());
	if (a.sock_path.size() >= sizeof(sockaddr_un{}.sun_path)) {
		std::fprintf(stderr, "socket path too long\n");
		::close(fd);
		return 2;
	}
	sockaddr_un addr{};
	addr.sun_family = AF_UNIX;
	std::memcpy(addr.sun_path, a.sock_path.c_str(), a.sock_path.size());
	if (::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
		std::fprintf(stderr, "bind %s: %s\n", a.sock_path.c_str(), std::strerror(errno));
		::close(fd);
		return 2;
	}
	if (::listen(fd, 1) != 0) {
		std::fprintf(stderr, "listen: %s\n", std::strerror(errno));
		::close(fd);
		return 2;
	}
	std::printf("READY\n");
	std::fflush(stdout);

	const int cfd = ::accept(fd, nullptr, nullptr);
	if (cfd < 0) {
		std::fprintf(stderr, "accept: %s\n", std::strerror(errno));
		::close(fd);
		return 2;
	}
	::close(fd);
	::unlink(a.sock_path.c_str());  // socket node is consumed by the connection

	const int64_t deadline_ms = a.max_time_ms > 0 ? edge_tool::monotonic_ms_now() +
	                                                        static_cast<int64_t>(a.max_time_ms)
	                                              : 0;
	const uint64_t start_cpu = cpu_us_now();
	edge_tool::RatePacer pacer(a.rate_hz);
	pacer.start();

	uint64_t published = 0;
	for (;;) {
		if (published > 0) pacer.wait_until_next();  // first sample is immediate
		const uint64_t publish_ns = monotonic_raw_now_ns();
		bench::BenchPayloadV1<N> v{};
		v.counter = published;
		v.publish_ns = publish_ns;
		std::array<std::byte, N> buf{};
		if (!edge_runtime::PayloadCodec<bench::BenchPayloadV1<N>>::encode(v, buf.data(),
		                                                                  buf.size())) {
			std::fprintf(stderr, "encode failed\n");
			::close(cfd);
			return 3;
		}
		const ssize_t wr = ::send(cfd, buf.data(), buf.size(), 0);
		if (wr < 0) {
			if (errno == EPIPE) break;  // consumer closed the connection: run over
			std::fprintf(stderr, "send: %s\n", std::strerror(errno));
			::close(cfd);
			return 3;
		}
		++published;
		if (a.samples > 0 && published >= a.samples) break;
		if (deadline_ms != 0 && edge_tool::monotonic_ms_now() >= deadline_ms) break;
	}
	::close(cfd);

	const uint64_t end_cpu = cpu_us_now();
	std::printf("DONE published=%" PRIu64 " cpu_us=%" PRIu64 "\n", published,
	            end_cpu - start_cpu);
	std::fflush(stdout);
	return 0;
}

template <size_t N>
int run_sock_consumer_n(const Args& a) {
	int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		std::fprintf(stderr, "socket: %s\n", std::strerror(errno));
		return 2;
	}
	sockaddr_un addr{};
	addr.sun_family = AF_UNIX;
	if (a.sock_path.size() >= sizeof(sockaddr_un{}.sun_path)) {
		std::fprintf(stderr, "socket path too long\n");
		::close(fd);
		return 2;
	}
	std::memcpy(addr.sun_path, a.sock_path.c_str(), a.sock_path.size());

	const int64_t deadline_ms =
	        edge_tool::monotonic_ms_now() + static_cast<int64_t>(a.open_retry_ms);
	while (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
		if (edge_tool::monotonic_ms_now() >= deadline_ms) {
			std::fprintf(stderr, "connect %s: %s\n", a.sock_path.c_str(),
			             std::strerror(errno));
			::close(fd);
			return 2;
		}
		struct timespec ts {};
		ts.tv_nsec = 10 * 1000000L;  // 10 ms
		::nanosleep(&ts, nullptr);
	}
	std::printf("READY\n");
	std::fflush(stdout);

	FILE* csv = std::fopen(a.csv_path.c_str(), "w");
	if (csv == nullptr) {
		std::fprintf(stderr, "cannot open --csv %s\n", a.csv_path.c_str());
		::close(fd);
		return 2;
	}

	const uint64_t start_cpu = cpu_us_now();
	const int64_t start_ms = edge_tool::monotonic_ms_now();
	const int64_t run_deadline_ms =
	        a.max_time_ms > 0 ? start_ms + static_cast<int64_t>(a.max_time_ms) : 0;
	uint64_t reads = 0;
	uint64_t torn = 0;
	uint64_t last_sample_raw = 0;
	const char* ended = "time";

	std::array<std::byte, N> buf{};
	for (;;) {
		size_t got = 0;
		bool producer_closed = false;
		while (got < N) {
			const ssize_t r = ::recv(fd, buf.data() + got, N - got, 0);
			if (r == 0) {  // producer closed (its own caps hit)
				producer_closed = true;
				break;
			}
			if (r < 0) {
				if (errno == EINTR) continue;
				std::fprintf(stderr, "recv: %s\n", std::strerror(errno));
				std::fclose(csv);
				::close(fd);
				return 3;
			}
			got += static_cast<size_t>(r);
		}
		if (producer_closed) {
			ended = "idle";
			break;
		}

		const uint64_t receive_ns = monotonic_raw_now_ns();
		bench::BenchPayloadV1<N> v{};
		if (!edge_runtime::PayloadCodec<bench::BenchPayloadV1<N>>::decode(
		            buf.data(), static_cast<size_t>(N), &v)) {
			++torn;
			break;
		}
		const uint64_t latency = receive_ns >= v.publish_ns ? receive_ns - v.publish_ns : 0;
		std::fprintf(csv, "%" PRIu64 ",0,%u,%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",0\n",
		             reads + 1, static_cast<unsigned>(N), v.publish_ns, receive_ns,
		             latency);
		++reads;
		last_sample_raw = receive_ns;
		if (a.samples > 0 && reads >= a.samples) {
			ended = "samples";
			break;
		}
		if (run_deadline_ms != 0 && edge_tool::monotonic_ms_now() >= run_deadline_ms) {
			ended = "time";
			break;
		}
		if (a.idle_ms > 0 && last_sample_raw != 0 &&
		    monotonic_raw_now_ns() - last_sample_raw >= a.idle_ms * 1000000ull) {
			ended = "idle";
			break;
		}
	}
	::close(fd);
	std::fclose(csv);

	const uint64_t end_cpu = cpu_us_now();
	const uint64_t elapsed_ms = static_cast<uint64_t>(edge_tool::monotonic_ms_now() - start_ms);
	std::printf("SUMMARY reads=%" PRIu64 " torn=%" PRIu64
	            " missed_total=0 "
	            "last_seq=%" PRIu64 " elapsed_ms=%" PRIu64 " cpu_us=%" PRIu64 " ended=%s\n",
	            reads, torn, reads, elapsed_ms, end_cpu - start_cpu, ended);
	std::fflush(stdout);
	return torn > 0 ? 4 : 0;
}

// C++17 has no templated lambdas, so the payload dispatch goes through two
// namespace-scope tag structs with static template member functions.
struct SockProducerRole {
	template <size_t N>
	static int run(const Args& args) {
		return run_sock_producer_n<N>(args);
	}
};
struct SockConsumerRole {
	template <size_t N>
	static int run(const Args& args) {
		return run_sock_consumer_n<N>(args);
	}
};

template <typename Role>
int dispatch_sock_payload(const Args& a) {
	switch (a.payload) {
		case 64:
			return Role::template run<64>(a);
		case 256:
			return Role::template run<256>(a);
		case 1024:
			return Role::template run<1024>(a);
		case 4096:
			return Role::template run<4096>(a);
		case 16384:
			return Role::template run<16384>(a);
		case 65536:
			return Role::template run<65536>(a);
		default:
			std::fprintf(
			        stderr,
			        "unsupported --payload %u (need 64|256|1024|4096|16384|65536)\n",
			        a.payload);
			return 2;
	}
}

}  // namespace

int main(int argc, char** argv) {
	const char* role = edge_tool::arg_value(argc, argv, "--role");
	const char* path = edge_tool::arg_value(argc, argv, "--sock-path");
	if (role == nullptr || path == nullptr || *path == '\0') {
		std::fprintf(stderr,
		             "usage: edge_shm_bench_sock --role producer|consumer "
		             "--sock-path <path> --payload <bytes> "
		             "[--samples N] [--max-time-ms T] [--rate hz] "
		             "[--idle-ms N] [--open-retry-ms N] [--csv <path>]\n");
		return 2;
	}
	Args a;
	a.role = role;
	a.sock_path = path;
	a.payload = static_cast<uint32_t>(arg_u64(argc, argv, "--payload", 64));
	a.samples = arg_u64(argc, argv, "--samples", 1000000);
	a.max_time_ms = arg_u64(argc, argv, "--max-time-ms", 60000);
	a.idle_ms = arg_u64(argc, argv, "--idle-ms", 1500);
	a.rate_hz = arg_u64(argc, argv, "--rate", 0);
	a.open_retry_ms = arg_u64(argc, argv, "--open-retry-ms", 10000);
	const char* csv = edge_tool::arg_value(argc, argv, "--csv");
	if (csv != nullptr) a.csv_path = csv;

	if (a.role == "producer") return dispatch_sock_payload<SockProducerRole>(a);
	if (a.role == "consumer") {
		if (a.csv_path.empty()) {
			std::fprintf(stderr, "edge_shm_bench_sock consumer: --csv is required\n");
			return 2;
		}
		return dispatch_sock_payload<SockConsumerRole>(a);
	}
	std::fprintf(stderr, "unknown --role %s (need producer|consumer)\n", a.role.c_str());
	return 2;
}
