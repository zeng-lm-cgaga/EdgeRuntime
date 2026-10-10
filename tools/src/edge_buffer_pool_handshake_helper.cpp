#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <inttypes.h>
#include <limits>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"

namespace {

using edge_runtime::BufferHandle;
using edge_runtime::ErrorCode;

struct Header {
	uint64_t magic;
	uint32_t producer;
	uint32_t reserved;
	uint64_t sequence;
	uint64_t send_time_ns;
	uint64_t checksum;
};

static_assert(sizeof(Header) == 40);
static_assert(std::is_trivially_copyable_v<BufferHandle>);

constexpr uint64_t kMagic = 0x4552504F4F4C5355ull;
constexpr uint64_t kFnvOffset = 14695981039346656037ull;
constexpr uint64_t kFnvPrime = 1099511628211ull;

uint64_t now_ns() noexcept {
	struct timespec ts {};
	(void)::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
	return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
	       static_cast<uint64_t>(ts.tv_nsec);
}

uint64_t checksum(const std::byte* data, size_t size) noexcept {
	const size_t offset = offsetof(Header, checksum);
	uint64_t result = kFnvOffset;
	for (size_t i = 0; i < size; ++i) {
		if (i >= offset && i < offset + sizeof(uint64_t)) continue;
		result ^= static_cast<uint64_t>(std::to_integer<unsigned char>(data[i]));
		result *= kFnvPrime;
	}
	return result;
}

unsigned char body_byte(uint32_t producer, uint64_t sequence, size_t offset) noexcept {
	const uint64_t pattern = static_cast<uint64_t>(producer) * 0x9E3779B97F4A7C15ull +
	                         sequence * 0xD1B54A32D192ED03ull + offset;
	return static_cast<unsigned char>(pattern & 0xffu);
}

bool fill(std::byte* data, size_t size, uint32_t producer, uint64_t sequence) noexcept {
	if (data == nullptr || size < sizeof(Header)) return false;
	Header header{kMagic, producer, 0, sequence, now_ns(), 0};
	std::memcpy(data, &header, sizeof(header));
	for (size_t i = sizeof(Header); i < size; ++i) {
		data[i] = std::byte{body_byte(producer, sequence, i)};
	}
	const uint64_t value = checksum(data, size);
	std::memcpy(data + offsetof(Header, checksum), &value, sizeof(value));
	return true;
}

bool valid(const std::byte* data, size_t size, uint32_t producers, uint64_t messages,
           uint64_t* id, uint64_t* latency_ns) noexcept {
	if (data == nullptr || size < sizeof(Header)) return false;
	Header header{};
	std::memcpy(&header, data, sizeof(header));
	if (header.magic != kMagic || header.reserved != 0 || header.producer >= producers ||
	    header.sequence >= messages || header.send_time_ns == 0 || header.send_time_ns > now_ns() ||
	    checksum(data, size) != header.checksum) {
		return false;
	}
	for (size_t i = sizeof(Header); i < size; ++i) {
		if (std::to_integer<unsigned char>(data[i]) != body_byte(header.producer, header.sequence, i)) {
			return false;
		}
	}
	*id = static_cast<uint64_t>(header.producer) * messages + header.sequence;
	*latency_ns = now_ns() - header.send_time_ns;
	return true;
}

std::array<std::byte, 32> fingerprint(uint32_t seed) noexcept {
	std::array<std::byte, 32> result{};
	for (size_t i = 0; i < result.size(); ++i) {
		result[i] = std::byte{static_cast<unsigned char>((seed + i * 13u) & 0xffu)};
	}
	return result;
}

edge_runtime::SchemaDescriptor pool_schema(uint32_t block_size) noexcept {
	return {fingerprint(0x504f4f4cu ^ block_size), 1, "EdgeRuntimePressurePoolPayload"};
}

edge_runtime::SchemaDescriptor queue_schema() noexcept {
	return {fingerprint(0x51554555u), 1, "EdgeRuntimePressurePoolHandle"};
}

const char* value_of(int argc, char** argv, const char* key) noexcept {
	for (int i = 1; i + 1 < argc; ++i) {
		if (std::strcmp(argv[i], key) == 0) return argv[i + 1];
	}
	return nullptr;
}

uint64_t number_of(int argc, char** argv, const char* key, uint64_t fallback) noexcept {
	const char* value = value_of(argc, argv, key);
	if (value == nullptr) return fallback;
	char* end = nullptr;
	const unsigned long long parsed = std::strtoull(value, &end, 10);
	if (end == value || *end != '\0') return fallback;
	return static_cast<uint64_t>(parsed);
}

bool full_write(int fd, const void* data, size_t size) noexcept {
	const auto* bytes = static_cast<const unsigned char*>(data);
	while (size != 0) {
		const ssize_t written = ::write(fd, bytes, size);
		if (written > 0) {
			bytes += written;
			size -= static_cast<size_t>(written);
			continue;
		}
		if (written < 0 && errno == EINTR) continue;
		return false;
	}
	return true;
}

bool full_read(int fd, void* data, size_t size) noexcept {
	auto* bytes = static_cast<unsigned char*>(data);
	while (size != 0) {
		const ssize_t count = ::read(fd, bytes, size);
		if (count > 0) {
			bytes += count;
			size -= static_cast<size_t>(count);
			continue;
		}
		if (count < 0 && errno == EINTR) continue;
		return false;
	}
	return true;
}

struct Report {
	char role = '?';
	pid_t pid = -1;
	uint64_t start_ns = 0;
	uint64_t finish_ns = 0;
	uint64_t published = 0;
	uint64_t delivered = 0;
	uint64_t pool_full = 0;
	uint64_t pool_contention = 0;
	uint64_t queue_full = 0;
	uint64_t queue_empty = 0;
	uint64_t queue_contention = 0;
	uint64_t errors = 0;
	uint64_t invalid = 0;
	std::vector<uint64_t> ids;
	std::vector<uint64_t> latency_ns;
};

struct Child {
	pid_t pid = -1;
	char role = '?';
	int control_fd = -1;
	int ready_fd = -1;
	std::string report_path;
	bool reaped = false;
	int status = 0;
};

const char* role_name(char role) noexcept { return role == 'P' ? "producer" : "consumer"; }

int run_child(int argc, char** argv, char role) {
	const char* pool_name = value_of(argc, argv, "--pool-name");
	const char* queue_name = value_of(argc, argv, "--queue-name");
	const char* report_path = value_of(argc, argv, "--report");
	const int control_fd = static_cast<int>(number_of(argc, argv, "--control-fd", 0));
	const int ready_fd = static_cast<int>(number_of(argc, argv, "--ready-fd", 0));
	const uint32_t block_size = static_cast<uint32_t>(number_of(argc, argv, "--block-size", 0));
	const uint32_t capacity = static_cast<uint32_t>(number_of(argc, argv, "--capacity", 0));
	const uint32_t producers = static_cast<uint32_t>(number_of(argc, argv, "--producers", 0));
	const uint64_t messages = number_of(argc, argv, "--messages", 0);
	const uint64_t target = number_of(argc, argv, "--target", 0);
	const uint64_t delay_us = number_of(argc, argv, "--delay-us", 0);
	if (pool_name == nullptr || queue_name == nullptr || report_path == nullptr || control_fd < 0 ||
	    ready_fd < 0 || (block_size != 4096 && block_size != 16384) || capacity == 0 ||
	    producers == 0 || messages == 0 || target == 0) {
		return 2;
	}
	edge_runtime::SharedBufferPoolOptions pool_options;
	pool_options.name = pool_name;
	pool_options.block_size = block_size;
	pool_options.block_count = static_cast<uint32_t>(number_of(argc, argv, "--block-count", 0));
	pool_options.schema = pool_schema(block_size);
	auto pool = edge_runtime::SharedBufferPool::open(pool_options);
	edge_runtime::MpmcQueueOptions queue_options;
	queue_options.name = queue_name;
	queue_options.capacity = capacity;
	auto queue = edge_runtime::MpmcQueue<BufferHandle>::open(queue_options, queue_schema());
	if (!pool || !queue) {
		const char value = 'E';
		(void)full_write(ready_fd, &value, 1);
		return 3;
	}
	const char ready = 'R';
	if (!full_write(ready_fd, &ready, 1)) return 3;
	::close(ready_fd);
	char go = 0;
	if (!full_read(control_fd, &go, 1) || go != 'G') return 3;

	Report report;
	report.role = role;
	report.pid = ::getpid();
	report.start_ns = now_ns();
	uint64_t completed = 0;
	while (completed < target) {
		if (role == 'P') {
			auto write = pool.value().try_acquire_write();
			if (!write) {
				if (write.error().code == ErrorCode::kBufferPoolFull) {
					++report.pool_full;
				} else if (write.error().code == ErrorCode::kBufferPoolContention) {
					++report.pool_contention;
				} else {
					++report.errors;
					break;
				}
				std::this_thread::yield();
				continue;
			}
			if (!fill(write.value().data(), write.value().size(),
			          static_cast<uint32_t>(number_of(argc, argv, "--producer-id", 0)), completed)) {
				++report.errors;
				write.value().abort();
				break;
			}
			auto descriptor = write.value().publish();
			if (!descriptor) {
				++report.errors;
				break;
			}
			bool queued = false;
			while (!queued) {
				auto pushed = queue.value().try_push(descriptor.value());
				if (pushed) {
					queued = true;
					++report.published;
					++completed;
					continue;
				}
				if (pushed.error().code == ErrorCode::kQueueFull) {
					++report.queue_full;
				} else if (pushed.error().code == ErrorCode::kQueueContention) {
					++report.queue_contention;
				} else {
					++report.errors;
					break;
				}
				std::this_thread::yield();
			}
			if (!queued) break;
		} else {
			auto popped = queue.value().try_pop();
			if (!popped) {
				if (popped.error().code == ErrorCode::kQueueEmpty) {
					++report.queue_empty;
				} else if (popped.error().code == ErrorCode::kQueueContention) {
					++report.queue_contention;
				} else {
					++report.errors;
					break;
				}
				std::this_thread::yield();
				continue;
			}
			auto read = pool.value().acquire_read(popped.value());
			if (!read) {
				++report.errors;
				break;
			}
			uint64_t id = 0;
			uint64_t latency = 0;
			const bool ok = valid(read.value().data(), read.value().size(), producers, messages,
			                      &id, &latency);
			read.value().release();
			if (!ok) {
				++report.invalid;
				++report.errors;
				break;
			}
			report.ids.push_back(id);
			report.latency_ns.push_back(latency);
			++report.delivered;
			++completed;
			if (delay_us != 0) ::usleep(static_cast<useconds_t>(delay_us));
		}
	}
	report.finish_ns = now_ns();
	std::ofstream out(report_path);
	if (!out) return 5;
	out << "REPORT " << report.role << " " << report.pid << " " << report.start_ns << " "
	    << report.finish_ns << " " << report.published << " " << report.delivered << " "
	    << report.pool_full << " " << report.pool_contention << " " << report.queue_full << " "
	    << report.queue_empty << " " << report.queue_contention << " " << report.errors << " "
	    << report.invalid << " " << report.ids.size() << "\n";
	for (size_t i = 0; i < report.ids.size(); ++i) {
		out << "RECORD " << report.ids[i] << " " << report.latency_ns[i] << "\n";
	}
	out.close();
	::close(control_fd);
	return report.errors == 0 && completed == target ? 0 : 4;
}

pid_t spawn_child(const std::string& executable, const std::vector<std::string>& args) {
	const pid_t pid = ::fork();
	if (pid != 0) return pid;
	std::vector<char*> child_argv;
	child_argv.reserve(args.size() + 1);
	for (const auto& arg : args) child_argv.push_back(const_cast<char*>(arg.c_str()));
	child_argv.push_back(nullptr);
	::execv(executable.c_str(), child_argv.data());
	::_exit(127);
}

bool wait_ready(std::vector<Child>* children, uint64_t timeout_ms) {
	std::vector<struct pollfd> fds;
	fds.reserve(children->size());
	for (const auto& child : *children) fds.push_back({child.ready_fd, POLLIN | POLLHUP, 0});
	size_t ready_count = 0;
	const uint64_t deadline = now_ns() + timeout_ms * 1000000ull;
	while (ready_count != children->size()) {
		if (now_ns() >= deadline) return false;
		const int result = ::poll(fds.data(), fds.size(), 1000);
		if (result < 0 && errno == EINTR) continue;
		if (result < 0) return false;
		for (size_t i = 0; i < fds.size(); ++i) {
			if (fds[i].fd < 0 || (fds[i].revents & (POLLIN | POLLHUP)) == 0) continue;
			char value = 0;
			const ssize_t count = ::read(fds[i].fd, &value, 1);
			if (count == 1 && value == 'R') {
				::close(fds[i].fd);
				fds[i].fd = -1;
				++ready_count;
			} else {
				return false;
			}
		}
	}
	std::printf("HANDSHAKE ready=%zu total=%zu\n", ready_count, children->size());
	return true;
}

bool reap_all(std::vector<Child>* children, uint64_t timeout_ms) {
	const uint64_t deadline = now_ns() + timeout_ms * 1000000ull;
	size_t remaining = children->size();
	while (remaining != 0) {
		for (auto& child : *children) {
			if (child.reaped) continue;
			int status = 0;
			const pid_t result = ::waitpid(child.pid, &status, WNOHANG);
			if (result == child.pid) {
				child.status = status;
				child.reaped = true;
				--remaining;
			} else if (result < 0 && errno != EINTR) {
				child.status = 127 << 8;
				child.reaped = true;
				--remaining;
			}
		}
		if (remaining == 0) return true;
		if (now_ns() >= deadline) {
			for (auto& child : *children) {
				if (!child.reaped) (void)::kill(child.pid, SIGKILL);
			}
			for (auto& child : *children) {
				if (child.reaped) continue;
				int status = 0;
				while (::waitpid(child.pid, &status, 0) < 0 && errno == EINTR) {
				}
				child.status = status;
				child.reaped = true;
			}
			return false;
		}
		::usleep(1000);
	}
	return true;
}

bool read_report(const std::string& path, Report* report) {
	std::ifstream in(path);
	std::string tag;
	size_t count = 0;
	if (!(in >> tag >> report->role >> report->pid >> report->start_ns >> report->finish_ns >>
	      report->published >> report->delivered >> report->pool_full >> report->pool_contention >>
	      report->queue_full >> report->queue_empty >> report->queue_contention >> report->errors >>
	      report->invalid >> count) ||
	    tag != "REPORT") {
		return false;
	}
	report->ids.reserve(count);
	report->latency_ns.reserve(count);
	for (size_t i = 0; i < count; ++i) {
		uint64_t id = 0;
		uint64_t latency = 0;
		if (!(in >> tag >> id >> latency) || tag != "RECORD") return false;
		report->ids.push_back(id);
		report->latency_ns.push_back(latency);
	}
	return true;
}

uint64_t percentile(std::vector<uint64_t> values, uint32_t numerator) {
	if (values.empty()) return 0;
	std::sort(values.begin(), values.end());
	const size_t rank = std::max<size_t>(
	        1, (values.size() * static_cast<size_t>(numerator) + 99u) / 100u);
	return values[std::min(rank, values.size()) - 1];
}

int run_parent(int argc, char** argv, const std::string& executable) {
	const uint32_t block_size = static_cast<uint32_t>(number_of(argc, argv, "--block-size", 0));
	const uint32_t block_count = static_cast<uint32_t>(number_of(argc, argv, "--block-count", 0));
	const uint32_t producers = static_cast<uint32_t>(number_of(argc, argv, "--producers", 0));
	const uint32_t consumers = static_cast<uint32_t>(number_of(argc, argv, "--consumers", 0));
	const uint64_t messages = number_of(argc, argv, "--messages", 0);
	const uint32_t capacity = static_cast<uint32_t>(number_of(argc, argv, "--capacity", 0));
	const uint64_t timeout_ms = number_of(argc, argv, "--timeout-ms", 120000);
	const uint64_t delay_us = number_of(argc, argv, "--delay-us", 0);
	if ((block_size != 4096 && block_size != 16384) || block_count == 0 || producers == 0 ||
	    producers > 16 || consumers == 0 || consumers > 16 || messages == 0 || capacity == 0 ||
	    timeout_ms == 0 || delay_us > 1000000) {
		return 2;
	}
	const uint64_t expected = messages * producers;
	const std::string base = "er_pool_pressure_helper_" + std::to_string(::getpid()) + "_" +
	                         std::to_string(now_ns());
	edge_runtime::SharedBufferPoolOptions pool_options;
	pool_options.name = base + "_pool";
	pool_options.block_size = block_size;
	pool_options.block_count = block_count;
	pool_options.schema = pool_schema(block_size);
	edge_runtime::MpmcQueueOptions queue_options;
	queue_options.name = base + "_queue";
	queue_options.capacity = capacity;
	auto pool = edge_runtime::SharedBufferPool::create(pool_options);
	if (!pool) return 2;
	auto queue = edge_runtime::MpmcQueue<BufferHandle>::create(queue_options, queue_schema());
	if (!queue) {
		(void)pool.value().remove_if_creator();
		return 2;
	}
	std::vector<Child> children;
	children.reserve(producers + consumers);
	auto launch = [&](char role, uint32_t index, uint64_t target) -> bool {
		int control[2] = {-1, -1};
		int ready[2] = {-1, -1};
		if (::pipe(control) != 0 || ::pipe(ready) != 0) return false;
		const std::string report = "/tmp/" + base + "_" + role + "_" +
		                           std::to_string(index) + ".report";
		std::vector<std::string> args = {
		        executable,
		        "--role",
		        role == 'P' ? "producer" : "consumer",
		        "--pool-name",
		        pool_options.name,
		        "--queue-name",
		        queue_options.name,
		        "--block-size",
		        std::to_string(block_size),
		        "--block-count",
		        std::to_string(block_count),
		        "--capacity",
		        std::to_string(capacity),
		        "--producers",
		        std::to_string(producers),
		        "--messages",
		        std::to_string(messages),
		        "--target",
		        std::to_string(target),
		        "--producer-id",
		        std::to_string(index),
		        "--delay-us",
		        std::to_string(delay_us),
		        "--control-fd",
		        std::to_string(control[0]),
		        "--ready-fd",
		        std::to_string(ready[1]),
		        "--report",
		        report};
		const pid_t pid = spawn_child(executable, args);
		if (pid < 0) return false;
		::close(control[0]);
		::close(ready[1]);
		children.push_back({pid, role, control[1], ready[0], report});
		return true;
	};
	for (uint32_t i = 0; i < consumers; ++i) {
		const uint64_t base_target = expected / consumers;
		const uint64_t target =
		        base_target + (static_cast<uint64_t>(i) < expected % consumers ? 1 : 0);
		if (!launch('C', i, target)) return 3;
	}
	for (uint32_t i = 0; i < producers; ++i) {
		if (!launch('P', i, messages)) return 3;
	}
	if (!wait_ready(&children, timeout_ms)) {
		(void)reap_all(&children, 1000);
		(void)queue.value().remove_if_creator();
		(void)pool.value().remove_if_creator();
		return 3;
	}
	const uint64_t go_ns = now_ns();
	for (auto& child : children) {
		const char go = 'G';
		(void)full_write(child.control_fd, &go, 1);
		::close(child.control_fd);
		child.control_fd = -1;
	}
	std::printf("GO_SENT ns=%" PRIu64 " delay_us=%" PRIu64 "\n", go_ns, delay_us);
	const bool reaped = reap_all(&children, timeout_ms);
	bool success = reaped;
	std::vector<uint8_t> seen(static_cast<size_t>(expected), 0);
	std::vector<uint64_t> latencies;
	uint64_t published = 0;
	uint64_t delivered = 0;
	uint64_t pool_full = 0;
	uint64_t pool_contention = 0;
	uint64_t queue_full = 0;
	uint64_t queue_empty = 0;
	uint64_t queue_contention = 0;
	uint64_t errors = reaped ? 0 : 1;
	uint64_t invalid = 0;
	uint64_t start_max = 0;
	uint64_t finish_min = std::numeric_limits<uint64_t>::max();
	uint64_t finish_max = 0;
	for (const auto& child : children) {
		Report report;
		const bool report_ok = read_report(child.report_path, &report);
		const bool exited_zero = child.reaped && WIFEXITED(child.status) &&
		                         WEXITSTATUS(child.status) == 0;
		std::printf("CHILD role=%s pid=%ld exit=%d report=%s start_ns=%" PRIu64
		            " finish_ns=%" PRIu64 " published=%" PRIu64 " delivered=%" PRIu64
		            " pool_full=%" PRIu64 " pool_contention=%" PRIu64
		            " queue_full=%" PRIu64 " queue_empty=%" PRIu64
		            " queue_contention=%" PRIu64 " errors=%" PRIu64 " invalid=%" PRIu64 "\n",
		            role_name(child.role), static_cast<long>(child.pid), exited_zero ? 0 : 1,
		            report_ok ? "ok" : "missing", report.start_ns, report.finish_ns,
		            report.published, report.delivered, report.pool_full, report.pool_contention,
		            report.queue_full, report.queue_empty, report.queue_contention, report.errors,
		            report.invalid);
		if (!report_ok || !exited_zero || report.start_ns == 0 || report.finish_ns <= report.start_ns) {
			success = false;
			++errors;
		}
		if (report_ok) {
			published += report.published;
			delivered += report.delivered;
			pool_full += report.pool_full;
			pool_contention += report.pool_contention;
			queue_full += report.queue_full;
			queue_empty += report.queue_empty;
			queue_contention += report.queue_contention;
			errors += report.errors;
				invalid += report.invalid;
				start_max = std::max(start_max, report.start_ns);
				finish_min = std::min(finish_min, report.finish_ns);
				finish_max = std::max(finish_max, report.finish_ns);
			for (size_t i = 0; i < report.ids.size(); ++i) {
				if (report.ids[i] >= expected || seen[static_cast<size_t>(report.ids[i])] != 0) {
					success = false;
					++invalid;
				} else {
					seen[static_cast<size_t>(report.ids[i])] = 1;
				}
				latencies.push_back(report.latency_ns[i]);
			}
		}
		(void)::unlink(child.report_path.c_str());
	}
	const bool overlap = start_max != 0 && finish_min != std::numeric_limits<uint64_t>::max() &&
	                     start_max < finish_min;
	if (!overlap) success = false;
	for (uint8_t value : seen) {
		if (value == 0) {
			success = false;
			break;
		}
	}
	auto removed_queue = queue.value().remove_if_creator();
	auto removed_pool = pool.value().remove_if_creator();
	auto reopened_queue = edge_runtime::MpmcQueue<BufferHandle>::open(queue_options, queue_schema());
	auto reopened_pool = edge_runtime::SharedBufferPool::open(pool_options);
	const bool absent = !reopened_queue && !reopened_pool &&
	                    reopened_queue.error().code == ErrorCode::kNotFound &&
	                    reopened_pool.error().code == ErrorCode::kNotFound;
	std::printf("CLEANUP queue=%s pool=%s reopen_queue=%s reopen_pool=%s\n",
	            removed_queue ? "PASS" : edge_runtime::to_string(removed_queue.error().code),
	            removed_pool ? "PASS" : edge_runtime::to_string(removed_pool.error().code),
	            reopened_queue ? "FOUND" : edge_runtime::to_string(reopened_queue.error().code),
	            reopened_pool ? "FOUND" : edge_runtime::to_string(reopened_pool.error().code));
	if (!removed_queue || !removed_pool || !absent) success = false;
	std::printf(
	        "RESULT label=VM_ONLY block_size=%u block_count=%u producers=%u consumers=%u "
	        "messages=%" PRIu64 " capacity=%u expected=%" PRIu64 " published=%" PRIu64
	        " delivered=%" PRIu64 " pool_full=%" PRIu64 " pool_contention=%" PRIu64
	        " queue_full=%" PRIu64 " queue_empty=%" PRIu64 " queue_contention=%" PRIu64
	        " errors=%" PRIu64 " invalid=%" PRIu64 " overlap=%s duration_us=%" PRIu64
	        " p50_ns=%" PRIu64 " p99_ns=%" PRIu64 " cleanup=%s correctness=%s\n",
	        block_size, block_count, producers, consumers, messages, capacity, expected, published,
	        delivered, pool_full, pool_contention, queue_full, queue_empty, queue_contention, errors,
	        invalid, overlap ? "PASS" : "FAIL",
	        static_cast<uint64_t>(finish_max > go_ns ? (finish_max - go_ns) / 1000ull : 0ull),
	        percentile(latencies, 50), percentile(latencies, 99), removed_queue && removed_pool && absent
	                ? "PASS"
	                : "FAIL",
	        success && errors == 0 ? "PASS" : "FAIL");
	return success && errors == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
	const char* role = value_of(argc, argv, "--role");
	if (role != nullptr) {
		if (std::strcmp(role, "producer") == 0) return run_child(argc, argv, 'P');
		if (std::strcmp(role, "consumer") == 0) return run_child(argc, argv, 'C');
		return 2;
	}
	char path[4096]{};
	const ssize_t length = ::readlink("/proc/self/exe", path, sizeof(path) - 1);
	if (length <= 0) return 2;
	path[length] = '\0';
	return run_parent(argc, argv, path);
}
