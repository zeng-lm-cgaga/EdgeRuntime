#include <poll.h>
#include <signal.h>
#include <time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <inttypes.h>
#include <limits>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "edge_runtime/common/error.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"

namespace {

using edge_runtime::ErrorCode;
using Payload = struct alignas(8) {
	uint64_t magic;
	uint32_t producer;
	uint32_t reserved;
	uint64_t sequence;
	uint64_t send_time_ns;
	uint64_t checksum;
	std::array<std::byte, 24> body;
};

static_assert(sizeof(Payload) == 64);
static_assert(std::is_trivially_copyable_v<Payload>);

constexpr uint64_t kMagic = 0x4552505245535355ull;
constexpr uint64_t kFnvOffset = 14695981039346656037ull;
constexpr uint64_t kFnvPrime = 1099511628211ull;

uint64_t now_ns() noexcept {
	struct timespec ts {};
	(void)::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
	return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
	       static_cast<uint64_t>(ts.tv_nsec);
}

uint64_t checksum(const Payload& value) noexcept {
	const auto* bytes = reinterpret_cast<const unsigned char*>(&value);
	constexpr size_t offset = offsetof(Payload, checksum);
	uint64_t result = kFnvOffset;
	for (size_t i = 0; i < sizeof(Payload); ++i) {
		if (i >= offset && i < offset + sizeof(value.checksum)) continue;
		result ^= static_cast<uint64_t>(bytes[i]);
		result *= kFnvPrime;
	}
	return result;
}

Payload make_payload(uint32_t producer, uint64_t sequence) noexcept {
	Payload value{};
	value.magic = kMagic;
	value.producer = producer;
	value.sequence = sequence;
	value.send_time_ns = now_ns();
	for (size_t i = 0; i < value.body.size(); ++i) {
		const uint64_t pattern = static_cast<uint64_t>(producer) * 0x9E3779B97F4A7C15ull +
		                         sequence * 0xD1B54A32D192ED03ull + i;
		value.body[i] = std::byte{static_cast<unsigned char>(pattern & 0xffu)};
	}
	value.checksum = checksum(value);
	return value;
}

bool valid_payload(const Payload& value, uint32_t producers, uint64_t messages) noexcept {
	if (value.magic != kMagic || value.reserved != 0 || value.producer >= producers ||
	    value.sequence >= messages || value.send_time_ns == 0 || value.send_time_ns > now_ns()) {
		return false;
	}
	const Payload expected = make_payload(value.producer, value.sequence);
	return expected.body == value.body && expected.magic == value.magic &&
	       checksum(value) == value.checksum;
}

std::array<std::byte, 32> fingerprint() noexcept {
	std::array<std::byte, 32> result{};
	for (size_t i = 0; i < result.size(); ++i) {
		result[i] = std::byte{static_cast<unsigned char>(0x31u + i)};
	}
	return result;
}

edge_runtime::SchemaDescriptor schema() noexcept {
	return {fingerprint(), 1, "EdgeRuntimePressurePayload"};
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

struct ChildReport {
	char role = '?';
	pid_t pid = -1;
	uint64_t start_ns = 0;
	uint64_t finish_ns = 0;
	uint64_t published = 0;
	uint64_t delivered = 0;
	uint64_t full = 0;
	uint64_t empty = 0;
	uint64_t contention = 0;
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
	struct rusage usage {};
};

struct Config {
	uint32_t producers = 1;
	uint32_t consumers = 1;
	uint64_t messages = 1000;
	uint32_t capacity = 64;
	uint64_t timeout_ms = 120000;
	uint64_t delay_us = 0;
	bool blocking = false;
};

const char* role_name(char role) noexcept { return role == 'P' ? "producer" : "consumer"; }

bool parse_config(int argc, char** argv, Config* config) noexcept {
	config->producers = static_cast<uint32_t>(number_of(argc, argv, "--producers", 1));
	config->consumers = static_cast<uint32_t>(number_of(argc, argv, "--consumers", 1));
	config->messages = number_of(argc, argv, "--messages", 1000);
	config->capacity = static_cast<uint32_t>(number_of(argc, argv, "--capacity", 64));
	config->timeout_ms = number_of(argc, argv, "--timeout-ms", 120000);
	config->delay_us = number_of(argc, argv, "--delay-us", 0);
	const char* policy = value_of(argc, argv, "--wait-policy");
	config->blocking = policy != nullptr && std::strcmp(policy, "blocking") == 0;
	return config->producers > 0 && config->producers <= 16 && config->consumers > 0 &&
	       config->consumers <= 16 && config->messages > 0 && config->capacity > 0 &&
	       config->capacity <= 4096 && config->timeout_ms > 0 && config->delay_us <= 1000000;
}

int run_child(int argc, char** argv, char role) {
	const char* name = value_of(argc, argv, "--name");
	const char* report_path = value_of(argc, argv, "--report");
	const int control_fd = static_cast<int>(number_of(argc, argv, "--control-fd", 0));
	const int ready_fd = static_cast<int>(number_of(argc, argv, "--ready-fd", 0));
	const uint32_t producers = static_cast<uint32_t>(number_of(argc, argv, "--producers", 0));
	const uint64_t messages = number_of(argc, argv, "--messages", 0);
	const uint64_t target = number_of(argc, argv, "--target", 0);
	const uint64_t delay_us = number_of(argc, argv, "--delay-us", 0);
	const bool blocking = value_of(argc, argv, "--wait-policy") != nullptr &&
	                      std::strcmp(value_of(argc, argv, "--wait-policy"), "blocking") == 0;
	if (name == nullptr || report_path == nullptr || control_fd < 0 || ready_fd < 0 ||
	    producers == 0 || messages == 0 || target == 0) {
		return 2;
	}

	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = static_cast<uint32_t>(number_of(argc, argv, "--capacity", 0));
	auto queue = edge_runtime::MpmcQueue<Payload>::open(options, schema());
	if (!queue) {
		const char ready = 'E';
		(void)full_write(ready_fd, &ready, 1);
		return 3;
	}
	const char ready = 'R';
	if (!full_write(ready_fd, &ready, 1)) return 3;
	::close(ready_fd);
	char go = 0;
	if (!full_read(control_fd, &go, 1) || go != 'G') return 3;

	ChildReport report;
	report.role = role;
	report.pid = ::getpid();
	report.start_ns = now_ns();
	uint64_t completed = 0;
	const auto deadline = std::chrono::steady_clock::now() +
	                      std::chrono::milliseconds(number_of(argc, argv, "--timeout-ms", 120000));
	while (completed < target) {
		if (role == 'P') {
			const Payload payload = make_payload(
			        static_cast<uint32_t>(number_of(argc, argv, "--producer-id", 0)), completed);
			for (;;) {
				auto result = queue.value().try_push(payload);
				if (result) {
					++report.published;
					++completed;
					break;
				}
				if (result.error().code == ErrorCode::kQueueFull) {
					++report.full;
				} else if (result.error().code == ErrorCode::kQueueContention) {
					++report.contention;
				} else {
					++report.errors;
					std::fprintf(stderr, "HELPER_ERROR pid=%ld role=%s op=push code=%s ctx=%s\n",
					             static_cast<long>(::getpid()), role_name(role),
					             edge_runtime::to_string(result.error().code), result.error().context);
					break;
				}
				if (blocking && (result.error().code == ErrorCode::kQueueFull ||
				                 result.error().code == ErrorCode::kQueueContention)) {
					++report.contention;
					result = queue.value().wait_push_until(payload, deadline);
					if (result) {
						++report.published;
						++completed;
						break;
					}
					if (result.error().code != ErrorCode::kTimeout) ++report.errors;
				}
				if (!blocking) std::this_thread::yield();
				if (std::chrono::steady_clock::now() >= deadline) {
					++report.errors;
					break;
				}
			}
		} else {
			auto result = queue.value().try_pop();
			if (!result) {
				if (result.error().code == ErrorCode::kQueueEmpty) {
					++report.empty;
				} else if (result.error().code == ErrorCode::kQueueContention) {
					++report.contention;
				} else {
					++report.errors;
					std::fprintf(stderr, "HELPER_ERROR pid=%ld role=%s op=pop code=%s ctx=%s\n",
					             static_cast<long>(::getpid()), role_name(role),
					             edge_runtime::to_string(result.error().code), result.error().context);
					break;
				}
				if (blocking && (result.error().code == ErrorCode::kQueueEmpty ||
				                 result.error().code == ErrorCode::kQueueContention)) {
					++report.contention;
					result = queue.value().wait_pop_until(deadline);
					if (result) {
						if (!valid_payload(result.value(), producers, messages)) {
							++report.invalid;
							++report.errors;
							break;
						}
						report.ids.push_back(
						        static_cast<uint64_t>(result.value().producer) * messages +
						        result.value().sequence);
						report.latency_ns.push_back(now_ns() - result.value().send_time_ns);
						++report.delivered;
						++completed;
						if (delay_us != 0) ::usleep(static_cast<useconds_t>(delay_us));
						continue;
					}
					if (result.error().code != ErrorCode::kTimeout) ++report.errors;
				}
				if (!blocking) std::this_thread::yield();
				if (std::chrono::steady_clock::now() >= deadline) {
					++report.errors;
					break;
				}
				continue;
			}
			if (!valid_payload(result.value(), producers, messages)) {
				++report.invalid;
				++report.errors;
				break;
			}
			report.ids.push_back(
			        static_cast<uint64_t>(result.value().producer) * messages +
			        result.value().sequence);
			report.latency_ns.push_back(now_ns() - result.value().send_time_ns);
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
	    << report.full << " " << report.empty << " " << report.contention << " "
	    << report.errors << " " << report.invalid << " " << report.ids.size() << "\n";
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
	std::vector<char*> argv;
	argv.reserve(args.size() + 1);
	for (const auto& arg : args) argv.push_back(const_cast<char*>(arg.c_str()));
	argv.push_back(nullptr);
	::execv(executable.c_str(), argv.data());
	::_exit(127);
}

bool wait_ready(std::vector<Child>* children, uint64_t timeout_ms) {
	std::vector<struct pollfd> pollfds;
	pollfds.reserve(children->size());
	for (const auto& child : *children) pollfds.push_back({child.ready_fd, POLLIN | POLLHUP, 0});
	size_t ready_count = 0;
	const uint64_t deadline = now_ns() + timeout_ms * 1000000ull;
	while (ready_count != children->size()) {
		const uint64_t current = now_ns();
		if (current >= deadline) return false;
		const uint64_t remain_ms = (deadline - current + 999999ull) / 1000000ull;
		const int result = ::poll(pollfds.data(), pollfds.size(),
		                          static_cast<int>(std::min<uint64_t>(remain_ms, 1000)));
		if (result < 0 && errno == EINTR) continue;
		if (result < 0) return false;
		for (size_t i = 0; i < pollfds.size(); ++i) {
			if (pollfds[i].fd < 0 || (pollfds[i].revents & (POLLIN | POLLHUP)) == 0) continue;
			char value = 0;
			const ssize_t count = ::read(pollfds[i].fd, &value, 1);
			if (count == 1 && value == 'R') {
				::close(pollfds[i].fd);
				pollfds[i].fd = -1;
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
			struct rusage usage {};
			const pid_t result = ::wait4(child.pid, &status, WNOHANG, &usage);
			if (result == child.pid) {
				child.reaped = true;
				child.status = status;
				child.usage = usage;
				--remaining;
			} else if (result < 0 && errno != EINTR) {
				child.reaped = true;
				child.status = 127 << 8;
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
				struct rusage usage {};
				while (::wait4(child.pid, &status, 0, &usage) < 0 && errno == EINTR) {
				}
				child.reaped = true;
				child.status = status;
				child.usage = usage;
			}
			return false;
		}
		::usleep(1000);
	}
	return true;
}

bool read_report(const std::string& path, ChildReport* report) {
	std::ifstream in(path);
	std::string tag;
	size_t count = 0;
	if (!(in >> tag >> report->role >> report->pid >> report->start_ns >> report->finish_ns >>
	      report->published >> report->delivered >> report->full >> report->empty >>
	      report->contention >> report->errors >> report->invalid >> count) ||
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
	Config config;
	if (!parse_config(argc, argv, &config)) {
		std::fprintf(stderr, "invalid helper arguments\n");
		return 2;
	}
	const std::string base = "er_pressure_helper_" + std::to_string(::getpid()) + "_" +
	                         std::to_string(now_ns());
	edge_runtime::MpmcQueueOptions options;
	options.name = base;
	options.capacity = config.capacity;
	auto queue = edge_runtime::MpmcQueue<Payload>::create(options, schema());
	if (!queue) {
		std::fprintf(stderr, "CREATE_FAIL code=%s ctx=%s\n",
		             edge_runtime::to_string(queue.error().code), queue.error().context);
		return 2;
	}
	const uint64_t expected = config.messages * config.producers;
	std::vector<Child> children;
	children.reserve(config.producers + config.consumers);
	const std::string report_dir = "/tmp";
	auto launch = [&](char role, uint32_t index, uint64_t target) -> bool {
		int control[2] = {-1, -1};
		int ready[2] = {-1, -1};
		if (::pipe(control) != 0 || ::pipe(ready) != 0) return false;
		const std::string report = report_dir + "/" + base + "_" + role + "_" +
		                           std::to_string(index) + ".report";
		std::vector<std::string> args = {
		        executable,
		        "--role",
		        role == 'P' ? "producer" : "consumer",
		        "--name",
		        options.name,
		        "--capacity",
		        std::to_string(config.capacity),
		        "--producers",
		        std::to_string(config.producers),
		        "--messages",
		        std::to_string(config.messages),
		        "--target",
		        std::to_string(target),
		        "--producer-id",
		        std::to_string(index),
		        "--delay-us",
		        std::to_string(config.delay_us),
		        "--timeout-ms",
		        std::to_string(config.timeout_ms),
		        "--wait-policy",
		        config.blocking ? "blocking" : "busy",
		        "--control-fd",
		        std::to_string(control[0]),
		        "--ready-fd",
		        std::to_string(ready[1]),
		        "--report",
		        report};
		const pid_t pid = spawn_child(executable, args);
		if (pid < 0) {
			::close(control[0]);
			::close(control[1]);
			::close(ready[0]);
			::close(ready[1]);
			return false;
		}
		::close(control[0]);
		::close(ready[1]);
		children.push_back({pid, role, control[1], ready[0], report});
		return true;
	};
	for (uint32_t i = 0; i < config.consumers; ++i) {
		const uint64_t base_target = expected / config.consumers;
		const uint64_t target =
		        base_target + (static_cast<uint64_t>(i) < expected % config.consumers ? 1 : 0);
		if (!launch('C', i, target)) return 3;
	}
	for (uint32_t i = 0; i < config.producers; ++i) {
		if (!launch('P', i, config.messages)) return 3;
	}
	if (!wait_ready(&children, config.timeout_ms)) {
		std::fprintf(stderr, "HANDSHAKE_FAIL\n");
		(void)reap_all(&children, 1000);
		(void)queue.value().remove_if_creator();
		return 3;
	}
	const uint64_t go_ns = now_ns();
	for (auto& child : children) {
		const char go = 'G';
		(void)full_write(child.control_fd, &go, 1);
		::close(child.control_fd);
		child.control_fd = -1;
	}
	std::printf("GO_SENT ns=%" PRIu64 " delay_us=%" PRIu64 " blocking=%s\n", go_ns,
	            config.delay_us, config.blocking ? "true" : "false");
	const bool reaped = reap_all(&children, config.timeout_ms);
	bool success = reaped;
	std::vector<uint8_t> seen(static_cast<size_t>(expected), 0);
	std::vector<uint64_t> latencies;
	uint64_t published = 0;
	uint64_t delivered = 0;
	uint64_t full = 0;
	uint64_t empty = 0;
	uint64_t contention = 0;
	uint64_t errors = reaped ? 0 : 1;
	uint64_t invalid = 0;
	uint64_t start_max = 0;
	uint64_t finish_min = std::numeric_limits<uint64_t>::max();
	uint64_t finish_max = 0;
	for (const auto& child : children) {
		ChildReport report;
		const bool report_ok = read_report(child.report_path, &report);
		const bool exited_zero = child.reaped && WIFEXITED(child.status) &&
		                         WEXITSTATUS(child.status) == 0;
		std::printf("CHILD role=%s pid=%ld exit=%d report=%s start_ns=%" PRIu64
		            " finish_ns=%" PRIu64 " published=%" PRIu64 " delivered=%" PRIu64
		            " full=%" PRIu64 " empty=%" PRIu64 " contention=%" PRIu64
		            " errors=%" PRIu64 " invalid=%" PRIu64 "\n",
		            role_name(child.role), static_cast<long>(child.pid),
		            exited_zero ? 0 : (child.reaped && WIFSIGNALED(child.status)
		                                       ? 128 + WTERMSIG(child.status)
		                                       : 1),
		            report_ok ? "ok" : "missing", report.start_ns, report.finish_ns,
		            report.published, report.delivered, report.full, report.empty,
		            report.contention, report.errors, report.invalid);
		if (!report_ok || !exited_zero || report.start_ns == 0 || report.finish_ns <= report.start_ns) {
			success = false;
			++errors;
		}
		if (report_ok) {
			published += report.published;
			delivered += report.delivered;
			full += report.full;
			empty += report.empty;
			contention += report.contention;
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
	auto removed = queue.value().remove_if_creator();
	auto reopened = edge_runtime::MpmcQueue<Payload>::open(options, schema());
	const bool absent = !reopened && reopened.error().code == ErrorCode::kNotFound;
	if (!removed || !absent) success = false;
	std::printf(
	        "RESULT label=VM_ONLY producers=%u consumers=%u messages=%" PRIu64
	        " capacity=%u expected=%" PRIu64 " published=%" PRIu64 " delivered=%" PRIu64
	        " queue_full=%" PRIu64 " queue_empty=%" PRIu64 " queue_contention=%" PRIu64
	        " errors=%" PRIu64 " invalid=%" PRIu64 " overlap=%s duration_us=%" PRIu64
	        " p50_ns=%" PRIu64 " p99_ns=%" PRIu64 " cleanup=%s correctness=%s\n",
	        config.producers, config.consumers, config.messages, config.capacity, expected,
	        published, delivered, full, empty, contention, errors, invalid,
	        overlap ? "PASS" : "FAIL",
	        static_cast<uint64_t>(finish_max > go_ns ? (finish_max - go_ns) / 1000ull : 0ull),
	        percentile(latencies, 50), percentile(latencies, 99),
	        removed && absent ? "PASS" : "FAIL", success && errors == 0 ? "PASS" : "FAIL");
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
	if (value_of(argc, argv, "--help") != nullptr) {
		std::printf("mpmc_handshake_helper --producers N --consumers N --messages N "
		            "--capacity N --delay-us N --wait-policy busy|blocking\n");
		return 0;
	}
	const char* executable = "/proc/self/exe";
	char path[4096]{};
	const ssize_t length = ::readlink(executable, path, sizeof(path) - 1);
	if (length <= 0) return 2;
	path[length] = '\0';
	return run_parent(argc, argv, path);
}
