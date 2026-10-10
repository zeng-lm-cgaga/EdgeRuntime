// Cross-process SharedBufferPool baseline benchmark. Payload bytes stay in mmap blocks; only
// BufferHandle descriptors cross the MPMC queue.
#include <fcntl.h>
#include <linux/perf_event.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "edge_runtime_tool/buffer_pool_bench_payload.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime_tool/tool_common.hpp"

namespace {

using edge_runtime::BufferHandle;
using edge_runtime::ErrorCode;
using Queue = edge_runtime::MpmcQueue<BufferHandle>;

constexpr uint32_t kReportMagic = 0x45524250u;  // "ERBP"
constexpr uint16_t kReportVersion = 1;
constexpr uint32_t kNoErrorCode = std::numeric_limits<uint32_t>::max();
constexpr uint32_t kMaxWorkers = 16;
constexpr uint64_t kMaxMessagesPerProducer = 10000000;
constexpr uint64_t kMaxExpectedMessages = 20000000;
constexpr uint32_t kMaxRuns = 1000;

enum class ChildRole : uint16_t {
	kProducer = 1,
	kConsumer = 2,
};

struct ReportHeader {
	uint32_t magic = kReportMagic;
	uint16_t version = kReportVersion;
	uint16_t role = 0;
	uint64_t published = 0;
	uint64_t delivered = 0;
	uint64_t pool_full = 0;
	uint64_t pool_contention = 0;
	uint64_t queue_full = 0;
	uint64_t queue_empty = 0;
	uint64_t queue_contention = 0;
	uint64_t recovery = 0;
	uint64_t errors = 0;
	uint64_t invalid_payload = 0;
	uint32_t first_error_code = kNoErrorCode;
	uint32_t reserved = 0;
	uint64_t record_count = 0;
	uint32_t perf_available = 0;
	uint32_t reserved2 = 0;
	uint64_t perf_context_switches = 0;
};

struct DeliveryRecord {
	uint32_t producer = 0;
	uint32_t reserved = 0;
	uint64_t sequence = 0;
	uint64_t latency_ns = 0;
};

static_assert(std::is_trivially_copyable_v<ReportHeader>);
static_assert(std::is_trivially_copyable_v<DeliveryRecord>);

struct Config {
	uint32_t block_size = 4096;
	uint32_t block_count = 1;
	uint32_t producers = 1;
	uint32_t consumers = 1;
	uint64_t messages = 1000;
	uint32_t capacity = 64;
	uint32_t runs = 1;
	uint64_t timeout_ms = 30000;
	uint64_t test_consumer_hold_ms = 0;
	bool test_ignore_term_consumer = false;
	std::string out_dir;
	std::string ready_file;
};

struct ChildProcess {
	pid_t pid = -1;
	ChildRole role = ChildRole::kProducer;
	std::string report_path;
	uint64_t expected_records = 0;
	bool waited = false;
	int status = 0;
	struct rusage usage {};
};

struct ParsedReport {
	ReportHeader header{};
	std::vector<DeliveryRecord> records;
	bool valid = false;
};

struct RunAggregate {
	uint64_t expected = 0;
	uint64_t published = 0;
	uint64_t delivered = 0;
	uint64_t pool_full = 0;
	uint64_t pool_contention = 0;
	uint64_t queue_full = 0;
	uint64_t queue_empty = 0;
	uint64_t queue_contention = 0;
	uint64_t recovery = 0;
	uint64_t errors = 0;
	uint64_t invalid_payload = 0;
	uint64_t wall_us = 0;
	uint64_t user_cpu_us = 0;
	uint64_t system_cpu_us = 0;
	uint64_t perf_context_switches = 0;
	bool perf_available = true;
	bool timed_out = false;
	bool ready_failed = false;
	bool child_kill_sent = false;
	bool correctness = false;
	std::vector<uint64_t> latency_ns;
};

volatile sig_atomic_t g_stop_requested = 0;

void stop_signal_handler(int) noexcept { g_stop_requested = 1; }

bool install_stop_signal_handler() noexcept {
	struct sigaction action {};
	action.sa_handler = stop_signal_handler;
	if (::sigemptyset(&action.sa_mask) != 0) return false;
	action.sa_flags = 0;
	return ::sigaction(SIGTERM, &action, nullptr) == 0 &&
	       ::sigaction(SIGINT, &action, nullptr) == 0;
}

uint64_t monotonic_ns() noexcept {
	return edge_tool::monotonic_raw_now_ns();
}

uint64_t rusage_user_us(const struct rusage& usage) noexcept {
	return static_cast<uint64_t>(usage.ru_utime.tv_sec) * 1000000ull +
	       static_cast<uint64_t>(usage.ru_utime.tv_usec);
}

uint64_t rusage_system_us(const struct rusage& usage) noexcept {
	return static_cast<uint64_t>(usage.ru_stime.tv_sec) * 1000000ull +
	       static_cast<uint64_t>(usage.ru_stime.tv_usec);
}

bool write_full(int fd, const void* data, size_t size) noexcept {
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

void report_ready(int fd, char value) noexcept {
	if (fd < 0) return;
	(void)write_full(fd, &value, sizeof(value));
	(void)::close(fd);
}

bool read_full(int fd, void* data, size_t size) noexcept {
	auto* bytes = static_cast<unsigned char*>(data);
	while (size != 0) {
		const ssize_t read_count = ::read(fd, bytes, size);
		if (read_count > 0) {
			bytes += read_count;
			size -= static_cast<size_t>(read_count);
			continue;
		}
		if (read_count < 0 && errno == EINTR) continue;
		return false;
	}
	return true;
}

bool mkdirs(const std::string& path) {
	if (path.empty()) return true;
	std::string current;
	for (size_t i = 0; i < path.size(); ++i) {
		current.push_back(path[i]);
		if (path[i] != '/' && i + 1 != path.size()) continue;
		if (current == "/" || current.empty()) continue;
		if (::mkdir(current.c_str(), 0755) != 0 && errno != EEXIST) return false;
	}
	return true;
}

std::string join_path(const std::string& directory, const std::string& file) {
	if (directory.empty()) return file;
	if (directory.back() == '/') return directory + file;
	return directory + "/" + file;
}

std::string self_executable() {
	char path[4096]{};
	const ssize_t length = ::readlink("/proc/self/exe", path, sizeof(path) - 1);
	if (length <= 0) return {};
	path[length] = '\0';
	return path;
}

std::string make_base_name(uint32_t run_index) {
	return "er_bp_bench_" + std::to_string(static_cast<unsigned long>(::getpid())) + "_" +
	       std::to_string(run_index) + "_" +
	       std::to_string(static_cast<unsigned long long>(monotonic_ns()));
}

std::string make_report_path(uint32_t run_index, uint32_t child_index) {
	return "/tmp/er_buffer_pool_bench_" +
	       std::to_string(static_cast<unsigned long>(::getpid())) + "_" +
	       std::to_string(run_index) + "_" + std::to_string(child_index) + ".report";
}

void note_error(ReportHeader* header, ErrorCode code) noexcept {
	++header->errors;
	if (code == ErrorCode::kRecoveryBlocked) ++header->recovery;
	if (header->first_error_code == kNoErrorCode) {
		header->first_error_code = static_cast<uint32_t>(code);
	}
}

class PerfCounter {
       public:
	PerfCounter() = default;
	PerfCounter(const PerfCounter&) = delete;
	PerfCounter& operator=(const PerfCounter&) = delete;

	void start() noexcept {
		struct perf_event_attr attr {};
		attr.type = PERF_TYPE_SOFTWARE;
		attr.size = static_cast<__u32>(sizeof(attr));
		attr.config = PERF_COUNT_SW_CONTEXT_SWITCHES;
		attr.disabled = 1;
		fd_ = static_cast<int>(::syscall(SYS_perf_event_open, &attr, 0, -1, -1, 0));
		if (fd_ < 0) return;
		if (::ioctl(fd_, PERF_EVENT_IOC_RESET, 0) != 0 ||
		    ::ioctl(fd_, PERF_EVENT_IOC_ENABLE, 0) != 0) {
			::close(fd_);
			fd_ = -1;
			return;
		}
		available_ = true;
	}

	void stop() noexcept {
		if (fd_ < 0) return;
		(void)::ioctl(fd_, PERF_EVENT_IOC_DISABLE, 0);
		uint64_t value = 0;
		if (::read(fd_, &value, sizeof(value)) == static_cast<ssize_t>(sizeof(value))) {
			value_ = value;
		} else {
			available_ = false;
		}
		::close(fd_);
		fd_ = -1;
	}

	bool available() const noexcept { return available_; }
	uint64_t value() const noexcept { return value_; }

       private:
	int fd_ = -1;
	bool available_ = false;
	uint64_t value_ = 0;
};

bool write_report(const std::string& path, ReportHeader header,
                  const std::vector<DeliveryRecord>& records) {
	header.record_count = static_cast<uint64_t>(records.size());
	const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
	if (fd < 0) return false;
	bool ok = write_full(fd, &header, sizeof(header));
	if (ok && !records.empty()) {
		ok = write_full(fd, records.data(), records.size() * sizeof(records[0]));
	}
	if (::close(fd) != 0) ok = false;
	return ok;
}

ParsedReport read_report(const std::string& path, ChildRole expected_role,
                         uint64_t expected_records) {
	ParsedReport report;
	const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
	if (fd < 0) return report;
	if (!read_full(fd, &report.header, sizeof(report.header))) {
		::close(fd);
		return report;
	}
	const uint16_t expected_role_value = static_cast<uint16_t>(expected_role);
	const bool role_shape_ok =
	        expected_role == ChildRole::kProducer
                ? report.header.record_count == 0 && report.header.delivered == 0
                : report.header.published == 0 &&
                          report.header.record_count == report.header.delivered;
	if (report.header.magic != kReportMagic || report.header.version != kReportVersion ||
	    report.header.role != expected_role_value || report.header.reserved != 0 ||
	    report.header.reserved2 != 0 || report.header.perf_available > 1 ||
	    report.header.record_count > expected_records || !role_shape_ok ||
	    report.header.record_count > std::numeric_limits<size_t>::max() / sizeof(DeliveryRecord)) {
		::close(fd);
		return report;
	}
	report.records.resize(static_cast<size_t>(report.header.record_count));
	if (!report.records.empty() &&
	    !read_full(fd, report.records.data(), report.records.size() * sizeof(report.records[0]))) {
		::close(fd);
		return report;
	}
	struct stat file_stat {};
	bool size_ok =
	        ::fstat(fd, &file_stat) == 0 && S_ISREG(file_stat.st_mode) &&
	        static_cast<uint64_t>(file_stat.st_size) ==
	                sizeof(report.header) + report.header.record_count * sizeof(DeliveryRecord);
	if (size_ok) {
		for (const auto& record : report.records) {
			if (record.reserved != 0) {
				size_ok = false;
				break;
			}
		}
	}
	::close(fd);
	report.valid = size_ok;
	return report;
}

bool prepare_report_path(const std::string& path) noexcept {
	return ::unlink(path.c_str()) == 0 || errno == ENOENT;
}

edge_runtime::SharedBufferPoolOptions make_pool_options(const std::string& name,
                                                        const Config& config) {
	edge_runtime::SharedBufferPoolOptions options;
	options.name = name;
	options.block_size = config.block_size;
	options.block_count = config.block_count;
	options.schema = edge_buffer_pool_bench::pool_schema(config.block_size);
	return options;
}

edge_runtime::MpmcQueueOptions make_queue_options(const std::string& name,
                                                  const Config& config) {
	edge_runtime::MpmcQueueOptions options;
	options.name = name;
	options.capacity = config.capacity;
	return options;
}

int run_child_producer(int argc, char** argv) {
	if (!install_stop_signal_handler()) return 2;
	const char* pool_name_arg = edge_tool::arg_value(argc, argv, "--pool-name");
	const char* queue_name_arg = edge_tool::arg_value(argc, argv, "--queue-name");
	const char* report_path = edge_tool::arg_value(argc, argv, "--report-file");
	if (pool_name_arg == nullptr || queue_name_arg == nullptr || report_path == nullptr) return 2;
	const int ready_fd = static_cast<int>(edge_tool::arg_u64(argc, argv, "--ready-fd", 0));
	Config config;
	config.block_size = static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--block-size", 0));
	config.block_count = static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--block-count", 0));
	config.capacity = static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--capacity", 0));
	config.messages = edge_tool::arg_u64(argc, argv, "--messages", 0);
	const uint32_t producer_id = static_cast<uint32_t>(
	        edge_tool::arg_u64(argc, argv, "--producer-id", std::numeric_limits<uint32_t>::max()));
	ReportHeader header;
	header.role = static_cast<uint16_t>(ChildRole::kProducer);
	PerfCounter perf;
	perf.start();
	edge_runtime::SharedBufferPoolOptions pool_opts =
	        make_pool_options(pool_name_arg, config);
	edge_runtime::MpmcQueueOptions queue_opts =
	        make_queue_options(queue_name_arg, config);
	auto pool = edge_runtime::SharedBufferPool::open(pool_opts);
	auto queue = Queue::open(queue_opts, edge_buffer_pool_bench::queue_schema());
	bool success = pool.has_value() && queue.has_value();
	if (!pool) note_error(&header, pool.error().code);
	if (!queue) note_error(&header, queue.error().code);
	if (!pool || !queue) {
		report_ready(ready_fd, 'E');
	}
	if (pool && queue && producer_id != std::numeric_limits<uint32_t>::max()) {
		report_ready(ready_fd, 'R');
		for (uint64_t sequence = 0; sequence < config.messages && !g_stop_requested;) {
			if (g_stop_requested) {
				success = false;
				break;
			}
			std::optional<edge_runtime::WriteBuffer> write;
			for (;;) {
				if (g_stop_requested) {
					success = false;
					break;
				}
				auto candidate = pool.value().try_acquire_write();
				if (g_stop_requested) {
					success = false;
					break;
				}
				if (candidate) {
					write.emplace(std::move(candidate.value()));
					break;
				}
				switch (candidate.error().code) {
				case ErrorCode::kBufferPoolFull:
					++header.pool_full;
					::sched_yield();
					break;
				case ErrorCode::kBufferPoolContention:
					++header.pool_contention;
					::sched_yield();
					break;
				default:
					note_error(&header, candidate.error().code);
					success = false;
					sequence = config.messages;
					break;
				}
				if (!success) break;
			}
			if (!success || !write) break;
			if (!edge_buffer_pool_bench::fill(write->data(), write->size(), producer_id, sequence,
			                                  monotonic_ns())) {
				note_error(&header, ErrorCode::kPayloadEncodeFailed);
				success = false;
				break;
			}
			auto published = write->publish();
			if (!published) {
				note_error(&header, published.error().code);
				success = false;
				break;
			}
			bool queued = false;
			while (!queued && !g_stop_requested) {
				auto pushed = queue.value().try_push(published.value());
				if (pushed) {
					queued = true;
					++header.published;
					continue;
				}
				switch (pushed.error().code) {
				case ErrorCode::kQueueFull:
					++header.queue_full;
					::sched_yield();
					break;
				case ErrorCode::kQueueContention:
					++header.queue_contention;
					::sched_yield();
					break;
				default:
					note_error(&header, pushed.error().code);
					success = false;
					queued = true;
					break;
				}
			}
			if (!queued) {
				// A published descriptor that was not queued is still owned by this
				// worker.  Settle it before the process leaves so the creator can
				// remove the pool without an unaccounted published block.
				auto settled = pool.value().acquire_read(published.value());
				if (settled) {
					settled.value().release();
				} else {
					note_error(&header, settled.error().code);
				}
			}
			if (g_stop_requested) success = false;
			if (!success) break;
			++sequence;
		}
	} else if (producer_id == std::numeric_limits<uint32_t>::max()) {
		report_ready(ready_fd, 'E');
		note_error(&header, ErrorCode::kInvalidOptions);
		success = false;
	}
	perf.stop();
	header.perf_available = perf.available() ? 1u : 0u;
	header.perf_context_switches = perf.value();
	if (!write_report(report_path, header, {})) return 5;
	return success ? 0 : 3;
}

int run_child_consumer(int argc, char** argv) {
	if (!install_stop_signal_handler()) return 2;
	const char* pool_name_arg = edge_tool::arg_value(argc, argv, "--pool-name");
	const char* queue_name_arg = edge_tool::arg_value(argc, argv, "--queue-name");
	const char* report_path = edge_tool::arg_value(argc, argv, "--report-file");
	if (pool_name_arg == nullptr || queue_name_arg == nullptr || report_path == nullptr) return 2;
	const int ready_fd = static_cast<int>(edge_tool::arg_u64(argc, argv, "--ready-fd", 0));
	Config config;
	config.block_size = static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--block-size", 0));
	config.block_count = static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--block-count", 0));
	config.capacity = static_cast<uint32_t>(edge_tool::arg_u64(argc, argv, "--capacity", 0));
	config.producers = static_cast<uint32_t>(
	        edge_tool::arg_u64(argc, argv, "--producer-count", 0));
	config.messages = edge_tool::arg_u64(argc, argv, "--messages", 0);
	const uint64_t target = edge_tool::arg_u64(argc, argv, "--consumer-target", 0);
	const uint64_t test_consumer_hold_ms =
	        edge_tool::arg_u64(argc, argv, "--test-consumer-hold-ms", 0);
	const bool ignore_term = edge_tool::arg_flag(argc, argv, "--test-ignore-term-consumer");
	if (ignore_term && ::signal(SIGTERM, SIG_IGN) == SIG_ERR) return 2;
	ReportHeader header;
	header.role = static_cast<uint16_t>(ChildRole::kConsumer);
	std::vector<DeliveryRecord> records;
	records.reserve(static_cast<size_t>(target));
	PerfCounter perf;
	perf.start();
	edge_runtime::SharedBufferPoolOptions pool_opts =
	        make_pool_options(pool_name_arg, config);
	edge_runtime::MpmcQueueOptions queue_opts =
	        make_queue_options(queue_name_arg, config);
	auto pool = edge_runtime::SharedBufferPool::open(pool_opts);
	auto queue = Queue::open(queue_opts, edge_buffer_pool_bench::queue_schema());
	bool success = pool.has_value() && queue.has_value();
	if (!pool) note_error(&header, pool.error().code);
	if (!queue) note_error(&header, queue.error().code);
	if (!pool || !queue) report_ready(ready_fd, 'E');
	if (pool && queue) {
		report_ready(ready_fd, 'R');
		if (test_consumer_hold_ms != 0) {
			::usleep(static_cast<useconds_t>(test_consumer_hold_ms * 1000ull));
		}
		for (uint64_t delivered = 0; delivered < target && !g_stop_requested;) {
			if (g_stop_requested) break;
			auto popped = queue.value().try_pop();
			if (g_stop_requested) break;
			if (!popped) {
				switch (popped.error().code) {
				case ErrorCode::kQueueEmpty:
					++header.queue_empty;
					::sched_yield();
					break;
				case ErrorCode::kQueueContention:
					++header.queue_contention;
					::sched_yield();
					break;
				default:
					note_error(&header, popped.error().code);
					success = false;
					delivered = target;
					break;
				}
				continue;
			}
			auto read = pool.value().acquire_read(popped.value());
			if (!read) {
				note_error(&header, read.error().code);
				success = false;
				break;
			}
			const uint64_t receive_ns = monotonic_ns();
			uint64_t latency_ns = 0;
			uint32_t producer = 0;
			uint64_t sequence = 0;
			const bool valid = edge_buffer_pool_bench::validate(
			        read.value().data(), read.value().size(), config.producers, config.messages,
			        receive_ns, &latency_ns, &producer, &sequence);
			read.value().release();
			if (!valid) {
				++header.invalid_payload;
				note_error(&header, ErrorCode::kPayloadCorrupt);
				success = false;
				break;
			}
			records.push_back({producer, 0, sequence, latency_ns});
			++header.delivered;
			++delivered;
		}
	}
	perf.stop();
	header.perf_available = perf.available() ? 1u : 0u;
	header.perf_context_switches = perf.value();
	if (!write_report(report_path, header, records)) return 5;
	return success ? 0 : 4;
}

pid_t spawn_child(const std::string& executable, const std::vector<std::string>& arguments) {
	const pid_t pid = ::fork();
	if (pid != 0) return pid;
	std::vector<char*> child_argv;
	child_argv.reserve(arguments.size() + 1);
	for (const auto& argument : arguments) {
		child_argv.push_back(const_cast<char*>(argument.c_str()));
	}
	child_argv.push_back(nullptr);
	::execv(executable.c_str(), child_argv.data());
	::_exit(127);
}

bool wait_for_ready(int fd, size_t expected, uint64_t timeout_ms, bool* failed) {
	*failed = false;
	if (expected == 0) return true;
	size_t ready = 0;
	const uint64_t deadline = monotonic_ns() + timeout_ms * 1000000ull;
	while (ready < expected) {
		if (g_stop_requested != 0) return false;
		const uint64_t now = monotonic_ns();
		if (now >= deadline) return false;
		const uint64_t remaining_ms = (deadline - now + 999999ull) / 1000000ull;
		struct pollfd descriptor {fd, POLLIN | POLLHUP, 0};
		const int poll_result = ::poll(&descriptor, 1,
		                              static_cast<int>(std::min<uint64_t>(remaining_ms, 50)));
		if (poll_result < 0) {
			if (errno == EINTR) continue;
			return false;
		}
		if (poll_result == 0) continue;
		char values[32]{};
		const ssize_t count = ::read(fd, values, sizeof(values));
		if (count > 0) {
			for (ssize_t i = 0; i < count; ++i) {
				if (values[i] == 'R') {
					++ready;
				} else if (values[i] == 'E') {
					*failed = true;
				}
			}
			if (*failed) return false;
			continue;
		}
		if (count == 0) return false;
		if (errno != EINTR) return false;
	}
	return true;
}

bool write_ready_file(const Config& config, const std::string& pool_name,
	                      const std::string& queue_name, uint32_t run_index,
	                      const std::vector<ChildProcess>& children) {
	if (config.ready_file.empty()) return true;
	FILE* file = std::fopen(config.ready_file.c_str(), "w");
	if (file == nullptr) return false;
	bool ok = std::fprintf(file, "READY run=%" PRIu32 " pool=%s queue=%s\n", run_index,
	                       pool_name.c_str(), queue_name.c_str()) >= 0;
	for (const auto& child : children) {
		if (!ok) break;
		ok = std::fprintf(file, "ROLE role=%s pid=%ld report=%s\n",
		                  child.role == ChildRole::kProducer ? "producer" : "consumer",
		                  static_cast<long>(child.pid), child.report_path.c_str()) >= 0;
	}
	if (std::fclose(file) != 0) ok = false;
	return ok;
}

bool reap_children(std::vector<ChildProcess>* children, uint64_t timeout_ms, bool* timed_out,
	                   bool* kill_sent) {
	const uint64_t normal_deadline = monotonic_ns() + timeout_ms * 1000000ull;
	uint64_t deadline = normal_deadline;
	bool stop_sent = false;
	*kill_sent = false;
	size_t remaining = children->size();
	while (remaining != 0) {
		for (auto& child : *children) {
			if (child.waited) continue;
			int status = 0;
			struct rusage usage {};
			const pid_t waited = ::wait4(child.pid, &status, WNOHANG, &usage);
			if (waited == child.pid) {
				child.status = status;
				child.usage = usage;
				child.waited = true;
				--remaining;
			} else if (waited < 0 && errno != EINTR) {
				child.waited = true;
				child.status = 127 << 8;
				--remaining;
			}
		}
		if (remaining == 0) return true;
		if (!stop_sent && (g_stop_requested != 0 || monotonic_ns() >= normal_deadline)) {
			*timed_out = true;
			stop_sent = true;
			deadline = monotonic_ns() + 1000ull * 1000000ull;
			for (auto& child : *children) {
				if (child.waited) continue;
				(void)::kill(child.pid, SIGCONT);
				(void)::kill(child.pid, SIGTERM);
			}
		}
		if (stop_sent && monotonic_ns() >= deadline) {
			for (auto& child : *children) {
				if (child.waited) continue;
				if (::kill(child.pid, SIGKILL) == 0) *kill_sent = true;
			}
			for (auto& child : *children) {
				if (child.waited) continue;
				int status = 0;
				struct rusage usage {};
				while (::wait4(child.pid, &status, 0, &usage) < 0 && errno == EINTR) {
				}
				child.status = status;
				child.usage = usage;
				child.waited = true;
			}
			return false;
		}
		::usleep(1000);
	}
	return true;
}

bool child_succeeded(const ChildProcess& child) noexcept {
	return child.waited && WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0;
}

bool drain_published_descriptors(edge_runtime::SharedBufferPool* pool, Queue* queue,
                                 uint64_t limit) {
	const uint64_t deadline = monotonic_ns() + 2000ull * 1000000ull;
	uint64_t settled_count = 0;
	while (monotonic_ns() < deadline && settled_count < limit) {
		auto popped = queue->try_pop();
		if (popped) {
			for (;;) {
				auto read = pool->acquire_read(popped.value());
				if (read) {
					read.value().release();
					++settled_count;
					break;
				}
				if (read.error().code != ErrorCode::kBufferPoolContention) return false;
				if (monotonic_ns() >= deadline) return false;
				::sched_yield();
			}
			continue;
		}
		if (popped.error().code == ErrorCode::kQueueEmpty) return true;
		if (popped.error().code != ErrorCode::kQueueContention) return false;
		::sched_yield();
	}
	return settled_count == limit || monotonic_ns() < deadline;
}

bool remove_and_verify_resources(edge_runtime::SharedBufferPool* pool, Queue* queue,
                                 const edge_runtime::SharedBufferPoolOptions& pool_options,
                                 const edge_runtime::MpmcQueueOptions& queue_options,
                                 uint32_t run_index, bool stopped, bool child_kill_sent) {
	const auto removed_pool = pool->remove_if_creator();
	const auto removed_queue = queue->remove_if_creator();
	auto reopened_pool = edge_runtime::SharedBufferPool::open(pool_options);
	auto reopened_queue = Queue::open(queue_options, edge_buffer_pool_bench::queue_schema());
	const bool pool_absent = !reopened_pool && reopened_pool.error().code == ErrorCode::kNotFound;
	const bool queue_absent = !reopened_queue && reopened_queue.error().code == ErrorCode::kNotFound;
	std::printf("CLEANUP run=%" PRIu32
	            " resource=pool_queue pool=%s queue=%s reopen_pool=%s reopen_queue=%s "
	            "stopped=%s child_kill=%s\n",
	            run_index, removed_pool ? "SUCCESS" : "FAIL", removed_queue ? "SUCCESS" : "FAIL",
	            reopened_pool ? "FOUND" : edge_runtime::to_string(reopened_pool.error().code),
	            reopened_queue ? "FOUND" : edge_runtime::to_string(reopened_queue.error().code),
	            stopped ? "true" : "false", child_kill_sent ? "true" : "false");
	std::fflush(stdout);
	return removed_pool && removed_queue && pool_absent && queue_absent;
}

uint64_t percentile_nearest_rank(const std::vector<uint64_t>& sorted, uint32_t numerator,
                                 uint32_t denominator) noexcept {
	if (sorted.empty()) return 0;
	const uint64_t count = static_cast<uint64_t>(sorted.size());
	uint64_t rank = (count * numerator + denominator - 1) / denominator;
	if (rank == 0) rank = 1;
	if (rank > count) rank = count;
	return sorted[static_cast<size_t>(rank - 1)];
}

bool write_latency_csv(const std::string& out_dir, uint32_t run_index, const Config& config,
                       const std::vector<DeliveryRecord>& records) {
	const std::string filename =
	        "buffer_pool_bench_" + std::to_string(static_cast<unsigned long>(::getpid())) +
	        "_run_" + std::to_string(run_index) + "_block_" +
	        std::to_string(config.block_size) + "_" + std::to_string(config.producers) + "P" +
	        std::to_string(config.consumers) + "C.csv";
	const std::string path = join_path(out_dir, filename);
	FILE* file = std::fopen(path.c_str(), "w");
	if (file == nullptr) return false;
	bool ok = std::fprintf(file, "producer,sequence,latency_ns\n") >= 0;
	for (const auto& record : records) {
		if (!ok) break;
		ok = std::fprintf(file, "%" PRIu32 ",%" PRIu64 ",%" PRIu64 "\n", record.producer,
		                  record.sequence, record.latency_ns) >= 0;
	}
	if (std::fclose(file) != 0) ok = false;
	return ok;
}

void print_summary(uint32_t run_index, const Config& config, const RunAggregate& aggregate) {
	std::vector<uint64_t> sorted = aggregate.latency_ns;
	std::sort(sorted.begin(), sorted.end());
	std::printf(
	        "RESULT run=%" PRIu32
	        " label=VM_ONLY block_bytes=%" PRIu32 " block_count=%" PRIu32
	        " producers=%" PRIu32 " consumers=%" PRIu32 " messages_per_producer=%" PRIu64
	        " capacity=%" PRIu32 " expected=%" PRIu64 " published=%" PRIu64
	        " delivered=%" PRIu64 " pool_full=%" PRIu64 " pool_contention=%" PRIu64
	        " queue_full=%" PRIu64 " queue_empty=%" PRIu64 " queue_contention=%" PRIu64
	        " recovery=%" PRIu64 " errors=%" PRIu64 " invalid_payload=%" PRIu64
	        " wall_us=%" PRIu64 " user_cpu_us=%" PRIu64 " system_cpu_us=%" PRIu64
	        " cpu_total_us=%" PRIu64 " p50_ns=",
	        run_index, config.block_size, config.block_count, config.producers, config.consumers,
	        config.messages, config.capacity, aggregate.expected, aggregate.published,
	        aggregate.delivered, aggregate.pool_full, aggregate.pool_contention,
	        aggregate.queue_full, aggregate.queue_empty, aggregate.queue_contention,
	        aggregate.recovery, aggregate.errors, aggregate.invalid_payload, aggregate.wall_us,
	        aggregate.user_cpu_us, aggregate.system_cpu_us,
	        aggregate.user_cpu_us + aggregate.system_cpu_us);
	if (sorted.empty()) {
		std::printf("NA p95_ns=NA p99_ns=NA max_ns=NA ");
	} else {
		std::printf("%" PRIu64 " p95_ns=%" PRIu64 " p99_ns=%" PRIu64 " max_ns=%" PRIu64 " ",
		            percentile_nearest_rank(sorted, 50, 100),
		            percentile_nearest_rank(sorted, 95, 100),
		            percentile_nearest_rank(sorted, 99, 100), sorted.back());
	}
	if (aggregate.perf_available) {
		std::printf("perf_context_switches=%" PRIu64 " ", aggregate.perf_context_switches);
	} else {
		std::printf("perf_context_switches=UNAVAILABLE ");
	}
	std::printf("spsc_missed_samples=NOT_APPLICABLE correctness=%s\n",
	            aggregate.correctness ? "PASS" : "FAIL");
	std::fflush(stdout);
}

int run_parent_once(const std::string& executable, const Config& config, uint32_t run_index) {
	const std::string base_name = make_base_name(run_index);
	const std::string pool_name = base_name + "_pool";
	const std::string queue_name = base_name + "_queue";
	edge_runtime::SharedBufferPoolOptions pool_opts = make_pool_options(pool_name, config);
	edge_runtime::MpmcQueueOptions queue_opts = make_queue_options(queue_name, config);
	auto pool = edge_runtime::SharedBufferPool::create(pool_opts);
	if (!pool) {
		std::fprintf(stderr, "POOL_CREATE_FAIL run=%" PRIu32 " code=%s ctx=%s\n", run_index,
		             edge_runtime::to_string(pool.error().code), pool.error().context);
		return 2;
	}
	auto queue = Queue::create(queue_opts, edge_buffer_pool_bench::queue_schema());
	if (!queue) {
		std::fprintf(stderr, "QUEUE_CREATE_FAIL run=%" PRIu32 " code=%s ctx=%s\n", run_index,
		             edge_runtime::to_string(queue.error().code), queue.error().context);
		const auto removed = pool.value().remove_if_creator();
		auto reopened = edge_runtime::SharedBufferPool::open(pool_opts);
		if (!removed || reopened || reopened.error().code != ErrorCode::kNotFound) {
			std::fprintf(stderr, "POOL_CREATE_CLEANUP_FAIL run=%" PRIu32 "\n", run_index);
		}
		return 2;
	}

	RunAggregate aggregate;
	aggregate.expected = config.messages * static_cast<uint64_t>(config.producers);
	aggregate.latency_ns.reserve(static_cast<size_t>(aggregate.expected));
	const uint64_t wall_start = monotonic_ns();
	std::vector<ChildProcess> children;
	children.reserve(static_cast<size_t>(config.producers + config.consumers));
	int ready_pipe[2] = {-1, -1};
	if (::pipe(ready_pipe) != 0) {
		std::fprintf(stderr, "READY_PIPE_FAIL run=%" PRIu32 " errno=%d\n", run_index, errno);
		(void)remove_and_verify_resources(&pool.value(), &queue.value(), pool_opts, queue_opts,
		                                  run_index, false, false);
		return 2;
	}
	const int read_flags = ::fcntl(ready_pipe[0], F_GETFD);
	if (read_flags >= 0) (void)::fcntl(ready_pipe[0], F_SETFD, read_flags | FD_CLOEXEC);
	const uint64_t base_target = aggregate.expected / config.consumers;
	const uint64_t remainder = aggregate.expected % config.consumers;

	for (uint32_t i = 0; i < config.consumers; ++i) {
		const uint64_t target = base_target + (static_cast<uint64_t>(i) < remainder ? 1 : 0);
		const std::string report = make_report_path(run_index, i);
		if (!prepare_report_path(report)) {
			std::fprintf(stderr, "REPORT_PATH_FAIL run=%" PRIu32 " role=consumer path=%s errno=%d\n",
			             run_index, report.c_str(), errno);
			++aggregate.errors;
			continue;
		}
		std::vector<std::string> args = {
		        executable,
		        "--role",
		        "consumer",
		        "--block-size",
		        std::to_string(config.block_size),
		        "--block-count",
		        std::to_string(config.block_count),
		        "--pool-name",
		        pool_name,
		        "--queue-name",
		        queue_name,
		        "--capacity",
		        std::to_string(config.capacity),
		        "--producer-count",
		        std::to_string(config.producers),
		        "--messages",
		        std::to_string(config.messages),
		        "--consumer-target",
		        std::to_string(target),
		        "--test-consumer-hold-ms",
		        std::to_string(config.test_consumer_hold_ms),
		        "--ready-fd",
		        std::to_string(ready_pipe[1]),
		        "--report-file",
		        report};
		if (config.test_ignore_term_consumer) args.push_back("--test-ignore-term-consumer");
		const pid_t pid = spawn_child(executable, args);
		if (pid < 0) {
			std::fprintf(stderr, "FORK_FAIL run=%" PRIu32 " role=consumer errno=%d\n", run_index,
			             errno);
			++aggregate.errors;
			continue;
		}
		children.push_back({pid, ChildRole::kConsumer, report, target});
	}
	for (uint32_t i = 0; i < config.producers; ++i) {
		const std::string report = make_report_path(run_index, config.consumers + i);
		if (!prepare_report_path(report)) {
			std::fprintf(stderr, "REPORT_PATH_FAIL run=%" PRIu32 " role=producer path=%s errno=%d\n",
			             run_index, report.c_str(), errno);
			++aggregate.errors;
			continue;
		}
		const std::vector<std::string> args = {
		        executable,
		        "--role",
		        "producer",
		        "--block-size",
		        std::to_string(config.block_size),
		        "--block-count",
		        std::to_string(config.block_count),
		        "--pool-name",
		        pool_name,
		        "--queue-name",
		        queue_name,
		        "--capacity",
		        std::to_string(config.capacity),
		        "--producer-id",
		        std::to_string(i),
		        "--messages",
		        std::to_string(config.messages),
		        "--ready-fd",
		        std::to_string(ready_pipe[1]),
		        "--report-file",
		        report};
		const pid_t pid = spawn_child(executable, args);
		if (pid < 0) {
			std::fprintf(stderr, "FORK_FAIL run=%" PRIu32 " role=producer errno=%d\n", run_index,
			             errno);
			++aggregate.errors;
			continue;
		}
		children.push_back({pid, ChildRole::kProducer, report, 0});
	}
	(void)::close(ready_pipe[1]);
	ready_pipe[1] = -1;
	bool ready_failed = false;
	const bool ready = wait_for_ready(ready_pipe[0], children.size(), config.timeout_ms, &ready_failed);
	(void)::close(ready_pipe[0]);
	ready_pipe[0] = -1;
	aggregate.ready_failed = !ready || ready_failed;
	if (ready && !aggregate.ready_failed &&
	    !write_ready_file(config, pool_name, queue_name, run_index, children)) {
		std::fprintf(stderr, "READY_FILE_FAIL run=%" PRIu32 " path=%s errno=%d\n", run_index,
		             config.ready_file.c_str(), errno);
		aggregate.ready_failed = true;
	}
	if (aggregate.ready_failed) g_stop_requested = 1;

	bool timed_out = false;
	bool child_kill_sent = false;
	const bool all_reaped =
	        reap_children(&children, config.timeout_ms, &timed_out, &child_kill_sent);
	aggregate.timed_out = !all_reaped || timed_out || aggregate.ready_failed ||
	                      g_stop_requested != 0;
	aggregate.child_kill_sent = child_kill_sent;
	std::vector<ParsedReport> reports;
	reports.reserve(children.size());
	for (const auto& child : children) {
		aggregate.user_cpu_us += rusage_user_us(child.usage);
		aggregate.system_cpu_us += rusage_system_us(child.usage);
		if (!child_succeeded(child)) ++aggregate.errors;
		reports.push_back(read_report(child.report_path, child.role, child.expected_records));
	}
	for (size_t i = 0; i < children.size(); ++i) {
		const ParsedReport& report = reports[i];
		if (!report.valid) {
			++aggregate.errors;
			aggregate.perf_available = false;
			continue;
		}
		aggregate.published += report.header.published;
		aggregate.delivered += report.header.delivered;
		aggregate.pool_full += report.header.pool_full;
		aggregate.pool_contention += report.header.pool_contention;
		aggregate.queue_full += report.header.queue_full;
		aggregate.queue_empty += report.header.queue_empty;
		aggregate.queue_contention += report.header.queue_contention;
		aggregate.recovery += report.header.recovery;
		aggregate.errors += report.header.errors;
		aggregate.invalid_payload += report.header.invalid_payload;
		if (report.header.perf_available == 0) {
			aggregate.perf_available = false;
		} else {
			aggregate.perf_context_switches += report.header.perf_context_switches;
		}
		if (children[i].role == ChildRole::kConsumer) {
			aggregate.latency_ns.reserve(aggregate.latency_ns.size() + report.records.size());
			for (const auto& record : report.records) {
				aggregate.latency_ns.push_back(record.latency_ns);
			}
		}
	}
	if (aggregate.timed_out) ++aggregate.errors;
	aggregate.wall_us = (monotonic_ns() - wall_start) / 1000ull;

	bool ids_valid = aggregate.expected <= kMaxExpectedMessages &&
	                 aggregate.delivered == aggregate.expected &&
	                 aggregate.published == aggregate.expected && !aggregate.timed_out &&
	                 aggregate.latency_ns.size() == static_cast<size_t>(aggregate.expected);
	std::vector<uint8_t> seen;
	if (aggregate.expected <= kMaxExpectedMessages) {
		seen.assign(static_cast<size_t>(aggregate.expected), 0);
	} else {
		ids_valid = false;
	}
	for (size_t i = 0; i < children.size(); ++i) {
		if (children[i].role != ChildRole::kConsumer || !reports[i].valid) continue;
		for (const auto& record : reports[i].records) {
			if (record.producer >= config.producers || record.sequence >= config.messages) {
				ids_valid = false;
				continue;
			}
			const uint64_t index =
			        static_cast<uint64_t>(record.producer) * config.messages + record.sequence;
			if (index >= aggregate.expected || seen[static_cast<size_t>(index)] != 0) {
				ids_valid = false;
				continue;
			}
			seen[static_cast<size_t>(index)] = 1;
		}
	}
	if (ids_valid) {
		for (uint8_t value : seen) {
			if (value == 0) {
				ids_valid = false;
				break;
			}
		}
	}
	if (!config.out_dir.empty()) {
		if (!mkdirs(config.out_dir)) {
			std::fprintf(stderr, "OUTPUT_DIR_FAIL path=%s errno=%d\n", config.out_dir.c_str(),
			             errno);
			++aggregate.errors;
			ids_valid = false;
		} else {
			std::vector<DeliveryRecord> records;
			for (size_t i = 0; i < children.size(); ++i) {
				if (children[i].role == ChildRole::kConsumer && reports[i].valid) {
					records.insert(records.end(), reports[i].records.begin(),
					               reports[i].records.end());
				}
			}
			if (!write_latency_csv(config.out_dir, run_index, config, records)) {
				std::fprintf(stderr, "LATENCY_OUTPUT_FAIL run=%" PRIu32 " errno=%d\n", run_index,
				             errno);
				++aggregate.errors;
				ids_valid = false;
			}
		}
	}
	for (const auto& child : children) (void)::unlink(child.report_path.c_str());
	const bool drained = drain_published_descriptors(&pool.value(), &queue.value(),
	                                                aggregate.expected);
	std::printf("CLEANUP_DRAIN run=%" PRIu32 " published_descriptors=%s\n", run_index,
	            drained ? "SETTLED" : "INCOMPLETE");
	std::fflush(stdout);
	if (!drained) {
		++aggregate.errors;
		ids_valid = false;
	}
	if (!remove_and_verify_resources(&pool.value(), &queue.value(), pool_opts, queue_opts,
	                                 run_index, g_stop_requested != 0, child_kill_sent)) {
		std::fprintf(stderr, "REMOVE_FAIL run=%" PRIu32 "\n", run_index);
		++aggregate.errors;
		ids_valid = false;
	}
	aggregate.correctness = ids_valid && aggregate.errors == 0;
	print_summary(run_index, config, aggregate);
	return aggregate.correctness ? 0 : 1;
}

int run_parent(const std::string& executable, const Config& config, uint32_t first_run) {
	int result = 0;
	for (uint32_t i = 0; i < config.runs; ++i) {
		if (run_parent_once(executable, config, first_run + i) != 0) result = 1;
		if (g_stop_requested != 0) break;
	}
	return result;
}

bool parse_config(int argc, char** argv, Config* config) {
	const uint64_t block_size = edge_tool::arg_u64(argc, argv, "--block-size", 4096);
	const uint64_t block_count = edge_tool::arg_u64(argc, argv, "--block-count", 1);
	const uint64_t producers = edge_tool::arg_u64(argc, argv, "--producers", 1);
	const uint64_t consumers = edge_tool::arg_u64(argc, argv, "--consumers", 1);
	const uint64_t messages = edge_tool::arg_u64(argc, argv, "--messages", 1000);
	const uint64_t capacity = edge_tool::arg_u64(argc, argv, "--capacity", 64);
	const uint64_t runs = edge_tool::arg_u64(argc, argv, "--runs", 1);
	config->timeout_ms = edge_tool::arg_u64(argc, argv, "--timeout-ms", 30000);
	config->test_consumer_hold_ms =
	        edge_tool::arg_u64(argc, argv, "--test-consumer-hold-ms", 0);
	config->test_ignore_term_consumer =
	        edge_tool::arg_flag(argc, argv, "--test-ignore-term-consumer");
	const char* out_dir = edge_tool::arg_value(argc, argv, "--out-dir");
	const char* ready_file = edge_tool::arg_value(argc, argv, "--ready-file");
	if (block_size != 4096 && block_size != 16384) {
		std::fprintf(stderr, "--block-size must be 4096 or 16384\n");
		return false;
	}
	if (block_count == 0 || block_count > 4096 || producers == 0 || producers > kMaxWorkers ||
	    consumers == 0 || consumers > kMaxWorkers || messages == 0 ||
	    messages > kMaxMessagesPerProducer || capacity == 0 || capacity > 4096 || runs == 0 ||
	    runs > kMaxRuns || config->timeout_ms == 0 || config->test_consumer_hold_ms > 60000) {
		std::fprintf(stderr, "invalid benchmark bounds\n");
		return false;
	}
	if (messages > std::numeric_limits<uint64_t>::max() / producers ||
	    messages * producers > kMaxExpectedMessages) {
		std::fprintf(stderr, "expected message count exceeds benchmark bound\n");
		return false;
	}
	config->block_size = static_cast<uint32_t>(block_size);
	config->block_count = static_cast<uint32_t>(block_count);
	config->producers = static_cast<uint32_t>(producers);
	config->consumers = static_cast<uint32_t>(consumers);
	config->messages = messages;
	config->capacity = static_cast<uint32_t>(capacity);
	config->runs = static_cast<uint32_t>(runs);
	config->out_dir = out_dir == nullptr ? "" : out_dir;
	config->ready_file = ready_file == nullptr ? "" : ready_file;
	return true;
}

void print_usage(const char* executable) {
	std::printf(
	        "usage: %s [--block-size 4096|16384] [--block-count N] [--producers N] "
	        "[--consumers N] [--messages N] [--capacity N] [--runs N] [--timeout-ms N] "
	        "[--test-consumer-hold-ms N] [--test-ignore-term-consumer] "
	        "[--out-dir DIR] [--ready-file FILE]\n       %s --smoke\n",
	        executable, executable);
}

int run_smoke(const std::string& executable) {
	const uint32_t block_sizes[] = {4096, 16384};
	uint32_t run_index = 1;
	for (uint32_t block_size : block_sizes) {
		for (uint32_t mode = 1; mode <= 2; ++mode) {
			Config config;
			config.block_size = block_size;
			config.block_count = mode;
			config.producers = mode;
			config.consumers = mode;
			config.messages = mode == 1 ? 24 : 16;
			config.capacity = 8;
			config.runs = 1;
			config.timeout_ms = 10000;
			config.out_dir.clear();
			const int status = run_parent(executable, config, run_index);
			++run_index;
			if (status != 0) return status;
		}
	}
	return 0;
}

int run_as_child(int argc, char** argv) {
	const char* role = edge_tool::arg_value(argc, argv, "--role");
	if (role == nullptr) return 2;
	const uint64_t block_size = edge_tool::arg_u64(argc, argv, "--block-size", 0);
	if (block_size != 4096 && block_size != 16384) return 2;
	if (std::strcmp(role, "producer") == 0) return run_child_producer(argc, argv);
	if (std::strcmp(role, "consumer") == 0) return run_child_consumer(argc, argv);
	return 2;
}

}  // namespace

int main(int argc, char** argv) {
	if (!install_stop_signal_handler()) return 2;
	if (edge_tool::arg_value(argc, argv, "--role") != nullptr) return run_as_child(argc, argv);
	if (edge_tool::arg_flag(argc, argv, "--help")) {
		print_usage(argv[0]);
		return 0;
	}
	const std::string executable = self_executable();
	if (executable.empty()) {
		std::fprintf(stderr, "cannot resolve /proc/self/exe\n");
		return 2;
	}
	if (edge_tool::arg_flag(argc, argv, "--smoke")) return run_smoke(executable);
	Config config;
	if (!parse_config(argc, argv, &config)) {
		print_usage(argv[0]);
		return 2;
	}
	return run_parent(executable, config, 1);
}
