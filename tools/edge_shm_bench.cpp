// edge_shm_bench: the ER7 benchmark driver (design §21). Each run is one cell of
// the §21.2 matrix (transport x mode x payload x rate x placement). The driver
// FORKS+EXECs the separate helper binaries — edge_shm_bench_producer /
// edge_shm_bench_consumer for ShmChannel, edge_shm_bench_sock for the Unix
// domain socket baseline (§18.3: never an in-process thread) — under optional
// taskset pinning, collects their one-line markers and wait4 rusage, then
// writes evidence/performance/<run_id>/ per §21.4:
//
//   environment.txt  command.txt  samples.csv  summary.json
//   perf_stat.txt    process_status.txt       RESULT.md
//
// Sets: --smoke (CTest dev-loop gate), --evidence (the curated §21.1 Q1-Q6
// answer set), --matrix (the full grid — long), --spec <csv> (one ad-hoc cell).
// Every result is labeled VM_ONLY: this host's perf_event_paranoid=4 blocks
// perf, so perf_stat.txt records the probe failure rather than fake numbers.

#include <sched.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "tool_common.hpp"

namespace {

using edge_tool::arg_u64;
using edge_tool::arg_value;

constexpr const char* kPerfEventParanoidPath = "/proc/sys/kernel/perf_event_paranoid";
constexpr const char* kYamaPtraceScopePath = "/proc/sys/kernel/yama/ptrace_scope";

// ---------------------------------------------------------------------------
// RunSpec: one matrix cell.
// ---------------------------------------------------------------------------
struct RunSpec {
	std::string id;
	std::string transport;  // shm | socket
	std::string mode;       // futex | poll | block
	uint32_t payload = 64;
	std::string placement;   // same | different | unpinned
	std::string rate_label;  // max | 100 | 1k | 10k
	uint64_t rate_hz = 0;
	uint64_t samples = 1000000;
	uint64_t max_time_ms = 60000;
	uint64_t warmup = 10000;
};

struct Config {
	std::string producer_bin;
	std::string consumer_bin;
	std::string sock_bin;
	std::string ctl_bin;
	std::string out_dir = "evidence/performance";
	int cpu_a = 0;
	int cpu_b = 1;
	bool taskset_ok_a = false;
	bool taskset_ok_b = false;
	std::string git_dir = ".";
	std::string git_commit = "n/a";
	std::string perf_probe;
	bool perf_ok = false;
};

struct ChildResult {
	int exit_code = 127;
	int signum = 0;
	bool exec_ok = false;
	std::string out;
	uint64_t wall_us = 0;
	struct rusage ru {};
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
bool mkdirs(const std::string& path) {
	std::string cur;
	for (size_t i = 0; i < path.size(); ++i) {
		cur.push_back(path[i]);
		if (path[i] == '/' || i + 1 == path.size()) {
			if (cur.empty() || cur == "/") continue;
			if (::mkdir(cur.c_str(), 0755) != 0 && errno != EEXIST) return false;
		}
	}
	return true;
}

std::string read_sys_file(const char* path) {
	FILE* f = std::fopen(path, "r");
	if (f == nullptr) return "n/a";
	char buf[64]{};
	const size_t n = std::fread(buf, 1, sizeof(buf) - 1, f);
	std::fclose(f);
	buf[n] = '\0';
	std::string s = buf;
	while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) s.pop_back();
	return s;
}

struct ChildHandle {
	pid_t pid = -1;
	int pipe_read = -1;
	struct timespec spawned {};
	bool valid() const { return pid > 0 && pipe_read >= 0; }
};

// fork + exec WITHOUT waiting (the benchmark pair must run concurrently: the
// producer and consumer overlap in time, so the driver spawns both and only
// then collects each). The helper processes write only small marker output, so
// the pipe never fills between spawn and collect.
ChildHandle spawn_child(const std::vector<std::string>& argv) {
	int pipefd[2] = {-1, -1};
	if (::pipe(pipefd) != 0) return {};
	const pid_t pid = ::fork();
	if (pid < 0) {
		::close(pipefd[0]);
		::close(pipefd[1]);
		return {};
	}
	if (pid == 0) {
		::close(pipefd[0]);
		::dup2(pipefd[1], STDOUT_FILENO);
		::dup2(pipefd[1], STDERR_FILENO);
		::close(pipefd[1]);
		std::vector<char*> cargv;
		cargv.reserve(argv.size() + 1);
		for (const auto& a : argv) cargv.push_back(const_cast<char*>(a.c_str()));
		cargv.push_back(nullptr);
		::execvp(cargv[0], cargv.data());
		std::fprintf(stderr, "exec %s: %s\n", cargv[0], std::strerror(errno));
		_exit(127);
	}
	::close(pipefd[1]);
	struct timespec spawned {};
	::clock_gettime(CLOCK_MONOTONIC, &spawned);
	return {pid, pipefd[0], spawned};
}

// Read the child's stdout/stderr to EOF, then wait4 for rusage. Wall time is
// measured from the spawn timestamp, NOT around wait4 — by the time the pipe
// hits EOF the child has already exited, so a wait4-adjacent clock pair would
// measure ~0 and produce a bogus throughput.
ChildResult collect_child(ChildHandle h) {
	ChildResult res;
	if (!h.valid()) return res;
	std::string out;
	char buf[4096];
	for (;;) {
		const ssize_t n = ::read(h.pipe_read, buf, sizeof(buf));
		if (n > 0) {
			out.append(buf, static_cast<size_t>(n));
		} else if (n == 0) {
			break;
		} else if (errno == EINTR) {
			continue;
		} else {
			break;
		}
	}
	::close(h.pipe_read);
	res.out = out;
	int status = 0;
	::wait4(h.pid, &status, 0, &res.ru);
	struct timespec t1 {};
	::clock_gettime(CLOCK_MONOTONIC, &t1);
	int64_t ns = static_cast<int64_t>(t1.tv_nsec) - static_cast<int64_t>(h.spawned.tv_nsec);
	int64_t sec = static_cast<int64_t>(t1.tv_sec) - static_cast<int64_t>(h.spawned.tv_sec);
	if (ns < 0) {
		ns += 1000000000LL;
		sec -= 1;
	}
	res.wall_us = static_cast<uint64_t>(sec) * 1000000ull + static_cast<uint64_t>(ns) / 1000ull;
	if (WIFEXITED(status)) {
		res.exec_ok = true;
		res.exit_code = WEXITSTATUS(status);
	} else if (WIFSIGNALED(status)) {
		res.exec_ok = true;
		res.signum = WTERMSIG(status);
	}
	return res;
}

// Sequential convenience wrapper (git/perf probes, ctl cleanup): the child must
// run to completion before the caller proceeds.
ChildResult spawn_collect(const std::vector<std::string>& argv) {
	return collect_child(spawn_child(argv));
}

uint64_t rusage_cpu_us(const struct rusage& ru) {
	return static_cast<uint64_t>(ru.ru_utime.tv_sec) * 1000000ull +
	       static_cast<uint64_t>(ru.ru_utime.tv_usec) +
	       static_cast<uint64_t>(ru.ru_stime.tv_sec) * 1000000ull +
	       static_cast<uint64_t>(ru.ru_stime.tv_usec);
}

bool cpu_allowed(int cpu) {
	cpu_set_t set;
	CPU_ZERO(&set);
	if (::sched_getaffinity(0, sizeof(set), &set) != 0) return true;
	return CPU_ISSET(cpu, &set) != 0;
}

std::string placement_prefix(const Config& cfg, const RunSpec& spec, bool producer) {
	if (spec.placement == "same") {
		return cfg.taskset_ok_a ? "taskset -c " + std::to_string(cfg.cpu_a) : "";
	}
	if (spec.placement == "different") {
		const bool ok = producer ? cfg.taskset_ok_a : cfg.taskset_ok_b;
		const int cpu = producer ? cfg.cpu_a : cfg.cpu_b;
		return ok ? "taskset -c " + std::to_string(cpu) : "";
	}
	return "";
}

std::vector<std::string> split_words(const std::string& s) {
	std::vector<std::string> out;
	size_t i = 0;
	while (i < s.size()) {
		while (i < s.size() && (s[i] == ' ' || s[i] == '\t')) ++i;
		const size_t start = i;
		while (i < s.size() && s[i] != ' ' && s[i] != '\t') ++i;
		if (i > start) out.push_back(s.substr(start, i - start));
	}
	return out;
}

// ---------------------------------------------------------------------------
// markers
// ---------------------------------------------------------------------------
struct ProducerMarkers {
	uint64_t published = 0;
	uint64_t cpu_us = 0;
	bool ok = false;
};

struct ConsumerMarkers {
	uint64_t reads = 0;
	uint64_t torn = 0;
	uint64_t missed_total = 0;
	uint64_t last_seq = 0;
	uint64_t elapsed_ms = 0;
	uint64_t cpu_us = 0;
	std::string ended;
	bool ok = false;
};

ProducerMarkers parse_producer(const std::string& out) {
	ProducerMarkers m;
	const char* done = std::strstr(out.c_str(), "DONE ");
	if (done == nullptr) return m;
	unsigned long long pub = 0;
	unsigned long long cpu = 0;
	if (std::sscanf(done, "DONE published=%llu cpu_us=%llu", &pub, &cpu) == 2) {
		m.published = static_cast<uint64_t>(pub);
		m.cpu_us = static_cast<uint64_t>(cpu);
		m.ok = true;
	}
	return m;
}

ConsumerMarkers parse_consumer(const std::string& out) {
	ConsumerMarkers m;
	const char* sum = std::strstr(out.c_str(), "SUMMARY ");
	if (sum == nullptr) return m;
	unsigned long long reads = 0, torn = 0, missed = 0, last = 0, el = 0, cpu = 0;
	char ended[16] = "";
	if (std::sscanf(sum,
	                "SUMMARY reads=%llu torn=%llu missed_total=%llu last_seq=%llu "
	                "elapsed_ms=%llu cpu_us=%llu ended=%15s",
	                &reads, &torn, &missed, &last, &el, &cpu, ended) == 7) {
		m.reads = static_cast<uint64_t>(reads);
		m.torn = static_cast<uint64_t>(torn);
		m.missed_total = static_cast<uint64_t>(missed);
		m.last_seq = static_cast<uint64_t>(last);
		m.elapsed_ms = static_cast<uint64_t>(el);
		m.cpu_us = static_cast<uint64_t>(cpu);
		m.ended = ended;
		m.ok = true;
	}
	return m;
}

// ---------------------------------------------------------------------------
// CSV + stats
// ---------------------------------------------------------------------------
struct RawSample {
	uint64_t seq = 0;
	uint64_t gen = 0;
	uint32_t payload = 0;
	uint64_t publish_ns = 0;
	uint64_t receive_ns = 0;
	uint64_t latency_ns = 0;
	uint64_t missed = 0;
};

bool parse_raw_csv(const std::string& path, std::vector<RawSample>* out) {
	FILE* f = std::fopen(path.c_str(), "r");
	if (f == nullptr) return false;
	char line[256];
	while (std::fgets(line, sizeof(line), f) != nullptr) {
		RawSample s;
		if (std::sscanf(line,
		                "%" SCNu64 ",%" SCNu64 ",%u,%" SCNu64 ",%" SCNu64 ",%" SCNu64
		                ",%" SCNu64,
		                &s.seq, &s.gen, &s.payload, &s.publish_ns, &s.receive_ns,
		                &s.latency_ns, &s.missed) == 7) {
			out->push_back(s);
		}
	}
	std::fclose(f);
	return true;
}

struct LatencyStats {
	uint64_t n = 0;
	double mean = 0.0;
	double stddev = 0.0;
	uint64_t p50 = 0;
	uint64_t p95 = 0;
	uint64_t p99 = 0;
	uint64_t min = 0;
	uint64_t max = 0;
};

uint64_t percentile_sorted(const std::vector<uint64_t>& v, double pct) {
	if (v.empty()) return 0;
	const double idx = std::ceil(pct / 100.0 * static_cast<double>(v.size())) - 1.0;
	const size_t i = idx < 0.0 ? 0 : static_cast<size_t>(idx);
	return v[std::min(i, v.size() - 1)];
}

LatencyStats compute_stats(const std::vector<RawSample>& rows, size_t warmup) {
	LatencyStats st;
	if (rows.size() <= warmup) return st;
	std::vector<uint64_t> lat;
	lat.reserve(rows.size() - warmup);
	uint64_t sum = 0;
	for (size_t i = warmup; i < rows.size(); ++i) {
		const uint64_t l = rows[i].latency_ns;
		lat.push_back(l);
		sum += l;
	}
	const size_t n = lat.size();
	st.n = static_cast<uint64_t>(n);
	st.min = *std::min_element(lat.begin(), lat.end());
	st.max = *std::max_element(lat.begin(), lat.end());
	st.mean = static_cast<double>(sum) / static_cast<double>(n);
	std::vector<uint64_t> sorted = lat;
	std::sort(sorted.begin(), sorted.end());
	st.p50 = percentile_sorted(sorted, 50.0);
	st.p95 = percentile_sorted(sorted, 95.0);
	st.p99 = percentile_sorted(sorted, 99.0);
	double var = 0.0;
	for (const uint64_t l : lat) {
		const double d = static_cast<double>(l) - st.mean;
		var += d * d;
	}
	st.stddev = std::sqrt(var / static_cast<double>(n));
	return st;
}

std::string json_num(double v) {
	char buf[32];
	std::snprintf(buf, sizeof(buf), "%.3f", v);
	return buf;
}

// ---------------------------------------------------------------------------
// evidence writers
// ---------------------------------------------------------------------------
void write_environment(const Config& cfg, const RunSpec& spec, const std::string& dir) {
	std::string s;
	s += "generated_by=edge_shm_bench\n";
	s += "date=" + std::string(__DATE__) + " " + __TIME__ + "\n";
	s += "git_commit=" + cfg.git_commit + "\n";
	s += "git_dir=" + cfg.git_dir + "\n";
	struct utsname un {};
	if (::uname(&un) == 0) {
		s += std::string("uname=") + un.sysname + " " + un.release + " " + un.version +
		     "\n";
		s += "machine=" + std::string(un.machine) + "\n";
	}
	s += "nproc=" + std::to_string(::sysconf(_SC_NPROCESSORS_ONLN)) + "\n";
	s += "perf_event_paranoid=" + read_sys_file(kPerfEventParanoidPath) + "\n";
	s += "yama_ptrace_scope=" + read_sys_file(kYamaPtraceScopePath) + "\n";
	s += "label=VM_ONLY (benchmark host is a VM; results are not generalizable to "
	     "hard-real-time or production control loops)\n";
	s += "placement=" + spec.placement + " (cpu_a=" + std::to_string(cfg.cpu_a) +
	     " available=" + (cfg.taskset_ok_a ? "yes" : "no") +
	     ", cpu_b=" + std::to_string(cfg.cpu_b) +
	     " available=" + (cfg.taskset_ok_b ? "yes" : "no") + ")\n";
	s += std::string("perf_stat=") + (cfg.perf_ok ? "available" : "UNAVAILABLE (probe below)") +
	     "\n";
	const std::string path = dir + "/environment.txt";
	FILE* f = std::fopen(path.c_str(), "w");
	if (f != nullptr) {
		std::fwrite(s.data(), 1, s.size(), f);
		std::fclose(f);
	}
}

void write_command(const RunSpec& spec, const std::string& dir,
                   const std::vector<std::string>& producer_argv,
                   const std::vector<std::string>& consumer_argv) {
	std::string s = "driver: edge_shm_bench " + spec.id + "\n\n";
	s += "producer:\n  ";
	for (const auto& a : producer_argv) s += a + " ";
	s += "\n\nconsumer:\n  ";
	for (const auto& a : consumer_argv) s += a + " ";
	s += "\n";
	const std::string path = dir + "/command.txt";
	FILE* f = std::fopen(path.c_str(), "w");
	if (f != nullptr) {
		std::fwrite(s.data(), 1, s.size(), f);
		std::fclose(f);
	}
}

void write_process_status(const Config&, const std::string& dir, const ChildResult& prod,
                          const ChildResult& cons) {
	std::string s;
	s += "producer (wait4 rusage):\n";
	s += "  cpu_us=" + std::to_string(rusage_cpu_us(prod.ru)) +
	     " voluntary_csw=" + std::to_string(prod.ru.ru_nvcsw) +
	     " nonvoluntary_csw=" + std::to_string(prod.ru.ru_nivcsw) + "\n";
	s += "  minor_faults=" + std::to_string(prod.ru.ru_minflt) +
	     " major_faults=" + std::to_string(prod.ru.ru_majflt) +
	     " maxrss_kb=" + std::to_string(prod.ru.ru_maxrss) + "\n";
	s += "consumer (wait4 rusage):\n";
	s += "  cpu_us=" + std::to_string(rusage_cpu_us(cons.ru)) +
	     " voluntary_csw=" + std::to_string(cons.ru.ru_nvcsw) +
	     " nonvoluntary_csw=" + std::to_string(cons.ru.ru_nivcsw) + "\n";
	s += "  minor_faults=" + std::to_string(cons.ru.ru_minflt) +
	     " major_faults=" + std::to_string(cons.ru.ru_majflt) +
	     " maxrss_kb=" + std::to_string(cons.ru.ru_maxrss) + "\n";
	s += "note: rusage accounts the whole helper process (open + create + run + teardown).\n";
	const std::string path = dir + "/process_status.txt";
	FILE* f = std::fopen(path.c_str(), "w");
	if (f != nullptr) {
		std::fwrite(s.data(), 1, s.size(), f);
		std::fclose(f);
	}
}

void write_perf_stat(const Config& cfg, const std::string& dir) {
	const std::string path = dir + "/perf_stat.txt";
	FILE* f = std::fopen(path.c_str(), "w");
	if (f == nullptr) return;
	if (cfg.perf_ok) {
		std::fputs(cfg.perf_probe.c_str(), f);
	} else {
		std::fputs("perf stat is UNAVAILABLE on this host (VM_ONLY evidence).\n", f);
		std::fputs("kernel perf_event_paranoid=", f);
		std::fputs(read_sys_file(kPerfEventParanoidPath).c_str(), f);
		std::fputs(" blocks non-root perf_event_open.\n", f);
		std::fputs("probe result:\n", f);
		std::fputs(cfg.perf_probe.c_str(), f);
		std::fputs(
		        "\nper-run perf sampling was therefore not attempted; the "
		        "artifact is the honest negative result.\n",
		        f);
	}
	std::fclose(f);
}

void write_summary_json(const Config& cfg, const RunSpec& spec, const std::string& dir,
                        const std::vector<RawSample>& rows, size_t warmup, const LatencyStats& st,
                        const ChildResult& prod, const ChildResult& cons, uint64_t published,
                        uint64_t missed_total, uint64_t torn) {
	const size_t measured = rows.size() > warmup ? rows.size() - warmup : 0;
	uint64_t read_span_ns = 0;
	if (measured >= 2) {
		read_span_ns = rows[rows.size() - 1].receive_ns - rows[warmup].receive_ns;
	}
	const double read_tps = read_span_ns > 0 ? static_cast<double>(measured) * 1e9 /
	                                                   static_cast<double>(read_span_ns)
	                                         : 0.0;
	const double publish_tps = prod.wall_us > 0 ? static_cast<double>(published) * 1e6 /
	                                                      static_cast<double>(prod.wall_us)
	                                            : 0.0;

	std::string j;
	j += "{\n";
	j += "  \"run_id\": \"" + spec.id + "\",\n";
	j += "  \"transport\": \"" + spec.transport + "\",\n";
	j += "  \"mode\": \"" + spec.mode + "\",\n";
	j += "  \"payload_bytes\": " + std::to_string(spec.payload) + ",\n";
	j += "  \"placement\": \"" + spec.placement + "\",\n";
	j += "  \"rate_hz\": " + std::to_string(spec.rate_hz) + ",\n";
	j += "  \"rate_label\": \"" + spec.rate_label + "\",\n";
	j += "  \"count\": " + std::to_string(rows.size()) + ",\n";
	j += "  \"warmup_count\": " + std::to_string(warmup) + ",\n";
	j += "  \"measured_count\": " + std::to_string(measured) + ",\n";
	j += "  \"latency_ns\": {\n";
	j += "    \"min\": " + std::to_string(st.min) + ",\n";
	j += "    \"mean\": " + json_num(st.mean) + ",\n";
	j += "    \"p50\": " + std::to_string(st.p50) + ",\n";
	j += "    \"p95\": " + std::to_string(st.p95) + ",\n";
	j += "    \"p99\": " + std::to_string(st.p99) + ",\n";
	j += "    \"max\": " + std::to_string(st.max) + ",\n";
	j += "    \"stddev\": " + json_num(st.stddev) + "\n";
	j += "  },\n";
	j += "  \"publish_throughput_per_s\": " + json_num(publish_tps) + ",\n";
	j += "  \"read_throughput_per_s\": " + json_num(read_tps) + ",\n";
	j += "  \"producer_cpu_ms\": " +
	     json_num(static_cast<double>(rusage_cpu_us(prod.ru)) / 1000.0) + ",\n";
	j += "  \"consumer_cpu_ms\": " +
	     json_num(static_cast<double>(rusage_cpu_us(cons.ru)) / 1000.0) + ",\n";
	j += "  \"context_switches\": {\n";
	j += "    \"producer_voluntary\": " + std::to_string(prod.ru.ru_nvcsw) + ",\n";
	j += "    \"producer_nonvoluntary\": " + std::to_string(prod.ru.ru_nivcsw) + ",\n";
	j += "    \"consumer_voluntary\": " + std::to_string(cons.ru.ru_nvcsw) + ",\n";
	j += "    \"consumer_nonvoluntary\": " + std::to_string(cons.ru.ru_nivcsw) + "\n";
	j += "  },\n";
	j += "  \"page_faults\": {\n";
	j += "    \"producer_minor\": " + std::to_string(prod.ru.ru_minflt) + ",\n";
	j += "    \"producer_major\": " + std::to_string(prod.ru.ru_majflt) + ",\n";
	j += "    \"consumer_minor\": " + std::to_string(cons.ru.ru_minflt) + ",\n";
	j += "    \"consumer_major\": " + std::to_string(cons.ru.ru_majflt) + "\n";
	j += "  },\n";
	j += "  \"missed_samples\": " + std::to_string(missed_total) + ",\n";
	j += "  \"torn_reads\": " + std::to_string(torn) + ",\n";
	j += "  \"git_commit\": \"" + cfg.git_commit + "\",\n";
	j += "  \"environment\": \"VM_ONLY\"\n";
	j += "}\n";
	const std::string path = dir + "/summary.json";
	FILE* f = std::fopen(path.c_str(), "w");
	if (f != nullptr) {
		std::fwrite(j.data(), 1, j.size(), f);
		std::fclose(f);
	}
}

void write_result_md(const RunSpec& spec, const std::string& dir, const LatencyStats& st,
                     const ChildResult& prod, const ChildResult& cons, uint64_t published,
                     uint64_t missed_total, bool ok, const std::string& fail_reason) {
	std::string s;
	s += "# " + spec.id + "\n\n";
	s += "- transport: " + spec.transport + ", mode: " + spec.mode +
	     ", payload: " + std::to_string(spec.payload) + " B, placement: " + spec.placement +
	     ", rate: " + spec.rate_label + "\n";
	s += "- VM_ONLY (see environment.txt)\n\n";
	s += "| stat | value |\n|---|---|\n";
	s += "| measured samples | " + std::to_string(st.n) + " |\n";
	s += "| min / p50 / p95 / p99 / max latency (ns) | " + std::to_string(st.min) + " / " +
	     std::to_string(st.p50) + " / " + std::to_string(st.p95) + " / " +
	     std::to_string(st.p99) + " / " + std::to_string(st.max) + " |\n";
	s += "| mean / stddev (ns) | " + json_num(st.mean) + " / " + json_num(st.stddev) + " |\n";
	s += "| missed samples | " + std::to_string(missed_total) + " |\n";
	s += "| producer cpu (ms) | " +
	     json_num(static_cast<double>(rusage_cpu_us(prod.ru)) / 1000.0) + " |\n";
	s += "| consumer cpu (ms) | " +
	     json_num(static_cast<double>(rusage_cpu_us(cons.ru)) / 1000.0) + " |\n";
	s += "| published / read | " + std::to_string(published) + " / " + std::to_string(st.n) +
	     " |\n";
	s += "\n";
	if (!ok) s += "**RUN FAILED**: " + fail_reason + "\n";
	const std::string path = dir + "/RESULT.md";
	FILE* f = std::fopen(path.c_str(), "w");
	if (f != nullptr) {
		std::fwrite(s.data(), 1, s.size(), f);
		std::fclose(f);
	}
}

// Write the final samples.csv: header + raw rows + appended cpu columns.
bool compose_samples_csv(const std::string& dir, const std::vector<RawSample>& rows,
                         uint64_t producer_cpu_us, uint64_t consumer_cpu_us) {
	const std::string raw = dir + "/samples.raw.csv";
	const std::string final = dir + "/samples.csv";
	FILE* f = std::fopen(final.c_str(), "w");
	if (f == nullptr) return false;
	std::fputs(
	        "sequence,generation,payload_bytes,publish_ns,receive_ns,latency_ns,"
	        "missed_samples,producer_cpu,consumer_cpu\n",
	        f);
	for (const auto& s : rows) {
		std::fprintf(f,
		             "%" PRIu64 ",%" PRIu64 ",%u,%" PRIu64 ",%" PRIu64 ",%" PRIu64
		             ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
		             s.seq, s.gen, s.payload, s.publish_ns, s.receive_ns, s.latency_ns,
		             s.missed, producer_cpu_us, consumer_cpu_us);
	}
	std::fclose(f);
	::unlink(raw.c_str());
	return true;
}

// ---------------------------------------------------------------------------
// runs
// ---------------------------------------------------------------------------
struct RunOutcome {
	bool ok = false;
	std::string fail_reason;
	uint64_t measured = 0;
	uint64_t p50 = 0;
	uint64_t p95 = 0;
	uint64_t p99 = 0;
	uint64_t max_lat = 0;
	uint64_t missed = 0;
};

RunOutcome run_shm(const Config& cfg, const RunSpec& spec, size_t ordinal) {
	RunOutcome oc;
	const std::string dir = cfg.out_dir + "/" + spec.id;
	if (!mkdirs(dir)) {
		oc.fail_reason = "mkdir " + dir + " failed";
		return oc;
	}
	const std::string chan = "er7_bench_" + std::to_string(ordinal);
	const std::string csv_raw = dir + "/samples.raw.csv";

	std::vector<std::string> pargv;
	const std::string pp = placement_prefix(cfg, spec, true);
	if (!pp.empty()) {
		const auto w = split_words(pp);
		for (const auto& a : w) pargv.push_back(a);
	}
	pargv.push_back(cfg.producer_bin);
	pargv.push_back("--name");
	pargv.push_back(chan);
	pargv.push_back("--payload");
	pargv.push_back(std::to_string(spec.payload));
	pargv.push_back("--samples");
	pargv.push_back(std::to_string(spec.samples));
	pargv.push_back("--max-time-ms");
	pargv.push_back(std::to_string(spec.max_time_ms));
	pargv.push_back("--rate");
	pargv.push_back(std::to_string(spec.rate_hz));
	pargv.push_back("--checksum");
	pargv.push_back("1");

	std::vector<std::string> cargv;
	const std::string cp = placement_prefix(cfg, spec, false);
	if (!cp.empty()) {
		const auto w = split_words(cp);
		for (const auto& a : w) cargv.push_back(a);
	}
	cargv.push_back(cfg.consumer_bin);
	cargv.push_back("--name");
	cargv.push_back(chan);
	cargv.push_back("--payload");
	cargv.push_back(std::to_string(spec.payload));
	cargv.push_back("--csv");
	cargv.push_back(csv_raw);
	cargv.push_back("--mode");
	cargv.push_back(spec.mode);
	cargv.push_back("--max-samples");
	cargv.push_back(std::to_string(spec.samples));
	cargv.push_back("--max-time-ms");
	cargv.push_back(std::to_string(spec.max_time_ms));
	cargv.push_back("--checksum");
	cargv.push_back("1");

	write_command(spec, dir, pargv, cargv);

	// Spawn both first, then collect: producer and consumer must overlap in time
	// (the whole point of a latency measurement). Sequential spawn would let the
	// producer finish — or block forever on accept — before the consumer exists.
	const ChildHandle ph = spawn_child(pargv);
	const ChildHandle ch = spawn_child(cargv);
	ChildResult prod = collect_child(ph);
	ChildResult cons = collect_child(ch);

	const ProducerMarkers pm = parse_producer(prod.out);
	const ConsumerMarkers cm = parse_consumer(cons.out);

	std::vector<RawSample> rows;
	parse_raw_csv(csv_raw, &rows);
	const size_t warmup = spec.warmup <= rows.size() ? spec.warmup : 0;
	const LatencyStats st = compute_stats(rows, warmup);

	const uint64_t missed_total = cm.missed_total;
	write_process_status(cfg, dir, prod, cons);
	write_perf_stat(cfg, dir);
	write_environment(cfg, spec, dir);
	write_summary_json(cfg, spec, dir, rows, warmup, st, prod, cons, pm.published, missed_total,
	                   cm.torn);
	compose_samples_csv(dir, rows, rusage_cpu_us(prod.ru), rusage_cpu_us(cons.ru));
	write_result_md(spec, dir, st, prod, cons, pm.published, missed_total, true, "");

	std::string reason;
	if (prod.signum != 0) reason += "producer killed by signal " + std::to_string(prod.signum);
	if (!prod.exec_ok || prod.exit_code != 0) {
		if (!reason.empty()) reason += "; ";
		reason += "producer exit " + std::to_string(prod.exit_code);
		reason += " (" + (prod.out.empty() ? "no output" : prod.out) + ")";
	}
	if (cons.signum != 0)
		reason += "; consumer killed by signal " + std::to_string(cons.signum);
	if (!cons.exec_ok || cons.exit_code != 0) {
		if (!reason.empty()) reason += "; ";
		reason += "consumer exit " + std::to_string(cons.exit_code);
		if (cons.exit_code == 4) reason += " (torn read)";
	}
	if (cm.torn > 0) reason += "; torn_reads=" + std::to_string(cm.torn);
	if (rows.empty()) reason += "; empty samples";

	oc.ok = reason.empty();
	oc.fail_reason = reason;
	oc.measured = st.n;
	oc.p50 = st.p50;
	oc.p95 = st.p95;
	oc.p99 = st.p99;
	oc.max_lat = st.max;
	oc.missed = missed_total;

	if (!oc.ok)
		write_result_md(spec, dir, st, prod, cons, pm.published, missed_total, false,
		                reason);

	// Best-effort forensic + cleanup (never fail the run on these).
	if (!cfg.ctl_bin.empty()) {
		const std::vector<std::string> insp = {cfg.ctl_bin, "inspect", chan};
		const ChildResult ctlr = spawn_collect(insp);
		FILE* f = std::fopen((dir + "/ctl_inspect.txt").c_str(), "w");
		if (f != nullptr) {
			std::fwrite(ctlr.out.data(), 1, ctlr.out.size(), f);
			std::fclose(f);
		}
		const std::vector<std::string> rm = {cfg.ctl_bin, "remove", chan};
		(void)spawn_collect(rm);
	}
	return oc;
}

RunOutcome run_socket(const Config& cfg, const RunSpec& spec) {
	RunOutcome oc;
	const std::string dir = cfg.out_dir + "/" + spec.id;
	if (!mkdirs(dir)) {
		oc.fail_reason = "mkdir " + dir + " failed";
		return oc;
	}
	const std::string sock_path = dir + "/channel.sock";
	const std::string csv_raw = dir + "/samples.raw.csv";

	std::vector<std::string> pargv;
	const std::string pp = placement_prefix(cfg, spec, true);
	if (!pp.empty()) {
		const auto w = split_words(pp);
		for (const auto& a : w) pargv.push_back(a);
	}
	pargv.push_back(cfg.sock_bin);
	pargv.push_back("--role");
	pargv.push_back("producer");
	pargv.push_back("--sock-path");
	pargv.push_back(sock_path);
	pargv.push_back("--payload");
	pargv.push_back(std::to_string(spec.payload));
	pargv.push_back("--samples");
	pargv.push_back(std::to_string(spec.samples));
	pargv.push_back("--max-time-ms");
	pargv.push_back(std::to_string(spec.max_time_ms));
	pargv.push_back("--rate");
	pargv.push_back(std::to_string(spec.rate_hz));

	std::vector<std::string> cargv;
	const std::string cp = placement_prefix(cfg, spec, false);
	if (!cp.empty()) {
		const auto w = split_words(cp);
		for (const auto& a : w) cargv.push_back(a);
	}
	cargv.push_back(cfg.sock_bin);
	cargv.push_back("--role");
	cargv.push_back("consumer");
	cargv.push_back("--sock-path");
	cargv.push_back(sock_path);
	cargv.push_back("--payload");
	cargv.push_back(std::to_string(spec.payload));
	cargv.push_back("--csv");
	cargv.push_back(csv_raw);
	cargv.push_back("--samples");
	cargv.push_back(std::to_string(spec.samples));
	cargv.push_back("--max-time-ms");
	cargv.push_back(std::to_string(spec.max_time_ms));

	write_command(spec, dir, pargv, cargv);

	// Spawn both first, then collect: producer and consumer must overlap in time
	// (the whole point of a latency measurement). Sequential spawn would let the
	// producer finish — or block forever on accept — before the consumer exists.
	const ChildHandle ph = spawn_child(pargv);
	const ChildHandle ch = spawn_child(cargv);
	ChildResult prod = collect_child(ph);
	ChildResult cons = collect_child(ch);

	const ProducerMarkers pm = parse_producer(prod.out);
	const ConsumerMarkers cm = parse_consumer(cons.out);

	std::vector<RawSample> rows;
	parse_raw_csv(csv_raw, &rows);
	const size_t warmup = spec.warmup <= rows.size() ? spec.warmup : 0;
	const LatencyStats st = compute_stats(rows, warmup);

	write_process_status(cfg, dir, prod, cons);
	write_perf_stat(cfg, dir);
	write_environment(cfg, spec, dir);
	write_summary_json(cfg, spec, dir, rows, warmup, st, prod, cons, pm.published,
	                   cm.missed_total, cm.torn);
	compose_samples_csv(dir, rows, rusage_cpu_us(prod.ru), rusage_cpu_us(cons.ru));
	write_result_md(spec, dir, st, prod, cons, pm.published, cm.missed_total, true, "");

	std::string reason;
	if (!prod.exec_ok || prod.exit_code != 0 || prod.signum != 0) {
		reason += "producer exit " + std::to_string(prod.exit_code) + " signum " +
		          std::to_string(prod.signum) + " (" + prod.out + ")";
	}
	if (!cons.exec_ok || cons.exit_code != 0 || cons.signum != 0) {
		if (!reason.empty()) reason += "; ";
		reason += "consumer exit " + std::to_string(cons.exit_code);
		if (cons.exit_code == 4) reason += " (torn read)";
	}
	if (cm.torn > 0) reason += "; torn_reads=" + std::to_string(cm.torn);
	if (rows.empty()) reason += "; empty samples";

	oc.ok = reason.empty();
	oc.fail_reason = reason;
	oc.measured = st.n;
	oc.p50 = st.p50;
	oc.p95 = st.p95;
	oc.p99 = st.p99;
	oc.max_lat = st.max;
	oc.missed = cm.missed_total;
	if (!oc.ok)
		write_result_md(spec, dir, st, prod, cons, pm.published, cm.missed_total, false,
		                reason);
	return oc;
}

RunOutcome run_one(const Config& cfg, const RunSpec& spec, size_t ordinal) {
	if (spec.transport == "socket") return run_socket(cfg, spec);
	return run_shm(cfg, spec, ordinal);
}

// ---------------------------------------------------------------------------
// spec tables
// ---------------------------------------------------------------------------
RunSpec spec_of(std::string id, std::string transport, std::string mode, uint32_t payload,
                std::string placement, std::string rate_label, uint64_t rate_hz, uint64_t samples,
                uint64_t max_time_ms, uint64_t warmup) {
	RunSpec s;
	s.id = std::move(id);
	s.transport = std::move(transport);
	s.mode = std::move(mode);
	s.payload = payload;
	s.placement = std::move(placement);
	s.rate_label = std::move(rate_label);
	s.rate_hz = rate_hz;
	s.samples = samples;
	s.max_time_ms = max_time_ms;
	s.warmup = warmup;
	return s;
}

std::vector<RunSpec> smoke_specs() {
	return {
	        spec_of("smoke-shm-futex-64B-same-max", "shm", "futex", 64, "same", "max", 0,
	                200000, 30000, 0),
	        spec_of("smoke-socket-64B-same-max", "socket", "block", 64, "same", "max", 0,
	                100000, 30000, 0),
	};
}

std::vector<RunSpec> evidence_specs() {
	return {
	        // Q1/Q3: end-to-end latency distribution + memcpy scaling (shm, futex).
	        spec_of("shm-futex-64B-same-max", "shm", "futex", 64, "same", "max", 0, 5000000,
	                60000, 100000),
	        spec_of("shm-futex-1KiB-same-max", "shm", "futex", 1024, "same", "max", 0, 2000000,
	                60000, 50000),
	        spec_of("shm-futex-64KiB-same-max", "shm", "futex", 65536, "same", "max", 0,
	                1000000, 60000, 10000),
	        // Q2: CPU cost of futex-wait vs bounded busy-poll (shm, max rate).
	        spec_of("shm-poll-64B-same-max", "shm", "poll", 64, "same", "max", 0, 5000000,
	                60000, 100000),
	        // Q6: placement — same CPU vs different CPU vs unpinned.
	        spec_of("shm-futex-64B-different-max", "shm", "futex", 64, "different", "max", 0,
	                2000000, 60000, 50000),
	        spec_of("shm-futex-64B-unpinned-max", "shm", "futex", 64, "unpinned", "max", 0,
	                2000000, 60000, 50000),
	        spec_of("shm-poll-64B-different-max", "shm", "poll", 64, "different", "max", 0,
	                2000000, 60000, 50000),
	        // Q4: Unix domain socket baseline (same size, same placement).
	        spec_of("socket-64B-same-max", "socket", "block", 64, "same", "max", 0, 1000000,
	                60000, 10000),
	        spec_of("socket-1KiB-same-max", "socket", "block", 1024, "same", "max", 0, 1000000,
	                60000, 10000),
	        spec_of("socket-64KiB-same-max", "socket", "block", 65536, "same", "max", 0, 300000,
	                60000, 5000),
	        // Q5: slow consumer (bounded latency, observable gaps) at fixed rates.
	        spec_of("shm-futex-64B-same-10k", "shm", "futex", 64, "same", "10k", 10000, 1000000,
	                60000, 0),
	        spec_of("shm-futex-64B-same-100", "shm", "futex", 64, "same", "100", 100, 100000,
	                60000, 0),
	};
}

struct RateDim {
	const char* label;
	uint64_t hz;
};

std::vector<RunSpec> matrix_specs() {
	const uint32_t payloads[] = {64, 256, 1024, 4096, 16384, 65536};
	const char* placements[] = {"same", "different", "unpinned"};
	const RateDim rates[] = {{"max", 0}, {"100", 100}, {"1k", 1000}, {"10k", 10000}};
	std::vector<RunSpec> out;
	for (const uint32_t p : payloads) {
		for (const char* mode : {"futex", "poll"}) {
			for (const RateDim& r : rates) {
				for (const char* place : placements) {
					out.push_back(spec_of("shm-" + std::string(mode) + "-" +
					                              std::to_string(p) + "B-" +
					                              std::string(place) + "-" +
					                              r.label,
					                      "shm", mode, p, place, r.label, r.hz,
					                      1000000, 60000, 10000));
				}
			}
		}
	}
	for (const uint32_t p : payloads) {
		for (const RateDim& r : rates) {
			for (const char* place : placements) {
				out.push_back(spec_of("socket-" + std::to_string(p) + "B-" +
				                              std::string(place) + "-" + r.label,
				                      "socket", "block", p, place, r.label, r.hz,
				                      1000000, 60000, 10000));
			}
		}
	}
	return out;
}

}  // namespace

int main(int argc, char** argv) {
	Config cfg;
	cfg.producer_bin = edge_tool::arg_value(argc, argv, "--producer") != nullptr
	                           ? edge_tool::arg_value(argc, argv, "--producer")
	                           : "";
	cfg.consumer_bin = edge_tool::arg_value(argc, argv, "--consumer") != nullptr
	                           ? edge_tool::arg_value(argc, argv, "--consumer")
	                           : "";
	cfg.sock_bin = edge_tool::arg_value(argc, argv, "--sock") != nullptr
	                       ? edge_tool::arg_value(argc, argv, "--sock")
	                       : "";
	cfg.ctl_bin = edge_tool::arg_value(argc, argv, "--ctl") != nullptr
	                      ? edge_tool::arg_value(argc, argv, "--ctl")
	                      : "";
	if (const char* od = edge_tool::arg_value(argc, argv, "--out-dir")) {
		cfg.out_dir = od;
	}
	if (const char* gd = edge_tool::arg_value(argc, argv, "--git-dir")) {
		cfg.git_dir = gd;
	}
	cfg.cpu_a = static_cast<int>(edge_tool::arg_u64(argc, argv, "--cpu-a", 0));
	cfg.cpu_b = static_cast<int>(edge_tool::arg_u64(argc, argv, "--cpu-b", 1));
	cfg.taskset_ok_a = cpu_allowed(cfg.cpu_a);
	cfg.taskset_ok_b = cpu_allowed(cfg.cpu_b);

	bool want_smoke = edge_tool::arg_flag(argc, argv, "--smoke");
	bool want_evidence = edge_tool::arg_flag(argc, argv, "--evidence");
	bool want_matrix = edge_tool::arg_flag(argc, argv, "--matrix");
	bool want_list = edge_tool::arg_flag(argc, argv, "--list");
	const char* only = edge_tool::arg_value(argc, argv, "--only");

	const bool has_helpers =
	        !cfg.producer_bin.empty() && !cfg.consumer_bin.empty() && !cfg.sock_bin.empty();
	if (want_list) {
		for (const auto& s : smoke_specs()) {
			std::printf("%-40s %-6s %-5s %6uB %-10s %-6s\n", s.id.c_str(),
			            s.transport.c_str(), s.mode.c_str(), s.payload,
			            s.placement.c_str(), s.rate_label.c_str());
		}
		std::printf("--- evidence ---\n");
		for (const auto& s : evidence_specs()) {
			std::printf("%-40s %-6s %-5s %6uB %-10s %-6s\n", s.id.c_str(),
			            s.transport.c_str(), s.mode.c_str(), s.payload,
			            s.placement.c_str(), s.rate_label.c_str());
		}
		return 0;
	}
	if (!has_helpers) {
		std::fprintf(stderr,
		             "usage: edge_shm_bench (--smoke|--evidence|--matrix) "
		             "--producer <bin> --consumer <bin> --sock <bin> "
		             "[--ctl <bin>] [--out-dir <dir>] [--only <id,...>]\n");
		return 2;
	}

	// git commit (evidence reproducibility).
	const std::vector<std::string> git = {"git", "-C", cfg.git_dir, "rev-parse", "HEAD"};
	const ChildResult gr = spawn_collect(git);
	if (gr.exec_ok && gr.exit_code == 0) {
		cfg.git_commit = gr.out;
		while (!cfg.git_commit.empty() &&
		       (cfg.git_commit.back() == '\n' || cfg.git_commit.back() == '\r')) {
			cfg.git_commit.pop_back();
		}
	} else {
		cfg.git_commit = "n/a (no commits or not a git repo)";
	}

	// perf capability probe (VM_ONLY: paranoid=4 normally blocks it).
	const std::vector<std::string> perf = {"perf", "stat", "-e",
	                                       "cycles,instructions,cache-misses,context-switches,"
	                                       "page-faults",
	                                       "true"};
	const ChildResult pr = spawn_collect(perf);
	cfg.perf_probe = pr.out;
	cfg.perf_ok = pr.exec_ok && pr.exit_code == 0 && pr.out.find("Error") == std::string::npos;

	std::vector<RunSpec> runs;
	if (want_smoke)
		runs = smoke_specs();
	else if (want_evidence)
		runs = evidence_specs();
	else if (want_matrix)
		runs = matrix_specs();
	if (only != nullptr) {
		// --only is a comma-separated list of EXACT run ids (no substring match).
		std::vector<std::string> want;
		std::string cur;
		for (const char* c = only;; ++c) {
			if (*c == ',' || *c == '\0') {
				if (!cur.empty()) want.push_back(cur);
				cur.clear();
				if (*c == '\0') break;
			} else {
				cur.push_back(*c);
			}
		}
		std::vector<RunSpec> filtered;
		for (const auto& r : runs) {
			for (const auto& w : want) {
				if (r.id == w) {
					filtered.push_back(r);
					break;
				}
			}
		}
		runs = std::move(filtered);
	}
	if (runs.empty()) {
		std::fprintf(stderr,
		             "edge_shm_bench: no runs selected (--smoke/--evidence/--matrix)\n");
		return 2;
	}

	uint64_t passed = 0;
	uint64_t failed = 0;
	for (size_t i = 0; i < runs.size(); ++i) {
		const RunSpec& s = runs[i];
		const RunOutcome oc = run_one(cfg, s, i + 1);
		if (oc.ok) {
			++passed;
			std::printf("RUN %-40s PASS measured=%-8" PRIu64 " p50=%-8" PRIu64
			            " p95=%-8" PRIu64 " p99=%-8" PRIu64 " max=%-10" PRIu64
			            " missed=%" PRIu64 "\n",
			            s.id.c_str(), oc.measured, oc.p50, oc.p95, oc.p99, oc.max_lat,
			            oc.missed);
		} else {
			++failed;
			std::printf("RUN %-40s FAIL %s\n", s.id.c_str(), oc.fail_reason.c_str());
		}
		std::fflush(stdout);
	}
	std::printf("SUMMARY total=%zu passed=%" PRIu64 " failed=%" PRIu64 "\n", runs.size(),
	            passed, failed);
	return failed > 0 ? 1 : 0;
}
