#ifndef EDGE_RUNTIME_TEST_BROKER_OBSERVER_FIXTURE_HPP
#define EDGE_RUNTIME_TEST_BROKER_OBSERVER_FIXTURE_HPP

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "edge_runtime/transport/shm_object.hpp"

namespace edge_test::broker_observer {

inline constexpr int kWatchdogMs = 600;
inline constexpr uint64_t kClientBudgetMs = 120;

class OwnedPath;

struct ClientReport {
	std::string code;
	uint64_t duration_ms = 0;
	int64_t fd_delta = -1;
	int signals = 0;
	int access = -1;
	int cloexec = 0;
	uint32_t syscall_interruptions = 0;
};

class Child {
       public:
	Child();
	~Child();
	Child(const Child&) = delete;
	Child& operator=(const Child&) = delete;
	bool start(const std::vector<std::string>& arguments);
	std::string line(int timeout_ms = kWatchdogMs);
	bool command(char command = 'q');
	bool wait(int timeout_ms = kWatchdogMs);
	pid_t pid() const;
	int exit_code() const;

       private:
	pid_t pid_ = -1;
	int exit_code_ = -1;
	edge_runtime::detail::UniqueFd input_;
	edge_runtime::detail::UniqueFd output_;
	std::unique_ptr<OwnedPath> owned_path_;
};

class OwnedPath {
       public:
	explicit OwnedPath(std::string path);
	~OwnedPath();
	bool capture();

       private:
	std::string path_;
	uint64_t dev_ = 0;
	uint64_t ino_ = 0;
};

std::string unique_name();
std::string peer_binary();
edge_runtime::detail::UniqueFd connect_peer(const std::string& path);
size_t fd_count();
uint64_t elapsed_ms(uint64_t start_ns);
bool parse_report(const std::string& text, ClientReport* report);

}

#endif
