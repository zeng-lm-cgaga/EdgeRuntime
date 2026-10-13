#ifndef EDGE_RUNTIME_CHANNEL_OPTIONS_HPP
#define EDGE_RUNTIME_CHANNEL_OPTIONS_HPP

#include <chrono>
#include <cstdint>
#include <string>

namespace edge_runtime {

enum class Transport : uint32_t {
	// 使用命名 POSIX 共享内存，兼容默认运行方式。
	kPosixShm = 0,
	// 使用 memfd，并通过每个通道的 Unix 套接字传递文件描述符。
	kMemfdFdPass = 1,
};

struct ChannelOptions {
	std::string name;
	// 源兼容保留的弃用字段：仅接受默认 100ms；样本新鲜度由调用方按时间戳判断。
	std::chrono::milliseconds stale_timeout{100};
	std::chrono::milliseconds reconnect_timeout{1000};
	// 完整性校验始终开启；false 在 create/open 时返回 InvalidOptions。
	bool enable_payload_checksum{true};
	Transport transport{Transport::kPosixShm};
	// 大于零时，Producer 可用 BOOTTIME 报告应用仍在推进；零表示关闭。
	std::chrono::nanoseconds heartbeat_interval{0};
};

}  // 命名空间 edge_runtime

#endif  // EDGE_RUNTIME_CHANNEL_OPTIONS_HPP
