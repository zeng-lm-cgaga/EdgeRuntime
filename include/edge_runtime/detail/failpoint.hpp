#ifndef EDGE_RUNTIME_DETAIL_FAILPOINT_HPP
#define EDGE_RUNTIME_DETAIL_FAILPOINT_HPP

#include <cstddef>

namespace edge_runtime::detail {

// 故障点只用于验证失败路径；发布构建关闭宏后不会留下运行时代码。
struct FailpointRecord {
	const char* id;
};

}  // 命名空间 edge_runtime::detail

#if EDGERUNTIME_ENABLE_FAILPOINTS

#include <cstdint>

namespace edge_runtime::detail {

extern "C" {
extern const FailpointRecord __start_edg_failpoints[];
extern const FailpointRecord __stop_edg_failpoints[];
}

void failpoint_trigger(const FailpointRecord* fp) noexcept;

size_t failpoint_list(const char* const** ids_out) noexcept;

}  // 命名空间 edge_runtime::detail

#define EDGE_FAILPOINT(id)                                                          \
	do {                                                                        \
		static const ::edge_runtime::detail::FailpointRecord fp_record_##id \
		        __attribute__((section("edg_failpoints"), used)) = {#id};   \
		::edge_runtime::detail::failpoint_trigger(&fp_record_##id);         \
	} while (0)

#else  // 未启用故障注入

#define EDGE_FAILPOINT(id) \
	do {               \
	} while (0)

#endif  // EDGERUNTIME_ENABLE_FAILPOINTS

#endif  // EDGE_RUNTIME_DETAIL_FAILPOINT_HPP
