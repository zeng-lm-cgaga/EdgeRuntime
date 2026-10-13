#ifndef SELF_IPCS_CONSUME_DEMO_API_HPP
#define SELF_IPCS_CONSUME_DEMO_API_HPP

#if defined(SELFIPCS_DEMO_USE_LEGACY)
#include "edge_runtime/buffer/shared_buffer_pool.hpp"
#include "edge_runtime/channel/consumer.hpp"
#include "edge_runtime/channel/loan.hpp"
#include "edge_runtime/channel/producer.hpp"
#include "edge_runtime/common/error.hpp"
#include "edge_runtime/common/schema.hpp"
#include "edge_runtime/queue/mpmc_queue.hpp"
#else
#include "self_ipcs/buffer/shared_buffer_pool.hpp"
#include "self_ipcs/channel/consumer.hpp"
#include "self_ipcs/channel/loan.hpp"
#include "self_ipcs/channel/producer.hpp"
#include "self_ipcs/common/error.hpp"
#include "self_ipcs/common/schema.hpp"
#include "self_ipcs/queue/mpmc_queue.hpp"
#endif

namespace consume_demo {

#if defined(SELFIPCS_DEMO_USE_LEGACY)
namespace ipc = edge_runtime;
inline constexpr const char* kPackageName = "EdgeRuntime";
#else
namespace ipc = self_ipcs;
inline constexpr const char* kPackageName = "SelfIPCs";
#endif

}  // namespace consume_demo

#endif
