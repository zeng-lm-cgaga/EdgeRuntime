#ifndef ER_CONSUME_DEMO_POOL_FLOW_HPP
#define ER_CONSUME_DEMO_POOL_FLOW_HPP

#include <string>

namespace consume_demo {

enum class PoolFlowScenario {
	kNormal,
	kQueueCreateFailure,
	kPoolCreateFailure,
	kPeerFailsAfterPop,
	kPeerFailsAfterRead,
};

int run_pool_child(const std::string& queue_name, const std::string& pool_name, int event_fd,
	               bool fail_after_pop = false, bool fail_after_read = false);
bool run_pool_flow(PoolFlowScenario scenario = PoolFlowScenario::kNormal);

}  // namespace consume_demo

#endif  // ER_CONSUME_DEMO_POOL_FLOW_HPP
