#ifndef ER_CONSUME_DEMO_QUEUE_FLOW_HPP
#define ER_CONSUME_DEMO_QUEUE_FLOW_HPP

#include <string>

namespace consume_demo {

int run_queue_child(const std::string& name, int event_fd, int command_fd);
bool run_queue_flow();

}  // namespace consume_demo

#endif  // ER_CONSUME_DEMO_QUEUE_FLOW_HPP
