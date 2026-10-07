#ifndef ER_CONSUME_DEMO_CHANNEL_FLOW_HPP
#define ER_CONSUME_DEMO_CHANNEL_FLOW_HPP

#include <string>

namespace consume_demo {

int run_channel_child(const std::string& name, int event_fd);
bool run_channel_flow();

}  // namespace consume_demo

#endif  // ER_CONSUME_DEMO_CHANNEL_FLOW_HPP
