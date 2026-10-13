#ifndef EDGE_RUNTIME_TEST_BROKER_OBSERVER_SYSCALL_FIXTURE_HPP
#define EDGE_RUNTIME_TEST_BROKER_OBSERVER_SYSCALL_FIXTURE_HPP

#include <cstdint>

namespace edge_test::broker_observer {

void interrupt_operation(const char* operation) noexcept;
uint32_t syscall_interruption_count() noexcept;

}

#endif
