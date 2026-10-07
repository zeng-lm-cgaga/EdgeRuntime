#ifndef EDGE_RUNTIME_DETAIL_MPMC_QUEUE_HPP
#define EDGE_RUNTIME_DETAIL_MPMC_QUEUE_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "edge_runtime/queue/mpmc_queue.hpp"

namespace edge_runtime::detail {

struct MpmcQueueHandle;

Result<std::shared_ptr<MpmcQueueHandle>> mpmc_create_impl(const MpmcQueueOptions& options,
                                                          const SchemaDescriptor& schema,
                                                          uint32_t payload_size);
Result<std::shared_ptr<MpmcQueueHandle>> mpmc_open_impl(const MpmcQueueOptions& options,
                                                        const SchemaDescriptor& schema,
                                                        uint32_t payload_size);
Result<void> mpmc_try_push_impl(const std::shared_ptr<MpmcQueueHandle>& handle,
                                const void* payload, uint32_t payload_size) noexcept;
Result<void> mpmc_try_pop_impl(const std::shared_ptr<MpmcQueueHandle>& handle, void* payload,
                               uint32_t payload_size) noexcept;
Result<void> mpmc_wait_push_until_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle, const void* payload,
        uint32_t payload_size, std::chrono::steady_clock::time_point deadline) noexcept;
Result<void> mpmc_wait_pop_until_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle, void* payload, uint32_t payload_size,
        std::chrono::steady_clock::time_point deadline) noexcept;
Result<MpmcQueueStatus> mpmc_status_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
Result<void> mpmc_remove_if_creator_impl(
        const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
uint32_t mpmc_capacity_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;
uint64_t mpmc_generation_impl(const std::shared_ptr<MpmcQueueHandle>& handle) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_MPMC_QUEUE_HPP
