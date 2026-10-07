add_library(edge_runtime STATIC
  edge_runtime/common/error.cpp
  edge_runtime/channel/channel_abi.cpp
  edge_runtime/channel/channel_layout.cpp
  edge_runtime/channel/channel_observer.cpp
  edge_runtime/channel/consumer_impl.cpp
  edge_runtime/channel/control_lock.cpp
  edge_runtime/channel/loan.cpp
  edge_runtime/channel/producer_impl.cpp
  edge_runtime/channel/slot_protocol.cpp
  edge_runtime/queue/mpmc_queue.cpp
  edge_runtime/buffer/shared_buffer_pool.cpp
  edge_runtime/process/process_identity.cpp
  edge_runtime/process/process_spawn.cpp
  edge_runtime/process/supervisor_impl.cpp
  edge_runtime/transport/fd_broker.cpp
  edge_runtime/transport/shm_object.cpp
  edge_runtime/sync/clock.cpp
  edge_runtime/sync/futex.cpp
  edge_runtime/utility/checksum.cpp
  edge_runtime/utility/failpoint.cpp)
add_library(EdgeRuntime::edge_runtime ALIAS edge_runtime)

target_compile_features(edge_runtime PUBLIC cxx_std_17)
target_include_directories(
  edge_runtime
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR})

edgeruntime_apply_project_options(edge_runtime)
target_link_libraries(edge_runtime PRIVATE rt)
