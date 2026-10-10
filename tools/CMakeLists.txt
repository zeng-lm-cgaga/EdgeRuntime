# 工具用于运行诊断、故障恢复和跨进程性能采集，不属于公共 API。
add_executable(edge_runtime_platform_probe src/platform_probe.cpp)
edgeruntime_apply_project_options(edge_runtime_platform_probe)
target_link_libraries(edge_runtime_platform_probe PRIVATE EdgeRuntime::edge_runtime rt)

function(edgeruntime_tool name)
  add_executable(${name} src/${name}.cpp)
  edgeruntime_apply_project_options(${name})
  target_link_libraries(${name} PRIVATE EdgeRuntime::edge_runtime Threads::Threads rt)
  target_include_directories(
    ${name} PRIVATE
      ${CMAKE_SOURCE_DIR}/test/fixtures
      ${CMAKE_SOURCE_DIR}/src
      ${CMAKE_CURRENT_SOURCE_DIR}/include
      ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

edgeruntime_tool(edge_shm_producer)
edgeruntime_tool(edge_shm_consumer)

edgeruntime_tool(edge_shm_ctl)
edgeruntime_tool(edge_shm_supervisor)

edgeruntime_tool(edge_crash_matrix)

edgeruntime_tool(edge_shm_bench)
edgeruntime_tool(edge_shm_bench_producer)
edgeruntime_tool(edge_shm_bench_consumer)
edgeruntime_tool(edge_shm_bench_sock)
edgeruntime_tool(edge_mpmc_bench)
edgeruntime_tool(edge_buffer_pool_bench)
edgeruntime_tool(edge_mpmc_handshake_helper)
edgeruntime_tool(edge_buffer_pool_handshake_helper)
edgeruntime_tool(edge_benchmark_watchdog_helper)

if(BUILD_TESTING)
  find_package(Python3 QUIET COMPONENTS Interpreter)

  add_test(NAME platform_contract COMMAND edge_runtime_platform_probe)
  set_tests_properties(platform_contract PROPERTIES TIMEOUT 10)

  add_test(
    NAME crash_matrix_smoke
    COMMAND edge_crash_matrix --smoke
            --producer $<TARGET_FILE:edge_shm_producer>
            --consumer $<TARGET_FILE:edge_shm_consumer>
            --ctl $<TARGET_FILE:edge_shm_ctl>
            --out-dir ${CMAKE_BINARY_DIR}/evidence/crash)
  set_tests_properties(crash_matrix_smoke PROPERTIES TIMEOUT 240 LABELS crash)

  add_test(
    NAME crash_matrix
    COMMAND edge_crash_matrix
            --producer $<TARGET_FILE:edge_shm_producer>
            --consumer $<TARGET_FILE:edge_shm_consumer>
            --ctl $<TARGET_FILE:edge_shm_ctl>
            --out-dir ${CMAKE_BINARY_DIR}/evidence/crash)
  set_tests_properties(
    crash_matrix PROPERTIES TIMEOUT 900 LABELS crash DISABLED TRUE)

  add_test(
    NAME bench_smoke
    COMMAND edge_shm_bench --smoke
            --producer $<TARGET_FILE:edge_shm_bench_producer>
            --consumer $<TARGET_FILE:edge_shm_bench_consumer>
            --sock $<TARGET_FILE:edge_shm_bench_sock>
            --ctl $<TARGET_FILE:edge_shm_ctl>
            --out-dir ${CMAKE_BINARY_DIR}/evidence/performance)
  set_tests_properties(
    bench_smoke PROPERTIES TIMEOUT 300 LABELS benchmark)

  add_test(NAME mpmc_bench_smoke COMMAND edge_mpmc_bench --smoke)
  set_tests_properties(mpmc_bench_smoke PROPERTIES TIMEOUT 120 LABELS benchmark)

  add_test(
    NAME mpmc_bench_spin_smoke
    COMMAND edge_mpmc_bench --retry-policy spin --payload-size 64 --producers 2
            --consumers 2 --messages 8 --capacity 4 --timeout-ms 10000)
  set_tests_properties(mpmc_bench_spin_smoke PROPERTIES TIMEOUT 30 LABELS benchmark)

  add_test(
    NAME mpmc_wait_bench_smoke
    COMMAND edge_mpmc_bench --retry-policy yield --wait-policy blocking --payload-size 64
            --producers 2 --consumers 2 --messages 8 --capacity 4 --timeout-ms 10000)
  set_tests_properties(mpmc_wait_bench_smoke PROPERTIES TIMEOUT 30 LABELS benchmark)

  add_test(NAME buffer_pool_bench_smoke COMMAND edge_buffer_pool_bench --smoke)
  set_tests_properties(buffer_pool_bench_smoke PROPERTIES TIMEOUT 120 LABELS benchmark)

  if(Python3_Interpreter_FOUND)
    add_test(
      NAME benchmark_pressure_watchdog_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_pressure_watchdog.py
              --mpmc-binary $<TARGET_FILE:edge_mpmc_bench>
              --pool-binary $<TARGET_FILE:edge_buffer_pool_bench>
              --owner-binary $<TARGET_FILE:edge_benchmark_watchdog_helper>
              --output-root /tmp)
    set_tests_properties(
      benchmark_pressure_watchdog_regression PROPERTIES TIMEOUT 45 LABELS benchmark)

    add_test(
      NAME mpmc_payload_retry_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_mpmc_payload_retry.py
              --binary $<TARGET_FILE:edge_mpmc_bench> --output-root /tmp)
    set_tests_properties(mpmc_payload_retry_regression PROPERTIES TIMEOUT 45 LABELS benchmark)

    add_test(
      NAME benchmark_handshake_timing_regression
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_handshake_timing.py
              --mpmc-binary $<TARGET_FILE:edge_mpmc_handshake_helper>
              --pool-binary $<TARGET_FILE:edge_buffer_pool_handshake_helper>
              --output-root /tmp)
    set_tests_properties(
      benchmark_handshake_timing_regression PROPERTIES TIMEOUT 45 LABELS benchmark)
  endif()
endif()
