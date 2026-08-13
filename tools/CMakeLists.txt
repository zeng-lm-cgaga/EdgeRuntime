# Platform/contract probe: proves the host satisfies EdgeRuntime's hard
# preconditions (kernel >= 5.15, glibc >= 2.35, lock-free atomics, pidfd,
# shared futex, full shm lifecycle) before anything else is trusted.
add_executable(edge_runtime_platform_probe platform_probe.cpp)
edgeruntime_apply_project_options(edge_runtime_platform_probe)
target_link_libraries(edge_runtime_platform_probe PRIVATE EdgeRuntime::edge_runtime rt)

# ER2 demo/integration/crash workhorses (design §21.3): publish/read a
# counter payload and emit stable one-line markers (READY/DONE/SUMMARY/...)
# that test drivers and the crash matrix parse. They link the test fixture
# codec so cross-exec codec bytes are identical to the gtest helpers.
function(edgeruntime_tool name)
  add_executable(${name} ${name}.cpp)
  edgeruntime_apply_project_options(${name})
  target_link_libraries(${name} PRIVATE EdgeRuntime::edge_runtime Threads::Threads rt)
  target_include_directories(
    ${name} PRIVATE
      ${CMAKE_SOURCE_DIR}/test/fixtures
      ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

edgeruntime_tool(edge_shm_producer)
edgeruntime_tool(edge_shm_consumer)

# ER4 diagnostic/forensic CLI (design §22): inspect reads a channel without
# dumping payload; remove performs verified, journal-bound unlink.
edgeruntime_tool(edge_shm_ctl)

# ER4 crash matrix (design §20.3/§24): the C01-C13 table-driven runner. It
# pauses victims at failpoints, dumps forensics via edge_shm_ctl, runs the
# recovery actor, and writes evidence under <out-dir>/<run_id>/<case>/.
edgeruntime_tool(edge_crash_matrix)

# ER7 benchmark (design §21): a fork+exec driver plus SEPARATE helper binaries
# (producer / consumer / Unix-socket baseline) and the shared BenchPayloadV1
# codec. The driver writes evidence/performance/<run_id>/ per §21.4.
edgeruntime_tool(edge_shm_bench)
edgeruntime_tool(edge_shm_bench_producer)
edgeruntime_tool(edge_shm_bench_consumer)
edgeruntime_tool(edge_shm_bench_sock)

if(BUILD_TESTING)
  add_test(NAME platform_contract COMMAND edge_runtime_platform_probe)
  set_tests_properties(platform_contract PROPERTIES TIMEOUT 10)

  # The smoke subset (C01/C03/C08) covers the three recovery families cheaply
  # and runs in the normal dev loop; the full matrix is a longer, opt-in run
  # (LABELS crash + DISABLED) invoked explicitly via ctest -R or the binary.
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

  # The ER7 smoke subset (shm-futex 64B max + socket 64B max) exercises the
  # full fork+exec pipeline and evidence writer in the normal dev loop; the
  # curated --evidence set and the full --matrix grid are opt-in runs.
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
endif()
