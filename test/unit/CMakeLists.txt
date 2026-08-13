# ER1 white-box unit tests (U01–U04/U06/U08/U11/U13). Each is a gtest
# executable discovered at build time. The internal headers they exercise live
# in include/edge_runtime/detail/ (resolved via the library's PUBLIC include
# dir); the fixtures dir provides the shared test payload/codec.

function(edgeruntime_unit_test name)
  add_executable(${name} ${name}.cpp)
  edgeruntime_apply_project_options(${name})
  target_link_libraries(
    ${name} PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
  target_include_directories(
    ${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)
  gtest_discover_tests(${name})
endfunction()

edgeruntime_unit_test(abi_layout_test)        # U01-U04 frozen ABI + naming
edgeruntime_unit_test(error_test)             # U08 error table + classification
edgeruntime_unit_test(process_identity_test)  # U06/U11/U13 pidfd + starttime
edgeruntime_unit_test(control_journal_test)   # U11/U13 journal round-trip
edgeruntime_unit_test(checksum_test)          # fnv1a64 golden vectors
edgeruntime_unit_test(slot_protocol_test)     # U07/U09/U14 slot machine
edgeruntime_unit_test(futex_test)             # U10 futex wait/wake/EAGAIN/EINTR/timeout
edgeruntime_unit_test(clock_test)             # §15.7 deadline + remaining-time helpers
edgeruntime_unit_test(failpoint_test)         # §20.3 registry + env modes + hit counting
edgeruntime_unit_test(fd_broker_test)         # v0.2 §33 wire record layouts + checksum
edgeruntime_unit_test(channel_observer_test)  # v0.3 §35.3 readonly view + stall classification
edgeruntime_unit_test(liveness_watch_test)    # v0.3 §35.3 long-held pidfd + CLOEXEC
edgeruntime_unit_test(process_spawn_test)     # v0.3 §35.3 fork+exec + nonblock stdout pipe
