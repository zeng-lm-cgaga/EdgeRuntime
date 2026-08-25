# 集成测试必须通过独立可执行文件和 fork/exec 验证进程边界行为。

add_executable(edge_shm_reopen_helper helpers/reopen_child.cpp)
edgeruntime_apply_project_options(edge_shm_reopen_helper)
target_link_libraries(
  edge_shm_reopen_helper PRIVATE edge_runtime Threads::Threads rt)
target_include_directories(
  edge_shm_reopen_helper PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_executable(reopen_test reopen_test.cpp)
edgeruntime_apply_project_options(reopen_test)
target_link_libraries(
  reopen_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  reopen_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_test(
  NAME reopen_cross_exec
  COMMAND reopen_test $<TARGET_FILE:edge_shm_reopen_helper>)
set_tests_properties(reopen_cross_exec PROPERTIES TIMEOUT 30 LABELS integration)

add_executable(pattern_test pattern_test.cpp)
edgeruntime_apply_project_options(pattern_test)
target_link_libraries(
  pattern_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  pattern_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_test(
  NAME pattern_er2
  COMMAND pattern_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(
  pattern_er2 PROPERTIES TIMEOUT 300 LABELS integration)

add_executable(wait_test wait_test.cpp)
edgeruntime_apply_project_options(wait_test)
target_link_libraries(
  wait_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  wait_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_test(
  NAME wait_er3
  COMMAND wait_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(wait_er3 PROPERTIES TIMEOUT 90 LABELS integration)

add_executable(recovery_test recovery_test.cpp)
edgeruntime_apply_project_options(recovery_test)
target_link_libraries(
  recovery_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  recovery_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_test(
  NAME recovery_er4
  COMMAND recovery_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(recovery_er4 PROPERTIES TIMEOUT 90 LABELS integration)

add_executable(fd_transport_test fd_transport_test.cpp)
edgeruntime_apply_project_options(fd_transport_test)
target_link_libraries(
  fd_transport_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  fd_transport_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_test(
  NAME fd_transport_er10
  COMMAND fd_transport_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>
          $<TARGET_FILE:edge_shm_ctl>)
set_tests_properties(fd_transport_er10 PROPERTIES TIMEOUT 120 LABELS integration)

add_executable(heartbeat_test heartbeat_test.cpp)
edgeruntime_apply_project_options(heartbeat_test)
target_link_libraries(
  heartbeat_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  heartbeat_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_test(
  NAME heartbeat_er11
  COMMAND heartbeat_test
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(heartbeat_er11 PROPERTIES TIMEOUT 90 LABELS integration)

add_executable(supervisor_test supervisor_test.cpp)
edgeruntime_apply_project_options(supervisor_test)
target_link_libraries(
  supervisor_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  supervisor_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_test(
  NAME supervisor_er13
  COMMAND supervisor_test
          $<TARGET_FILE:edge_shm_supervisor>
          $<TARGET_FILE:edge_shm_producer>
          $<TARGET_FILE:edge_shm_consumer>)
set_tests_properties(supervisor_er13 PROPERTIES TIMEOUT 150 LABELS integration)

add_executable(edge_shm_loan_helper helpers/loan_child.cpp)
edgeruntime_apply_project_options(edge_shm_loan_helper)
target_link_libraries(
  edge_shm_loan_helper PRIVATE edge_runtime Threads::Threads rt)
target_include_directories(
  edge_shm_loan_helper PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_executable(loan_integration_test loan_test.cpp)
edgeruntime_apply_project_options(loan_integration_test)
target_link_libraries(
  loan_integration_test PRIVATE edge_runtime GTest::gtest_main Threads::Threads rt)
target_include_directories(
  loan_integration_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures)

add_test(
  NAME loan_er15
  COMMAND loan_integration_test $<TARGET_FILE:edge_shm_loan_helper>)
set_tests_properties(loan_er15 PROPERTIES TIMEOUT 30 LABELS integration)
