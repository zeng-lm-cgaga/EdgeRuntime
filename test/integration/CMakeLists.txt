# Cross-process integration drivers (design §18.3). Each driver is a gtest
# executable; the helper is a SEPARATE binary the driver fork+execs. The
# helper re-opens the channel by name in its own image, proving a true reopen.

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

# ER2 pattern driver (I02-I08, I17): fork+execs the two tool binaries.
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

# ER3 wait_latest driver (I12-I14): fork+execs the two tool binaries.
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

# ER4 recovery-engine driver (narrow window / ABA fail-closed + I10/I11):
# fork+execs the tool binaries and SIGSTOPs them at failpoints C01/C02/C08.
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

# ER10 v0.2 fd-pass transport driver (F1-F4): fork+execs the tool binaries and
# inspects via edge_shm_ctl's readonly broker fd.
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

# ER11 v0.2 heartbeat driver (H1/H2): SIGSTOP-freeze vs healthy heartbeat loop.
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

# ER13 v0.3 ProducerSupervisor driver (S1-S5): fork+execs the supervisor tool.
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
