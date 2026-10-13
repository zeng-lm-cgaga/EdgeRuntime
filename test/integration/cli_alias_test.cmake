execute_process(
  COMMAND "${SUPERVISOR}" --name cli_compat_help_child
          --producer-argv "${PRODUCER} --help" --max-restarts 0 --watch-interval-ms 10
  WORKING_DIRECTORY /
  TIMEOUT 10
  RESULT_VARIABLE result
  OUTPUT_VARIABLE output
  ERROR_VARIABLE errors)
set(combined "${output}${errors}")
if(NOT result STREQUAL "3" OR
   NOT combined MATCHES "usage: self_ipcs_shm_producer" OR
   NOT combined MATCHES "GAVE_UP attempts=1 restarts=0")
  message(FATAL_ERROR "Unexpected supervised help-child result: ${result}\n${combined}")
endif()
message(STATUS "HELP_CHILD_EXECUTED; pre-READY child exit rejected as expected\n${combined}")
