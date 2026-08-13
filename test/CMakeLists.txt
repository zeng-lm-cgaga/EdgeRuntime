# Tests land in waves (ER1 unit + cross-exec reopen, then pattern/futex/
# crash-matrix suites). Unit tests are registered per file with
# gtest_discover_tests; cross-process integration drivers are plain add_test
# entries that fork+exec helper binaries (design §18.3 forbids in-process
# threads and gtest death tests masquerading as cross-process).

find_package(Threads REQUIRED)
find_package(GTest REQUIRED)
include(GoogleTest)

# Unit tests: white-box (include internal src/ headers) so they can assert
# ABI offsets, journal records, and identity internals directly.
add_subdirectory(unit)

# Integration tests: separate driver + helper binaries, fork+exec across
# processes; helpers re-open channels by name (never inherit mappings).
add_subdirectory(integration)
