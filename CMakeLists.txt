cmake_minimum_required(VERSION 3.22)

project(
  EdgeRuntime
  VERSION 0.1.0
  DESCRIPTION "Linux SPSC latest-value shared-memory runtime"
  LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

find_package(Threads REQUIRED)

option(EDGERUNTIME_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" ON)
option(EDGERUNTIME_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

# Compile-time failpoints (design §22 / crash matrix C01-C13). Default ON for
# Debug builds so the fault-injection toolchain is exercisable; OFF for Release
# so production binaries carry zero failpoint cost.
set(_edgeruntime_failpoints_default OFF)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(_edgeruntime_failpoints_default ON)
endif()
option(EDGERUNTIME_ENABLE_FAILPOINTS "Enable compile-time failpoints" ${_edgeruntime_failpoints_default})
unset(_edgeruntime_failpoints_default)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  set(EDGERUNTIME_PRIVATE_COMPILE_OPTIONS -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
  if(EDGERUNTIME_WARNINGS_AS_ERRORS)
    list(APPEND EDGERUNTIME_PRIVATE_COMPILE_OPTIONS -Werror)
  endif()
else()
  message(FATAL_ERROR "EdgeRuntime v0.1 currently supports GCC or Clang on Linux only")
endif()

# First-party flags applied PRIVATE to every target in this repo (library,
# tools, tests). Deliberately not an INTERFACE target: build-time options must
# never leak into the installed/exported package.
function(edgeruntime_apply_project_options target)
  target_compile_options(${target} PRIVATE ${EDGERUNTIME_PRIVATE_COMPILE_OPTIONS})
  if(EDGERUNTIME_ENABLE_SANITIZERS)
    target_compile_options(
      ${target} PRIVATE -fno-omit-frame-pointer -fsanitize=address,undefined)
    target_link_options(
      ${target} PRIVATE -fno-omit-frame-pointer -fsanitize=address,undefined)
  endif()
  if(EDGERUNTIME_ENABLE_FAILPOINTS)
    target_compile_definitions(${target} PRIVATE EDGERUNTIME_ENABLE_FAILPOINTS=1)
  endif()
endfunction()

# ---------------------------------------------------------------------------
# edge_runtime: STATIC. Headers are classified by kind: every .hpp lives under
# include/ (public API at include/edge_runtime/, internal "detail" headers at
# include/edge_runtime/detail/); every .cpp lives under src/edge_runtime/detail/.
# Internal headers are referenced with the same "edge_runtime/detail/..." prefix
# and are excluded from the installed package. Every consumer declares its own
# warnings flags and links rt.
# ---------------------------------------------------------------------------
add_library(edge_runtime STATIC
  src/edge_runtime/detail/error.cpp
  src/edge_runtime/detail/channel_abi.cpp
  src/edge_runtime/detail/channel_layout.cpp
  src/edge_runtime/detail/channel_observer.cpp
  src/edge_runtime/detail/checksum.cpp
  src/edge_runtime/detail/clock.cpp
  src/edge_runtime/detail/consumer_impl.cpp
  src/edge_runtime/detail/control_lock.cpp
  src/edge_runtime/detail/failpoint.cpp
  src/edge_runtime/detail/fd_broker.cpp
  src/edge_runtime/detail/futex.cpp
  src/edge_runtime/detail/process_identity.cpp
  src/edge_runtime/detail/process_spawn.cpp
  src/edge_runtime/detail/producer_impl.cpp
  src/edge_runtime/detail/shm_object.cpp
  src/edge_runtime/detail/slot_protocol.cpp
  src/edge_runtime/detail/supervisor_impl.cpp)
add_library(EdgeRuntime::edge_runtime ALIAS edge_runtime)

target_compile_features(edge_runtime PUBLIC cxx_std_17)
target_include_directories(
  edge_runtime
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

edgeruntime_apply_project_options(edge_runtime)
target_link_libraries(edge_runtime PRIVATE rt)

add_subdirectory(tools)
if(BUILD_TESTING)
  add_subdirectory(test)
endif()

install(
  TARGETS edge_runtime
  EXPORT EdgeRuntimeTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
# Internal "detail" headers are intentionally NOT installed: include/edge_runtime/detail/
# exists so the build tree classifies headers by kind, but it is not part of the public ABI.
install(
  DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  FILES_MATCHING
    PATTERN "*.hpp"
    PATTERN "detail" EXCLUDE)
install(
  EXPORT EdgeRuntimeTargets
  FILE EdgeRuntimeTargets.cmake
  NAMESPACE EdgeRuntime::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/EdgeRuntime)

configure_package_config_file(
  cmake/EdgeRuntimeConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/EdgeRuntimeConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/EdgeRuntime)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/EdgeRuntimeConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

install(
  FILES
    ${CMAKE_CURRENT_BINARY_DIR}/EdgeRuntimeConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/EdgeRuntimeConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/EdgeRuntime)
