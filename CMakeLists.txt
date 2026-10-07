# EdgeRuntime 是面向 Linux 的单生产者单消费者最新值共享内存运行库。
cmake_minimum_required(VERSION 3.22)

project(
  EdgeRuntime
  VERSION 0.4.0
  DESCRIPTION "Linux SPSC latest-value shared-memory runtime"
  LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

find_package(Threads REQUIRED)

option(EDGERUNTIME_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" ON)
option(EDGERUNTIME_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

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
  message(FATAL_ERROR "EdgeRuntime currently supports GCC or Clang on Linux only")
endif()

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

add_subdirectory(src)
add_subdirectory(tools)
if(BUILD_TESTING)
  add_subdirectory(test)
endif()

install(
  TARGETS edge_runtime
  EXPORT EdgeRuntimeTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(
  DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  FILES_MATCHING
    PATTERN "*.h"
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
