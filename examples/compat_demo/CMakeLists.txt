cmake_minimum_required(VERSION 3.22)
project(selfipcs_compat_demo VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SELFIPCS_COMPAT_LEGACY_FIRST "Find EdgeRuntime before SelfIPCs" OFF)

if(SELFIPCS_COMPAT_LEGACY_FIRST)
  find_package(EdgeRuntime CONFIG REQUIRED)
  find_package(SelfIPCs CONFIG REQUIRED)
else()
  find_package(SelfIPCs CONFIG REQUIRED)
  find_package(EdgeRuntime CONFIG REQUIRED)
endif()

add_executable(compat_demo main.cpp)
target_link_libraries(compat_demo PRIVATE SelfIPCs::self_ipcs EdgeRuntime::edge_runtime)
target_compile_options(compat_demo PRIVATE
  -Wall
  -Wextra
  -Wpedantic
  -Wconversion
  -Wshadow
  -Werror)

enable_testing()
add_test(NAME installed_compat_demo COMMAND compat_demo)
