# 独立下游工程只通过安装包验证公共头和导出目标可用。
cmake_minimum_required(VERSION 3.22)
project(selfipcs_consume_demo VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SELFIPCS_DEMO_USE_LEGACY "Use the legacy EdgeRuntime installed package" OFF)

if(SELFIPCS_DEMO_USE_LEGACY)
  find_package(EdgeRuntime CONFIG REQUIRED)
  set(_consume_demo_target EdgeRuntime::edge_runtime)
else()
  find_package(SelfIPCs CONFIG REQUIRED)
  set(_consume_demo_target SelfIPCs::self_ipcs)
endif()

add_executable(consume_demo
  main.cpp
  src/channel_flow.cpp
  src/common.cpp
  src/pool_flow.cpp
  src/queue_flow.cpp)
target_include_directories(consume_demo PRIVATE include)
target_link_libraries(consume_demo PRIVATE ${_consume_demo_target})
if(SELFIPCS_DEMO_USE_LEGACY)
  target_compile_definitions(consume_demo PRIVATE SELFIPCS_DEMO_USE_LEGACY=1)
endif()
target_compile_options(consume_demo PRIVATE
  -Wall
  -Wextra
  -Wpedantic
  -Wconversion
  -Wshadow
  -Werror)

enable_testing()
add_test(NAME installed_consume_demo COMMAND consume_demo)
