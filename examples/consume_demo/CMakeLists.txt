# 独立下游工程只通过安装包验证公共头和导出目标可用。
cmake_minimum_required(VERSION 3.22)
project(er_consume_demo CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(EdgeRuntime CONFIG REQUIRED)
add_executable(consume_demo main.cpp)
target_link_libraries(consume_demo PRIVATE EdgeRuntime::edge_runtime)
target_compile_options(consume_demo PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
