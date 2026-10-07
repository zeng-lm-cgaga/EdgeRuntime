# 独立下游工程只通过安装包验证公共头和导出目标可用。
cmake_minimum_required(VERSION 3.22)
project(er_consume_demo VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(EdgeRuntime CONFIG REQUIRED)

add_executable(consume_demo
	main.cpp
	src/channel_flow.cpp
	src/common.cpp
	src/pool_flow.cpp
	src/queue_flow.cpp)
target_include_directories(consume_demo PRIVATE include)
target_link_libraries(consume_demo PRIVATE EdgeRuntime::edge_runtime)
target_compile_options(consume_demo PRIVATE
	-Wall
	-Wextra
	-Wpedantic
	-Wconversion
	-Wshadow
	-Werror)
