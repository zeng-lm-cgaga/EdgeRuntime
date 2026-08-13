# consume_demo — standalone downstream consumer (design §26: "public API 和
# target 可安装"). This project does NOT belong to the EdgeRuntime build; it
# consumes the INSTALLED package via find_package, proving the exported target
# and the 7 installed public headers work in a fresh, independent CMake project.
#
#   cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/install-prefix
#   cmake --build build
#   ./build/consume_demo
cmake_minimum_required(VERSION 3.22)
project(er_consume_demo CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(EdgeRuntime CONFIG REQUIRED)
add_executable(consume_demo main.cpp)
target_link_libraries(consume_demo PRIVATE EdgeRuntime::edge_runtime)
# Deliberately as strict as the library itself: the public headers must be
# -Werror-clean even for downstream consumers, not just in-tree.
target_compile_options(consume_demo PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
