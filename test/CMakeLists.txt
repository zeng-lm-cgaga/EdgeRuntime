# 测试按单元测试和真实跨进程集成测试分开构建。

find_package(Threads REQUIRED)
find_package(GTest REQUIRED)
include(GoogleTest)

add_subdirectory(unit)

add_subdirectory(integration)
