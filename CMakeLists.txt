cmake_minimum_required(VERSION 3.20)

project(NetworkFramework LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_library(network_framework
    src/net/epoll_server.cpp
    src/net/connection.cpp
    src/net/memory_pool.cpp
    src/net/io_uring_server.cpp

    src/log/Logger.cpp
    src/net/main_reactor.cpp 
    src/thread/MessageQueue.cpp
    src/thread/ThreadPool.cpp
    src/thread/work_thread.cpp
)

target_include_directories(network_framework
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/src/net
        ${CMAKE_CURRENT_SOURCE_DIR}/src/log
        ${CMAKE_CURRENT_SOURCE_DIR}/src/thread
)

target_link_libraries(network_framework
    PUBLIC Threads::Threads
)

add_executable(server
    src/net/main.cpp
)

target_link_libraries(server
    PRIVATE network_framework
)

include(CTest)

find_package(Python3 REQUIRED COMPONENTS Interpreter)

add_test(
    NAME integration_echo
    COMMAND
        ${Python3_EXECUTABLE}
        -m pytest
        ${CMAKE_SOURCE_DIR}/tests/integration/test_echo.py
)

set_tests_properties(
    integration_echo
    PROPERTIES
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        ENVIRONMENT "NF_SERVER_BINARY=$<TARGET_FILE:server>;NF_SERVER_MODE=echo"
)

add_test(
    NAME integration_http
    COMMAND
        ${Python3_EXECUTABLE}
        -m pytest
        ${CMAKE_SOURCE_DIR}/tests/integration/test_http_demo.py
)

set_tests_properties(
    integration_http
    PROPERTIES
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        ENVIRONMENT "NF_SERVER_BINARY=$<TARGET_FILE:server>;NF_SERVER_MODE=http"
)

add_custom_target(bench
    COMMAND
        ${CMAKE_COMMAND} -E env
        NF_SERVER_BINARY=$<TARGET_FILE:server>
        ${Python3_EXECUTABLE}
        -m benchmarks.echo

    DEPENDS server

    WORKING_DIRECTORY
        ${CMAKE_SOURCE_DIR}

    USES_TERMINAL
)
add_custom_target(bench_mp
    COMMAND
        \ -E env
        NF_SERVER_BINARY=\$<TARGET_FILE:server>
                -m benchmarks.echo_mp

    DEPENDS server

    WORKING_DIRECTORY
        
    USES_TERMINAL
)
