cmake_minimum_required(VERSION 3.20)
project(NetworkFramework LANGUAGES CXX)

find_package(Threads REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

add_library(network_framework
    src/net/epoll_server.cpp
    src/net/connection.cpp
    src/log/Logger.cpp
    src/thread/MessageQueue.cpp
    src/thread/ThreadPool.cpp
    src/thread/work_thread.cpp
)

target_compile_features(network_framework PUBLIC cxx_std_17)
target_include_directories(network_framework PUBLIC
    src
    src/net
    src/log
    src/thread
)
target_link_libraries(network_framework PUBLIC Threads::Threads)

add_executable(server src/net/main.cpp)
target_link_libraries(server PRIVATE network_framework)

include(CTest)

if(BUILD_TESTING)
    add_test(
        NAME integration
        COMMAND ${CMAKE_COMMAND} -E env
            NF_SERVER_BINARY=$<TARGET_FILE:server>
            ${Python3_EXECUTABLE} -m pytest tests/integration
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    )
endif()

add_custom_target(run
    COMMAND $<TARGET_FILE:server>
    DEPENDS server
    USES_TERMINAL
)

add_custom_target(bench
    COMMAND ${CMAKE_COMMAND} -E env
        NF_SERVER_BINARY=$<TARGET_FILE:server>
        ${Python3_EXECUTABLE} -m benchmarks.echo
    DEPENDS server
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    USES_TERMINAL
)