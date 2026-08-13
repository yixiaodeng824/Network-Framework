#!/bin/bash
cd ~/projects/for_linux
g++ -std=c++17 -pthread -I src/thread -I src/log src/net/main.cpp src/net/epoll_server.cpp src/log/Logger.cpp src/thread/MessageQueue.cpp src/thread/ThreadPool.cpp src/thread/work_thread.cpp -o server.out
echo "Build done!"
