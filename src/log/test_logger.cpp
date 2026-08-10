// 临时测试程序:验证日志系统
//
// 编译(g++):
//   g++ -std=c++17 -Wall -Wextra -pedantic -I src/log src/log/Logger.cpp src/log/test_logger.cpp -o test_logger
//
// 运行:
//   ./test_logger
//
// 验证点:
//   1. 格式为 [时间] [级别] [tid=...] 消息
//   2. setLogLevel 设为 INFO 后,DEBUG 不再输出
//   3. 多线程同时打日志,每一行都完整、不交错

#include "Logger.h"

#include <thread>
#include <vector>

int main() {
    // 先设成 INFO:下面的 DEBUG 应该不显示
    logger::setLogLevel(logger::LOG_LEVEL_INFO);

    LOG_DEBUG("this line is DEBUG, should NOT appear after setting INFO");
    LOG_INFO("start listening on port %d", 8888);
    LOG_WARN("send buffer full, len=%d", 1234);
    LOG_ERROR("recv failed: %s", "Connection reset by peer");

    // 再放回 DEBUG:这行应该显示
    logger::setLogLevel(logger::LOG_LEVEL_DEBUG);
    LOG_DEBUG("new client fd=%d", 7);

    // 4 个线程同时打日志,检查输出不交错
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([i] {
            for (int j = 0; j < 100; ++j) {
                LOG_INFO("worker=%d j=%d", i, j);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
