// 异步日志测试程序:验证 LOG_* 只入队、后台线程批量落盘
//
// 编译(g++ / MinGW):
//   g++ -std=c++17 -Wall -Wextra -pedantic -I src/log -I src/thread
//       src/log/Logger.cpp src/thread/MessageQueue.cpp src/log/test_logger.cpp
//       -o test_logger
// 运行:
//   ./test_logger
//
// 验证点:
//   1. 异步:LOG_* 只把消息推入队列,立即返回(不碰磁盘)
//   2. setLogLevel 设为 INFO 后,DEBUG 不落盘
//   3. 多线程同时打日志,每行完整、不交错
//   4. shutdown 后剩余日志全部落盘
//   5. 有界队列满时"丢最旧保最新"策略正确、不阻塞生产者

#include "Logger.h"
#include "MessageQueue.h"

#include <cstdio>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace {

int g_failures = 0;

void expect(bool ok, const char* what) {
    std::printf("%s: %s\n", ok ? "PASS" : "FAIL", what);
    if (!ok) {
        ++g_failures;
    }
}

// 解析日志行里 "key=数字" 后面的数字;找不到返回 -1
int parseNumberAfter(const std::string& line, const std::string& key) {
    const size_t pos = line.find(key);
    if (pos == std::string::npos) {
        return -1;
    }
    size_t i = pos + key.size();
    if (i >= line.size() || line[i] < '0' || line[i] > '9') {
        return -1;
    }
    int v = 0;
    while (i < line.size() && line[i] >= '0' && line[i] <= '9') {
        v = v * 10 + (line[i] - '0');
        ++i;
    }
    return v;
}

struct Stats {
    int total = 0;
    int worker = 0;        // 确定性阶段:4 线程 x 100 条
    int stress = 0;        // 压测阶段:8 线程 x 25000 条
    int debugFiltered = 0; // 应为 0(INFO 期间打的 DEBUG)
    int newClient = 0;     // 应为 1
    int tidMarkers = 0;    // 普通日志行应恰好 1 个
    int overflowWarn = 0;  // 队列丢弃提示行数
    bool seen[4][100] = {}; // worker 每对 (i,j) 应恰好出现一次
};

Stats scanFile(const char* path) {
    Stats s;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        ++s.total;
        if (line.find("worker=") != std::string::npos) {
            ++s.worker;
            const int i = parseNumberAfter(line, "worker=");
            const int j = parseNumberAfter(line, " j=");
            if (i >= 0 && i < 4 && j >= 0 && j < 100) {
                s.seen[i][j] = true;
            }
        }
        if (line.find("stress=") != std::string::npos) {
            ++s.stress;
        }
        if (line.find("this line is DEBUG") != std::string::npos) {
            ++s.debugFiltered;
        }
        if (line.find("new client fd=7") != std::string::npos) {
            ++s.newClient;
        }
        if (line.find("queue overflow") != std::string::npos) {
            ++s.overflowWarn;
        }
        for (size_t pos = line.find("[tid="); pos != std::string::npos;
             pos = line.find("[tid=", pos + 1)) {
            ++s.tidMarkers;
        }
    }
    return s;
}

// 直接验证 MessageQueue 有界模式:满时丢最旧、保留最新、计数正确
void testMessageQueueDropPolicy() {
    MessageQueue q(4);
    std::vector<int> got;
    for (int i = 0; i < 10; ++i) {
        q.pushOverrunOldest([&got, i] { got.push_back(i); });
    }
    expect(q.size() == 4, "有界队列容量保持为 4");
    expect(q.droppedCount() == 6, "丢弃计数 = 6(丢最旧保最新)");

    MessageQueue::Task t;
    while (q.tryPop(t)) {
        t();
    }
    expect(got.size() == 4 && got[0] == 6 && got[3] == 9,
           "丢最旧:保留最后 4 条(6,7,8,9)");
}

} // namespace

int main() {
    // 第一次打日志前配置:日志文件 + 小容量有界队列(满时丢最旧)
    logger::init("test_logger.log", 1024);

    // ---- 阶段 1:确定性正确性 ----
    logger::setLogLevel(logger::LOG_LEVEL_INFO);
    LOG_DEBUG("this line is DEBUG, should NOT appear after setting INFO");
    LOG_INFO("start listening on port %d", 8888);
    LOG_WARN("send buffer full, len=%d", 1234);
    LOG_ERROR("recv failed: %s", "Connection reset by peer");

    logger::setLogLevel(logger::LOG_LEVEL_DEBUG);
    LOG_DEBUG("new client fd=%d", 7);

    // 4 个线程同时打日志,检查每行完整、不交错
    {
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
    }
    // 同步点:等阶段 1 的日志全部落盘,再进入压测阶段
    logger::flush();

    // ---- 阶段 2:高并发压日志,验证"丢最旧不阻塞生产者" ----
    {
        std::vector<std::thread> threads;
        for (int i = 0; i < 8; ++i) {
            threads.emplace_back([i] {
                for (int j = 0; j < 25000; ++j) {
                    LOG_DEBUG("stress=%d j=%d", i, j);
                }
            });
        }
        for (auto& t : threads) {
            t.join();
        }
    }

    // 关闭后台线程:排空队列 + flush,确保落盘后再检查
    logger::shutdown();

    // ---- 校验 ----
    testMessageQueueDropPolicy();

    const Stats s = scanFile("test_logger.log");

    expect(s.worker == 400, "400 条 worker 日志全部落盘");
    int missing = 0;
    for (int i = 0; i < 4; ++i) {
        for (int j = 0; j < 100; ++j) {
            if (!s.seen[i][j]) {
                ++missing;
            }
        }
    }
    expect(missing == 0, "400 条 worker 日志每行完整、无丢失无交错");
    expect(s.debugFiltered == 0, "INFO 级别下 DEBUG 不落盘");
    expect(s.newClient == 1, "恢复 DEBUG 级别后日志正常落盘");
    expect(s.tidMarkers == s.total - s.overflowWarn, "普通日志行每行恰好一个 [tid=] 标记");
    expect(s.stress > 0, "压测日志有落盘(部分被丢弃也可接受)");
    std::printf("统计: 总行数=%d, worker=%d, stress=%d, 丢弃提示=%d, tid标记=%d\n",
                s.total, s.worker, s.stress, s.overflowWarn, s.tidMarkers);

    if (g_failures == 0) {
        std::printf("\nALL TESTS PASSED\n");
        return 0;
    }
    std::printf("\n%d TEST(S) FAILED\n", g_failures);
    return 1;
}
