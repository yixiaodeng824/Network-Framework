#include "Logger.h"

#include <atomic>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <sstream>
#include <thread>

namespace logger {

namespace {

// 保护时间格式化 + 控制台输出,保证整行日志不会被其他线程插入
std::mutex g_mutex;

// 最低显示级别,原子变量让 setLogLevel / isEnabled 无需加锁
std::atomic<int> g_minLevel{LOG_LEVEL_DEBUG};

const char* kLevelNames[] = {"DEBUG", "INFO", "WARN", "ERROR"};
constexpr int kLevelCount = 4;

} // namespace

void setLogLevel(int level) {
    g_minLevel.store(level, std::memory_order_relaxed);
}

int getLogLevel() {
    return g_minLevel.load(std::memory_order_relaxed);
}

bool isEnabled(int level) {
    return level >= g_minLevel.load(std::memory_order_relaxed);
}

void logMessage(int level, const char* fmt, ...) {
    if (!isEnabled(level)) {
        return;
    }

    // 1. 格式化消息本体(超过 1024 字节会被截断)
    char msg[1024];
    va_list args;
    va_start(args, fmt);
    vsnprintf(msg, sizeof(msg), fmt, args);
    va_end(args);

    // 2. 取当前线程 id(std::thread::id 可以无锁安全读取)
    std::ostringstream tid;
    tid << std::this_thread::get_id();

    // 3. 进入临界区:时间格式化 + 一次输出,保证一行完整、不交错
    std::lock_guard<std::mutex> lock(g_mutex);

    std::time_t now = std::time(nullptr);
    std::tm timeInfo{};
#if defined(_WIN32)
    localtime_s(&timeInfo, &now);
#else
    localtime_r(&now, &timeInfo);
#endif

    char timeBuf[32];
    std::strftime(timeBuf, sizeof(timeBuf), "%Y-%m-%d %H:%M:%S", &timeInfo);

    int idx = level;
    if (idx < 0) {
        idx = 0;
    }
    if (idx >= kLevelCount) {
        idx = kLevelCount - 1;
    }

    std::fprintf(stdout, "[%s] [%s] [tid=%s] %s\n",
                 timeBuf, kLevelNames[idx], tid.str().c_str(), msg);
    std::fflush(stdout);
}

} // namespace logger
