#pragma once

#include <cstdarg>
#include <cstddef>

namespace logger {

// 日志级别
enum LogLevel {
    LOG_LEVEL_DEBUG = 0,
    LOG_LEVEL_INFO  = 1,
    LOG_LEVEL_WARN  = 2,
    LOG_LEVEL_ERROR = 3,
};

// 设置最低显示级别:低于该级别的日志不会输出
// 例如 setLogLevel(logger::LOG_LEVEL_INFO) 后,DEBUG 不再打印
void setLogLevel(int level);

// 获取当前最低显示级别
int getLogLevel();

// 某个级别当前是否会被输出(宏内部用来避免无谓的格式化开销)
bool isEnabled(int level);

// 内部实现:打印一条格式化日志(线程安全)。
// 请勿直接调用,统一使用 LOG_DEBUG / LOG_INFO / LOG_WARN / LOG_ERROR 宏。
void logMessage(int level, const char* fmt, ...)
#if defined(__GNUC__)
    __attribute__((format(printf, 2, 3)))
#endif
    ;

// 初始化异步日志(必须在第一次打日志之前调用,否则使用默认值):
//   filePath      - 日志文件路径,默认 "server.log"
//   queueCapacity - 日志队列容量,默认 65536;传 0 表示用默认容量
//
// 队列满策略:丢日志(丢弃最旧、保留最新),而不是阻塞生产者。理由:
//   1) 本 issue 的核心目标是压测时 worker 线程不被日志拖垮,
//      阻塞式背压会把队列压力传回业务线程,违背初衷;
//   2) 最新日志通常比最旧日志更有价值(错误/故障信息集中在最近),
//      丢旧保新对"关键信息不丢"的损失最小;
//   3) 丢弃不是无声的:后台线程会把累计丢弃条数周期性写进日志文件。
//
// 重复调用、或第一次打日志之后再调用,会被忽略。
void init(const char* filePath = nullptr, size_t queueCapacity = 65536);

// 停止后台日志线程:排空剩余日志、flush 并关闭文件。
// 进程退出时会自动调用;测试中可显式调用以检查落盘结果。
// 调用后再打日志会被直接丢弃(幂等,可重复调用)。
void shutdown();

// 阻塞等待:调用之前已入队的日志全部落盘后返回。
// 测试或需要同步点时使用;shutdown 之后调用会立即返回。
void flush();

} // namespace logger

// 用法同 printf,例如:
//   LOG_INFO("start listening on port %d", port);
//   LOG_ERROR("recv failed: %s", strerror(errno));
#define LOG_DEBUG(...)                                                        \
    do {                                                                      \
        if (::logger::isEnabled(::logger::LOG_LEVEL_DEBUG)) {                 \
            ::logger::logMessage(::logger::LOG_LEVEL_DEBUG, __VA_ARGS__);     \
        }                                                                     \
    } while (0)

#define LOG_INFO(...)                                                         \
    do {                                                                      \
        if (::logger::isEnabled(::logger::LOG_LEVEL_INFO)) {                  \
            ::logger::logMessage(::logger::LOG_LEVEL_INFO, __VA_ARGS__);      \
        }                                                                     \
    } while (0)

#define LOG_WARN(...)                                                         \
    do {                                                                      \
        if (::logger::isEnabled(::logger::LOG_LEVEL_WARN)) {                  \
            ::logger::logMessage(::logger::LOG_LEVEL_WARN, __VA_ARGS__);      \
        }                                                                     \
    } while (0)

#define LOG_ERROR(...)                                                        \
    do {                                                                      \
        if (::logger::isEnabled(::logger::LOG_LEVEL_ERROR)) {                 \
            ::logger::logMessage(::logger::LOG_LEVEL_ERROR, __VA_ARGS__);     \
        }                                                                     \
    } while (0)
