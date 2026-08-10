#pragma once

#include <cstdarg>

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
