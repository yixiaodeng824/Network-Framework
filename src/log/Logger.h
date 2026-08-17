#pragma once

#include <cstdarg>
#include <cstddef>
#include <memory>

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

// ---- 配置文件 ----
// 支持通过配置文件定制日志行为,默认配置文件名为 "logger.conf"
// (与可执行文件同目录)。程序第一次打日志时会自动加载;
// 也可用 initFromFile() 显式指定路径。配置文件格式:
//
// 配置文件格式示例(注释必须单独成行,# 开头):
//   # 日志级别:debug / info / warn / error(或 0~3)
//   level = debug
//   # 输出格式(占位符见下方说明)
//   format = [%t] [%l] %m
//   # 输出朝向:console / file,逗号分隔可同时多个
//   target = console,file
//   # 日志文件路径(仅 target 含 file 时生效)
//   file = server.log
//   # 异步队列容量(0 表示默认 65536)
//   queue_capacity = 65536
//
// format 支持的占位符:
//   %t  时间(YYYY-MM-DD HH:MM:SS)
//   %l  日志级别(DEBUG / INFO / WARN / ERROR)
//   %T  线程 id
//   %m  消息正文
//   %%  字面量 '%'
// 格式末尾会自动补换行。默认格式: [%t] [%l] [tid=%T] %m

// 输出目标接口(可扩展):
//   内置输出目标:控制台(Console)和文件(File),由配置 target 控制;
//   需要新增输出方式时,继承 Sink 并在第一次打日志前调用 addSink() 注册。
//   注意:write / flush 只在后台日志线程中被调用,实现无需自加锁。
class Sink {
public:
    virtual ~Sink() = default;

    // 写入一行完整的日志(渲染后的内容,含结尾 '\n')
    virtual void write(const char* data, size_t len) = 0;

    // 冲刷底层缓冲,保证数据对外可见
    virtual void flush() = 0;
};

// 注册自定义输出目标(必须在第一次打日志之前调用,之后调用被忽略)。
// sink 的所有权移交给日志器,shutdown 后会被销毁,请勿继续访问原指针。
void addSink(std::unique_ptr<Sink> sink);

// 初始化异步日志(必须在第一次打日志之前调用,否则使用默认值):
//   filePath      - 日志文件路径,默认 "server.log"
//   queueCapacity - 日志队列容量,默认 65536;传 0 表示用默认容量
// 显式调用 init / initFromFile 后,不再自动加载 logger.conf。
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

// 从配置文件加载日志配置(必须在第一次打日志之前调用,之后调用被忽略)。
// path 默认 "logger.conf";文件不存在或解析失败时,未配置的项保持默认值。
// 与 init() 是二选一的关系(重叠项以最后一次调用为准)。
void initFromFile(const char* path = "logger.conf");

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
