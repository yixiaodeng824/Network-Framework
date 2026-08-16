#include "Logger.h"

#include "MessageQueue.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

namespace logger {

namespace {

// 最低显示级别(原子变量:setLogLevel / isEnabled 无需加锁)
std::atomic<int> g_minLevel{LOG_LEVEL_DEBUG};

const char* kLevelNames[] = {"DEBUG", "INFO", "WARN", "ERROR"};
constexpr int kLevelCount = 4;

// 默认日志文件与队列容量(未调用 init 时使用)
constexpr char kDefaultFilePath[] = "server.log";
constexpr size_t kDefaultQueueCapacity = 65536;

// 单条日志消息最大长度(超出部分截断,与旧版一致)
constexpr size_t kMaxMessageLen = 1024;
// 后台线程单次最多连续处理的任务数(攒批写盘)
constexpr size_t kMaxBatch = 64;
// stdio 缓冲区大小:攒满才真正发起写盘,减少 I/O 次数
constexpr size_t kWriteBufferSize = 64 * 1024;

// ---- 启动前配置(init 使用;后台线程启动后不再生效) ----
std::mutex g_configMutex;
std::string g_filePath = kDefaultFilePath;
size_t g_queueCapacity = kDefaultQueueCapacity;
bool g_started = false;

// 线程 id 字符串化缓存:用 POD 字符数组(无动态初始化、无析构函数),
// 避免 MinGW 下 thread_local std::string 在高并发堆分配时出现堆损坏
thread_local char g_tidBuf[48];
thread_local bool g_tidReady = false;

int clampLevel(int level) {
    if (level < 0) {
        return 0;
    }
    if (level >= kLevelCount) {
        return kLevelCount - 1;
    }
    return level;
}

// 线程安全的时间字符串(localtime_s / localtime_r 都输出到调用方提供的 tm)
std::string nowTimeString() {
    std::time_t now = std::time(nullptr);
    std::tm timeInfo{};
#if defined(_WIN32)
    localtime_s(&timeInfo, &now);
#else
    localtime_r(&now, &timeInfo);
#endif
    char timeBuf[32];
    std::strftime(timeBuf, sizeof(timeBuf), "%Y-%m-%d %H:%M:%S", &timeInfo);
    return std::string(timeBuf);
}

// 异步日志器:
//   LOG_* 只负责格式化并入队,立即返回(不碰磁盘);
//   真正的写盘由后台线程从 MessageQueue 批量取出后完成。
class AsyncLogger {
public:
    AsyncLogger() {
        {
            std::lock_guard<std::mutex> lock(g_configMutex);
            filePath_ = g_filePath;
            capacity_ = g_queueCapacity;
            g_started = true;
        }
        queue_ = std::make_unique<MessageQueue>(capacity_);
        writer_ = std::thread(&AsyncLogger::run, this);
    }

    ~AsyncLogger() {
        shutdown();
    }

    AsyncLogger(const AsyncLogger&) = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;

    // 生产端:把整行日志塞进队列,立即返回。
    // 队列满时采用"丢最旧保最新"策略(pushOverrunOldest),绝不阻塞生产者。
    void enqueue(std::string line) {
        if (closed_.load(std::memory_order_relaxed)) {
            return; // 已关闭,后续日志直接丢弃
        }
        queue_->pushOverrunOldest([this, line = std::move(line)] {
            writeLine(line);
        });
    }

    // 关闭:先禁止新消息入队,再排空队列 + flush,最后 join 后台线程
    void shutdown() {
        bool expected = false;
        if (!closed_.compare_exchange_strong(expected, true)) {
            return; // 已经关过
        }
        queue_->close();
        if (writer_.joinable()) {
            writer_.join();
        }
    }

    // 屏障任务:等到它被后台线程执行,说明它之前入队的日志都已写盘
    void flush() {
        if (closed_.load(std::memory_order_relaxed)) {
            return; // 已关闭,无可等待
        }
        auto done = std::make_shared<std::promise<void>>();
        std::future<void> fut = done->get_future();
        const bool ok = queue_->pushOverrunOldest([done] { done->set_value(); });
        if (ok) {
            fut.wait();
        }
    }

private:
    void run() {
        file_ = std::fopen(filePath_.c_str(), "a");
        if (file_ == nullptr) {
            // 打不开文件就退回 stdout,保证日志不丢
            file_ = stdout;
            std::fprintf(stderr,
                         "[logger] cannot open log file '%s', fallback to stdout\n",
                         filePath_.c_str());
        } else {
            // 大缓冲区:攒满才真正写盘,配合下面的批量取队列减少 I/O
            std::setvbuf(file_, writeBuf_, _IOFBF, kWriteBufferSize);
        }

        size_t lastReportedDrops = 0;
        while (true) {
            MessageQueue::Task task;
            if (!queue_->pop(task)) {
                break; // 队列已关闭且已排空
            }
            task();

            // 批量:把已经堆积的消息尽量一次取完,减少写盘次数
            size_t batch = 1;
            while (batch < kMaxBatch && queue_->tryPop(task)) {
                task();
                ++batch;
            }

            // 队列已空 → 立即 flush,保证日志及时可见;
            // 队列仍有积压 → 继续攒着,下次批量再写
            if (queue_->size() == 0) {
                std::fflush(file_);
                reportDropsIfChanged(lastReportedDrops);
            }
        }

        std::fflush(file_);
        reportDropsIfChanged(lastReportedDrops);
        if (file_ != nullptr && file_ != stdout) {
            std::fclose(file_);
            file_ = nullptr;
        }
    }

    void writeLine(const std::string& line) {
        if (file_ != nullptr) {
            std::fwrite(line.data(), 1, line.size(), file_);
        }
    }

    // 队列发生过"丢最旧"时,把累计丢弃数写进日志,让丢弃可见可审计
    void reportDropsIfChanged(size_t& lastReported) {
        const size_t dropped = queue_->droppedCount();
        if (dropped == lastReported) {
            return;
        }
        lastReported = dropped;
        if (file_ != nullptr) {
            std::fprintf(file_,
                         "[%s] [WARN] [logger] queue overflow: dropped %zu messages so far\n",
                         nowTimeString().c_str(), dropped);
        }
    }

    std::string filePath_;
    size_t capacity_ = kDefaultQueueCapacity;
    std::unique_ptr<MessageQueue> queue_;
    std::thread writer_;
    std::atomic<bool> closed_{false};
    FILE* file_ = nullptr;
    char writeBuf_[kWriteBufferSize];
};

// 首次使用(打第一条日志 / shutdown)时才构造并启动后台线程
AsyncLogger& instance() {
    static AsyncLogger logger;
    return logger;
}

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

void init(const char* filePath, size_t queueCapacity) {
    std::lock_guard<std::mutex> lock(g_configMutex);
    if (g_started) {
        return; // 后台线程已启动,配置不再生效
    }
    if (filePath != nullptr && filePath[0] != '\0') {
        g_filePath = filePath;
    }
    g_queueCapacity = (queueCapacity == 0) ? kDefaultQueueCapacity : queueCapacity;
}

void shutdown() {
    instance().shutdown();
}

void flush() {
    instance().flush();
}

void logMessage(int level, const char* fmt, ...) {
    if (!isEnabled(level)) {
        return;
    }

    // 1. 格式化消息本体(超过 1024 字节会被截断,与旧版一致)
    char msg[kMaxMessageLen];
    va_list args;
    va_start(args, fmt);
    vsnprintf(msg, sizeof(msg), fmt, args);
    va_end(args);

    // 2. 组装完整行:时间在入队前打好,后台按入队顺序落盘,
    //    同一生产者线程的日志严格保持产生顺序
    const std::string timeStr = nowTimeString();
    const int idx = clampLevel(level);

    if (!g_tidReady) {
        std::ostringstream oss;
        oss << std::this_thread::get_id();
        const std::string tidStr = oss.str();
        std::snprintf(g_tidBuf, sizeof(g_tidBuf), "%s", tidStr.c_str());
        g_tidReady = true;
    }

    char line[kMaxMessageLen + 96];
    const int written = std::snprintf(line, sizeof(line), "[%s] [%s] [tid=%s] %s\n",
                                      timeStr.c_str(), kLevelNames[idx],
                                      g_tidBuf, msg);
    if (written <= 0) {
        return;
    }
    const size_t len = (static_cast<size_t>(written) < sizeof(line) - 1)
                           ? static_cast<size_t>(written)
                           : sizeof(line) - 1;

    // 3. 只入队,立即返回:磁盘 I/O 全部交给后台线程
    instance().enqueue(std::string(line, len));
}

} // namespace logger
