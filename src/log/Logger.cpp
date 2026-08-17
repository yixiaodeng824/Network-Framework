#include "Logger.h"

#include "MessageQueue.h"

#include <atomic>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace logger {

namespace {

// 最低显示级别(原子变量:setLogLevel / isEnabled 无需加锁)
std::atomic<int> g_minLevel{LOG_LEVEL_DEBUG};

const char* kLevelNames[] = {"DEBUG", "INFO", "WARN", "ERROR"};
constexpr int kLevelCount = 4;

// 默认配置(未显式 init / initFromFile 且没有配置文件时使用)
constexpr char kDefaultConfigFile[] = "logger.conf";
constexpr char kDefaultFilePath[] = "server.log";
constexpr char kDefaultFormat[] = "[%t] [%l] [tid=%T] %m";
constexpr size_t kDefaultQueueCapacity = 65536;

// 单条日志消息最大长度(超出部分截断,与旧版一致)
constexpr size_t kMaxMessageLen = 1024;
// 后台线程单次最多连续处理的任务数(攒批写盘)
constexpr size_t kMaxBatch = 64;
// stdio 缓冲区大小:攒满才真正发起写盘,减少 I/O 次数
constexpr size_t kWriteBufferSize = 64 * 1024;

// ---- 启动前配置(init / initFromFile / addSink 使用;后台线程启动后不再生效) ----
std::mutex g_configMutex;
std::string g_filePath = kDefaultFilePath;
std::string g_format = kDefaultFormat;
size_t g_queueCapacity = kDefaultQueueCapacity;
bool g_fileEnabled = true;    // 默认只输出到文件(与旧版行为一致)
bool g_consoleEnabled = false;
bool g_configLoaded = false;  // 是否已显式 init / initFromFile
bool g_started = false;
std::vector<std::unique_ptr<Sink>> g_extraSinks;

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

std::string trim(const std::string& s) {
    size_t begin = 0;
    size_t end = s.size();
    while (begin < end && std::isspace(static_cast<unsigned char>(s[begin]))) {
        ++begin;
    }
    while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(begin, end - begin);
}

std::string toLower(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

// 解析级别配置项:debug / info / warn / error(或 0~3);失败返回 -1
int parseLevel(const std::string& raw) {
    const std::string s = toLower(trim(raw));
    if (s == "debug" || s == "0") {
        return LOG_LEVEL_DEBUG;
    }
    if (s == "info" || s == "1") {
        return LOG_LEVEL_INFO;
    }
    if (s == "warn" || s == "warning" || s == "2") {
        return LOG_LEVEL_WARN;
    }
    if (s == "error" || s == "err" || s == "3") {
        return LOG_LEVEL_ERROR;
    }
    return -1;
}

// 处理一行 "key = value" 配置;必须在持有 g_configMutex 时调用
void parseConfigLine(const std::string& key, const std::string& value, int lineNo) {
    if (key == "level") {
        const int lv = parseLevel(value);
        if (lv >= 0) {
            g_minLevel.store(lv, std::memory_order_relaxed);
        } else {
            std::fprintf(stderr, "[logger] config line %d: invalid level '%s'\n",
                         lineNo, value.c_str());
        }
    } else if (key == "format") {
        if (!value.empty()) {
            g_format = value;
        }
    } else if (key == "target") {
        // 输出朝向:逗号分隔的多个目标,便于后续扩展更多输出方式
        bool console = false;
        bool file = false;
        size_t pos = 0;
        while (pos <= value.size()) {
            const size_t comma = value.find(',', pos);
            const std::string token = toLower(trim(
                value.substr(pos, comma == std::string::npos
                                      ? std::string::npos
                                      : comma - pos)));
            if (token == "console" || token == "stdout") {
                console = true;
            } else if (token == "file") {
                file = true;
            } else if (!token.empty()) {
                std::fprintf(stderr, "[logger] config line %d: unknown target '%s'\n",
                             lineNo, token.c_str());
            }
            if (comma == std::string::npos) {
                break;
            }
            pos = comma + 1;
        }
        if (!console && !file) {
            std::fprintf(stderr,
                         "[logger] config line %d: no valid target, logs discarded\n",
                         lineNo);
        }
        g_consoleEnabled = console;
        g_fileEnabled = file;
    } else if (key == "file" || key == "file_path" || key == "log_file") {
        if (!value.empty()) {
            g_filePath = value;
        }
    } else if (key == "queue_capacity" || key == "queue") {
        char* end = nullptr;
        const unsigned long long v = std::strtoull(value.c_str(), &end, 10);
        if (end != value.c_str() && v > 0) {
            g_queueCapacity = static_cast<size_t>(v);
        }
    } else {
        std::fprintf(stderr, "[logger] config line %d: unknown key '%s'\n",
                     lineNo, key.c_str());
    }
}

// 解析配置文件;# 开头为注释,空行忽略;必须在持有 g_configMutex 时调用
void parseConfigFile(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        std::fprintf(stderr, "[logger] config file '%s' not found, use defaults\n",
                     path.c_str());
        return;
    }
    std::string line;
    int lineNo = 0;
    while (std::getline(in, line)) {
        ++lineNo;
        const std::string s = trim(line);
        if (s.empty() || s[0] == '#') {
            continue;
        }
        const size_t eq = s.find('=');
        if (eq == std::string::npos) {
            std::fprintf(stderr,
                         "[logger] config line %d ignored (expect 'key = value'): %s\n",
                         lineNo, line.c_str());
            continue;
        }
        const std::string key = toLower(trim(s.substr(0, eq)));
        const std::string value = trim(s.substr(eq + 1));
        if (!key.empty()) {
            parseConfigLine(key, value, lineNo);
        }
    }
}

// 按格式字符串渲染一行日志(末尾自动补 '\n')
std::string renderLine(const std::string& format, const std::string& time,
                       const char* levelName, const std::string& tid,
                       const std::string& msg) {
    std::string out;
    out.reserve(format.size() + msg.size() + 48);
    for (size_t i = 0; i < format.size(); ++i) {
        if (format[i] != '%') {
            out.push_back(format[i]);
            continue;
        }
        ++i;
        if (i >= format.size()) {
            out.push_back('%');
            break;
        }
        switch (format[i]) {
            case 't':
                out += time;
                break;
            case 'l':
                out += levelName;
                break;
            case 'T':
                out += tid;
                break;
            case 'm':
                out += msg;
                break;
            case '%':
                out.push_back('%');
                break;
            default:
                out.push_back('%');
                out.push_back(format[i]);
                break;
        }
    }
    out.push_back('\n');
    return out;
}

// 一条待写日志:消息本体与时间/线程id在生产者线程准备好,
// 行格式交给后台线程渲染,格式字符串只在后台线程读取,无数据竞争
struct LogRecord {
    int level = LOG_LEVEL_INFO;
    std::string time;
    std::string tid;
    std::string msg;
};

// 控制台输出:写 stdout
class ConsoleSink : public Sink {
public:
    void write(const char* data, size_t len) override {
        std::fwrite(data, 1, len, stdout);
    }

    void flush() override {
        std::fflush(stdout);
    }
};

// 文件输出:追加写 + 大缓冲区;打不开文件时退回 stdout(保证日志不丢)
class FileSink : public Sink {
public:
    explicit FileSink(std::string path) : path_(std::move(path)) {
        file_ = std::fopen(path_.c_str(), "a");
        if (file_ == nullptr) {
            std::fprintf(stderr,
                         "[logger] cannot open log file '%s', fallback to stdout\n",
                         path_.c_str());
            file_ = stdout;
        } else {
            std::setvbuf(file_, writeBuf_, _IOFBF, kWriteBufferSize);
        }
    }

    ~FileSink() override {
        if (file_ != nullptr && file_ != stdout) {
            std::fclose(file_);
            file_ = nullptr;
        }
    }

    void write(const char* data, size_t len) override {
        if (file_ != nullptr) {
            std::fwrite(data, 1, len, file_);
        }
    }

    void flush() override {
        if (file_ != nullptr) {
            std::fflush(file_);
        }
    }

private:
    std::string path_;
    FILE* file_ = nullptr;
    char writeBuf_[kWriteBufferSize];
};

// 异步日志器:
//   LOG_* 只负责格式化并入队,立即返回(不碰磁盘);
//   真正的写盘由后台线程从 MessageQueue 批量取出后,分发给所有输出目标。
class AsyncLogger {
public:
    AsyncLogger() {
        {
            std::lock_guard<std::mutex> lock(g_configMutex);
            // 未显式 init / initFromFile 时,自动加载默认配置文件
            if (!g_configLoaded) {
                parseConfigFile(kDefaultConfigFile);
            }
            format_ = g_format;
            capacity_ = g_queueCapacity;

            // 内置输出目标按配置的"输出朝向"创建(console / file)
            if (g_consoleEnabled) {
                sinks_.push_back(std::make_unique<ConsoleSink>());
            }
            if (g_fileEnabled) {
                sinks_.push_back(std::make_unique<FileSink>(g_filePath));
            }
            // 用户通过 addSink 注册的自定义输出目标(可扩展)
            for (auto& sink : g_extraSinks) {
                sinks_.push_back(std::move(sink));
            }
            g_extraSinks.clear();
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

    // 生产端:把一条日志塞进队列,立即返回。
    // 队列满时采用"丢最旧保最新"策略(pushOverrunOldest),绝不阻塞生产者。
    void enqueue(LogRecord rec) {
        if (closed_.load(std::memory_order_relaxed)) {
            return; // 已关闭,后续日志直接丢弃
        }
        queue_->pushOverrunOldest([this, rec = std::move(rec)] {
            writeRecord(rec);
        });
    }

    // 关闭:先禁止新消息入队,再排空队列 + flush,最后 join 后台线程,
    // 并销毁输出目标(文件 flush + 关闭),保证 shutdown 后内容立即可见
    void shutdown() {
        bool expected = false;
        if (!closed_.compare_exchange_strong(expected, true)) {
            return; // 已经关过
        }
        queue_->close();
        if (writer_.joinable()) {
            writer_.join();
        }
        sinks_.clear();
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
                flushSinks();
                reportDropsIfChanged(lastReportedDrops);
            }
        }

        flushSinks();
        reportDropsIfChanged(lastReportedDrops);
    }

    void writeRecord(const LogRecord& rec) {
        const std::string line = renderLine(format_, rec.time,
                                            kLevelNames[clampLevel(rec.level)],
                                            rec.tid, rec.msg);
        for (const auto& sink : sinks_) {
            sink->write(line.data(), line.size());
        }
    }

    void flushSinks() {
        for (const auto& sink : sinks_) {
            sink->flush();
        }
    }

    // 队列发生过"丢最旧"时,把累计丢弃数写进日志,让丢弃可见可审计
    void reportDropsIfChanged(size_t& lastReported) {
        const size_t dropped = queue_->droppedCount();
        if (dropped == lastReported) {
            return;
        }
        lastReported = dropped;
        char line[192];
        std::snprintf(line, sizeof(line),
                      "[%s] [WARN] [logger] queue overflow: dropped %zu messages so far\n",
                      nowTimeString().c_str(), dropped);
        for (const auto& sink : sinks_) {
            sink->write(line, std::strlen(line));
        }
    }

    std::string format_ = kDefaultFormat;
    size_t capacity_ = kDefaultQueueCapacity;
    std::vector<std::unique_ptr<Sink>> sinks_;
    std::unique_ptr<MessageQueue> queue_;
    std::thread writer_;
    std::atomic<bool> closed_{false};
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

void addSink(std::unique_ptr<Sink> sink) {
    std::lock_guard<std::mutex> lock(g_configMutex);
    if (g_started || !sink) {
        return; // 后台线程已启动,或空指针,忽略
    }
    g_extraSinks.push_back(std::move(sink));
}

void init(const char* filePath, size_t queueCapacity) {
    std::lock_guard<std::mutex> lock(g_configMutex);
    if (g_started) {
        return; // 后台线程已启动,配置不再生效
    }
    g_configLoaded = true; // 显式配置,不再自动加载 logger.conf
    if (filePath != nullptr && filePath[0] != '\0') {
        g_filePath = filePath;
    }
    g_queueCapacity = (queueCapacity == 0) ? kDefaultQueueCapacity : queueCapacity;
}

void initFromFile(const char* path) {
    std::lock_guard<std::mutex> lock(g_configMutex);
    if (g_started) {
        return; // 后台线程已启动,配置不再生效
    }
    g_configLoaded = true;
    parseConfigFile(path != nullptr && path[0] != '\0' ? path : kDefaultConfigFile);
}

void shutdown() {
    instance().shutdown();
}

void flush() {
    instance().flush();
}

void logMessage(int level, const char* fmt, ...) {
    // 首次调用会构造后台日志器并(未显式配置时)加载默认配置文件,
    // 保证配置文件里的 level 在第一次日志判断之前生效
    AsyncLogger& logger = instance();
    if (!isEnabled(level)) {
        return;
    }

    // 1. 格式化消息本体(超过 1024 字节会被截断,与旧版一致)
    char msg[kMaxMessageLen];
    va_list args;
    va_start(args, fmt);
    vsnprintf(msg, sizeof(msg), fmt, args);
    va_end(args);

    // 2. 组装记录:时间在入队前打好,后台按入队顺序落盘,
    //    同一生产者线程的日志严格保持产生顺序
    LogRecord rec;
    rec.level = clampLevel(level);
    rec.time = nowTimeString();

    if (!g_tidReady) {
        std::ostringstream oss;
        oss << std::this_thread::get_id();
        const std::string tidStr = oss.str();
        std::snprintf(g_tidBuf, sizeof(g_tidBuf), "%s", tidStr.c_str());
        g_tidReady = true;
    }
    rec.tid = g_tidBuf;
    rec.msg = msg;

    // 3. 只入队,立即返回:磁盘 I/O 全部交给后台线程
    logger.enqueue(std::move(rec));
}

} // namespace logger
