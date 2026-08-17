// 日志配置文件测试:验证 logger.conf 加载、格式定制、输出朝向(console/file)、
// 自定义 Sink 扩展、级别过滤
//
// 编译(g++ / MinGW):
//   g++ -std=c++17 -Wall -Wextra -pedantic -I src/log -I src/thread
//       src/log/Logger.cpp src/thread/MessageQueue.cpp src/log/test_logger_config.cpp
//       -o test_logger_config
// 运行:
//   ./test_logger_config
//
// 验证点:
//   1. initFromFile 正确解析 level / format / target / file / queue_capacity
//   2. 自定义格式生效:按占位符渲染,无 [tid=] 标记
//   3. target = console,file 同时输出到控制台和文件
//   4. 自定义 Sink 注册后同样收到日志(可扩展性)
//   5. 配置文件中的级别过滤生效

#include "Logger.h"

#include <cstdio>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace {

int g_failures = 0;

void expect(bool ok, const char* what) {
    std::printf("%s: %s\n", ok ? "PASS" : "FAIL", what);
    if (!ok) {
        ++g_failures;
    }
}

// 自定义输出目标:把收到的行存进内存,验证可扩展性
// 用 shared_ptr 保存数据,避免 sink 被日志器销毁后测试仍访问它
class CaptureSink : public logger::Sink {
public:
    explicit CaptureSink(std::shared_ptr<std::vector<std::string>> lines)
        : lines_(std::move(lines)) {}

    void write(const char* data, size_t len) override {
        lines_->push_back(std::string(data, len));
    }

    void flush() override {}

private:
    std::shared_ptr<std::vector<std::string>> lines_;
};

std::string readFile(const char* path) {
    std::ifstream in(path);
    std::ostringstream oss;
    oss << in.rdbuf();
    return oss.str();
}

} // namespace

int main() {
    // 写一个测试用配置文件(不污染仓库,运行结束前删除)
    {
        std::ofstream out("test_logger.conf");
        out << "# 测试配置\n";
        out << "level = info\n";
        out << "format = [%l] {%t} %m\n";
        out << "target = console,file\n";
        out << "file = test_logger_config.log\n";
        out << "queue_capacity = 1024\n";
    }

    // 注册自定义输出目标(必须在第一次打日志之前)
    std::shared_ptr<std::vector<std::string>> lines =
        std::make_shared<std::vector<std::string>>();
    logger::addSink(std::make_unique<CaptureSink>(lines));

    logger::initFromFile("test_logger.conf");
    expect(logger::getLogLevel() == logger::LOG_LEVEL_INFO,
           "配置文件 level=info 生效");

    // 级别过滤:INFO 下 DEBUG 不输出
    LOG_DEBUG("this DEBUG line should be filtered");
    LOG_INFO("hello config logger");
    LOG_WARN("warning number %d", 3);
    LOG_ERROR("error text %s", "boom");

    logger::shutdown();

    // 1) 文件输出:target 含 file,且自定义格式生效
    const std::string fileContent = readFile("test_logger_config.log");
    expect(fileContent.find("hello config logger") != std::string::npos,
           "文件收到日志(输出朝向 file 生效)");
    expect(fileContent.find("[INFO]") != std::string::npos,
           "自定义格式 [%l] 生效");
    expect(fileContent.find("} hello config logger\n") != std::string::npos,
           "自定义格式 {%t} %m 生效");
    expect(fileContent.find("[tid=") == std::string::npos,
           "格式不含 %T 时没有 [tid=] 标记");
    expect(fileContent.find("this DEBUG line") == std::string::npos,
           "配置文件 level=info 过滤 DEBUG");

    // 2) 自定义 Sink(可扩展性)
    expect(lines->size() == 3, "自定义 Sink 收到 3 条日志(DEBUG 被过滤)");
    if (lines->size() == 3) {
        expect((*lines)[0].rfind("[INFO] {", 0) == 0,
               "自定义 Sink 行首为 [INFO] {");
        expect((*lines)[0].find("} hello config logger\n") != std::string::npos,
               "自定义 Sink 行尾为 } 消息");
        expect((*lines)[2].find("[ERROR]") != std::string::npos,
               "自定义 Sink 收到 ERROR 级别日志");
    }

    // 清理测试产物
    std::remove("test_logger.conf");
    std::remove("test_logger_config.log");

    if (g_failures == 0) {
        std::printf("\nALL TESTS PASSED\n");
        return 0;
    }
    std::printf("\n%d TEST(S) FAILED\n", g_failures);
    return 1;
}
