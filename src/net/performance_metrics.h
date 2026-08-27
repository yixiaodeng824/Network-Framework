#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

// 连接关闭原因。保留显式原因，便于压测后判断失败来自哪里。
enum class CloseReason {
    PeerClosed = 0,
    HeartbeatTimeout,
    IoError,
    FrameError,
    SendError,
    SendOverflow,
    ServerShutdown,
    Other,
    Count,
};

struct PerformanceSnapshot {
    static constexpr size_t kLatencyBucketCount = 32;

    uint64_t requests{0};
    uint64_t total_latency_ns{0};
    uint64_t max_latency_ns{0};
    int64_t active_connections{0};
    int64_t output_buffer_bytes{0};
    uint64_t recv_eagain{0};
    uint64_t send_eagain{0};
    uint64_t accept_eagain{0};
    uint64_t accept_failures{0};
    std::array<uint64_t, kLatencyBucketCount> latency_buckets{};
    std::array<uint64_t, static_cast<size_t>(CloseReason::Count)> close_reasons{};
};

class PerformanceMetrics {
public:
    using Clock = std::chrono::steady_clock;
    static constexpr size_t kLatencyBucketCount = PerformanceSnapshot::kLatencyBucketCount;

    PerformanceMetrics();

    void recordRequest(std::chrono::nanoseconds latency);
    void recordRecvEagain();
    void recordSendEagain();
    void recordAcceptEagain();
    void recordAcceptFailure();

    void connectionAccepted();
    void connectionClosed(CloseReason reason);
    void addOutputBufferBytes(int64_t delta);

    PerformanceSnapshot snapshot() const;
    double elapsedSeconds() const;

    static double averageLatencyMs(const PerformanceSnapshot& snapshot);
    static double percentileMs(const PerformanceSnapshot& snapshot, double percentile);
    static const char* closeReasonName(CloseReason reason);

private:
    static size_t latencyBucket(uint64_t microseconds);
    static void updateMax(std::atomic<uint64_t>& target, uint64_t value);

    Clock::time_point start_time_;
    std::atomic<uint64_t> requests_{0};
    std::atomic<uint64_t> total_latency_ns_{0};
    std::atomic<uint64_t> max_latency_ns_{0};
    std::atomic<int64_t> active_connections_{0};
    std::atomic<int64_t> output_buffer_bytes_{0};
    std::atomic<uint64_t> recv_eagain_{0};
    std::atomic<uint64_t> send_eagain_{0};
    std::atomic<uint64_t> accept_eagain_{0};
    std::atomic<uint64_t> accept_failures_{0};
    std::array<std::atomic<uint64_t>, kLatencyBucketCount> latency_buckets_{};
    std::array<std::atomic<uint64_t>, static_cast<size_t>(CloseReason::Count)> close_reasons_{};

    // 延迟桶上界，单位为微秒。最后一个桶覆盖超出 60 秒的异常值。
    inline static constexpr std::array<uint64_t, kLatencyBucketCount> kLatencyUpperBoundsUs{
        1, 2, 5, 10, 20, 50, 100, 200, 500,
        1'000, 2'000, 5'000, 10'000, 20'000, 50'000, 100'000,
        200'000, 500'000, 1'000'000, 2'000'000, 5'000'000, 10'000'000,
        20'000'000, 50'000'000, 100'000'000, 200'000'000, 500'000'000,
        1'000'000'000, 2'000'000'000, 5'000'000'000, 10'000'000'000,
        60'000'000'000,
    };
};
