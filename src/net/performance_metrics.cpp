#include "performance_metrics.h"

#include <algorithm>
#include <cmath>

PerformanceMetrics::PerformanceMetrics() : start_time_(Clock::now()) {
    for (auto& bucket : latency_buckets_) {
        bucket.store(0, std::memory_order_relaxed);
    }
    for (auto& count : close_reasons_) {
        count.store(0, std::memory_order_relaxed);
    }
}

void PerformanceMetrics::recordRequest(std::chrono::nanoseconds latency) {
    const auto raw_ns = latency.count();
    const uint64_t nanoseconds = raw_ns > 0 ? static_cast<uint64_t>(raw_ns) : 0;
    uint64_t microseconds = nanoseconds / 1'000;
    if (microseconds == 0) {
        microseconds = 1;
    }

    requests_.fetch_add(1, std::memory_order_relaxed);
    total_latency_ns_.fetch_add(nanoseconds, std::memory_order_relaxed);
    updateMax(max_latency_ns_, nanoseconds);
    latency_buckets_[latencyBucket(microseconds)].fetch_add(1, std::memory_order_relaxed);
}

void PerformanceMetrics::recordRecvEagain() {
    recv_eagain_.fetch_add(1, std::memory_order_relaxed);
}

void PerformanceMetrics::recordSendEagain() {
    send_eagain_.fetch_add(1, std::memory_order_relaxed);
}

void PerformanceMetrics::recordAcceptEagain() {
    accept_eagain_.fetch_add(1, std::memory_order_relaxed);
}

void PerformanceMetrics::recordAcceptFailure() {
    accept_failures_.fetch_add(1, std::memory_order_relaxed);
}

void PerformanceMetrics::connectionAccepted() {
    active_connections_.fetch_add(1, std::memory_order_relaxed);
}

void PerformanceMetrics::connectionClosed(CloseReason reason) {
    active_connections_.fetch_sub(1, std::memory_order_relaxed);
    const auto index = static_cast<size_t>(reason);
    if (index < close_reasons_.size()) {
        close_reasons_[index].fetch_add(1, std::memory_order_relaxed);
    }
}

void PerformanceMetrics::addOutputBufferBytes(int64_t delta) {
    output_buffer_bytes_.fetch_add(delta, std::memory_order_relaxed);
}

PerformanceSnapshot PerformanceMetrics::snapshot() const {
    PerformanceSnapshot result;
    result.requests = requests_.load(std::memory_order_relaxed);
    result.total_latency_ns = total_latency_ns_.load(std::memory_order_relaxed);
    result.max_latency_ns = max_latency_ns_.load(std::memory_order_relaxed);
    result.active_connections = active_connections_.load(std::memory_order_relaxed);
    result.output_buffer_bytes = output_buffer_bytes_.load(std::memory_order_relaxed);
    result.recv_eagain = recv_eagain_.load(std::memory_order_relaxed);
    result.send_eagain = send_eagain_.load(std::memory_order_relaxed);
    result.accept_eagain = accept_eagain_.load(std::memory_order_relaxed);
    result.accept_failures = accept_failures_.load(std::memory_order_relaxed);
    for (size_t i = 0; i < latency_buckets_.size(); ++i) {
        result.latency_buckets[i] = latency_buckets_[i].load(std::memory_order_relaxed);
    }
    for (size_t i = 0; i < close_reasons_.size(); ++i) {
        result.close_reasons[i] = close_reasons_[i].load(std::memory_order_relaxed);
    }
    return result;
}

double PerformanceMetrics::elapsedSeconds() const {
    const auto elapsed = std::chrono::duration<double>(Clock::now() - start_time_);
    return std::max(0.000001, elapsed.count());
}

double PerformanceMetrics::averageLatencyMs(const PerformanceSnapshot& snapshot) {
    if (snapshot.requests == 0) {
        return 0.0;
    }
    return static_cast<double>(snapshot.total_latency_ns) /
           static_cast<double>(snapshot.requests) / 1'000'000.0;
}

double PerformanceMetrics::percentileMs(
    const PerformanceSnapshot& snapshot, double percentile) {
    if (snapshot.requests == 0) {
        return 0.0;
    }
    percentile = std::clamp(percentile, 0.0, 1.0);
    const auto rank = static_cast<uint64_t>(
        std::ceil(static_cast<double>(snapshot.requests) * percentile));
    const uint64_t target = std::max<uint64_t>(1, rank);
    uint64_t cumulative = 0;
    for (size_t i = 0; i < snapshot.latency_buckets.size(); ++i) {
        cumulative += snapshot.latency_buckets[i];
        if (cumulative >= target) {
            return static_cast<double>(kLatencyUpperBoundsUs[i]) / 1'000.0;
        }
    }
    return static_cast<double>(kLatencyUpperBoundsUs.back()) / 1'000.0;
}

const char* PerformanceMetrics::closeReasonName(CloseReason reason) {
    switch (reason) {
    case CloseReason::PeerClosed:
        return "peer_closed";
    case CloseReason::HeartbeatTimeout:
        return "heartbeat_timeout";
    case CloseReason::IoError:
        return "io_error";
    case CloseReason::FrameError:
        return "frame_error";
    case CloseReason::SendError:
        return "send_error";
    case CloseReason::SendOverflow:
        return "send_overflow";
    case CloseReason::ServerShutdown:
        return "server_shutdown";
    case CloseReason::Other:
        return "other";
    case CloseReason::Count:
        break;
    }
    return "unknown";
}

size_t PerformanceMetrics::latencyBucket(uint64_t microseconds) {
    const auto it = std::lower_bound(
        kLatencyUpperBoundsUs.begin(), kLatencyUpperBoundsUs.end(), microseconds);
    if (it == kLatencyUpperBoundsUs.end()) {
        return kLatencyUpperBoundsUs.size() - 1;
    }
    return static_cast<size_t>(it - kLatencyUpperBoundsUs.begin());
}

void PerformanceMetrics::updateMax(std::atomic<uint64_t>& target, uint64_t value) {
    auto current = target.load(std::memory_order_relaxed);
    while (current < value &&
           !target.compare_exchange_weak(
               current, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}
