#include "performance_metrics.h"

#include <cassert>
#include <chrono>
#include <iostream>

int main() {
    PerformanceMetrics metrics;
    metrics.recordRequest(std::chrono::microseconds(1));
    metrics.recordRequest(std::chrono::microseconds(2));
    metrics.recordRequest(std::chrono::microseconds(10));
    metrics.recordRequest(std::chrono::milliseconds(1));

    metrics.connectionAccepted();
    metrics.connectionAccepted();
    metrics.addOutputBufferBytes(512);
    metrics.connectionClosed(CloseReason::PeerClosed);

    metrics.recordRecvEagain();
    metrics.recordSendEagain();
    metrics.recordAcceptEagain();
    metrics.recordAcceptFailure();

    const auto snapshot = metrics.snapshot();
    assert(snapshot.requests == 4);
    assert(snapshot.active_connections == 1);
    assert(snapshot.output_buffer_bytes == 512);
    assert(snapshot.recv_eagain == 1);
    assert(snapshot.send_eagain == 1);
    assert(snapshot.accept_eagain == 1);
    assert(snapshot.accept_failures == 1);
    assert(snapshot.close_reasons[static_cast<size_t>(CloseReason::PeerClosed)] == 1);
    assert(PerformanceMetrics::percentileMs(snapshot, 0.50) == 0.002);
    assert(PerformanceMetrics::percentileMs(snapshot, 0.95) == 1.0);
    assert(PerformanceMetrics::percentileMs(snapshot, 0.999) == 1.0);
    assert(PerformanceMetrics::averageLatencyMs(snapshot) > 0.25);

    std::cout << "performance metrics checks passed\n";
    return 0;
}
