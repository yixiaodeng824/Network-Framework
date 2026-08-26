#include "ThreadPool.h"
#include <algorithm>
#include <thread>
using namespace std;

ThreadPool::ThreadPool(size_t thread_count) {
	if (thread_count == 0) {
		thread_count = std::thread::hardware_concurrency();//自动选取cpu核数开线程
	}
	workers_.reserve(thread_count);
	for (size_t i = 0;i < thread_count;i++) {
		workers_.push_back(make_unique<WorkThread>(&msg_que_));
	}
}

void ThreadPool::close() {
	if (exit_.exchange(true))	return;
	msg_que_.close();
	for (auto& w : workers_) {
		w->join();
	}
}

void ThreadPool::recordQueueWait(chrono::steady_clock::time_point enqueued_at) {
    const auto elapsed = chrono::steady_clock::now() - enqueued_at;
    const auto wait_ns = static_cast<uint64_t>(
        max<chrono::steady_clock::duration>(elapsed, chrono::steady_clock::duration::zero()).count());
    queue_wait_samples_.fetch_add(1, memory_order_relaxed);
    queue_wait_ns_.fetch_add(wait_ns, memory_order_relaxed);
    auto current = queue_wait_max_ns_.load(memory_order_relaxed);
    while (current < wait_ns &&
           !queue_wait_max_ns_.compare_exchange_weak(
               current, wait_ns, memory_order_relaxed, memory_order_relaxed)) {
    }
}

double ThreadPool::queueWaitAverageMs() const {
    const auto samples = queue_wait_samples_.load(memory_order_relaxed);
    if (samples == 0) {
        return 0.0;
    }
    return static_cast<double>(queue_wait_ns_.load(memory_order_relaxed)) /
           static_cast<double>(samples) / 1'000'000.0;
}

double ThreadPool::queueWaitMaxMs() const {
    return static_cast<double>(queue_wait_max_ns_.load(memory_order_relaxed)) / 1'000'000.0;
}

ThreadPool::~ThreadPool() {
	close();
}
