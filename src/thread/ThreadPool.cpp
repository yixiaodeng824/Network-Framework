#include "ThreadPool.h"
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
ThreadPool::~ThreadPool() {
	close();
}

