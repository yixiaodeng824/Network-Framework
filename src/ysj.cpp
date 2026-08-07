// ysj.cpp - ThreadPool 测试
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include "ThreadPool.h"

using namespace std;

int main() {
    atomic<int> done{ 0 };
    mutex io_mutex;   // 保护 cout,避免多线程输出交错

    {
        ThreadPool pool(4);   // 开 4 条 worker 线程

        for (int i = 0; i < 20; ++i) {
            pool.submit([i, &done, &io_mutex] {
                {
                    lock_guard<mutex> lock(io_mutex);
                    cout << "Task " << i
                         << "  on thread " << this_thread::get_id() << endl;
                }
                this_thread::sleep_for(chrono::milliseconds(30));   // 模拟耗时
                ++done;
            });
        }
        // pool 离开花括号时析构:close + join,等所有任务干完
        std::future<int> ans = pool.submit([] { return 1 + 1; });
        std::cout << "1+1 = " << ans.get() << std::endl;
    }

    cout << "All " << done.load() << " tasks finished." << endl;
    
    return 0;
}
