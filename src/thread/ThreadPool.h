#pragma once
#include "MessageQueue.h"
#include "work_thread.h"
#include <vector>
#include <memory>
#include <future>
#include <atomic>
#include <utility>
class ThreadPool
{
public:
	explicit ThreadPool(size_t threadCount = 0);
	~ThreadPool();

    template<typename Func>
    auto submit(Func&& f) -> std::future<decltype(f())> {//加底下一坨主要是考虑之后函数会有返回值的情况
        using Ret = decltype(f());
        auto prom = std::make_shared<std::promise<Ret>>();//prom本质是不可复制的，但是如果用function，他会强制要求内部东西可以复制
        std::future<Ret> fut = prom->get_future();

        msg_que_.push(
            [prom, f = std::forward<Func>(f)]() mutable {
                    prom->set_value(f());        // 任务有结果:顺手把结果带上
            }
        );
        return fut;
    }
    template<typename Func>
    void post(Func&& f) {
        msg_que_.push([f = std::forward<Func>(f)]()mutable {f(); });//允许捕获之后修改自己的变量
    }
    void close();
private:
    MessageQueue msg_que_{65536};
	std::vector<std::unique_ptr<WorkThread>> workers_;
    std::atomic<bool> exit_{false};
};

