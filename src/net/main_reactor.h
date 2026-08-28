#pragma once
#include "ThreadPool.h"
#include "epoll_server.h"
#include <functional>
#include <vector>
#include <thread>
#include <memory>
#include <string_view>
#include <atomic>
class MainReactor{
public:
    MainReactor(int port, ThreadPool &thread_pool, int heartbeat_timeout, int sub_count, bool affinity,size_t max_conn);
    void start();
    void setMessageHandler(std::function<void(EpollServer &, ConnectionId, std::string_view)> f);
    
    ~MainReactor();

private:
    void acceptLoop(); // 主循环，分发fd到subfdlist
    int listen_fd;
    int epfd_;
    int port_;
    int heartbeat_timeout_;
    int sub_count_;
    bool affinity_{false};//绑核:accept 线程绑核 0,sub 线程依次绑核 1..N
	epoll_event events_[1024];
    ThreadPool &pool_;
    std::vector<std::thread> sub_epoll_threads_;//给sub分线程，按照下标对齐
    std::vector<std::unique_ptr<EpollServer>> subs_;//包含的sub
    int run_robin_{0};//轮询计数器
    std::function<void(EpollServer &, ConnectionId, std::string_view)> handler_;
    std::shared_ptr<PerformanceMetrics> metrics_;
    //最大连接数
    size_t max_conn_{0};
    std::shared_ptr<std::atomic<size_t>> conn_count_;
};

