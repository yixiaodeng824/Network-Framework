#include "main_reactor.h"
#include "Logger.h"
#include <sys/socket.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <cstdlib>
#include <unistd.h>
#include <cstring>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
using namespace std;

static void pin_to_cpu(int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}
MainReactor::MainReactor(int port, ThreadPool &thread_pool, int heartbeat_timeout, int handshake_timeout, int sub_count, bool affinity,size_t max_conn,size_t max_buffer_size):
port_(port), pool_(thread_pool), heartbeat_timeout_(heartbeat_timeout), handshake_timeout_(handshake_timeout), sub_count_(sub_count), affinity_(affinity),
metrics_(make_shared<PerformanceMetrics>()),max_conn_(max_conn), conn_count_(make_shared<atomic<size_t>>(0)),max_buffer_size_(max_buffer_size)
{
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int fl = fcntl(listen_fd, F_GETFL, 0);
    fcntl(listen_fd, F_SETFL, fl | O_NONBLOCK);

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_port = htons(port);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (bind(listen_fd, (sockaddr *)&addr, sizeof(addr)) < 0)
    { // ② 贴门牌
        LOG_ERROR("bind failed: %s", strerror(errno));
        exit(1);
    }
    epfd_ = epoll_create(1); // ③ 雇前台(自己也要 epoll)
}

void MainReactor::start(){
    if (affinity_) {
        int ncpu = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
        pin_to_cpu(0);//accept 主线程绑核 0
    }
    listen(listen_fd, 128);
    LOG_INFO("start listening on port %d, sub_count=%d", port_, sub_count_);
    epoll_event nev;
    nev.data.fd = listen_fd;
    nev.events = EPOLLIN | EPOLLET;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd, &nev);

    for (int i = 0; i < sub_count_;i++){
        subs_.push_back(make_unique<EpollServer>(i, pool_, heartbeat_timeout_, metrics_, conn_count_, handshake_timeout_,max_buffer_size_));
        //设置回调函数
        subs_[i]->setMessageHandler(handler_);
        //分配线程，启动sub
        sub_epoll_threads_.emplace_back([this, i]
                                     {
                                         if (affinity_) {
                                             int ncpu = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
                                             pin_to_cpu(1 + (i % (ncpu > 1 ? ncpu - 1 : 1)));//每个 sub 线程绑独立核
                                         }
                                         subs_[i]->start();
                                     });
    }
    //就是run
    acceptLoop();
}

void MainReactor::setMessageHandler(std::function<void(EpollServer &, ConnectionId, std::string_view)> f)
{
    handler_ = move(f);
}

void MainReactor::acceptLoop(){
    while (!EpollServer::shouldStop())
    {
        int n = epoll_wait(epfd_, events_, 1024, 100);
        if(n<0){
            if(errno==EINTR)
                continue;
            LOG_ERROR("epoll_wait error: %s", strerror(errno));
            continue;
        }
        for (int i = 0; i < n;i++){
            if(events_[i].data.fd==listen_fd){
                while(true){
                    
                    int client_fd = accept(listen_fd, nullptr, nullptr);
                    if(client_fd<0){
                        if(errno==EINTR)
                            continue;
                        if(errno==EAGAIN||errno==EWOULDBLOCK) {
                            metrics_->recordAcceptEagain();
                            break;
                        }
                        metrics_->recordAcceptFailure();
                        LOG_ERROR("accept failed: %s", strerror(errno));
                        break;
                    }
                    if (max_conn_ > 0 && conn_count_->fetch_add(1) >= max_conn_)
                    {
                        conn_count_->fetch_sub(1); // 回退计数
                        close(client_fd);          // 拒绝新连接
                        LOG_WARN("max_conn reached, reject fd=%d", client_fd);
                        continue;
                    }
                    //分发逻辑可以优化，做负载均衡
                    int idx = run_robin_++ % sub_count_; // 轮流分窗口
                    //不可以这样做，因为acceptnewconnection需要碰fdlist，subs_[idx]->acceptNewConnection(client_fd);
                    subs_[idx]->queueInLoop([this, idx, client_fd]
                                            {
                                                subs_[idx]->acceptNewConnection(client_fd); // ? 在 sub 线程执行,无竞争
                                            });
                    LOG_DEBUG("sub %d new client fd=%d", idx, client_fd);
                }
            }
        }
    }
}

MainReactor::~MainReactor(){
    for(auto& it:subs_)
        it->stopSub();
    for(auto& t:sub_epoll_threads_){
        if(t.joinable())
            t.join();
    }
    if (!subs_.empty()) {
        subs_.front()->logMetrics("final");
    }
    close(epfd_);
    close(listen_fd);
}
