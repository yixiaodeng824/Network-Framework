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
using namespace std;
MainReactor::MainReactor(int port, ThreadPool &thread_pool, int heartbeat_timeout, int sub_count):
port_(port), pool_(thread_pool), heartbeat_timeout_(heartbeat_timeout), sub_count_(sub_count)
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
    listen(listen_fd, 128);
    LOG_INFO("start listening on port %d, sub_count=%d", port_, sub_count_);
    epoll_event nev;
    nev.data.fd = listen_fd;
    nev.events = EPOLLIN | EPOLLONESHOT;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd, &nev);

    for (int i = 0; i < sub_count_;i++){
        subs_.push_back(make_unique<EpollServer>(i, pool_, heartbeat_timeout_));
        //设置回调函数
        subs_[i]->setMessageHandler(handler_);
        //分配线程，启动sub
        sub_epoll_threads_.emplace_back([this, i]
                                     { subs_[i]->start(); });
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
        int n = epoll_wait(epfd_, events_, 64, 100);
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
                        if(errno==EAGAIN||errno==EWOULDBLOCK)
                            break;
                        LOG_ERROR("accept failed: %s", strerror(errno));
                        break;
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
                epoll_event nev; // 重新武装门
                nev.data.fd = listen_fd;
                nev.events = EPOLLIN | EPOLLONESHOT;
                epoll_ctl(epfd_, EPOLL_CTL_MOD, listen_fd, &nev);
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
    close(epfd_);
    close(listen_fd);
}