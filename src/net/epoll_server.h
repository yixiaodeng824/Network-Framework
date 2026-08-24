#pragma once
#include <sys/epoll.h>
#include "ThreadPool.h"
#include "connection.h"
#include <map>
#include <string>
#include <mutex>
#include <atomic>
#include <ctime>
#include <functional>
#include <cstdint>
#include <deque>
#include <memory>
#include <vector>
#include <thread>
// 连接令牌:fd + 代际号。
struct ConnectionId{
    int fd{-1};
    uint64_t generation{0};
};
// 由于不知道消息有多长，在每条消息前面加四个字节的长度表示消息体的长度
class EpollServer
{
public:
	EpollServer(int sub_index, ThreadPool& pool,int heartbeat_timeout);
	void start();
	static void handle_signal(int);
	void epollserver_exit();
	void setMessageHandler(std::function<void(EpollServer&,ConnectionId,const std::string&)> f);
	void sendTo(ConnectionId id, const std::string& msg);//信息发给特定fd
    void sendToRaw(ConnectionId id, const std::string &msg);//不加长度头
    void broadcast(int exptr_fd, const std::string& msg);//广播
    void acceptNewConnection(int fd);//从主epoll接fd进来加代际，代替acceptnewclient
    void stopSub() { stop_ = true; }
    void queueInLoop(std::function<void()> cb);//worker干完活投递给主线程，主线程再发
    static bool shouldStop() { return stop_; }
    ~EpollServer();

private:
	void run();
    void handleClient(ConnectionId fd);
    void handleWrite(ConnectionId fd);
    ConnectionId makeId(int fd); // 新增:从 fd_list 查当前代际,拼出令牌
    void closeConnection(int fd);
	void heartBeatCheck();

    void handleWakeup();                       // 主线程收到门铃:消铃 + 取待办箱执行
    
    bool isInLoopThread() const { return std::this_thread::get_id() == loop_thread_id; } // 是不是在主线程工作
    void sendInLoop(ConnectionId id, const std::string &msg);//网络io直接发
    void flushPendingWrites();   // 批量发送:本批次积攒的 fd 统一 flush
    void flushOne(int fd, const std::shared_ptr<Connection>& conn, uint64_t gen);// 持有指针直接 flush,免再查找
    void markPendingSend(int fd);// 标记 fd 待本批次末统一批量发送

	int epfd_;
	static std::atomic<bool> stop_;
    std::atomic<uint64_t> next_generation_{1}; // 每 accept 一个新连接,发一个唯一代际号
    ThreadPool& pool_;
	epoll_event events_[1024];
	std::map<int, std::shared_ptr<Connection>> fd_list; // shared_ptr 保活:回调期间连接不被回收,免重复查找
	int heartbeat_timeout_;
	int sub_index_;   // sub 编号(日志/定位用)
	std::function<void(EpollServer&, ConnectionId , const std::string&)>	handler_;

    std::deque<std::function<void()>> workers_results_;//重活传回调到这里面，主线程做
    std::mutex workers_results_mutex_;//保护上面那玩意的锁，多个worker同时塞进来会坏掉的
    std::thread::id loop_thread_id;//主线程id
    int wake_up_fd_{-1};
    std::vector<int> pending_send_fds_;//批量发送:本批次积攒待 flush 的 fd(仅 loop 线程访问)
    const size_t kSendBatchThreshold{4096}; //发送缓冲积攒阈值,达到立即 flush
    const size_t kRecvBatchLimit{64 * 1024};//单次读事件最多收多少字节,防饿死
};