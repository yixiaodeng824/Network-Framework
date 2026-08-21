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
	EpollServer(int port, ThreadPool& pool,int heartbeat_timeout);
	void start();
	static void handle_signal(int);
	void epollserver_exit();
	void setMessageHandler(std::function<void(EpollServer&,ConnectionId,const std::string&)> f);
	void sendTo(ConnectionId id, const std::string& msg);//信息发给特定fd
	void broadcast(int exptr_fd, const std::string& msg);//广播
    
    ~EpollServer();

private:
	void run();
	void acceptNewClient();
    void handleClient(ConnectionId fd);
    void handleWrite(ConnectionId fd);
    ConnectionId makeId(int fd); // 新增:从 fd_list 查当前代际,拼出令牌
    void closeConnection(int fd);
	void heartBeatCheck();

    void handleWakeup();                       // 主线程收到门铃:消铃 + 取待办箱执行
    void queueInLoop(std::function<void()> cb);//worker干完活投递给主线程，主线程再发
    bool isInLoopThread() const { return std::this_thread::get_id() == loop_thread_id; } // 是不是在主线程工作
    void sendInLoop(ConnectionId id, const std::string &msg);//网络io直接发
    int port_; // 端口号
    int listen_fd_;
	int epfd_;
	static std::atomic<bool> stop_;
    std::atomic<uint64_t> next_generation_{1}; // 每 accept 一个新连接,发一个唯一代际号
    ThreadPool& pool_;
	epoll_event events_[64];
	std::map<int, Connection> fd_list;
	std::mutex client_mutex;
	int heartbeat_timeout_;
	std::function<void(EpollServer&, ConnectionId , const std::string&)>	handler_;

    std::deque<std::function<void()>> workers_results_;//重活传回调到这里面，主线程做
    std::mutex workers_results_mutex_;//保护上面那玩意的锁，多个worker同时塞进来会坏掉的
    std::thread::id loop_thread_id;//主线程id
    int wake_up_fd_{-1};//叫醒主线程
};
