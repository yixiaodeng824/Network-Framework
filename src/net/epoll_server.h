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
	void setMessageHandler(std::function<void(EpollServer&,int,const std::string&)> f);
	void sendTo(int fd, const std::string& msg);
	void broadcast(int exptr_fd, const std::string& msg);
	~EpollServer();
private:
	void run();
	void acceptNewClient();
    void handleClient(ConnectionId fd);
    void handleWrite(ConnectionId fd);
    ConnectionId makeId(int fd); // 新增:从 fd_list 查当前代际,拼出令牌
    void closeConnection(int fd);
	void heartBeatCheck();
	int port_;//端口号
	int listen_fd_;
	int epfd_;
	static std::atomic<bool> stop_;
    std::atomic<uint64_t> next_generation_{1}; // 每 accept 一个新连接,发一个唯一代际号
    ThreadPool& pool_;
	epoll_event events_[64];
	std::map<int, Connection> fd_list;
	std::mutex client_mutex;
	int heartbeat_timeout_;
	std::function<void(EpollServer&, int, const std::string&)>	handler_;
};
