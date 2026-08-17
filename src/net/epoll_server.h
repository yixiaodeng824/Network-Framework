#include <sys/epoll.h>
#include "ThreadPool.h"
#include "connection.h"
#include <map>
#include <string>
#include <mutex>
#include <atomic>
#include <ctime>

//由于不知道消息有多长，在每条消息前面加四个字节的长度表示消息体的长度
class EpollServer {
public:
	EpollServer(int port, ThreadPool& pool,int heartbeat_timeout);
	void start();
	static void handle_signal(int);
	void epollserver_exit();
	~EpollServer();
private:
	void run();
	void acceptNewClient();
	void handleClient(int fd);
	void handleWrite(int fd);
	void closeConnection(int fd);
	void heartBeatCheck();
	int port_;//端口号
	int listen_fd_;
	int epfd_;
	static std::atomic<bool> stop_;
	ThreadPool& pool_;
	epoll_event events_[64];
	std::map<int, Connection> fd_list;
	std::mutex client_mutex;
	int heartbeat_timeout_;
};
