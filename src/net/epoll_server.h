#include <sys/epoll.h>
#include "ThreadPool.h"
class EpollServer {
public:
	EpollServer(int port, ThreadPool& pool);
	void start();
	~EpollServer();
private:
	void run();
	void acceptNewClient();
	void handleClient(int fd);
	int port_;
	int listen_fd_;
	int epfd_;
	ThreadPool& pool_;
	epoll_event events_[64];
};
