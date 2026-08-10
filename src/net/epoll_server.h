#include <sys/epoll.h>
#include "ThreadPool.h"
#include <map>
#include <string>
#include <mutex>
class RecvBox
{	
private:
	std::map<int, std::string> boxes;
	std::mutex mtx;
public:
	void push(int fd, const std::string& data) {
		std::lock_guard<std::mutex> lock(mtx);
		boxes[fd] += data;
	}

	std::string get(int fd) {
		std::lock_guard<std::mutex> lock(mtx);
		auto it = boxes.find(fd);
		if (it != boxes.end()) {
			return it->second;
		}
		return "";
	}
	//扔掉箱子前n字节的消息
	void erase(int fd, size_t n) {
		std::lock_guard<std::mutex> lock(mtx);
		std::string& box = boxes[fd];
		box.erase(0, n);
		if(box.empty()){
			boxes.erase(fd);
		}
	}
	bool isEmpty(int fd) {
		std::lock_guard<std::mutex> lock(mtx);
		auto it = boxes.find(fd);
		if (it != boxes.end()) {
			return it->second.empty();
		}
		return true;
	}

	bool clear(int fd) {
		std::lock_guard<std::mutex> lock(mtx);
		auto it = boxes.find(fd);
		if (it != boxes.end()) {
			boxes.erase(it);
			return true;
		}
		return false;
	}

	~RecvBox() {
		std::lock_guard<std::mutex> lock(mtx);
		boxes.clear();
	}
};
//由于不知道消息有多长，在每条消息前面加四个字节的长度表示消息体的长度
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
	void processBufferedData(int fd);
	int listen_fd_;
	int epfd_;
	ThreadPool& pool_;
	epoll_event events_[64];
	RecvBox recv_box_;
};
