#include "epoll_server.h"
#include "Logger.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <cstdint>
#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
using namespace std;
atomic<bool> EpollServer::stop_{ false };

EpollServer::EpollServer(int port, ThreadPool& pool):port_(port), pool_(pool) {
	//创建套接字
	listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd_ < 0) { LOG_ERROR("socket failed: %s", strerror(errno)); exit(1); }
	//确定要监听的端口
	sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_port = htons(port);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	int opt = 1;
	//设置套接字选项，允许地址重用
	setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	//绑定端口
	if (bind(listen_fd_, (sockaddr*) &addr, sizeof(addr)) < 0) {
		LOG_ERROR("bind failed: %s", strerror(errno));
		exit(1);
	}
	//创建轮询器
	epfd_ = epoll_create(1);
}

void EpollServer::epollserver_exit() {
	LOG_INFO("server stopping, closing connections...");
	//先关线程池
	pool_.close();
	//clients并非线程安全，对他操作要加锁
	{
		lock_guard<mutex>	cltmtx(client_mutex);
		for (auto& fd : clients_) {
			close(fd);
		}
		clients_.clear();
	}
	recv_box_.clearAll();
	LOG_INFO("server stopped.");

}

void EpollServer::run() {
	while (!stop_) {
		int n = epoll_wait(epfd_, events_, 64, 100);//阻塞等待事件发生
		for (int i = 0;i < n;i++) {
			int fd = events_[i].data.fd;
			if (fd == listen_fd_) {
				acceptNewClient();
			}
			else {
				handleClient(fd);
			}
		}
	}
	epollserver_exit();
}

void EpollServer::start() {
	//开始监听
	listen(listen_fd_, 5);
	LOG_INFO("start listening on port %d", port_);
	//先把监听套接字加入epoll事件列表
	epoll_event ev;
	ev.events = EPOLLIN | EPOLLONESHOT;
	ev.data.fd = listen_fd_;
	epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev);
	signal(SIGINT, handle_signal);
	signal(SIGTERM, handle_signal);
	run();
}
//设置退出标识
void EpollServer::handle_signal(int) {
	stop_ = true;
}

void  EpollServer::handleClient(int fd) {
	pool_.submit([this, fd] {
		char buf[1024];
		int len = recv(fd, buf, sizeof(buf), 0);
		if (len <= 0) {
			LOG_INFO ( "client disconnected, fd: %d" ,fd );
			epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
			{
				lock_guard<mutex> mtx(client_mutex);
				clients_.erase(fd);
			}
			close(fd);
		}
		else {
			recv_box_.push(fd, std::string(buf, len));
			processBufferedData(fd);

			epoll_event nev;
			nev.data.fd = fd;
			nev.events = EPOLLIN | EPOLLONESHOT;
			epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
		}
	});
}

void EpollServer::acceptNewClient() {
	int client_fd = accept(listen_fd_, nullptr, nullptr); 
	
	if (client_fd < 0) {
		LOG_ERROR("accept failed: %s", strerror(errno));
		return;
	}
	{
		//维护客户名单
		lock_guard<mutex> clmtx(client_mutex);
		clients_.insert(client_fd);
	}
	LOG_DEBUG("new client fd=%d", client_fd);
	epoll_event nev;
	nev.data.fd = client_fd;
	nev.events = EPOLLIN | EPOLLONESHOT;
	epoll_ctl(epfd_, EPOLL_CTL_ADD, client_fd, &nev);

	epoll_event nev1;
	nev1.data.fd = listen_fd_;
	nev1.events = EPOLLIN | EPOLLONESHOT;
	epoll_ctl(epfd_, EPOLL_CTL_MOD,listen_fd_ , &nev1);
}
void EpollServer::processBufferedData(int fd) {
	while (true) {
		auto this_box = recv_box_.get(fd);
		uint32_t len;
		memcpy(&len, this_box.data(), 4);
		len = ntohl(len);
		if (this_box.length() < 4 || this_box.length() < 4 + len)	break;

		string msg = this_box.substr(4, len);
		uint32_t resp_len = htonl(msg.size());
		send(fd, &resp_len, 4, 0);
		send(fd, msg.data(), msg.size(), 0);
		recv_box_.erase(fd, 4 + len);
	}
}
EpollServer::~EpollServer(){
	close(epfd_);
	close(listen_fd_);
}
