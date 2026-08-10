#include "epoll_server.h"
#include "Logger.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <cstdint>
#include <arpa/inet.h>
#include <cerrno>
using namespace std;
EpollServer::EpollServer(int port, ThreadPool& pool):port_(port), pool_(pool) {
	listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd_ < 0) { LOG_ERROR("socket failed: %s", strerror(errno)); exit(1); }
	sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_port = htons(port);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	int opt = 1;
	setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	if (bind(listen_fd_, (sockaddr*) &addr, sizeof(addr)) < 0) {
		LOG_ERROR("bind failed: %s", strerror(errno));
		exit(1);
	}
	epfd_ = epoll_create(1);
}

void EpollServer::run() {
	while (true) {
		int n = epoll_wait(epfd_, events_, 64, -1);
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
}

void EpollServer::start() {
	listen(listen_fd_, 5);
	LOG_INFO("start listening on port %d", port_);
	epoll_event ev;
	ev.events = EPOLLIN | EPOLLONESHOT;
	ev.data.fd = listen_fd_;
	epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev);
	run();
}

void  EpollServer::handleClient(int fd) {
	pool_.submit([this, fd] {
		char buf[1024];
		int len = recv(fd, buf, sizeof(buf), 0);
		if (len <= 0) {
			LOG_INFO ( "client disconnected, fd: %d" ,fd );
			epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
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
		if (this_box.length() < 4|| this_box.length() < 4+len)	break;

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
