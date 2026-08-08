#include "epoll_server.h"
#include <sys/socket.h>
#include <iostream>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
using namespace std;
EpollServer::EpollServer(int port, ThreadPool& pool):pool_(pool) {
	listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd_ < 0) { cerr << "监听失败";exit(1); }
	sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_port = htons(port);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	int opt = 1;
	setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));//调整socket行为，让内核跟我我的请求操作
	if (bind(listen_fd_, (sockaddr*) &addr, sizeof(addr)) < 0) {
		cerr << "bind failed" << endl;
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
	cout << "start listening " << endl;
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
			cout << "client" << fd << "closed";
			epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
			close(fd);
		}
		else {
			send(fd, buf, len, 0);//这地方以后替换成业务函数
			//需要重新登记
			epoll_event nev;
			nev.data.fd = fd;
			nev.events = EPOLLIN | EPOLLONESHOT;
			epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
		}
	});
}

void EpollServer::acceptNewClient() {
	int client_fd = accept(listen_fd_, nullptr, nullptr);
	epoll_event nev;
	nev.data.fd = client_fd;
	nev.events = EPOLLIN | EPOLLONESHOT;
	epoll_ctl(epfd_, EPOLL_CTL_ADD, client_fd, &nev);//第三个参数对谁操作

	epoll_event nev1;
	nev1.data.fd = listen_fd_;
	nev1.events = EPOLLIN | EPOLLONESHOT;
	epoll_ctl(epfd_, EPOLL_CTL_MOD,listen_fd_ , &nev1);
}

EpollServer::~EpollServer(){
	close(epfd_);
	close(listen_fd_);
}