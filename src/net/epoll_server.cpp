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
#include <fcntl.h>
#include <vector>
using namespace std;
atomic<bool> EpollServer::stop_{ false };

EpollServer::EpollServer(int port, ThreadPool& pool,int heartbeat_timeout=60):port_(port), pool_(pool),heartbeat_timeout_(heartbeat_timeout) {
	//创建监听套接字
	listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd_ < 0) { LOG_ERROR("socket failed: %s", strerror(errno)); exit(1); }
	//确定要绑定的端口
	sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_port = htons(port);
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	int opt = 1;
	//设置套接字选项,允许地址复用
	setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	//绑定端口
	if (bind(listen_fd_, (sockaddr*) &addr, sizeof(addr)) < 0) {
		LOG_ERROR("bind failed: %s", strerror(errno));
		exit(1);
	}
	//创建 epoll 句柄
	epfd_ = epoll_create(1);
}

void EpollServer::epollserver_exit() {
	LOG_INFO("server stopping, closing connections...");
	//先关闭线程池
	pool_.close();
	//clients 需要线程安全,所以加锁
	{
		lock_guard<mutex>	cltmtx(client_mutex);
		for (auto& fd : fd_list) {
			close(fd.first);
		}
		fd_list.clear();
	}
	//清理connection

	LOG_INFO("server stopped.");

}

void EpollServer::heartBeatCheck() {
	time_t now = time(nullptr);
	vector<int> timeout_fds;
	{
		lock_guard<mutex> lck(client_mutex);
		for (auto& [fd, conn] : fd_list) {
			if (conn.isTimeout(now, heartbeat_timeout_)) {
				timeout_fds.push_back(fd);
			}
		}
	}
	for (auto fd : timeout_fds) {
		LOG_INFO("heartbeat timeout, close fd: %d", fd);
		closeConnection(fd);
	}
}

void EpollServer::run() {
	int tick = 0;
	while (!stop_) {
		int n = epoll_wait(epfd_, events_, 64, 100);//等待事件发生
		for (int i = 0;i < n;i++) {
			int fd = events_[i].data.fd;
			if (fd == listen_fd_) {
				acceptNewClient();
			}
			else {
				if (events_[i].events & EPOLLOUT) {
					handleWrite(fd);
				}
				else {
					handleClient(fd);
				}
			}
		}
		if (++tick >= 10) {
			//1s扫描一次
			heartBeatCheck();
			tick = 0;
		}
	}
	epollserver_exit();
}

void EpollServer::start() {
	//初始化
	listen(listen_fd_, 5);
	LOG_INFO("start listening on port %d", port_);
	//先把监听套接字加进epoll事件列表
	epoll_event ev;
	ev.events = EPOLLIN | EPOLLONESHOT;
	ev.data.fd = listen_fd_;
	epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev);
	signal(SIGINT, handle_signal);
	signal(SIGTERM, handle_signal);
	signal(SIGPIPE, SIG_IGN);
	run();
}
//优雅退出标志
void EpollServer::handle_signal(int) {
	stop_ = true;
}

void  EpollServer::handleClient(int fd) {
	pool_.submit([this, fd] {
		char buf[1024];
		int len = recv(fd, buf, sizeof(buf), 0);
		if (len == 0) {//关闭fd，可能fd对应的缓冲区仍然有东西，需要全发完再关
			SendResult result;
			{
				lock_guard<mutex> lck(client_mutex);
				auto it = fd_list.find(fd);
				if (it == fd_list.end())	return;
				it->second.markClosing();
				result = it->second.sendReadyMessage();
			}
			epoll_event nev;
			nev.data.fd = fd;
			//没东西了
			if (result == SendResult::SentAll) {
				closeConnection(fd);
			}
			else if (result == SendResult::Pending) {
				nev.events = EPOLLOUT | EPOLLONESHOT;
				epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
			}
			else {//出错
				closeConnection(fd);
			}
		}
		else if(len > 0){
			vector<string> msgs;
			{
				lock_guard<mutex> lck(client_mutex);
				auto it = fd_list.find(fd);
				if (it == fd_list.end())	return;
				it->second.appendRecv(buf, len);
				it->second.touchActive();//刷新一下活跃时间
				msgs = move(it->second.processBufferedData());
			}
			for (auto& msg : msgs) {
				if (handler_) handler_(*this, fd, msg);    //锁外触发回调
			}
			SendResult result{ SendResult::Pending };//消息一次是不是全发完了，全发完就直接无视了，
			//没全发完登记成epollout等handlewrite继续发
			{
				lock_guard<mutex> lck(client_mutex);
				auto it = fd_list.find(fd);
				if (it == fd_list.end()) return;
				result = it->second.sendReadyMessage();    // 收尾发送
			}

			epoll_event nev;
			nev.data.fd = fd;
			if (result == SendResult::SentAll) {
				nev.events = EPOLLIN | EPOLLONESHOT;
			}
			else if (result == SendResult::Pending) {
				nev.events = EPOLLOUT | EPOLLIN | EPOLLONESHOT;
			}
			else {
				closeConnection(fd);
			}
			epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
		}
		else {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				// 没数据,忽略;但 EPOLLONESHOT 触发过一次,要重新 MOD 回 EPOLLIN
				epoll_event nev;
				nev.data.fd = fd;
				nev.events = EPOLLIN | EPOLLONESHOT;
				epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
			}
			else {//连接真出错了
				closeConnection(fd);
			}
		}

	});
}

void EpollServer::handleWrite(int fd) {
	pool_.submit([this, fd] {
		SendResult result;
		bool closing = false;
		{
			lock_guard<mutex> lck(client_mutex);
			auto it = fd_list.find(fd);
			if (it == fd_list.end()) return;
			result = it->second.sendReadyMessage();
			closing = it->second.isClosing();
		}
		epoll_event nev;
		nev.data.fd = fd;
		if (result == SendResult::SentAll) {
			if (closing) {
				shutdown(fd, SHUT_WR);
				closeConnection(fd);
				return;
			}
			nev.events = EPOLLIN | EPOLLONESHOT;
		}//对方程序可能会阻塞不读,这个时候send就会卡住,我们需要解决这个问题
		else if (result == SendResult::Pending) {//可能还会有新数据同时进来，要加epollin
			if (closing) {
				nev.events = EPOLLOUT | EPOLLONESHOT;//关闭中，不挂读
			}
			else {
				nev.events = EPOLLIN | EPOLLOUT | EPOLLONESHOT;
			}
		}
		else {
			closeConnection(fd);
			return;
		}
		epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
		});
}

void EpollServer::closeConnection(int fd) {
	LOG_INFO("close connection, fd: %d", fd);
	epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
	{
		lock_guard<mutex> mtx(client_mutex);
		fd_list.erase(fd);
	}
	close(fd);
}

void EpollServer::acceptNewClient() {//调回调函数，确定fd对应的业务逻辑
	int client_fd = accept(listen_fd_, nullptr, nullptr);

	if (client_fd < 0) {
		LOG_ERROR("accept failed: %s", strerror(errno));
		return;
	}
	{
		//维护客户连接
		lock_guard<mutex> clmtx(client_mutex);
		int flag = fcntl(client_fd, F_GETFL, 0);
		fcntl(client_fd, F_SETFL, flag | O_NONBLOCK);//设置客户非阻塞，为了处理在send时候的阻塞问题
		fd_list.insert({ client_fd,Connection(client_fd) });
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

void EpollServer::setMessageHandler(std::function<void(EpollServer&, int, const std::string&)> f) {
	handler_ = move(f);
}

void EpollServer::sendTo(int fd, const string& msg) {
	lock_guard<mutex> lck(client_mutex);       
	auto it = fd_list.find(fd);                 
	if (it != fd_list.end())                    
		it->second.sendMsgToSendBuf(msg);                //塞进那个连接的缓冲
}

void EpollServer::broadcast(int exptr_fd, const std::string& msg) {
	std::vector<int> toClose;
	{
		lock_guard<mutex> lck(client_mutex);
		for (auto& it : fd_list) {
			if (it.first == exptr_fd)	continue;
			it.second.sendMsgToSendBuf(msg);
		}
		for (auto& it : fd_list) {
			auto result = it.second.sendReadyMessage(); 
			epoll_event nev;
			nev.data.fd = it.first;
			if (result == SendResult::SentAll) {
				nev.events = EPOLLIN | EPOLLONESHOT;
			}
			else if (result == SendResult::Pending) {
				nev.events = EPOLLOUT | EPOLLIN | EPOLLONESHOT;
			}
			else {
				toClose.push_back(it.first);
			}
			epoll_ctl(epfd_, EPOLL_CTL_MOD, it.first, &nev);
		}
	}
	for (auto fd : toClose) {              //  锁外,遍历结束才关
		closeConnection(fd);
	}
}
EpollServer::~EpollServer(){
	close(epfd_);
	close(listen_fd_);
}
