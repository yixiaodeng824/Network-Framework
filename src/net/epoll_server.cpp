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
#include <sys/eventfd.h>
using namespace std;
atomic<bool> EpollServer::stop_{ false };

EpollServer::EpollServer(int port, ThreadPool& pool,int heartbeat_timeout=60):port_(port), pool_(pool),heartbeat_timeout_(heartbeat_timeout) {
	//创建监听套接字
	listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
	//把listen_fd_也设置为非阻塞
	int fl = fcntl(listen_fd_, F_GETFL,0);
	fcntl(listen_fd_, F_SETFL, fl | O_NONBLOCK);

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
    wake_up_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    //创建 epoll 句柄
	epfd_ = epoll_create(1);
}

void EpollServer::epollserver_exit() {
	LOG_INFO("server stopping, closing connections...");
	//先关闭线程池
	pool_.close();
	//clients 需要线程安全,所以加锁
	{
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
		if (n < 0) {
			if (errno == EINTR) continue;   // 被信号打断,重新等
			LOG_ERROR("epoll_wait error: %s", strerror(errno));
			continue;
		}
		//原本结构会让epollout事件优先级大于epollin，这个时候如果事件又读又有东西没写完，写的事件会优先被认领，然后直接等到下一轮
		for (int i = 0;i < n;i++) {
			int fd = events_[i].data.fd;
			uint32_t ev = events_[i].events;
			if (fd == listen_fd_) {
				if(ev& (EPOLLIN | EPOLLERR | EPOLLHUP))
					acceptNewClient();
				continue;
			}
            if(fd==wake_up_fd_){
                handleWakeup();
                continue;
            }
            ConnectionId id = makeId(fd);
            if(ev & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
				handleClient(id);
			}
			if (ev & EPOLLOUT) {
				handleWrite(id);
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
	listen(listen_fd_, 128);// backlog=排队上限,加大防突发连接积压
	LOG_INFO("start listening on port %d", port_);
	//先把监听套接字加进epoll事件列表
	epoll_event ev;
	ev.events = EPOLLIN | EPOLLONESHOT;
	ev.data.fd = listen_fd_;
	epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev);
    //注册一下唤醒事件,不需要oneshot，因为线程不会抢他，wakeupfd原子性
    epoll_event wake_ev;
    wake_ev.data.fd = wake_up_fd_;
    wake_ev.events = EPOLLIN;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, wake_up_fd_, &wake_ev);

    loop_thread_id = this_thread::get_id();
    signal(SIGINT, handle_signal);
	signal(SIGTERM, handle_signal);
	signal(SIGPIPE, SIG_IGN);
	run();
}
//优雅退出标志
void EpollServer::handle_signal(int) {
	stop_ = true;
}

void  EpollServer::handleClient(ConnectionId id) {
//	pool_.post([this, id] {
        int fd = id.fd;
        //待际不对直接退出
        {
            auto it = fd_list.find(fd);
            if(it==fd_list.end()||it->second.generation()!=id.generation)
                return;
        }
        char buf[1024];
        int len = recv(fd, buf, sizeof(buf), 0);
		if (len == 0) {//关闭fd，可能fd对应的缓冲区仍然有东西，需要全发完再关
			SendResult result;
			{
				auto it = fd_list.find(fd);
                if (it == fd_list.end() || it->second.generation() != id.generation)
                    return;
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
			bool frame_error = false;
			{
				auto it = fd_list.find(fd);
                if (it == fd_list.end() || it->second.generation() != id.generation)
                    return;
                it->second.appendRecv(buf, len);
				it->second.touchActive();//刷新一下活跃时间
				msgs = move(it->second.processBufferedData());
				frame_error = it->second.hasFrameError();
			}   
			if (frame_error) {
				closeConnection(fd);
				return; 
			}
			for (auto& msg : msgs) {
                if (handler_) handler_(*this, id, msg);    //锁外触发回调
			}
            SendResult result{SendResult::Pending}; // 消息一次是不是全发完了，全发完就直接无视了，
            // 没全发完登记成epollout等handlewrite继续发
            int fd = id.fd;
            {
                auto it = fd_list.find(fd);
                if (it == fd_list.end() || it->second.generation() != id.generation)
                    return;
                result = it->second.sendReadyMessage(); // 收尾发送
            }

            epoll_event nev;
            nev.data.fd = fd;
            if (result == SendResult::SentAll)
            {
                nev.events = EPOLLIN | EPOLLONESHOT;
            }
            else if (result == SendResult::Pending)
            {
                nev.events = EPOLLOUT | EPOLLIN | EPOLLONESHOT;
            }
            else
            {
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

	//});
}

void EpollServer::handleWrite(ConnectionId id) {
	//pool_.post([this, id] {
        int fd=id.fd;
        {
            auto it = fd_list.find(fd);
            if (it == fd_list.end() || it->second.generation() != id.generation)
                return;
        }
        SendResult result;
		bool closing = false;
		{
			auto it = fd_list.find(fd);
            if (it == fd_list.end() || it->second.generation() != id.generation)
                return;
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
	//	});
}

void EpollServer::closeConnection(int fd) {
	LOG_INFO("close connection, fd: %d", fd);
	epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
	{
		fd_list.erase(fd);
	}
	close(fd);
}

ConnectionId EpollServer::makeId(int fd){
    auto it = fd_list.find(fd);
    if(it!=fd_list.end()){
        return {fd, it->second.generation()};
    }
    return {fd, 0};
}

void EpollServer::acceptNewClient() {//调回调函数，确定fd对应的业务逻辑
	while (true) {
		int client_fd = accept(listen_fd_, nullptr, nullptr);

		if (client_fd < 0) {
			//和发送的时候的三态处理类似
			if (errno == EINTR)	continue;//被信号打断，重试
			if (errno == EAGAIN || errno == EWOULDBLOCK)	break;//接完了，退出
			LOG_ERROR("accept failed: %s", strerror(errno));//出错
			return;
		}
		{
			//维护客户连接
			int flag = fcntl(client_fd, F_GETFL, 0);
			fcntl(client_fd, F_SETFL, flag | O_NONBLOCK);//设置客户非阻塞，为了处理在send时候的阻塞问题

            Connection conn(client_fd);
            conn.setGeneration(next_generation_.fetch_add(1));
            fd_list.insert({client_fd, move(conn)});

            //fd_list.insert({ client_fd,Connection(client_fd) });
		}
		LOG_DEBUG("new client fd=%d", client_fd);
		epoll_event nev;
		nev.data.fd = client_fd;
		nev.events = EPOLLIN | EPOLLONESHOT;
		epoll_ctl(epfd_, EPOLL_CTL_ADD, client_fd, &nev);
	}
	
	epoll_event nev1;
	nev1.data.fd = listen_fd_;
	nev1.events = EPOLLIN | EPOLLONESHOT;
	epoll_ctl(epfd_, EPOLL_CTL_MOD,listen_fd_ , &nev1);
}

void EpollServer::setMessageHandler(std::function<void(EpollServer&, ConnectionId, const std::string&)> f) {
	handler_ = move(f);
}

void EpollServer::sendInLoop(ConnectionId id, const string& msg){
    int fd = id.fd;
    auto it = fd_list.find(fd);
    if (it == fd_list.end() || it->second.generation() != id.generation)
        return;
    it->second.sendMsgToSendBuf(msg); // 只塞缓冲,不真发
}

void EpollServer::queueInLoop(std::function<void()> cb){
    {
        lock_guard<mutex> lck(workers_results_mutex_);
        workers_results_.push_back(cb);
    }
    uint64_t one = 1;
    write(wake_up_fd_, &one, sizeof(one));
}

void EpollServer::handleWakeup(){
    uint64_t dummy;
    while(read(wake_up_fd_,&dummy,sizeof(dummy)) > 0){}
    deque<function<void()>> dq;//临时信箱，一次全抱走，不要一次一次pop
    {
        lock_guard<mutex> lck(workers_results_mutex_);
        dq.swap(workers_results_);
    }
    for(auto& it:dq){
        it();
    }
}

void EpollServer::sendTo(ConnectionId id, const string &msg)
{
    if(isInLoopThread()){//小型网络io直接发
        sendInLoop(id, msg);
    }
    else{//线程池给过来的消息先扔到队列在发
        queueInLoop([id, msg,this]()
                    { sendInLoop(id, msg); });
    }
}

void EpollServer::broadcast(int exptr_fd, const std::string& msg) {
	std::vector<int> toClose;
	{
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
