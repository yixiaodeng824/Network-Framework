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


EpollServer::EpollServer(int subindex, ThreadPool &pool, int heartbeat_timeout = 60) : sub_index_(subindex), pool_(pool), heartbeat_timeout_(heartbeat_timeout)
{
    //从epoll不需要监听事件，等mainreactor分发
    //创建 epoll 句柄
	epfd_ = epoll_create(1);    
    wake_up_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
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
			vector<PoolString> msgs;
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

void EpollServer::acceptNewConnection(int fd){
    int flag = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flag|O_NONBLOCK);
    Connection conn(fd,&mem_pool_);
    conn.setGeneration(next_generation_.fetch_add(1));
    fd_list.insert({fd, move(conn)});
    LOG_DEBUG("new client fd=%d", fd);
    epoll_event ev;
    ev.data.fd = fd;
    ev.events = EPOLLIN | EPOLLONESHOT;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev);
}

void EpollServer::setMessageHandler(std::function<void(EpollServer&, ConnectionId, string_view)> f) {
	handler_ = move(f);
}

void EpollServer::sendInLoop(ConnectionId id, string_view msg){
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

// epoll_server.cpp:
void EpollServer::sendToRaw(ConnectionId id, string_view msg)
{
    int fd = id.fd;
    auto it = fd_list.find(fd);
    if (it == fd_list.end() || it->second.generation() != id.generation)
        return;
    it->second.sendRawToSendBuf(msg);             // ① 原样塞(不加长度头)
    SendResult r = it->second.sendReadyMessage(); // ② 尝试发
    epoll_event nev;
    nev.data.fd = fd;
    if (r == SendResult::SentAll)
        nev.events = EPOLLIN | EPOLLONESHOT;
    else if (r == SendResult::Pending)
        nev.events = EPOLLIN | EPOLLOUT | EPOLLONESHOT;
    else
    {
        closeConnection(fd);
        return;
    }
    epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
}

void EpollServer::sendTo(ConnectionId id, const string_view msg)
{
    if(isInLoopThread()){//小型网络io直接发
        sendInLoop(id, msg);
    }
    else{//线程池给过来的消息先扔到队列在发
        static std::once_flag flag;
        std::string owned(msg);
        queueInLoop([id, this,owned]()
                    { sendInLoop(id, owned); });
        
    }
}

void EpollServer::broadcast(int exptr_fd, string_view msg) {
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
	close(wake_up_fd_);
}
