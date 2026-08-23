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
		int n = epoll_wait(epfd_, events_, 1024, 100);//等待事件发生
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
		flushPendingWrites();//本批次事件处理完,统一把积攒的发送缓冲发出去
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

// 批量发送辅助:本批次积攒的 fd 统一 flush,一次 epoll 周期只做一轮 send,减少 syscall
void EpollServer::flushPendingWrites() {
    vector<int> fds = move(pending_send_fds_);
    pending_send_fds_.clear();
    for (int fd : fds)
        flushOne(fd);
}

void EpollServer::markPendingSend(int fd) {
    auto it = fd_list.find(fd);
    if (it == fd_list.end() || it->second.isPendingBatch())
        return;
    it->second.setPendingBatch(true);
    pending_send_fds_.push_back(fd);
}

void EpollServer::flushOne(int fd) {
    ConnectionId id = makeId(fd);
    auto it = fd_list.find(fd);
    if (it == fd_list.end() || it->second.generation() != id.generation)
        return;
    SendResult result = it->second.sendReadyMessage();
    it->second.setPendingBatch(false);
    bool closing = it->second.isClosing();
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
    else if (result == SendResult::Pending) {//可能还会有新数据同时进来,要加epollin
        if (closing) {
            nev.events = EPOLLOUT | EPOLLONESHOT;//关闭中,不挂读
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
}

void  EpollServer::handleClient(ConnectionId id) {
//	pool_.post([this, id] {
        int fd = id.fd;
        //代际不对直接退出
        {
            auto it = fd_list.find(fd);
            if(it==fd_list.end()||it->second.generation()!=id.generation)
                return;
        }
        //批量 recv:一次事件循环尽量把 socket 可读数据都收进来,攒够 kRecvBatchLimit 或 EAGAIN 为止
        char buf[4096];
        size_t total = 0;
        bool peer_closed = false;
        bool io_error = false;
        while (true) {
            int len = recv(fd, buf, sizeof(buf), 0);
            if (len > 0) {
                {
                    auto it = fd_list.find(fd);
                    if (it == fd_list.end() || it->second.generation() != id.generation)
                        return;
                    it->second.appendRecv(buf, len);
                    it->second.touchActive();//刷新一下活跃时间
                }
                total += (size_t)len;
                if (total >= kRecvBatchLimit) break; // 防饿死,剩下的下一轮再收
            }
            else if (len == 0) { peer_closed = true; break; }
            else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                io_error = true; break;
            }
        }
        if (io_error) { closeConnection(fd); return; }
        vector<string> msgs;
        bool frame_error = false;
        if (total > 0) {
            {
                auto it = fd_list.find(fd);
                if (it == fd_list.end() || it->second.generation() != id.generation)
                    return;
                msgs = move(it->second.processBufferedData());
                frame_error = it->second.hasFrameError();
            }
            if (frame_error) { closeConnection(fd); return; }
            {
                auto it = fd_list.find(fd);
                if (it == fd_list.end() || it->second.generation() != id.generation)
                    return;
                it->second.setBatchHint(msgs.size() > 1);//多条消息才走批量发送
            }
            for (auto& msg : msgs) {
                if (handler_) handler_(*this, id, msg);    //锁外触发回调(回包进发送缓冲,由 sendInLoop 决定批量或直发)
            }
            {
                auto it = fd_list.find(fd);
                if (it == fd_list.end() || it->second.generation() != id.generation)
                    return;
                it->second.setBatchHint(false);
            }
        }
        if (peer_closed) {//关闭fd,可能fd对应的缓冲区仍然有东西,需要全发完再关
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
            return;
        }
        //批量 send:回包已积攒在发送缓冲,由 sendInLoop 标记 pending,本批次结束统一 flush
        //有积攒时不再重复 MOD,统一由 flushPendingWrites 按发送结果挂事件;无积攒则重新挂 EPOLLIN
        bool batch_pending = false;
        {
            auto it = fd_list.find(fd);
            if (it == fd_list.end() || it->second.generation() != id.generation)
                return;
            batch_pending = it->second.isPendingBatch();
        }
        if (!batch_pending) {
            epoll_event nev;
            nev.data.fd = fd;
            nev.events = EPOLLIN | EPOLLONESHOT;
            epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
        }
	//});
}

void EpollServer::handleWrite(ConnectionId id) {
	//pool_.post([this, id] {
        //统一走批量 flush 的收尾逻辑(发送 + 按结果重新挂事件)
        flushOne(id.fd);
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
    Connection conn(fd);
    conn.setGeneration(next_generation_.fetch_add(1));
    fd_list.insert({fd, move(conn)});
    LOG_DEBUG("new client fd=%d", fd);
    epoll_event ev;
    ev.data.fd = fd;
    ev.events = EPOLLIN | EPOLLONESHOT;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev);
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
    //批量发送:积攒到阈值立即 flush,否则挂 pending 等本批次结束统一发
    //机会式批量:本批次拆出多条消息时积攒统一发(减少 send syscall);单条或达到阈值直接发
    if (it->second.sendBufferSize() >= kSendBatchThreshold || !it->second.batchHint())
        flushOne(fd);
    else
        markPendingSend(fd);
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
void EpollServer::sendToRaw(ConnectionId id, const std::string &msg)
{
    int fd = id.fd;
    auto it = fd_list.find(fd);
    if (it == fd_list.end() || it->second.generation() != id.generation)
        return;
    it->second.sendRawToSendBuf(msg);             // 原样塞(不加长度头)
    //批量发送:达到阈值立即发,否则积攒到本批次结束统一 flush
    if (it->second.sendBufferSize() >= kSendBatchThreshold)
        flushOne(fd);
    else
        markPendingSend(fd);
}

void EpollServer::sendTo(ConnectionId id, const string &msg)
{
    if(isInLoopThread()){//小型网络io直接发
        sendInLoop(id, msg);
    }
    else{//线程池给过来的消息先扔到队列在发
        static std::once_flag flag;
        std::call_once(flag, []
                       { LOG_WARN(">>> sendTo 走了跨线程投递!isInLoopThread 判断有问题"); });
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
	close(wake_up_fd_);
}
