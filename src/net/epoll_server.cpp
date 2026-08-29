#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
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
#include <chrono>
using namespace std;
static constexpr int RECVMMSG_BATCH = 8; // recvmmsg 一次系统调用最多收的段数(每段 4KB)
atomic<bool> EpollServer::stop_{ false };

EpollServer::EpollServer(int subindex, ThreadPool &pool, int heartbeat_timeout,
                         shared_ptr<PerformanceMetrics> metrics,
                         std::shared_ptr<std::atomic<size_t>> conn_count,
                        int handshake_timeout,size_t max_buffer_size)
    : sub_index_(subindex), pool_(pool), heartbeat_timeout_(heartbeat_timeout),
      metrics_(metrics ? move(metrics) : make_shared<PerformanceMetrics>()),
      conn_count_(conn_count),handshake_timeout_(handshake_timeout),max_buffer_size_(max_buffer_size)
{
    //从epoll不需要监听事件，等mainreactor分发
    //创建 epoll 句柄
	epfd_ = epoll_create(1);    
	wake_up_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
}

void EpollServer::logMetrics(const char* phase) const {
    const PerformanceSnapshot snapshot = metrics_->snapshot();
    const double elapsed = metrics_->elapsedSeconds();
    const double qps = static_cast<double>(snapshot.requests) / elapsed;
    const auto closeCount = [&snapshot](CloseReason reason) {
        return snapshot.close_reasons[static_cast<size_t>(reason)];
    };

    LOG_INFO(
        "performance_metrics phase=%s qps=%.1f requests=%llu "
        "latency_avg_ms=%.3f p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f p999_ms=%.3f "
        "latency_max_ms=%.3f active_connections=%lld output_buffer_bytes=%lld "
        "thread_pool_queue=%zu thread_pool_wait_avg_ms=%.3f thread_pool_wait_max_ms=%.3f",
        phase, qps, static_cast<unsigned long long>(snapshot.requests),
        PerformanceMetrics::averageLatencyMs(snapshot),
        PerformanceMetrics::percentileMs(snapshot, 0.50),
        PerformanceMetrics::percentileMs(snapshot, 0.95),
        PerformanceMetrics::percentileMs(snapshot, 0.99),
        PerformanceMetrics::percentileMs(snapshot, 0.999),
        static_cast<double>(snapshot.max_latency_ns) / 1'000'000.0,
        static_cast<long long>(snapshot.active_connections),
        static_cast<long long>(snapshot.output_buffer_bytes),
        pool_.queueSize(), pool_.queueWaitAverageMs(), pool_.queueWaitMaxMs());

    LOG_INFO(
        "performance_metrics counters recv_eagain=%llu send_eagain=%llu "
        "accept_eagain=%llu accept_failures=%llu close_peer=%llu "
        "close_heartbeat=%llu close_io=%llu close_frame=%llu close_send=%llu "
        "close_overflow=%llu close_shutdown=%llu close_other=%llu",
        static_cast<unsigned long long>(snapshot.recv_eagain),
        static_cast<unsigned long long>(snapshot.send_eagain),
        static_cast<unsigned long long>(snapshot.accept_eagain),
        static_cast<unsigned long long>(snapshot.accept_failures),
        static_cast<unsigned long long>(closeCount(CloseReason::PeerClosed)),
        static_cast<unsigned long long>(closeCount(CloseReason::HeartbeatTimeout)),
        static_cast<unsigned long long>(closeCount(CloseReason::IoError)),
        static_cast<unsigned long long>(closeCount(CloseReason::FrameError)),
        static_cast<unsigned long long>(closeCount(CloseReason::SendError)),
        static_cast<unsigned long long>(closeCount(CloseReason::SendOverflow)),
        static_cast<unsigned long long>(closeCount(CloseReason::ServerShutdown)),
        static_cast<unsigned long long>(closeCount(CloseReason::Other)));
}

void EpollServer::epollserver_exit() {
	LOG_INFO("server stopping, closing connections...");
	if (sub_index_ == 0) {
		logMetrics("stopping");
	}
	//先关闭线程池
	pool_.close();
	vector<int> fds;
	fds.reserve(fd_list.size());
	for (const auto& fd : fd_list) {
		fds.push_back(fd.first);
	}
	for (int fd : fds) {
		closeConnection(fd, CloseReason::ServerShutdown);
	}
	//清理connection

	LOG_INFO("server stopped.");

}

void EpollServer::heartBeatCheck() {
	time_t now = time(nullptr);
	vector<int> timeout_fds;
    size_t total = 0;
    int victim = -1;
    size_t max_size = 0;
    for (auto& [fd, conn] : fd_list) {//要是总大小超，则直接踢掉最大的链接
        size_t size = conn->send_buf_size() + conn->recv_buf_size();
        total += size;
        if(size > max_size){
            max_size = size;
            victim = fd;
        }
        //判断是否超时
        if(conn->hasRecvData()){
            if (conn->isTimeout(now, heartbeat_timeout_)) {
                timeout_fds.push_back(fd);
            }
        }
        else{
            if(conn->isHandShakeTimeout(now,handshake_timeout_)){
                timeout_fds.push_back(fd);
            }
        }
    }
    if (max_buffer_size_ > 0 && total > max_buffer_size_ && victim >= 0)
    {
        timeout_fds.push_back(victim);
    }
    for (auto fd : timeout_fds) {
		LOG_INFO("heartbeat timeout, close fd: %d", fd);
		closeConnection(fd, CloseReason::HeartbeatTimeout);
	}
}

void EpollServer::run() {
	int tick = 0;
	auto next_metrics_report = chrono::steady_clock::now() + chrono::seconds(1);
	while (!stop_) {
		int n = epoll_wait(epfd_, events_, 1024, 100);//等待事件发生
		if (n < 0) {
			if (errno == EINTR) continue;   // 被信号打断,重新等
			LOG_ERROR("epoll_wait error: %s", strerror(errno));
			continue;
		}
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
		if (sub_index_ == 0 && chrono::steady_clock::now() >= next_metrics_report) {
			logMetrics("interval");
			next_metrics_report = chrono::steady_clock::now() + chrono::seconds(1);
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
    for (int fd : fds) {
        auto it = fd_list.find(fd);
        if (it != fd_list.end())
            flushOne(fd, it->second, it->second->generation());
    }
}

void EpollServer::markPendingSend(int fd) {
    auto it = fd_list.find(fd);
    if (it == fd_list.end() || it->second->isPendingBatch())
        return;
    it->second->setPendingBatch(true);
    pending_send_fds_.push_back(fd);
}

void EpollServer::flushOne(int fd, const std::shared_ptr<Connection>& conn, uint64_t gen,
                           CloseReason close_reason) {
    if (!conn || conn->generation() != gen)
        return;
    bool ww = conn->isWriteWaiting();//必须在 sendReadyMessage 之前读(Pending 时它会置 true)
    const size_t buffer_before = conn->pendingSendBufferSize();
    SendResult result = conn->sendReadyMessage();
    const size_t buffer_after = conn->pendingSendBufferSize();
    metrics_->addOutputBufferBytes(static_cast<int64_t>(buffer_after) -
                                   static_cast<int64_t>(buffer_before));
    if (conn->lastSendWouldBlock()) {
        metrics_->recordSendEagain();
    }
    conn->setPendingBatch(false);
    bool closing = conn->isClosing();
    if (result == SendResult::SentAll) {
        if (closing) {
            shutdown(fd, SHUT_WR);
            closeConnection(fd, close_reason);
            return;
        }
        if (ww) {//之前挂了 EPOLLOUT,现在发完了,去掉(状态变化才 MOD)
            conn->setWriteWaiting(false);
            epoll_event nev;
            nev.data.fd = fd;
            nev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
            epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
        }//ET:没挂 EPOLLOUT 就不用 MOD,省一次 syscall
    }
    else if (result == SendResult::Pending) {
        if (!ww) {//第一次 Pending 才挂 EPOLLOUT(ET:仅状态变化时 MOD)
            conn->setWriteWaiting(true);
            epoll_event nev;
            nev.data.fd = fd;
            nev.events = closing ? (EPOLLOUT | EPOLLET) : (EPOLLIN | EPOLLOUT | EPOLLET);
            epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
        }//已挂则等 socket 可写边沿再触发,不 MOD
    }
    else {
        closeConnection(
            fd, conn->hasSendError() ? CloseReason::SendOverflow : CloseReason::SendError);
        return;
    }
}

void  EpollServer::handleClient(ConnectionId id) {
//	pool_.post([this, id] {
        int fd = id.fd;
        //一次 find,持有 shared_ptr 保活,整个事件复用同一对象,不再反复查找
        auto it = fd_list.find(fd);
        if (it == fd_list.end() || it->second->generation() != id.generation)
            return;
        std::shared_ptr<Connection> conn = it->second;
        //批量 recv:recvmmsg 一次系统调用收最多 RECVMMSG_BATCH 段(4KB x 8),循环到 EAGAIN 或 64KB 上限
        char bufs[RECVMMSG_BATCH][4096];
        struct iovec iovs[RECVMMSG_BATCH];
        struct mmsghdr mms[RECVMMSG_BATCH];
        memset(mms, 0, sizeof(mms));
        for (int i = 0; i < RECVMMSG_BATCH; ++i) {
            iovs[i].iov_base = bufs[i];
            iovs[i].iov_len = sizeof(bufs[i]);
            mms[i].msg_hdr.msg_iov = &iovs[i];
            mms[i].msg_hdr.msg_iovlen = 1;
        }
        size_t total = 0;
        bool peer_closed = false;
        bool io_error = false;
        bool need_rearm_read = false;
        while (true) {
            int n = recvmmsg(fd, mms, RECVMMSG_BATCH, MSG_DONTWAIT, nullptr);
            if (n > 0) {
                if (mms[0].msg_len == 0) { peer_closed = true; break; }// 流式 socket EOF:返回 vlen 个 0 长度消息
                for (int i = 0; i < n; ++i) {
                    conn->appendRecv(bufs[i], mms[i].msg_len);
                    total += mms[i].msg_len;
                }
                conn->touchActive();//刷新一下活跃时间
                if (total >= kRecvBatchLimit) { need_rearm_read = true; break; } // ET:到上限先处理,再重新制造读边沿
            }
            else if (n == 0) { peer_closed = true; break; }// 对端关闭(EOF)
            else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    metrics_->recordRecvEagain();
                    break;
                }
                if (errno == EINTR) continue;
                io_error = true; break;
            }
        }
        if (io_error) { closeConnection(fd, CloseReason::IoError); return; }
        vector<PoolString> msgs;
        bool frame_error = false;
        if (total > 0) {
            msgs = move(conn->processBufferedData());
            frame_error = conn->hasFrameError();
            if (frame_error) { closeConnection(fd, CloseReason::FrameError); return; }
            conn->setBatchHint(msgs.size() > 1);//多条消息才走批量发送
            for (auto& msg : msgs) {
                const auto request_started = chrono::steady_clock::now();
                if (handler_) handler_(*this, id, msg);    //锁外触发回调(回包进发送缓冲,由 sendInLoop 决定批量或直发)
                metrics_->recordRequest(chrono::steady_clock::now() - request_started);
            }
            //回调可能增删连接;shared_ptr 保证对象保活,只需确认它仍是当前 fd 的主人(指针相等,零拷贝比较)
            it = fd_list.find(fd);
            if (it == fd_list.end() || it->second != conn)
                return;
            conn->setBatchHint(false);
        }
        if (peer_closed) {//关闭fd,可能fd对应的缓冲区仍然有东西,需要全发完再关
            conn->markClosing();
            flushOne(fd, conn, id.generation, CloseReason::PeerClosed);//ET:立刻尝试 flush,没发完由 EPOLLOUT 边沿续发
            return;
        }
        if (need_rearm_read) {//ET 陷阱:没读到 EAGAIN 就停下会丢边沿,这里重新制造一次读边沿
            epoll_event nev;
            nev.data.fd = fd;
            bool ww = conn->isWriteWaiting();
            nev.events = ww ? (EPOLLIN | EPOLLOUT | EPOLLET) : (EPOLLIN | EPOLLRDHUP | EPOLLET);
            epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &nev);
        }
        //批量 send:回包已积攒在发送缓冲,由 sendInLoop 标记 pending,本批次结束统一 flush
        //ET:EPOLLIN 保持注册,无需 MOD,下一次数据到达会再次触发边沿
	//});
}

void EpollServer::handleWrite(ConnectionId id) {
	//pool_.post([this, id] {
        //统一走批量 flush 的收尾逻辑(发送 + 按结果重新挂事件)
        auto it = fd_list.find(id.fd);
        if (it != fd_list.end())
            flushOne(id.fd, it->second, id.generation);
	//	});
}

void EpollServer::closeConnection(int fd, CloseReason reason) {
	auto it = fd_list.find(fd);
	if (it == fd_list.end()) {
		return;
	}
	LOG_INFO("close connection, fd: %d", fd);
	epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
	metrics_->addOutputBufferBytes(-static_cast<int64_t>(it->second->pendingSendBufferSize()));
	metrics_->connectionClosed(reason);
	fd_list.erase(it);
    if (conn_count_)
        conn_count_->fetch_sub(1); // 连接关闭,全局计数减一
    close(fd);
}

ConnectionId EpollServer::makeId(int fd){
    auto it = fd_list.find(fd);
    if(it!=fd_list.end()){
        return {fd, it->second->generation()};
    }
    return {fd, 0};
}

void EpollServer::acceptNewConnection(int fd){
    int flag = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flag|O_NONBLOCK);
    auto conn = std::make_shared<Connection>(fd, &mem_pool_);
    conn->setGeneration(next_generation_.fetch_add(1));
    fd_list.insert({fd, std::move(conn)});
    metrics_->connectionAccepted();
    LOG_DEBUG("new client fd=%d", fd);
    epoll_event ev;
    ev.data.fd = fd;
    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
    epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev);
}

void EpollServer::setMessageHandler(std::function<void(EpollServer&, ConnectionId, string_view)> f) {
	handler_ = move(f);
}

void EpollServer::sendInLoop(ConnectionId id, string_view msg){
    int fd = id.fd;
    auto it = fd_list.find(fd);
    if (it == fd_list.end() || it->second->generation() != id.generation)
        return;
    const size_t buffer_before = it->second->pendingSendBufferSize();
    it->second->sendMsgToSendBuf(msg); // 只塞缓冲,不真发
    metrics_->addOutputBufferBytes(
        static_cast<int64_t>(it->second->pendingSendBufferSize()) -
        static_cast<int64_t>(buffer_before));
    //批量发送:积攒到阈值立即 flush,否则挂 pending 等本批次结束统一发
    //机会式批量:本批次拆出多条消息时积攒统一发(减少 send syscall);单条或达到阈值直接发
    if (it->second->sendBufferSize() >= kSendBatchThreshold || !it->second->batchHint())
        flushOne(fd, it->second, id.generation);
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
void EpollServer::sendToRaw(ConnectionId id, string_view msg)
{
    int fd = id.fd;
    auto it = fd_list.find(fd);
    if (it == fd_list.end() || it->second->generation() != id.generation)
        return;
    const size_t buffer_before = it->second->pendingSendBufferSize();
    it->second->sendRawToSendBuf(msg);             // 原样塞(不加长度头)
    metrics_->addOutputBufferBytes(
        static_cast<int64_t>(it->second->pendingSendBufferSize()) -
        static_cast<int64_t>(buffer_before));
    //批量发送:达到阈值立即发,否则积攒到本批次结束统一 flush
    if (it->second->sendBufferSize() >= kSendBatchThreshold)
        flushOne(fd, it->second, id.generation);
    else
        markPendingSend(fd);
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
	std::vector<std::pair<int, CloseReason>> toClose;
	{
		for (auto& it : fd_list) {
			if (it.first == exptr_fd)	continue;
			const size_t buffer_before = it.second->pendingSendBufferSize();
			it.second->sendMsgToSendBuf(msg);
			metrics_->addOutputBufferBytes(
				static_cast<int64_t>(it.second->pendingSendBufferSize()) -
				static_cast<int64_t>(buffer_before));
		}
		for (auto& it : fd_list) {
			const size_t buffer_before = it.second->pendingSendBufferSize();
			auto result = it.second->sendReadyMessage(); 
			const size_t buffer_after = it.second->pendingSendBufferSize();
			metrics_->addOutputBufferBytes(
				static_cast<int64_t>(buffer_after) -
				static_cast<int64_t>(buffer_before));
			if (it.second->lastSendWouldBlock()) {
				metrics_->recordSendEagain();
			}
			epoll_event nev;
			nev.data.fd = it.first;
			if (result == SendResult::SentAll) {
				nev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
			}
			else if (result == SendResult::Pending) {
				nev.events = EPOLLIN | EPOLLOUT | EPOLLET;
			}
			else {
				toClose.emplace_back(
					it.first, it.second->hasSendError() ? CloseReason::SendOverflow : CloseReason::SendError);
				continue;
			}
			epoll_ctl(epfd_, EPOLL_CTL_MOD, it.first, &nev);
		}
	}
	for (const auto& [fd, reason] : toClose) {              //  锁外,遍历结束才关
		closeConnection(fd, reason);
	}
}

EpollServer::~EpollServer(){
	close(epfd_);
	close(wake_up_fd_);
}
