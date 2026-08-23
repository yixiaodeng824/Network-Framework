#include "io_uring_server.h"

#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>

volatile sig_atomic_t IoUringServer::g_stop = 0;

#ifndef SYS_io_uring_setup
#define SYS_io_uring_setup __NR_io_uring_setup
#endif
#ifndef SYS_io_uring_enter
#define SYS_io_uring_enter __NR_io_uring_enter
#endif

namespace {

void pin_to_cpu(int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

}  // namespace

IoUringServer::IoUringServer(int port, int threads, int heartbeat_timeout, int affinity_core)
    : port_(port), threads_(threads), heartbeat_timeout_(heartbeat_timeout),
      affinity_core_(affinity_core) {
    // io_uring 下保持阻塞 fd:请求由内核异步等待,不会以 -EAGAIN 空转
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        perror("socket");
        exit(1);
    }
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        exit(1);
    }
    if (listen(listen_fd_, 128) < 0) {
        perror("listen");
        exit(1);
    }
    if (!ring_setup(4096)) {
        fprintf(stderr, "io_uring_setup failed: %s\n", strerror(errno));
        exit(1);
    }
}

IoUringServer::~IoUringServer() {
    if (ring_fd_ >= 0) close(ring_fd_);
    if (listen_fd_ >= 0) close(listen_fd_);
    if (sqes_) munmap(sqes_, params_.sq_entries * sizeof(struct io_uring_sqe));
}

bool IoUringServer::ring_setup(unsigned entries) {
    ring_fd_ = static_cast<int>(syscall(SYS_io_uring_setup, entries, &params_));
    if (ring_fd_ < 0) return false;

    void* sq = mmap(nullptr, params_.sq_off.array + params_.sq_entries * sizeof(unsigned),
                    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_,
                    IORING_OFF_SQ_RING);
    if (sq == MAP_FAILED) return false;
    sq_head_ = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + params_.sq_off.head);
    sq_tail_ = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + params_.sq_off.tail);
    sq_mask_ = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + params_.sq_off.ring_mask);
    sq_entries_ = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + params_.sq_off.ring_entries);
    sq_array_ = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + params_.sq_off.array);

    void* cq = mmap(nullptr, params_.cq_off.cqes + params_.cq_entries * sizeof(struct io_uring_cqe),
                    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_,
                    IORING_OFF_CQ_RING);
    if (cq == MAP_FAILED) return false;
    cq_head_ = reinterpret_cast<unsigned*>(static_cast<char*>(cq) + params_.cq_off.head);
    cq_tail_ = reinterpret_cast<unsigned*>(static_cast<char*>(cq) + params_.cq_off.tail);
    cq_mask_ = reinterpret_cast<unsigned*>(static_cast<char*>(cq) + params_.cq_off.ring_mask);
    cq_entries_ = reinterpret_cast<unsigned*>(static_cast<char*>(cq) + params_.cq_off.ring_entries);
    cqes_ = reinterpret_cast<struct io_uring_cqe*>(static_cast<char*>(cq) + params_.cq_off.cqes);

    sqes_ = static_cast<struct io_uring_sqe*>(
        mmap(nullptr, params_.sq_entries * sizeof(struct io_uring_sqe),
             PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd_, IORING_OFF_SQES));
    if (sqes_ == MAP_FAILED) return false;
    return true;
}

bool IoUringServer::submit_sqe(uint8_t op, int fd, uint64_t user_data, void* addr,
                               unsigned len, int extra_fd) {
    unsigned tail = *sq_tail_;
    unsigned next = tail + 1;
    if (next - *sq_head_ > *sq_entries_) return false;  // 队列满,等下一轮
    struct io_uring_sqe* sqe = &sqes_[tail & *sq_mask_];
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = op;
    sqe->fd = fd;
    sqe->user_data = user_data;
    sqe->addr = reinterpret_cast<unsigned long>(addr);
    sqe->len = len;
    if (op == IORING_OP_ACCEPT) {
        sqe->addr2 = 0;
    }
    (void)extra_fd;
    // 关键:sq_array_[tail & mask] 登记本次 SQE 下标,内核靠它取 SQE
    sq_array_[tail & *sq_mask_] = tail & *sq_mask_;
    __sync_synchronize();
    *sq_tail_ = next;
    ++pending_submits_;
    return true;
}

int IoUringServer::ring_enter(unsigned to_submit, unsigned min_complete) {
    return static_cast<int>(syscall(SYS_io_uring_enter, ring_fd_, to_submit, min_complete,
                                    IORING_ENTER_GETEVENTS, nullptr, 0));
}

void IoUringServer::submit_accept() {
    submit_sqe(IORING_OP_ACCEPT, listen_fd_, static_cast<uint64_t>(OP_ACCEPT));
}

void IoUringServer::submit_recv(int fd, Conn& c) {
    if (c.recv_inflight) return;
    c.recv_inflight = true;
    if (!submit_sqe(IORING_OP_RECV, fd, (static_cast<uint64_t>(fd) << 8) | OP_RECV,
                    c.buf, sizeof(c.buf))) {
        c.recv_inflight = false;
    }
}

void IoUringServer::submit_send(int fd, Conn& c) {
    if (c.send_inflight || c.out.empty()) return;
    c.send_inflight = true;
    const std::string& msg = c.out.front();
    if (!submit_sqe(IORING_OP_SEND, fd, (static_cast<uint64_t>(fd) << 8) | OP_SEND,
                    const_cast<char*>(msg.data()), static_cast<unsigned>(msg.size()))) {
        c.send_inflight = false;
    }
}

void IoUringServer::on_accept(int res) {
    if (res < 0) {
        if (res != -EAGAIN) perror("accept");
        submit_accept();
        return;
    }
    int fd = res;
    auto [it, inserted] = conns_.emplace(fd, Conn(fd));
    Conn& c = it->second;
    submit_recv(fd, c);
    submit_accept();  // 单发 accept,完成后再补
}

void IoUringServer::on_recv(int fd, Conn& c, int res) {
    c.recv_inflight = false;
    if (res == 0) {
        close_conn(fd);
        return;
    }
    if (res < 0) {
        if (res != -EAGAIN) close_conn(fd);  // EAGAIN 由 kick() 稍后重提交
        return;
    }
    c.conn.appendRecv(c.buf, static_cast<size_t>(res));
    c.conn.touchActive();
    std::vector<std::string> msgs = c.conn.processBufferedData();
    if (c.conn.hasFrameError()) {
        close_conn(fd);
        return;
    }
    for (auto& msg : msgs) {
        // echo:回完整协议帧(4 字节长度头 + payload)
        std::string frame;
        frame.resize(4 + msg.size());
        uint32_t n = htonl(static_cast<uint32_t>(msg.size()));
        memcpy(&frame[0], &n, 4);
        memcpy(&frame[4], msg.data(), msg.size());
        c.out.push_back(std::move(frame));
    }
    submit_send(fd, c);
    submit_recv(fd, c);
}

void IoUringServer::on_send(int fd, Conn& c, int res) {
    c.send_inflight = false;
    if (res < 0) {
        close_conn(fd);
        return;
    }
    c.out.pop_front();
    // 剩余回包由 kick() 或后续 recv 触发提交
}

void IoUringServer::close_conn(int fd) {
    auto it = conns_.find(fd);
    if (it != conns_.end()) conns_.erase(it);
    close(fd);
}

void IoUringServer::kick() {
    for (auto& [fd, c] : conns_) {
        if (!c.recv_inflight) submit_recv(fd, c);
        if (!c.send_inflight && !c.out.empty()) submit_send(fd, c);
    }
}

void IoUringServer::heartbeat_check() {
    time_t now = time(nullptr);
    if (now - last_tick_ < 1) return;
    last_tick_ = now;
    std::vector<int> dead;
    for (auto& [fd, c] : conns_) {
        if (c.conn.isTimeout(now, heartbeat_timeout_)) dead.push_back(fd);
    }
    for (int fd : dead) close_conn(fd);
}

void IoUringServer::reap_completions() {
    unsigned head = *cq_head_;
    while (head != *cq_tail_) {
        struct io_uring_cqe* cqe = &cqes_[head & *cq_mask_];
        int res = cqe->res;
        uint64_t ud = cqe->user_data;
        __sync_synchronize();
        *cq_head_ = head + 1;
        ++head;

        uint8_t op = static_cast<uint8_t>(ud & 0xff);
        int fd = static_cast<int>(ud >> 8);
        if (op == OP_ACCEPT) {
            on_accept(res);
            continue;
        }
        auto it = conns_.find(fd);
        if (it == conns_.end()) continue;  // 已关闭的残留完成
        if (op == OP_RECV) on_recv(fd, it->second, res);
        else if (op == OP_SEND) on_send(fd, it->second, res);
    }
}

void IoUringServer::start() {
    if (affinity_core_ >= 0) pin_to_cpu(affinity_core_);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);
    last_tick_ = time(nullptr);
    submit_accept();
    while (!g_stop) {
        int ret = ring_enter(pending_submits_, 1);
        pending_submits_ = 0;
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("io_uring_enter");
            break;
        }
        reap_completions();
        kick();
        heartbeat_check();
    }
    for (auto& [fd, c] : conns_) close(fd);
    conns_.clear();
}

void IoUringServer::handle_signal(int) {
    g_stop = 1;
}
