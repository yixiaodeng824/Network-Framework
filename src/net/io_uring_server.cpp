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
#include <sys/eventfd.h>
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

void IoUringServer::pin_to_cpu(int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

IoUringServer::IoUringServer(int port, int threads, int heartbeat_timeout, int affinity_core, int sub_count)
    : port_(port), threads_(threads), heartbeat_timeout_(heartbeat_timeout),
      affinity_core_(affinity_core), sub_count_(sub_count > 0 ? sub_count : 2) {
    // 阻塞 fd:io_uring 由内核异步等待,不会以 -EAGAIN 空转
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) { perror("socket"); exit(1); }
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) { perror("bind"); exit(1); }
    if (listen(listen_fd_, 128) < 0) { perror("listen"); exit(1); }
    if (!ring_setup(accept_ring_, 1024)) {
        fprintf(stderr, "accept ring setup failed: %s\n", strerror(errno));
        exit(1);
    }
    subs_.reserve(sub_count_);
    for (int i = 0; i < sub_count_; ++i) {
        subs_.emplace_back(new Sub);
        Sub& s = *subs_.back();
        // 阻塞 eventfd:READ SQE 等到有计数才完成,不空转
        s.wake_fd = eventfd(0, EFD_CLOEXEC);
        if (s.wake_fd < 0) { perror("eventfd"); exit(1); }
        if (!ring_setup(s.ring, 4096)) {
            fprintf(stderr, "sub ring setup failed: %s\n", strerror(errno));
            exit(1);
        }
    }
}

IoUringServer::~IoUringServer() {
    for (auto& sp : subs_) {
        if (sp->wake_fd >= 0) close(sp->wake_fd);
    }
    if (accept_ring_.fd >= 0) close(accept_ring_.fd);
    if (listen_fd_ >= 0) close(listen_fd_);
}

bool IoUringServer::ring_setup(Ring& r, unsigned entries) {
    r.fd = static_cast<int>(syscall(SYS_io_uring_setup, entries, &r.params));
    if (r.fd < 0) return false;

    void* sq = mmap(nullptr, r.params.sq_off.array + r.params.sq_entries * sizeof(unsigned),
                    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, r.fd, IORING_OFF_SQ_RING);
    if (sq == MAP_FAILED) return false;
    r.sq_head = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + r.params.sq_off.head);
    r.sq_tail = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + r.params.sq_off.tail);
    r.sq_mask = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + r.params.sq_off.ring_mask);
    r.sq_entries = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + r.params.sq_off.ring_entries);
    r.sq_array = reinterpret_cast<unsigned*>(static_cast<char*>(sq) + r.params.sq_off.array);

    void* cq = mmap(nullptr, r.params.cq_off.cqes + r.params.cq_entries * sizeof(struct io_uring_cqe),
                    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, r.fd, IORING_OFF_CQ_RING);
    if (cq == MAP_FAILED) return false;
    r.cq_head = reinterpret_cast<unsigned*>(static_cast<char*>(cq) + r.params.cq_off.head);
    r.cq_tail = reinterpret_cast<unsigned*>(static_cast<char*>(cq) + r.params.cq_off.tail);
    r.cq_mask = reinterpret_cast<unsigned*>(static_cast<char*>(cq) + r.params.cq_off.ring_mask);
    r.cq_entries = reinterpret_cast<unsigned*>(static_cast<char*>(cq) + r.params.cq_off.ring_entries);
    r.cqes = reinterpret_cast<struct io_uring_cqe*>(static_cast<char*>(cq) + r.params.cq_off.cqes);

    r.sqes = static_cast<struct io_uring_sqe*>(
        mmap(nullptr, r.params.sq_entries * sizeof(struct io_uring_sqe),
             PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, r.fd, IORING_OFF_SQES));
    if (r.sqes == MAP_FAILED) return false;
    return true;
}

bool IoUringServer::submit_sqe(Ring& r, uint8_t op, int fd, uint64_t user_data,
                               void* addr, unsigned len) {
    unsigned tail = *r.sq_tail;
    unsigned next = tail + 1;
    if (next - *r.sq_head > *r.sq_entries) return false;  // 队列满,等下一轮(kick 自愈)
    struct io_uring_sqe* sqe = &r.sqes[tail & *r.sq_mask];
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = op;
    sqe->fd = fd;
    sqe->user_data = user_data;
    sqe->addr = reinterpret_cast<unsigned long>(addr);
    sqe->len = len;
    r.sq_array[tail & *r.sq_mask] = tail & *r.sq_mask;  // 关键:登记 SQE 下标
    __sync_synchronize();
    *r.sq_tail = next;
    ++r.pending;
    return true;
}

int IoUringServer::ring_enter(Ring& r, unsigned to_submit, unsigned min_complete) {
    return static_cast<int>(syscall(SYS_io_uring_enter, r.fd, to_submit, min_complete,
                                    IORING_ENTER_GETEVENTS, nullptr, 0));
}

// ---------------- 主线程 accept ----------------
void IoUringServer::submit_accept() {
    submit_sqe(accept_ring_, IORING_OP_ACCEPT, listen_fd_, OP_ACCEPT);
}

void IoUringServer::reap_accept() {
    unsigned head = *accept_ring_.cq_head;
    while (head != *accept_ring_.cq_tail) {
        struct io_uring_cqe* cqe = &accept_ring_.cqes[head & *accept_ring_.cq_mask];
        int res = cqe->res;
        uint64_t ud = cqe->user_data;
        __sync_synchronize();
        *accept_ring_.cq_head = head + 1;
        ++head;
        if (ud == OP_ACCEPT) {
            if (res >= 0) dispatch(res);  // res = 新 fd
            submit_accept();              // 单发 accept,完成即补一发
        }
    }
}

void IoUringServer::dispatch(int fd) {
    int idx = run_robin_++ % sub_count_;  // 轮询分发:主从 token 分发
    Sub& s = *subs_[idx];
    {
        std::lock_guard<std::mutex> lk(s.mutex);
        s.new_fds.push_back(fd);
    }
    uint64_t one = 1;
    (void)write(s.wake_fd, &one, sizeof(one));  // 门铃:唤醒 sub 的 eventfd READ
}

void IoUringServer::accept_loop() {
    if (affinity_core_ >= 0) pin_to_cpu(affinity_core_);
    submit_accept();
    while (!g_stop) {
        int ret = ring_enter(accept_ring_, accept_ring_.pending, 1);
        accept_ring_.pending = 0;
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("accept ring_enter");
            break;
        }
        reap_accept();
    }
}

// ---------------- sub 线程 ----------------
void IoUringServer::submit_wake_read(Sub& s) {
    submit_sqe(s.ring, IORING_OP_READ, s.wake_fd, OP_WAKE, &s.wake_val, sizeof(s.wake_val));
}

void IoUringServer::submit_recv(Sub& s, int fd, Conn& c) {
    if (c.recv_inflight) return;
    c.recv_inflight = true;
    if (!submit_sqe(s.ring, IORING_OP_RECV, fd, (static_cast<uint64_t>(fd) << 8) | OP_RECV,
                    c.buf, sizeof(c.buf))) {
        c.recv_inflight = false;
    }
}

void IoUringServer::submit_send(Sub& s, int fd, Conn& c) {
    if (c.send_inflight || c.out.empty()) return;
    c.send_inflight = true;
    const PoolString& msg = c.out.front();
    if (!submit_sqe(s.ring, IORING_OP_SEND, fd, (static_cast<uint64_t>(fd) << 8) | OP_SEND,
                    const_cast<char*>(msg.data()), static_cast<unsigned>(msg.size()))) {
        c.send_inflight = false;
    }
}

void IoUringServer::on_wake(Sub& s) {
    std::deque<int> fds;
    {
        std::lock_guard<std::mutex> lk(s.mutex);
        fds.swap(s.new_fds);
    }
    for (int fd : fds) {
        auto [it, ok] = s.conns.emplace(fd, Conn(fd, s.pool));
        submit_recv(s, fd, it->second);
    }
    submit_wake_read(s);  // 重新挂 eventfd 读
}

void IoUringServer::on_recv(Sub& s, int fd, Conn& c, int res) {
    c.recv_inflight = false;
    if (res == 0) {
        close_conn(s, fd);
        return;
    }
    if (res < 0) {
        if (res != -EAGAIN) close_conn(s, fd);  // EAGAIN 由 kick() 稍后重提交
        return;
    }
    c.conn.appendRecv(c.buf, static_cast<size_t>(res));
    c.conn.touchActive();
    std::vector<PoolString> msgs = c.conn.processBufferedData();
    if (c.conn.hasFrameError()) {
        close_conn(s, fd);
        return;
    }
    for (auto& msg : msgs) {
        // echo:回完整协议帧(4 字节长度头 + payload),走内存池分配
        PoolString frame(s.pool ? PoolAllocator<char>(s.pool) : PoolAllocator<char>());
        frame.resize(4 + msg.size());
        uint32_t n = htonl(static_cast<uint32_t>(msg.size()));
        memcpy(&frame[0], &n, 4);
        memcpy(&frame[4], msg.data(), msg.size());
        c.out.push_back(std::move(frame));
    }
    submit_send(s, fd, c);
    submit_recv(s, fd, c);
}

void IoUringServer::on_send(Sub& s, int fd, Conn& c, int res) {
    c.send_inflight = false;
    if (res < 0) {
        close_conn(s, fd);
        return;
    }
    c.out.pop_front();
    // 剩余回包由 kick() 或后续 recv 触发提交
}

void IoUringServer::close_conn(Sub& s, int fd) {
    auto it = s.conns.find(fd);
    if (it != s.conns.end()) s.conns.erase(it);
    close(fd);
}

void IoUringServer::kick(Sub& s) {
    for (auto& [fd, c] : s.conns) {
        if (!c.recv_inflight) submit_recv(s, fd, c);
        if (!c.send_inflight && !c.out.empty()) submit_send(s, fd, c);
    }
}

void IoUringServer::heartbeat_check(Sub& s) {
    static thread_local time_t last_tick = 0;
    time_t now = time(nullptr);
    if (now - last_tick < 1) return;
    last_tick = now;
    std::vector<int> dead;
    for (auto& [fd, c] : s.conns) {
        if (c.conn.isTimeout(now, heartbeat_timeout_)) dead.push_back(fd);
    }
    for (int fd : dead) close_conn(s, fd);
}

void IoUringServer::reap_sub(Sub& s) {
    unsigned head = *s.ring.cq_head;
    while (head != *s.ring.cq_tail) {
        struct io_uring_cqe* cqe = &s.ring.cqes[head & *s.ring.cq_mask];
        int res = cqe->res;
        uint64_t ud = cqe->user_data;
        __sync_synchronize();
        *s.ring.cq_head = head + 1;
        ++head;

        uint8_t op = static_cast<uint8_t>(ud & 0xff);
        int fd = static_cast<int>(ud >> 8);
        if (op == OP_WAKE) {
            on_wake(s);
            continue;
        }
        auto it = s.conns.find(fd);
        if (it == s.conns.end()) continue;  // 已关闭的残留完成
        if (op == OP_RECV) on_recv(s, fd, it->second, res);
        else if (op == OP_SEND) on_send(s, fd, it->second, res);
    }
}

void IoUringServer::sub_loop(int idx) {
    Sub& s = *subs_[idx];
    if (affinity_core_ >= 0) pin_to_cpu(1 + idx);  // 每核独立
    s.pool = new MemoryPool(128, 1 << 17);         // 128B 块 x 131072 = 16MB
    submit_wake_read(s);
    while (!g_stop) {
        int ret = ring_enter(s.ring, s.ring.pending, 1);
        s.ring.pending = 0;
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("sub ring_enter");
            break;
        }
        reap_sub(s);
        kick(s);
        heartbeat_check(s);
    }
    for (auto& [fd, c] : s.conns) close(fd);
    s.conns.clear();
    delete s.pool;
    s.pool = nullptr;
}

void IoUringServer::start() {
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);
    for (int i = 0; i < sub_count_; ++i) {
        subs_[i]->thread = std::thread(&IoUringServer::sub_loop, this, i);
    }
    accept_loop();  // 主线程阻塞在 accept ring,完全替代 epoll
    g_stop = 1;
    for (auto& sp : subs_) {
        if (sp->thread.joinable()) {
            uint64_t one = 1;
            (void)write(sp->wake_fd, &one, sizeof(one));  // 唤醒 sub 退出
            sp->thread.join();
        }
    }
}

void IoUringServer::handle_signal(int) {
    g_stop = 1;
}
