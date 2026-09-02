#pragma once

#include <linux/io_uring.h>
#include <csignal>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "connection.h"

// io_uring 主从架构(替代主从 epoll):
//   主线程:一个 ring 只做 accept(单发,完成即补一发),轮询把 fd 分发给 sub;
//   sub 线程 xN:每个 sub 一个独立 ring + 独立 MemoryPool + 独立连接表,
//     通过 eventfd(阻塞,io_uring READ)接收主线程投递的 fd;
//   recv/send/accept 全部走 io_uring,一次 ring_enter 提交/收割 N 个 IO。
class IoUringServer {
public:
    IoUringServer(int port, int threads, int heartbeat_timeout, int affinity_core, int sub_count);
    ~IoUringServer();

    void start();
    static void handle_signal(int);
    static volatile sig_atomic_t g_stop;

private:
    enum Op : uint8_t { OP_ACCEPT = 1, OP_RECV = 2, OP_SEND = 3, OP_WAKE = 4 };

    struct Ring {
        int fd{-1};
        struct io_uring_params params{};
        unsigned* sq_head{nullptr};
        unsigned* sq_tail{nullptr};
        unsigned* sq_mask{nullptr};
        unsigned* sq_entries{nullptr};
        unsigned* sq_array{nullptr};
        struct io_uring_sqe* sqes{nullptr};
        unsigned* cq_head{nullptr};
        unsigned* cq_tail{nullptr};
        unsigned* cq_mask{nullptr};
        unsigned* cq_entries{nullptr};
        struct io_uring_cqe* cqes{nullptr};
        unsigned pending{0};
    };

    struct Conn {
        Connection conn;
        std::deque<PoolString> out;      // 待发送回包(echo 原样回,池分配)
        bool recv_inflight{false};
        bool send_inflight{false};
        char buf[4096];
        Conn(int fd, MemoryPool* pool) : conn(fd, pool) {}
    };

    struct Sub {
        Ring ring;
        int wake_fd{-1};
        uint64_t wake_val{0};
        std::mutex mutex;                // 只保护 new_fds(主线程写 / sub 线程取)
        std::deque<int> new_fds;
        std::map<int, Conn> conns;
        MemoryPool* pool{nullptr};
        std::thread thread;
    };

    // ---- ring 原始接口(不依赖 liburing) ----
    bool ring_setup(Ring& r, unsigned entries);
    bool submit_sqe(Ring& r, uint8_t op, int fd, uint64_t user_data,
                    void* addr = nullptr, unsigned len = 0);
    int ring_enter(Ring& r, unsigned to_submit, unsigned min_complete);

    // ---- 主线程 accept 侧 ----
    void accept_loop();
    void submit_accept();
    void reap_accept();
    void dispatch(int fd);

    // ---- sub 线程侧 ----
    void sub_loop(int idx);
    void submit_wake_read(Sub& s);
    void submit_recv(Sub& s, int fd, Conn& c);
    void submit_send(Sub& s, int fd, Conn& c);
    void reap_sub(Sub& s);
    void on_wake(Sub& s);
    void on_recv(Sub& s, int fd, Conn& c, int res);
    void on_send(Sub& s, int fd, Conn& c, int res);
    void kick(Sub& s);
    void close_conn(Sub& s, int fd);
    void heartbeat_check(Sub& s);

    static void pin_to_cpu(int cpu);

    int port_;
    int threads_;
    int heartbeat_timeout_;
    int affinity_core_;
    int sub_count_;
    Ring accept_ring_;
    int listen_fd_{-1};
    int run_robin_{0};
    std::vector<std::unique_ptr<Sub>> subs_;
};
