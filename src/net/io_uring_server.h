#pragma once

#include <linux/io_uring.h>
#include <csignal>
#include <cstdint>
#include <deque>
#include <map>
#include <string>

#include "connection.h"

// io_uring 版 echo 服务器(单线程,一个 ring)。
// 核心思路:recv/send/accept 全部挂到同一个 io_uring,一次 io_uring_enter
// 系统调用同时提交 N 个 SQE 并收割 M 个完成事件,从根上替代 epoll+recv+send
// 的多次 syscall;复用 Connection 做 4 字节长度头的拆包/半包处理。
class IoUringServer {
public:
    IoUringServer(int port, int threads, int heartbeat_timeout, int affinity_core);
    ~IoUringServer();

    void start();                       // 阻塞运行事件循环
    static void handle_signal(int);     // SIGINT/SIGTERM 置停止标志
    static volatile sig_atomic_t g_stop;

private:
    enum Op : uint8_t { OP_ACCEPT = 1, OP_RECV = 2, OP_SEND = 3 };

    struct Conn {
        Connection conn;                 // 复用拆包逻辑
        std::deque<std::string> out;     // 待发送回包(echo 模式原样回)
        bool recv_inflight{false};
        bool send_inflight{false};
        char buf[4096];                  // 单连接单 recv 在飞,缓冲可复用
        explicit Conn(int fd) : conn(fd) {}
    };

    // ---- ring 原始接口(不依赖 liburing,直接用 syscall) ----
    bool ring_setup(unsigned entries);
    bool submit_sqe(uint8_t op, int fd, uint64_t user_data,
                    void* addr = nullptr, unsigned len = 0, int extra_fd = -1);
    int ring_enter(unsigned to_submit, unsigned min_complete);
    void reap_completions();

    void submit_accept();
    void submit_recv(int fd, Conn& c);
    void submit_send(int fd, Conn& c);
    void on_accept(int res);
    void on_recv(int fd, Conn& c, int res);
    void on_send(int fd, Conn& c, int res);
    void kick();  // 每轮循环自愈:把因队列满/时序丢失的 recv/send 重新提交
    void close_conn(int fd);
    void heartbeat_check();

    int port_;
    int threads_;
    int heartbeat_timeout_;
    int affinity_core_;

    int ring_fd_{-1};
    int listen_fd_{-1};
    struct io_uring_params params_{};
    unsigned* sq_head_{nullptr};
    unsigned* sq_tail_{nullptr};
    unsigned* sq_mask_{nullptr};
    unsigned* sq_entries_{nullptr};
    unsigned* sq_array_{nullptr};
    struct io_uring_sqe* sqes_{nullptr};
    unsigned* cq_head_{nullptr};
    unsigned* cq_tail_{nullptr};
    unsigned* cq_mask_{nullptr};
    unsigned* cq_entries_{nullptr};
    struct io_uring_cqe* cqes_{nullptr};

    unsigned pending_submits_{0};
    std::map<int, Conn> conns_;
    uint64_t next_cookie_{1};
    time_t last_tick_{0};
};
