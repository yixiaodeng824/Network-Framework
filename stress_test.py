# -*- coding: gbk -*-
# =====================================================
#  stress_test.py - 服务器并发压测脚本
#
#  目标:压测服务器的并发表现,统计连接成功率、请求数、
#        QPS、平均/最大延迟、连接被拒情况。
#
#  协议(与 test.py 一致,已存在,直接用):
#     每条消息 = 4 字节长度(网络字节序/大端) + 消息体
#     服务器收到后把消息原样回传(回显)
#
#  用法:
#     1) 启动服务器: cd ~/projects/for_linux && ./server.out <端口>
#     2) 运行压测:
#        python stress_test.py --host 192.168.159.128 --port 8888
#        python stress_test.py --clients 100,500,1000 --messages 50
#
#  默认依次用 100 / 500 / 1000 个客户端同时连上,
#  每个客户端连上后连续发送 --messages 条消息。
# =====================================================
import argparse
import socket
import struct
import sys
import threading
import time


def build_msg(payload):
    """组一条消息: [4 字节长度(大端)][消息体]"""
    return struct.pack('>I', len(payload)) + payload


def recv_one(sock):
    """按协议读取一条完整消息;连接被关闭返回 None"""
    header = b''
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        if not chunk:
            return None
        header += chunk
    length = struct.unpack('>I', header)[0]
    body = b''
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            return None
        body += chunk
    return body


class RoundStats(object):
    """一轮压测的统计结果(多线程共享,内部加锁)"""

    def __init__(self):
        self.lock = threading.Lock()
        self.ok_conn = 0       # 连接成功数
        self.refused = 0       # 连接被拒数
        self.conn_timeout = 0  # 连接超时数
        self.conn_other = 0    # 其他连接失败数
        self.attempted = 0     # 尝试发送的请求总数
        self.ok_req = 0        # 回显正确的请求数
        self.fail_req = 0      # 失败请求数(回显错误/断开/超时)
        self.latency_sum = 0.0  # 成功请求延迟总和(秒)
        self.latency_max = 0.0  # 成功请求最大延迟(秒)

    def add_ok_req(self, dt):
        with self.lock:
            self.attempted += 1
            self.ok_req += 1
            self.latency_sum += dt
            if dt > self.latency_max:
                self.latency_max = dt

    def add_fail_req(self):
        with self.lock:
            self.attempted += 1
            self.fail_req += 1


def worker(host, port, payload, messages, timeout, stats, barrier):
    """单个模拟客户端:连上后连续发 messages 条消息并等待回显"""
    try:
        barrier.wait(timeout=120)
    except threading.BrokenBarrierError:
        return  # 线程启动太慢(极少发生),直接放弃这一路

    try:
        sock = socket.create_connection((host, port), timeout=timeout)
    except ConnectionRefusedError:
        with stats.lock:
            stats.refused += 1
        return
    except socket.timeout:
        with stats.lock:
            stats.conn_timeout += 1
        return
    except OSError:
        with stats.lock:
            stats.conn_other += 1
        return

    with stats.lock:
        stats.ok_conn += 1

    try:
        for _ in range(messages):
            try:
                t0 = time.perf_counter()
                sock.sendall(build_msg(payload))
                body = recv_one(sock)
            except socket.timeout:
                stats.add_fail_req()  # 读回显超时
                break
            except OSError:
                stats.add_fail_req()  # 连接中途断开
                break
            dt = time.perf_counter() - t0
            if body is None:
                stats.add_fail_req()  # 服务器提前关闭连接
                break
            if body == payload:
                stats.add_ok_req(dt)
            else:
                stats.add_fail_req()  # 回显内容不对
    finally:
        sock.close()


def run_round(host, port, clients, messages, payload_size, timeout):
    """跑一轮指定并发数的压测,返回 (统计结果, 总耗时秒数)"""
    payload = b'P' * payload_size
    stats = RoundStats()
    # 所有客户端线程 + 主线程一起等 barrier,保证"同时"开始连接
    barrier = threading.Barrier(clients + 1)
    threads = []
    for _ in range(clients):
        t = threading.Thread(
            target=worker,
            args=(host, port, payload, messages, timeout, stats, barrier))
        t.daemon = True
        t.start()
        threads.append(t)

    barrier.wait()
    t_start = time.perf_counter()

    # 等待全部线程结束,期间每 10% 打印一次进度(防止大并发卡住)
    last_reported = 0
    while True:
        done = sum(1 for t in threads if not t.is_alive())
        if done == clients:
            break
        if done - last_reported >= max(1, clients // 10):
            print("    已完成 %d/%d 客户端..." % (done, clients), flush=True)
            last_reported = done
        time.sleep(0.05)

    for t in threads:
        t.join()
    total = time.perf_counter() - t_start
    return stats, total


def print_round(clients, stats, total):
    """打印一轮压测的详细统计"""
    conn_total = (stats.ok_conn + stats.refused +
                  stats.conn_timeout + stats.conn_other)
    ok_rate = stats.ok_conn * 100.0 / conn_total if conn_total else 0.0
    qps = stats.ok_req / total if total > 0 else 0.0
    avg_ms = stats.latency_sum / stats.ok_req * 1000.0 if stats.ok_req else 0.0
    max_ms = stats.latency_max * 1000.0

    print("连接: 成功 %d/%d (%.1f%%), 被拒 %d, 超时 %d, 其他失败 %d"
          % (stats.ok_conn, conn_total, ok_rate,
             stats.refused, stats.conn_timeout, stats.conn_other))
    print("请求: 发送 %d, 成功 %d, 失败 %d"
          % (stats.attempted, stats.ok_req, stats.fail_req))
    print("总耗时: %.3f 秒" % total)
    print("QPS: %.1f" % qps)
    print("延迟: 平均 %.2f ms, 最大 %.2f ms" % (avg_ms, max_ms))

    # 返回汇总行,供最后的对比表格使用
    rate_str = "%.1f%%" % ok_rate
    return "%-8d %-9s %-10d %-10.3f %-8.1f %-12.2f %-12.2f %-6d" % (
        clients, rate_str, stats.ok_req, total, qps, avg_ms, max_ms, stats.refused)



def append_to_csv(rows, messages, payload_size):
    """把这一轮压测结果追加到 benchmark_log.csv(首次自动写表头)"""
    import csv
    import os
    path = 'benchmark_log.csv'
    new_file = not os.path.exists(path)
    with open(path, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(['时间', '并发', '成功率%', '成功请求', '总耗时(s)',
                             'QPS', '平均延迟(ms)', '最大延迟(ms)', '被拒',
                             '消息/连接', '消息体(字节)', 'p50(ms)', 'p95(ms)', 'p99(ms)'])
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        for r in rows:
            writer.writerow([now] + list(r) + [messages, payload_size] + ['', '', ''])


def main():
    parser = argparse.ArgumentParser(description="服务器并发压测脚本")
    parser.add_argument("--host", default="192.168.159.128", help="服务器 IP,默认 192.168.159.128")
    parser.add_argument("--port", type=int, default=8888, help="服务器端口,默认 8888")
    parser.add_argument("--clients", default="100,500,1000",
                        help="并发数,逗号分隔,默认 100,500,1000")
    parser.add_argument("--messages", type=int, default=20,
                        help="每个连接连续发送的消息条数,默认 20")
    parser.add_argument("--payload-size", type=int, default=64,
                        help="单条消息体大小(字节),默认 64")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="连接/读写超时(秒),默认 10")
    args = parser.parse_args()

    # 先探一下服务器是否在监听,给个友好提示
    try:
        probe = socket.create_connection((args.host, args.port), timeout=3)
        probe.close()
    except Exception:
        print("无法连接服务器 %s:%d,请先启动 ./server.out <端口>" % (args.host, args.port))
        sys.exit(1)

    levels = []
    for part in args.clients.split(','):
        part = part.strip()
        if part:
            levels.append(int(part))
    if not levels:
        print("--clients 参数无效,示例: --clients 100,500,1000")
        sys.exit(1)

    print("压测参数: 并发 %s, 每连接 %d 条消息, 消息体 %d 字节"
          % (",".join(str(x) for x in levels), args.messages, args.payload_size))
    print("服务器: %s:%d\n" % (args.host, args.port))

    rows = []
    raw_rows = []
    for clients in levels:
        print("========== 并发 %d ==========" % clients)
        stats, total = run_round(args.host, args.port, clients,
                                 args.messages, args.payload_size, args.timeout)
        row = print_round(clients, stats, total)
        rows.append(row)
        # 收集原始数值,供写 CSV
        conn_total = (stats.ok_conn + stats.refused +
                      stats.conn_timeout + stats.conn_other)
        ok_rate = stats.ok_conn * 100.0 / conn_total if conn_total else 0.0
        qps = stats.ok_req / total if total > 0 else 0.0
        avg_ms = stats.latency_sum / stats.ok_req * 1000.0 if stats.ok_req else 0.0
        max_ms = stats.latency_max * 1000.0
        raw_rows.append((clients, ok_rate, stats.ok_req, total,
                         qps, avg_ms, max_ms, stats.refused))
        print()

    print("================== 汇总对比 ==================")
    print("%-8s %-9s %-10s %-10s %-8s %-12s %-12s %-6s" % (
        "并发数", "成功率", "成功请求", "总耗时(s)", "QPS",
        "平均延迟(ms)", "最大延迟(ms)", "被拒"))
    for row in rows:
        print(row)

    # 记录本次压测数据
    append_to_csv(raw_rows, args.messages, args.payload_size)
    print("\n已记录到 benchmark_log.csv")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n压测被中断(Ctrl+C)")
        sys.exit(130)
