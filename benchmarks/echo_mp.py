"""多进程压测客户端 (echo_mp).

相对 benchmarks/echo.py 的改进:
单进程 asyncio 客户端在高吞吐时容易先被压满, 导致测不准服务器上限。
echo_mp 把连接与事件循环拆到多个进程, 每个进程独立跑 asyncio 回环,
进程数可调, 用更饱和的流量压服务器。

用法:
    python3 -m benchmarks.echo_mp                                  # 默认 1000 连接, 自动进程数
    python3 -m benchmarks.echo_mp --procs 8 --clients 100,500,1000 # 三个并发阶段各 10s
    python3 -m benchmarks.echo_mp --procs 4 --clients 500 --duration 5 --payload 128
    NF_NOTE=mp_p8 python3 -m benchmarks.echo_mp --procs 8          # 备注写入 benchmark_log.csv

输出: 每个阶段一行 client=.. procs=.. qps=.. p50/p95/p99, 并追加到 benchmark_log.csv。
"""

import argparse
import asyncio
import csv
import multiprocessing
import os
import socket
import statistics
import sys
import time

import benchmarks.config as cfg
import tests.common as c

# 每个进程上报给父进程的延迟样本上限(超出后均匀抽样), 防止队列撑爆内存
MAX_SAMPLES_PER_WORKER = 100_000


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values.sort()
    idx = int(len(values) * p - 1)
    if idx < 0:
        idx = 0
    return values[idx]


async def _run_worker_loop(
    clients: int, payload: bytes, duration: float, host: str, port: int
) -> list[float]:
    """一个进程内: 建 clients 条连接, duration 秒内持续回环, 返回延迟(ms)列表。"""
    connections = await asyncio.gather(
        *(asyncio.open_connection(host, port) for _ in range(clients))
    )
    latencies: list[float] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration

    async def pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while loop.time() < deadline:
            start = time.perf_counter()
            received = await c.async_roundtrip(reader, writer, payload)
            assert received == payload
            latencies.append((time.perf_counter() - start) * 1000)

    try:
        await asyncio.gather(*(pump(r, w) for r, w in connections))
    finally:
        for _, writer in connections:
            writer.close()
    return latencies


def _worker(
    queue: multiprocessing.Queue,
    proc_index: int,
    clients: int,
    payload_size: int,
    duration: float,
    host: str,
    port: int,
) -> None:
    """子进程入口: 跑完把本进程的成功次数和延迟样本放回队列。"""
    try:
        latencies = asyncio.run(
            _run_worker_loop(clients, b"P" * payload_size, duration, host, port)
        )
        count = len(latencies)
        if count > MAX_SAMPLES_PER_WORKER:
            stride = count // MAX_SAMPLES_PER_WORKER
            latencies = latencies[::stride]
        queue.put({"proc": proc_index, "count": count, "latencies": latencies})
    except Exception as exc:  # noqa: BLE001 - 子进程内任何异常都要回报给父进程
        queue.put({"proc": proc_index, "error": f"{type(exc).__name__}: {exc}"})


def _distribute(clients: int, procs: int) -> list[int]:
    """把 clients 条连接尽量均匀地分到 procs 个进程。"""
    per, rem = divmod(clients, procs)
    return [per + (1 if i < rem else 0) for i in range(procs)]


def _check_server(host: str, port: int) -> None:
    try:
        with socket.create_connection((host, port), timeout=3):
            pass
    except OSError as exc:
        sys.exit(f"ERROR: cannot connect to server {host}:{port} - {exc}")


def _run_phase(
    clients: int, procs: int, payload_size: int, duration: float, host: str, port: int
) -> tuple[int, float, list[float], list[str]]:
    """起 procs 个子进程压一个并发阶段, 返回 (成功次数, 墙钟耗时, 延迟样本, 错误列表)。"""
    ctx = multiprocessing.get_context()
    queue = ctx.Queue()
    chunks = _distribute(clients, procs)
    processes = [
        ctx.Process(target=_worker, args=(queue, i, chunks[i], payload_size, duration, host, port))
        for i in range(procs)
    ]
    for p in processes:
        p.start()

    results: list[dict] = []
    start = time.perf_counter()
    try:
        deadline = time.monotonic() + duration + 30
        for _ in processes:
            remain = deadline - time.monotonic()
            if remain <= 0:
                break
            try:
                results.append(queue.get(timeout=remain))
            except Exception:  # noqa: BLE001 - 子进程异常退出或超时
                break
    finally:
        for p in processes:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
    elapsed = time.perf_counter() - start

    errors = [r["error"] for r in results if "error" in r]
    ok = [r for r in results if "count" in r]
    total = sum(r["count"] for r in ok)
    latencies: list[float] = []
    for r in ok:
        latencies.extend(r["latencies"])
    return total, elapsed, latencies, errors


def _append_to_csv(
    clients: int,
    procs: int,
    success: int,
    elapsed: float,
    qps: float,
    avg_ms: float,
    max_ms: float,
    p50: float,
    p95: float,
    p99: float,
    note: str,
) -> None:
    path = "benchmark_log.csv"
    new_file = not os.path.exists(path)
    note = f"mp_p{procs}" + (f"_{note}" if note else "")
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow([
                "时间", "并发", "成功率%", "成功请求", "总耗时(s)",
                "QPS", "平均延迟(ms)", "最大延迟(ms)", "被拒",
                "消息/连接", "消息体(字节)", "p50(ms)", "p95(ms)", "p99(ms)", "备注",
            ])
        writer.writerow([
            time.strftime("%Y-%m-%d %H:%M:%S"),
            clients,
            "100.0",
            success,
            f"{elapsed:.4f}",
            f"{qps:.1f}",
            f"{avg_ms:.2f}",
            f"{max_ms:.2f}",
            "0",
            success // clients if clients else 0,
            cfg.PAYLOAD_SIZE,
            f"{p50:.2f}",
            f"{p95:.2f}",
            f"{p99:.2f}",
            note,
        ])


def main() -> None:
    parser = argparse.ArgumentParser(description="多进程回显压测客户端")
    parser.add_argument("--host", default=c.HOST, help=f"服务器地址(默认 {c.HOST})")
    parser.add_argument("--port", type=int, default=c.PORT, help=f"服务器端口(默认 {c.PORT})")
    parser.add_argument(
        "--clients",
        default="1000",
        help="连接数, 逗号分隔可跑多个阶段, 如 100,500,1000",
    )
    parser.add_argument(
        "--procs",
        type=int,
        default=0,
        help="压测进程数(默认 0=自动, 不超过 8 个)",
    )
    parser.add_argument("--duration", type=float, default=cfg.DURATION, help="每阶段秒数")
    parser.add_argument("--payload", type=int, default=cfg.PAYLOAD_SIZE, help="消息体字节数")
    parser.add_argument("--note", default="", help="写入 CSV 的备注")
    parser.add_argument("--no-csv", action="store_true", help="不写 benchmark_log.csv")
    args = parser.parse_args()

    phases = [int(x) for x in args.clients.split(",") if x.strip()]
    if not phases:
        parser.error("--clients 至少需要一个正整数")
    procs = args.procs or min(8, max(1, os.cpu_count() or 1))
    note = os.environ.get("NF_NOTE", args.note)

    _check_server(args.host, args.port)
    for clients in phases:
        effective_procs = min(procs, clients)
        total, elapsed, latencies, errors = _run_phase(
            clients, effective_procs, args.payload, args.duration, args.host, args.port
        )
        if errors:
            print(f"WARNING: {len(errors)} 个子进程出错: " + "; ".join(errors), file=sys.stderr)
        qps = total / elapsed if elapsed > 0 else 0.0
        avg_ms = statistics.mean(latencies) if latencies else 0.0
        max_ms = max(latencies) if latencies else 0.0
        p50 = _percentile(latencies, 0.50)
        p95 = _percentile(latencies, 0.95)
        p99 = _percentile(latencies, 0.99)
        print(
            f"client={clients} procs={effective_procs} "
            f"qps={qps:.1f} p50={p50:.2f}ms p95={p95:.2f}ms p99={p99:.2f}ms"
        )
        if not args.no_csv:
            _append_to_csv(
                clients, effective_procs, total, elapsed, qps,
                avg_ms, max_ms, p50, p95, p99, note,
            )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("interrupted")
