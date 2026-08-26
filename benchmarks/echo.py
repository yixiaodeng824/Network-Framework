import asyncio
import csv
import math
import os
import statistics
import time

import benchmarks.config as cfg
import tests.common as c

# 备注:压测前用环境变量注明本次条件,如 NF_NOTE="listen128+4worker"
NOTE = os.environ.get("NF_NOTE", "")


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if not 0 < p <= 1:
        raise ValueError(f"percentile must be in (0, 1], got {p}")
    ordered = sorted(values)
    idx = min(len(ordered) - 1, math.ceil(len(ordered) * p) - 1)
    return ordered[max(0, idx)]


def _append_to_csv(
    client: int,
    success: int,
    elapsed: float,
    qps: float,
    avg_ms: float,
    max_ms: float,
    p50: float,
    p95: float,
    p99: float,
    p999: float,
    note: str = "",
) -> None:
    path = "benchmark_log.csv"
    new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow([
                "时间", "并发", "成功率%", "成功请求", "总耗时(s)",
                "QPS", "平均延迟(ms)", "最大延迟(ms)", "被拒",
                "消息/连接", "消息体(字节)", "p50(ms)", "p95(ms)", "p99(ms)",
                "p999(ms)", "备注",
            ])
        writer.writerow([
            time.strftime("%Y-%m-%d %H:%M:%S"),
            client,
            "100.0",
            success,
            f"{elapsed:.4f}",
            f"{qps:.1f}",
            f"{avg_ms:.2f}",
            f"{max_ms:.2f}",
            "0",
            success // client,
            cfg.PAYLOAD_SIZE,
            f"{p50:.2f}",
            f"{p95:.2f}",
            f"{p99:.2f}",
            f"{p999:.2f}",
            note,
        ])


async def worker(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    payload: bytes,
    deadline: float,
    latencies: list[float],
) -> None:
    loop = asyncio.get_running_loop()
    while loop.time() < deadline:
        start = time.perf_counter()
        received = await c.async_roundtrip(reader, writer, payload)
        assert received == payload
        latencies.append((time.perf_counter() - start) * 1000)


async def bench(client: int) -> None:
    payload = b"P" * cfg.PAYLOAD_SIZE
    connections = await asyncio.gather(
        *(asyncio.open_connection(c.HOST, c.PORT) for _ in range(client))
    )
    latencies = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + cfg.DURATION
    start = time.perf_counter()
    await asyncio.gather(
        *(
            worker(reader, writer, payload, deadline, latencies)
            for reader, writer in connections
        )
    )
    elapsed = time.perf_counter() - start
    for _, writer in connections:
        writer.close()
    success = len(latencies)
    qps = success / elapsed
    avg_ms = statistics.mean(latencies) if latencies else 0.0
    max_ms = max(latencies) if latencies else 0.0
    p50 = _percentile(latencies, 0.50)
    p95 = _percentile(latencies, 0.95)
    p99 = _percentile(latencies, 0.99)
    p999 = _percentile(latencies, 0.999)
    print(
        f"client={client} "
        f"qps={qps:.1f} "
        f"p50={p50:.2f}ms "
        f"p95={p95:.2f}ms "
        f"p99={p99:.2f}ms "
        f"p999={p999:.2f}ms"
    )
    _append_to_csv(client, success, elapsed, qps, avg_ms, max_ms, p50, p95, p99, p999, NOTE)


async def main() -> None:
    for clients in cfg.CLIENTS:
        await bench(clients)


if __name__ == "__main__":
    asyncio.run(main())
