import asyncio
import math
import os
import statistics
import time

import benchmarks.config as cfg
import tests.common as c


def _percentile(values: list[float], p: float) -> float:
    values.sort()
    return values[int(len(values) - 1 * p)]


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
    print(
        f"client={client} "
        f"qps={(len(latencies) / elapsed):.1f} "
        f"p50={_percentile(latencies, 0.50):.2f}ms "
        f"p95={_percentile(latencies, 0.95):.2f}ms "
        f"p99={_percentile(latencies, 0.99):.2f}ms"
    )


async def main() -> None:
    for clients in cfg.CLIENTS:
        await bench(clients)


if __name__ == "__main__":
    asyncio.run(main())
