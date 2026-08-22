import asyncio
import csv
import json
import math
import statistics
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from benchmarks.config import ServerConfig, Workload
from support.protocol import async_roundtrip


@dataclass(frozen=True)
class RoundResult:
    architecture: str
    revision: str
    recorded_at: str
    workers: int
    heartbeat: int
    clients: int
    payload_size: int
    duration: float
    repetition: int
    elapsed: float
    requests: int
    qps: float
    average_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    p999_ms: float
    max_ms: float


@dataclass(frozen=True)
class BenchmarkRun:
    metadata: dict[str, object]
    rounds: tuple[RoundResult, ...]


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * fraction) - 1]


async def _client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    payload: bytes,
    deadline: float,
) -> list[float]:
    latencies = []
    loop = asyncio.get_running_loop()
    while loop.time() < deadline:
        started = time.perf_counter()
        if await async_roundtrip(reader, writer, payload) != payload:
            raise ValueError("server returned a different payload")
        latencies.append((time.perf_counter() - started) * 1000)
    return latencies


async def run_round(
    host: str, port: int, workload: Workload
) -> tuple[float, list[float]]:
    connections = await asyncio.gather(
        *(asyncio.open_connection(host, port) for _ in range(workload.clients))
    )
    payload = b"P" * workload.payload_size
    deadline = asyncio.get_running_loop().time() + workload.duration
    started = time.perf_counter()
    try:
        samples = await asyncio.gather(
            *(
                _client(reader, writer, payload, deadline)
                for reader, writer in connections
            )
        )
        elapsed = time.perf_counter() - started
    finally:
        for _, writer in connections:
            writer.close()
        await asyncio.gather(*(writer.wait_closed() for _, writer in connections))
    return elapsed, [sample for client_samples in samples for sample in client_samples]


def measure(
    architecture: str,
    revision: str,
    server: ServerConfig,
    workload: Workload,
    host: str,
    port: int,
) -> RoundResult:
    elapsed, latencies = asyncio.run(run_round(host, port, workload))
    requests = len(latencies)
    return RoundResult(
        architecture=architecture,
        revision=revision,
        recorded_at=datetime.now().astimezone().isoformat(timespec="seconds"),
        workers=server.workers,
        heartbeat=server.heartbeat,
        clients=workload.clients,
        payload_size=workload.payload_size,
        duration=workload.duration,
        repetition=workload.repetition,
        elapsed=elapsed,
        requests=requests,
        qps=requests / elapsed,
        average_ms=statistics.fmean(latencies),
        p50_ms=percentile(latencies, 0.50),
        p95_ms=percentile(latencies, 0.95),
        p99_ms=percentile(latencies, 0.99),
        p999_ms=percentile(latencies, 0.999),
        max_ms=max(latencies),
    )


def _summaries(rounds: list[RoundResult]) -> list[dict[str, object]]:
    groups = defaultdict(list)
    for result in rounds:
        key = (
            result.architecture,
            result.revision,
            result.workers,
            result.heartbeat,
            result.clients,
            result.payload_size,
            result.duration,
        )
        groups[key].append(result)
    rows = []
    for key, values in sorted(groups.items()):
        qps = [value.qps for value in values]
        rows.append(
            {
                "architecture": key[0],
                "revision": key[1],
                "workers": key[2],
                "heartbeat": key[3],
                "clients": key[4],
                "payload_size": key[5],
                "duration": key[6],
                "runs": len(values),
                "qps_median": statistics.median(qps),
                "qps_min": min(qps),
                "qps_max": max(qps),
                "p50_ms_median": statistics.median(v.p50_ms for v in values),
                "p95_ms_median": statistics.median(v.p95_ms for v in values),
                "p99_ms_median": statistics.median(v.p99_ms for v in values),
                "p999_ms_median": statistics.median(v.p999_ms for v in values),
                "max_ms": max(v.max_ms for v in values),
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_results(runs: list[BenchmarkRun], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    rounds = [result for run in runs for result in run.rounds]
    _write_csv(output_dir / "raw.csv", [asdict(result) for result in rounds])
    _write_csv(output_dir / "summary.csv", _summaries(rounds))
    (output_dir / "metadata.json").write_text(
        json.dumps([run.metadata for run in runs], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return output_dir
