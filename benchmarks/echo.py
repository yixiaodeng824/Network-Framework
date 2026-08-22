import argparse
import asyncio
import os
import platform
import random
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from benchmarks.config import CONFIG, BenchmarkConfig, Workload
from benchmarks.runner import BenchmarkRun, measure, run_round, write_results
from support.server import RunningServer, ServerLauncher, ServerLogMode


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def current_revision() -> str:
    return _git("rev-parse", "HEAD")


def current_architecture() -> str:
    return _git("branch", "--show-current") or "detached"


def _warm_up(server: RunningServer, config: BenchmarkConfig) -> None:
    if not config.warmup_duration:
        return
    workload = Workload(
        min(config.clients[0], 32), config.payload_sizes[0], config.warmup_duration, 0
    )
    asyncio.run(run_round(*server.address, workload))


def run_matrix(
    binary: Path, architecture: str, revision: str, config: BenchmarkConfig
) -> BenchmarkRun:
    launcher = ServerLauncher(str(binary.resolve()))
    rounds = []
    for server_config in config.server_configs():
        print(
            f"\n[{architecture}] workers={server_config.workers} heartbeat={server_config.heartbeat}"
        )
        with launcher.running(
            threads=server_config.workers,
            heartbeat_timeout=server_config.heartbeat,
            log_mode=ServerLogMode.DISCARD,
        ) as server:
            _warm_up(server, config)
            workloads = list(config.workloads())
            random.Random(
                config.seed + server_config.workers * 1000 + server_config.heartbeat
            ).shuffle(workloads)
            for index, workload in enumerate(workloads, 1):
                result = measure(
                    architecture, revision, server_config, workload, *server.address
                )
                rounds.append(result)
                print(
                    f"  {index:>3}/{len(workloads)} clients={workload.clients:<4} payload={workload.payload_size:<5} duration={workload.duration:g}s repeat={workload.repetition} qps={result.qps:>9.1f} p99={result.p99_ms:>7.2f}ms"
                )
    metadata = {
        "architecture": architecture,
        "revision": revision,
        "binary": str(binary.resolve()),
        "config": {**asdict(config), "results_dir": str(config.results_dir)},
        "environment": {
            "hostname": platform.node(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "python": sys.version,
        },
    }
    return BenchmarkRun(metadata, tuple(rounds))


def find_binary() -> Path:
    candidates = [
        os.getenv("NF_SERVER_BINARY"),
        "build/release/server",
        "build/server",
        "build/debug/server",
    ]
    for value in candidates:
        if value and Path(value).is_file():
            return Path(value)
    raise FileNotFoundError(
        "server binary not found; run `make configure-release` or pass --binary"
    )


def output_directory(config: BenchmarkConfig, suffix: str) -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    return config.results_dir / f"{timestamp}-{suffix.replace('/', '-')}"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the configured Echo benchmark matrix"
    )
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--architecture", default=current_architecture())
    parser.add_argument("--revision", default=current_revision())
    parser.add_argument("--output", type=Path)
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args(argv)
    config = CONFIG.quick() if args.quick else CONFIG
    run = run_matrix(
        args.binary or find_binary(), args.architecture, args.revision, config
    )
    output = write_results(
        [run], args.output or output_directory(config, args.architecture)
    )
    print(f"\nresults: {output.resolve()}")


if __name__ == "__main__":
    main()
