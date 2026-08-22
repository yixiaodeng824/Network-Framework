import argparse
import os
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from benchmarks.config import ARCHITECTURES, CONFIG
from benchmarks.echo import output_directory, run_matrix
from benchmarks.runner import write_results


@dataclass(frozen=True)
class Architecture:
    label: str
    ref: str
    revision: str


def run(
    command: list[str], cwd: Path, capture: bool = False
) -> subprocess.CompletedProcess[str]:
    print(f"$ {' '.join(command)}")
    return subprocess.run(
        command, cwd=cwd, check=True, text=True, capture_output=capture
    )


def repository_root() -> Path:
    return Path(
        run(["git", "rev-parse", "--show-toplevel"], Path.cwd(), True).stdout.strip()
    )


def resolve(label: str, ref: str, repository: Path) -> Architecture:
    revision = run(
        ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], repository, True
    ).stdout.strip()
    return Architecture(label, ref, revision)


@contextmanager
def worktree(
    repository: Path, root: Path, architecture: Architecture
) -> Iterator[Path]:
    path = root / architecture.label.replace("/", "-")
    run(
        ["git", "worktree", "add", "--detach", str(path), architecture.revision],
        repository,
    )
    try:
        yield path
    finally:
        run(["git", "worktree", "remove", "--force", str(path)], repository)


def build(source: Path) -> Path:
    build_dir = source / "build" / "benchmark"
    binary = build_dir / "server"
    if (source / "CMakeLists.txt").is_file():
        run(
            [
                "cmake",
                "-S",
                str(source),
                "-B",
                str(build_dir),
                "-DCMAKE_BUILD_TYPE=Release",
            ],
            source,
        )
        run(
            ["cmake", "--build", str(build_dir), "--target", "server", "--parallel"],
            source,
        )
        return binary
    build_dir.mkdir(parents=True)
    sources = [
        "src/net/main.cpp",
        "src/net/epoll_server.cpp",
        "src/net/connection.cpp",
        "src/log/Logger.cpp",
        "src/thread/MessageQueue.cpp",
        "src/thread/ThreadPool.cpp",
        "src/thread/work_thread.cpp",
    ]
    run(
        [
            os.getenv("CXX", "c++"),
            "-O3",
            "-DNDEBUG",
            "-std=c++17",
            "-pthread",
            "-Isrc",
            "-Isrc/net",
            "-Isrc/log",
            "-Isrc/thread",
            *sources,
            "-o",
            str(binary),
        ],
        source,
    )
    return binary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark architecture branches in detached worktrees"
    )
    parser.add_argument(
        "--architecture",
        action="append",
        help="label=git-ref; repeat to override config.py",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args(argv)
    configured = (
        dict(item.split("=", 1) for item in args.architecture)
        if args.architecture
        else ARCHITECTURES
    )
    config = CONFIG.quick() if args.quick else CONFIG
    repository = repository_root()
    architectures = [
        resolve(label, ref, repository) for label, ref in configured.items()
    ]
    runs = []
    with tempfile.TemporaryDirectory(prefix="network-framework-bench-") as directory:
        for architecture in architectures:
            print(
                f"\n=== {architecture.label}: {architecture.ref} ({architecture.revision[:12]}) ==="
            )
            with worktree(repository, Path(directory), architecture) as source:
                benchmark = run_matrix(
                    build(source), architecture.label, architecture.revision, config
                )
                benchmark.metadata["ref"] = architecture.ref
                runs.append(benchmark)
    output = write_results(runs, args.output or output_directory(config, "comparison"))
    print(f"\nresults: {output.resolve()}")


if __name__ == "__main__":
    main()
