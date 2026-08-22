from dataclasses import dataclass, replace
from itertools import product
from pathlib import Path

ARCHITECTURES = {
    "worker-io": "architecture/worker-io-fixed",
    "single-owner": "architecture/single-owner-120k",
}


@dataclass(frozen=True)
class ServerConfig:
    workers: int
    heartbeat: int


@dataclass(frozen=True)
class Workload:
    clients: int
    payload_size: int
    duration: float
    repetition: int


@dataclass(frozen=True)
class BenchmarkConfig:
    workers: tuple[int, ...] = (1, 4)
    heartbeats: tuple[int, ...] = (30, 60)
    clients: tuple[int, ...] = (100, 500, 1000)
    payload_sizes: tuple[int, ...] = (64, 1024)
    durations: tuple[float, ...] = (10.0,)
    repetitions: int = 5
    warmup_duration: float = 2.0
    seed: int = 20260822
    results_dir: Path = Path("benchmark-results")

    def quick(self) -> "BenchmarkConfig":
        return replace(
            self,
            workers=self.workers[:1],
            heartbeats=self.heartbeats[:1],
            clients=(min(self.clients[0], 32),),
            payload_sizes=self.payload_sizes[:1],
            durations=(0.5,),
            repetitions=1,
            warmup_duration=0.2,
        )

    def server_configs(self) -> tuple[ServerConfig, ...]:
        return tuple(
            ServerConfig(*values) for values in product(self.workers, self.heartbeats)
        )

    def workloads(self) -> tuple[Workload, ...]:
        return tuple(
            Workload(*values)
            for values in product(
                self.clients,
                self.payload_sizes,
                self.durations,
                range(1, self.repetitions + 1),
            )
        )


CONFIG = BenchmarkConfig()
