from benchmarks.config import BenchmarkConfig
from benchmarks.runner import RoundResult, _summaries, percentile


def _result(qps: float, repetition: int) -> RoundResult:
    return RoundResult(
        architecture="test",
        revision="abc123",
        recorded_at="2026-08-22T00:00:00+08:00",
        workers=1,
        heartbeat=30,
        clients=100,
        payload_size=64,
        duration=1.0,
        repetition=repetition,
        elapsed=1.0,
        requests=int(qps),
        qps=qps,
        average_ms=1.0,
        p50_ms=0.5,
        p95_ms=1.0,
        p99_ms=2.0,
        p999_ms=3.0,
        max_ms=4.0,
    )


def test_percentile_uses_nearest_rank() -> None:
    values = [4.0, 1.0, 3.0, 2.0]

    assert percentile(values, 0.50) == 2.0
    assert percentile(values, 0.95) == 4.0
    assert values == [4.0, 1.0, 3.0, 2.0]


def test_config_builds_cartesian_product() -> None:
    config = BenchmarkConfig(
        workers=(1, 4),
        heartbeats=(30, 60),
        clients=(100, 500),
        payload_sizes=(64, 1024),
        durations=(1.0, 2.0),
        repetitions=3,
    )

    assert len(config.server_configs()) == 4
    assert len(config.workloads()) == 24


def test_summary_uses_median() -> None:
    summary = _summaries([_result(100.0, 1), _result(300.0, 2)])

    assert len(summary) == 1
    assert summary[0]["qps_median"] == 200.0
    assert summary[0]["qps_min"] == 100.0
    assert summary[0]["qps_max"] == 300.0
