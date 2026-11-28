"""Unit test cho ResourceSampler với đồng hồ giả và nguồn số giả."""

from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path

from n06.observability.sampler import (
    ContainerStat,
    ResourceSampler,
    parse_docker_stats_lines,
    write_timeseries_csv,
)


def test_sampler_with_fake_clock_and_fake_sources(tmp_path: Path) -> None:
    mono_values = iter([100.0, 101.0, 102.0])
    base_dt = datetime(2026, 11, 28, 10, 0, 0, tzinfo=UTC)
    dt_values = iter([base_dt, base_dt + timedelta(seconds=1), base_dt + timedelta(seconds=2)])

    stat_samples = iter(
        [
            "cpu  100 0 100 800 0 0 0 0 0 0\n",
            "cpu  150 0 150 900 0 0 0 0 0 0\n",  # delta total=200, idle=100 -> 50%
            "cpu  225 0 225 950 0 0 0 0 0 0\n",  # delta total=200, idle=50  -> 75%
        ]
    )
    mem_samples = iter(
        [
            "MemTotal: 8388608 kB\nMemAvailable: 4194304 kB\n"
            "SwapTotal: 1048576 kB\nSwapFree: 1048576 kB\n",
            "MemTotal: 8388608 kB\nMemAvailable: 3145728 kB\n"
            "SwapTotal: 1048576 kB\nSwapFree: 1048576 kB\n",
            "MemTotal: 8388608 kB\nMemAvailable: 2097152 kB\n"
            "SwapTotal: 1048576 kB\nSwapFree: 1048576 kB\n",
        ]
    )
    rss_samples = iter([120.0, 240.0, 180.0])
    container_samples = iter(
        [
            {"n06-spark-1": ContainerStat("n06-spark-1", 10.0, 300.0, 4096.0)},
            {"n06-spark-1": ContainerStat("n06-spark-1", 80.0, 500.0, 4096.0)},
            {"n06-spark-1": ContainerStat("n06-spark-1", 30.0, 400.0, 4096.0)},
        ]
    )

    sampler = ResourceSampler(
        interval_seconds=1.0,
        clock=lambda: next(mono_values),
        utc_clock=lambda: next(dt_values),
        host_stat_reader=lambda: next(stat_samples),
        host_mem_reader=lambda: next(mem_samples),
        container_stats_reader=lambda: next(container_samples),
        process_rss_reader=lambda: next(rss_samples),
    )

    s0 = sampler.take_sample()
    s1 = sampler.take_sample()
    s2 = sampler.take_sample()

    assert s0.elapsed_seconds == 0.0
    assert s0.host_cpu_pct == 0.0
    assert s1.elapsed_seconds == 1.0
    assert s1.host_cpu_pct == 50.0
    assert s2.elapsed_seconds == 2.0
    assert s2.host_cpu_pct == 75.0

    host_sum = sampler.summarize(ram_scope="host")
    assert host_sum.sample_count == 3
    assert host_sum.cpu_peak_pct == 75.0
    assert host_sum.cpu_avg_pct == 41.67
    assert host_sum.ram_peak_mb == 6144.0
    assert host_sum.swap_peak_mb == 0.0
    assert host_sum.ram_available_start_mb == 4096.0
    assert host_sum.container_peaks["n06-spark-1"]["mem_peak_mb"] == 500.0

    proc_sum = sampler.summarize(ram_scope="process_tree")
    assert proc_sum.ram_peak_mb == 240.0
    assert proc_sum.ram_avg_mb == 180.0

    ts_path = tmp_path / "timeseries" / "run1.csv"
    write_timeseries_csv(ts_path, sampler.samples())
    rows = list(csv.reader(ts_path.open(encoding="utf-8")))
    assert len(rows) == 4
    assert "n06-spark-1_cpu_pct" in rows[0]


def test_parse_docker_stats_lines() -> None:
    raw = (
        '{"Name":"n06-kafka-1","CPUPerc":"12.34%","MemUsage":"474.5MiB / 8.726GiB"}\n'
        '{"Name":"n06-minio-1","CPUPerc":"0.50%","MemUsage":"128MiB / 2GiB"}\n'
    )
    parsed = parse_docker_stats_lines(raw)
    assert set(parsed.keys()) == {"n06-kafka-1", "n06-minio-1"}
    assert parsed["n06-kafka-1"].cpu_pct == 12.34
    assert parsed["n06-kafka-1"].mem_used_mb == 474.5
    assert parsed["n06-minio-1"].mem_limit_mb == 2048.0
