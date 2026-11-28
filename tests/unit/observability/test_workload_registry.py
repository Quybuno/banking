"""Unit test cho WorkloadRegistry và mã thoát WORKLOAD_UNAVAILABLE."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from n06.cli import main
from n06.cli.benchmark import run_benchmark_cli
from n06.cli.doctor import ExitCode
from n06.observability.workloads import (
    WorkloadNotRegisteredError,
    default_workload_registry,
)


def test_default_registry_has_e1_and_e2_only() -> None:
    reg = default_workload_registry()
    assert reg.available_experiments() == ["E1", "E2"]
    assert reg.get("e1").experiment == "E1"
    for missing in ("E3", "E4", "E5", "E6", "E7", "E8"):
        with pytest.raises(WorkloadNotRegisteredError, match="Chưa có workload"):
            reg.get(missing)


@pytest.mark.parametrize("experiment", ["E3", "E4", "E5", "E6", "E7", "E8"])
def test_cli_reports_missing_workload_with_named_exit_code(experiment: str, tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "N06_OBJECT_STORE_ACCESS_KEY=ak\n"
        "N06_OBJECT_STORE_SECRET_KEY=sk\n"
        "N06_POSTGRES_MART_PASSWORD=pw\n"
        "N06_POSTGRES_CONTROL_PASSWORD=pw\n"
        "N06_POSTGRES_AIRFLOW_PASSWORD=pw\n"
        "N06_POSTGRES_METABASE_PASSWORD=pw\n",
        encoding="utf-8",
    )
    buf = io.StringIO()
    code = run_benchmark_cli(
        experiment=experiment,
        levels=[100],
        repetitions=1,
        warmup=0,
        profile="test",
        env_file=env_file,
        as_json=True,
        out=buf,
    )
    assert code == int(ExitCode.WORKLOAD_UNAVAILABLE)
    payload = json.loads(buf.getvalue())
    assert payload["error_code"] == "WORKLOAD_UNAVAILABLE"
    assert "Chưa có workload" in payload["error"]


def test_cli_help_includes_benchmark() -> None:
    buf = io.StringIO()
    code = main(["benchmark", "--help"], out=buf)
    assert code == 0
    assert "--experiment" in buf.getvalue()


def test_makeup_runs_triggered_when_measured_run_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from n06.observability.runner import run_benchmark
    from n06.observability.workloads import (
        DatasetPrepResult,
        WorkloadRegistry,
        WorkloadResult,
        WorkloadSpec,
    )

    calls = 0

    def fake_runner(cfg, ds, run_id, level, params):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        if calls == 1:
            return WorkloadResult(
                exit_code=1,
                status="error",
                phase_seconds={"ingest": 0.1},
                records_processed=0,
                throughput_entity="transaction",
                throughput_scope="ingest",
                correctness_ok=False,
            )
        return WorkloadResult(
            exit_code=0,
            status="ok",
            phase_seconds={"ingest": 0.2},
            records_processed=100,
            throughput_entity="transaction",
            throughput_scope="ingest",
            correctness_ok=True,
        )

    reg = WorkloadRegistry()
    reg.register(WorkloadSpec("E1", "transaction_count", None, "host", "fake", fake_runner))

    monkeypatch.setattr(
        "n06.observability.runner.prepare_benchmark_dataset",
        lambda *a, **kw: DatasetPrepResult(
            0, tmp_path / "ds", tmp_path / "ds/source", 0.5, 10.0, "", "", 100, 42
        ),
    )
    monkeypatch.setattr(
        "n06.observability.runner.evaluate_run_counting",
        lambda **kw: (kw["status"] == "ok" and kw["correctness_ok"], None),
    )

    summary = run_benchmark(
        experiment="E1",
        levels=[100],
        repetitions=2,
        warmup=0,
        profile="test",
        output_dir=tmp_path / "out",
        allow_dirty=True,
        registry=reg,
        env={
            "N06_OBJECT_STORE_ACCESS_KEY": "ak",
            "N06_OBJECT_STORE_SECRET_KEY": "sk",
            "N06_POSTGRES_MART_PASSWORD": "pw",
            "N06_POSTGRES_CONTROL_PASSWORD": "pw",
            "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
            "N06_POSTGRES_METABASE_PASSWORD": "pw",
        },
    )
    # 2 lần chính (1 lỗi, 1 ok) + 1 lần chạy bù (ok) = 3 lần chạy, 2 lần đếm được
    assert summary.total_runs == 3
    assert summary.counted_runs == 2
