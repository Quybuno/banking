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
