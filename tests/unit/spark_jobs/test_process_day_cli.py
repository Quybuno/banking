"""`n06 process-day` ở phía host: tham số, đồng hồ chỉ đọc một lần, lệnh khởi chạy job."""

from __future__ import annotations

import io
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from n06.cli import main
from n06.cli.process_day import run_process_day

SECRETS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-marker-71",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-marker-72",
    "N06_POSTGRES_MART_PASSWORD": "pw-marker-73",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw-marker-74",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw-marker-75",
    "N06_POSTGRES_METABASE_PASSWORD": "pw-marker-76",
    "N06__SPARK__EVENT_LOG_ENABLED": "false",
}


class _Runner:
    def __init__(self, code: int = 0) -> None:
        self.code = code
        self.calls: list[list[str]] = []

    def __call__(self, command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(command)
        return subprocess.CompletedProcess(command, self.code)


def _run(tmp_path: Path, runner: _Runner, **kwargs: Any) -> tuple[int, str]:
    out = io.StringIO()
    code = run_process_day(
        dataset_id="n06-demo-clean",
        profile="default",
        env_file=tmp_path / "none.env",
        env=SECRETS,
        runner=runner,
        docker_bin="docker",
        out=out,
        **kwargs,
    )
    return code, out.getvalue()


def test_job_arguments_pass_through_and_script_is_the_process_day_job(tmp_path: Path) -> None:
    runner = _Runner()
    code, _ = _run(
        tmp_path,
        runner,
        as_of_time="2026-01-02T04:00:00Z",
        business_dates=["2026-01-01", "2026-01-02"],
        query_name="raw-bronze",
    )
    assert code == 0
    command = runner.calls[0]
    script = command.index("/opt/n06/src/n06/spark_jobs/process_day_job.py")
    assert command[script + 1 :] == [
        "--profile", "default", "--dataset-id", "n06-demo-clean",
        "--as-of-time", "2026-01-02T04:00:00Z", "--query-name", "raw-bronze",
        "--business-date", "2026-01-01", "--business-date", "2026-01-02",
    ]  # fmt: skip
    assert "-e N06_OBJECT_STORE_SECRET_KEY" in " ".join(command)
    assert "sk-marker-72" not in " ".join(command)


def test_clock_is_read_once_and_passed_explicitly(tmp_path: Path) -> None:
    runner = _Runner()
    reads: list[int] = []

    def clock() -> datetime:
        reads.append(1)
        return datetime(2026, 5, 6, 7, 8, 9, 123456, tzinfo=UTC)

    code, _ = _run(tmp_path, runner, clock=clock)
    assert code == 0 and len(reads) == 1
    assert runner.calls[0][runner.calls[0].index("--as-of-time") + 1] == "2026-05-06T07:08:09Z"


def test_explicit_as_of_time_never_reads_the_clock(tmp_path: Path) -> None:
    def clock() -> datetime:
        raise AssertionError("không được đọc đồng hồ")

    code, _ = _run(tmp_path, _Runner(), as_of_time="2026-01-01T00:00:00Z", clock=clock)
    assert code == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"as_of_time": "2026-01-01 00:00:00"},
        {"as_of_time": "2026-01-01T00:00:00+07:00"},
        {"as_of_time": "2026-01-01T00:00:00Z", "business_dates": ["01/01/2026"]},
    ],
)
def test_invalid_time_or_date_is_a_config_error_and_starts_no_job(
    tmp_path: Path, kwargs: dict[str, Any]
) -> None:
    runner = _Runner()
    code, text = _run(tmp_path, runner, **kwargs)
    assert code == 10 and not runner.calls and "--as-of-time" in text


def test_job_exit_code_is_passed_through_and_unknown_codes_become_service_error(
    tmp_path: Path,
) -> None:
    for code, expected in ((14, 14), (10, 10), (137, 14)):
        got, _ = _run(tmp_path, _Runner(code), as_of_time="2026-01-01T00:00:00Z")
        assert got == expected


def test_main_dispatches_the_process_day_command(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake(**kwargs: Any) -> int:
        seen.update(kwargs)
        return 0

    monkeypatch.setattr("n06.cli.run_process_day", fake)
    out = io.StringIO()
    code = main(
        [
            "process-day", "--dataset-id", "n06-demo-clean", "--as-of-time",
            "2026-01-01T00:00:00Z", "--business-date", "2026-01-01", "--profile", "demo",
        ],
        out=out,
    )  # fmt: skip
    assert code == 0
    assert seen["dataset_id"] == "n06-demo-clean" and seen["profile"] == "demo"
    assert seen["business_dates"] == ["2026-01-01"] and seen["run_id"] is None


def test_help_lists_the_command() -> None:
    out = io.StringIO()
    assert main(["--help"], out=out) == 0
    assert "process-day" in out.getvalue()
