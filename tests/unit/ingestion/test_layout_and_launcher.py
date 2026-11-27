"""Đường dẫn theo ADR-0002, ngày và offset theo ADR-0004, lệnh khởi chạy job. Expected viết tay."""

from __future__ import annotations

import io
import subprocess
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from n06.cli import main
from n06.cli.ingestion import run_stream
from n06.config import REPO_ROOT, load_config
from n06.ingestion import layout
from n06.ingestion.launcher import host_only_addresses, job_command

SECRETS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-marker-71",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-marker-72",
    "N06_POSTGRES_MART_PASSWORD": "pw-marker-73",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw-marker-74",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw-marker-75",
    "N06_POSTGRES_METABASE_PASSWORD": "pw-marker-76",
}


def _cfg(**overrides: str):  # type: ignore[no-untyped-def]
    return load_config(profile="default", env={**SECRETS, **overrides})


def test_batch_paths_follow_adr_0002() -> None:
    cfg = _cfg()
    day = date(2026, 1, 2)
    assert layout.raw_batch_uri(cfg, "raw-bronze", day, 12) == (
        "s3a://n06-lakehouse/raw/raw-bronze/ingestion_date=2026-01-02/batch_id=12/"
    )
    bronze = layout.bronze_batch_uri(cfg, "raw-bronze", day, 0)
    assert bronze == "s3a://n06-lakehouse/bronze/raw-bronze/ingestion_date=2026-01-02/batch_id=0/"
    assert layout.bronze_entity_uri(bronze, "parse_error") == bronze + "entity=parse_error/"
    assert layout.checkpoint_uri(cfg, "raw-bronze") == (
        "s3a://n06-lakehouse/checkpoints/raw-bronze/"
    )
    with pytest.raises(ValueError):
        layout.raw_batch_uri(cfg, "raw-bronze", day, -1)


@pytest.mark.parametrize(
    ("utc", "expected"),
    [
        (datetime(2026, 1, 1, 16, 59, 59, 999999, tzinfo=UTC), date(2026, 1, 1)),
        (datetime(2026, 1, 1, 17, 0, 0, tzinfo=UTC), date(2026, 1, 2)),  # 00:00 giờ Việt Nam
    ],
)
def test_ingestion_date_is_the_vietnam_date(utc: datetime, expected: date) -> None:
    assert layout.ingestion_date_of(utc, "Asia/Ho_Chi_Minh") == expected


def test_ingestion_date_refuses_naive_timestamps() -> None:
    with pytest.raises(ValueError):
        layout.ingestion_date_of(datetime(2026, 1, 1), "Asia/Ho_Chi_Minh")


def test_topic_offsets_use_exclusive_end() -> None:
    assert layout.topic_offsets([("t", 1, 5, 9), ("t", 0, 0, 0)]) == {
        "t": {"0": [0, 1], "1": [5, 10]}
    }


def test_query_name_is_validated_by_configuration() -> None:
    with pytest.raises(ValueError, match="kafka.query_name"):
        _cfg(N06__KAFKA__QUERY_NAME="../x")


def test_job_runs_in_a_throwaway_container_with_read_only_code() -> None:
    command = job_command(
        _cfg(), docker_bin="docker", repo_root=Path("/repo"),
        job_module="n06.ingestion.stream_job", job_args=["--available-now"],
        env_names=["N06_B", "N06_A"],
    )  # fmt: skip
    text = " ".join(command)
    assert command[:2] == ["docker", "compose"] and "run" in command and "--rm" in command
    for folder in ("src", "config", "contracts", "sql"):
        assert f"/repo/{folder}:/opt/n06/{folder}:ro" in command
    assert "-e N06_A -e N06_B" in text  # chỉ tên biến, không có giá trị
    assert command[command.index("spark") + 1] == "/opt/spark/bin/spark-submit"
    assert "--master local[2]" in text and "--driver-memory 2g" in text
    assert command[-2:] == [
        "/opt/n06/src/n06/ingestion/stream_job.py",
        "--available-now",
    ]


class _Runner:
    def __init__(self, code: int = 0) -> None:
        self.code = code
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def __call__(self, command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, self.code)


def _stream(
    tmp_path: Path, runner: _Runner, env: Mapping[str, str], **kwargs: Any
) -> tuple[int, str]:
    out = io.StringIO()
    code = run_stream(
        profile=kwargs.pop("profile", "default"),
        env_file=tmp_path / "none.env",
        env={**SECRETS, "N06__SPARK__EVENT_LOG_ENABLED": "false", **env},
        runner=runner,
        docker_bin=kwargs.pop("docker_bin", "docker"),
        out=out,
        **kwargs,
    )
    return code, out.getvalue()


@pytest.mark.parametrize(("job_code", "expected"), [(0, 0), (10, 10), (14, 14), (137, 14), (1, 14)])
def test_stream_maps_job_exit_codes(tmp_path: Path, job_code: int, expected: int) -> None:
    runner = _Runner(job_code)
    code, _ = _stream(tmp_path, runner, {}, available_now=True, query_name="q-one")
    assert code == expected
    command, kwargs = runner.calls[0]
    assert command[-5:] == ["--profile", "default", "--query-name", "q-one", "--available-now"]
    assert kwargs["cwd"] == REPO_ROOT
    assert kwargs["env"]["N06_OBJECT_STORE_SECRET_KEY"] == "sk-marker-72"
    assert not any("marker" in argument for argument in command)  # secret không nằm trong argv


def test_stream_refuses_host_only_addresses(tmp_path: Path) -> None:
    runner = _Runner()
    code, text = _stream(tmp_path, runner, {}, profile="test")
    assert code == 10 and runner.calls == []
    assert "kafka.bootstrap_servers" in text and "object_store.endpoint" in text


def test_stream_reports_missing_docker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("n06.cli.ingestion.shutil.which", lambda _name: None)
    runner = _Runner()
    code, _ = _stream(tmp_path, runner, {}, docker_bin=None)
    assert code == 13 and runner.calls == []


def test_stream_is_listed_in_cli_help(capsys: pytest.CaptureFixture[str]) -> None:
    out = io.StringIO()
    assert main(["stream", "--help"], out=out) == 0
    assert "--available-now" in out.getvalue() + capsys.readouterr().out


@pytest.mark.parametrize(
    ("address", "resolved", "loopback"),
    [
        ("127.0.0.9", None, True),
        ("::1", None, True),
        ("my-host-alias", "127.0.1.1", True),
        ("kafka", OSError("không phân giải được ngoài mạng Docker"), False),
        ("10.0.0.5", None, False),
    ],
)
def test_host_only_detection_uses_the_resolved_address(
    address: str, resolved: str | OSError | None, loopback: bool
) -> None:
    def resolve(name: str) -> str:
        if name != address:
            raise OSError("tên khác")
        assert resolved is not None, "địa chỉ IP không cần phân giải"
        if isinstance(resolved, OSError):
            raise resolved
        return resolved

    cfg = _cfg(N06__POSTGRES__CONTROL__HOST=address)
    found = host_only_addresses(cfg, resolve)
    assert found == (["postgres.control.host"] if loopback else [])
