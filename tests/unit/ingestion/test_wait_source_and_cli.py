"""Nguồn của `wait-ingestion` (ADR-0004 mục 9 bước 1) và lệnh host. Expected viết tay."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest

from n06.cli.ingestion import run_wait_ingestion
from n06.ingestion.source_lines import dataset_event_files
from n06.producer.errors import SourceDataError

SECRETS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak",
    "N06_OBJECT_STORE_SECRET_KEY": "sk",
    "N06_POSTGRES_MART_PASSWORD": "pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
    "N06_POSTGRES_METABASE_PASSWORD": "pw",
}


def _write(root: Path, name: str, dataset_id: str, data: bytes, **override: Any) -> None:
    (root / "events").mkdir(parents=True, exist_ok=True)
    (root / "manifests").mkdir(parents=True, exist_ok=True)
    relative = f"events/{name}.jsonl"
    (root / relative).write_bytes(data)
    entry = {
        "path": relative,
        "sha256": hashlib.sha256(data).hexdigest(),
        "physical_rows": data.count(b"\n"),
        **override,
    }
    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": dataset_id,
        "source_batch_id": str(uuid.uuid4()),
        "files": [entry],
    }
    (root / "manifests" / f"{name}.manifest.json").write_text(json.dumps(manifest), "utf-8")


def test_only_files_of_the_requested_dataset_are_counted(tmp_path: Path) -> None:
    _write(tmp_path, "events_batch_00000", "ds-one", b"a\nb\nb\n")
    _write(tmp_path, "events_batch_00001", "ds-two", b"c\n")
    _write(tmp_path, "events_batch_00002", "ds-one", b"d\n")

    files, total = dataset_event_files(tmp_path, "ds-one")

    assert [f.relative_path for f in files] == [
        "events/events_batch_00000.jsonl",
        "events/events_batch_00002.jsonl",
    ]
    assert total == 4  # kể cả dòng lặp


@pytest.mark.parametrize(
    ("data", "override", "dataset_id"),
    [
        pytest.param(b"a\n", {}, "ds-missing", id="dataset-without-files"),
        pytest.param(b"a\n", {"sha256": "0" * 64}, "ds-one", id="checksum-mismatch"),
        pytest.param(b"a\n", {"physical_rows": 2}, "ds-one", id="row-count-mismatch"),
        pytest.param(b"", {}, "ds-one", id="no-lines"),
    ],
)
def test_unusable_source_is_refused(
    tmp_path: Path, data: bytes, override: dict[str, Any], dataset_id: str
) -> None:
    _write(tmp_path, "events_batch_00000", "ds-one", data, **override)
    with pytest.raises(SourceDataError):
        dataset_event_files(tmp_path, dataset_id)


class _Runner:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def __call__(self, command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        self.commands.append(command)
        return subprocess.CompletedProcess(command, 14)


def _wait(tmp_path: Path, runner: _Runner, **kwargs: Any) -> tuple[int, str]:
    out = io.StringIO()
    code = run_wait_ingestion(
        dataset_id="ds-one",
        profile="default",
        env_file=tmp_path / "none.env",
        env={**SECRETS, "N06__SPARK__EVENT_LOG_ENABLED": "false"},
        runner=runner,
        docker_bin="docker",
        out=out,
        **kwargs,
    )
    return code, out.getvalue()


def test_source_directory_is_mounted_read_only(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    runner = _Runner()

    code, _ = _wait(tmp_path, runner, source_dir=source, timeout_seconds=7)

    assert code == 14
    command = runner.commands[0]
    assert f"{source.resolve()}:/opt/n06/source-input:ro" in command
    assert command[-6:] == [
        "--dataset-id", "ds-one", "--timeout-seconds", "7",
        "--source-dir", "/opt/n06/source-input",
    ]  # fmt: skip


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout_is_refused(tmp_path: Path, timeout: float) -> None:
    runner = _Runner()
    code, _ = _wait(tmp_path, runner, source_dir=tmp_path, timeout_seconds=timeout)
    assert code == 10 and runner.commands == []


def test_missing_source_directory_is_a_configuration_error(tmp_path: Path) -> None:
    runner = _Runner()
    code, text = _wait(tmp_path, runner, source_dir=tmp_path / "khong-co")
    assert code == 10 and runner.commands == [] and "source-dir" in text
