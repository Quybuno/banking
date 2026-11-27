"""`n06 wait-ingestion` trên Kafka, MinIO, PostgreSQL thật (ADR-0004 mục 9).

Publish một dataset nhỏ, chạy `n06 stream --available-now`, rồi gọi lệnh qua đúng đường CLI (gắn
chỉ đọc thư mục nguồn của host vào container). Số dòng kỳ vọng đếm ở đây từ file nguồn.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from n06.cli import main
from n06.cli.ingestion import run_stream, run_wait_ingestion
from n06.cli.init_local import parse_dotenv_file
from n06.cli.object_store import ensure_bucket, target_from_config
from n06.config import load_config
from n06.generator import build_dataset
from n06.storage import make_compose_psql_runner

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIR = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1"
DATASET_ID = "n06-it-wait"


class _Capture:
    """Chạy lệnh thật và giữ output để đọc dòng `N06_WAIT_RESULT=`."""

    def __init__(self) -> None:
        self.stdout = ""
        self.stderr = ""

    def __call__(self, command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        proc = subprocess.run(command, capture_output=True, text=True, timeout=400, **kwargs)
        self.stdout, self.stderr = proc.stdout, proc.stderr
        return proc

    def result(self) -> dict[str, Any]:
        line = next(x for x in self.stdout.splitlines() if x.startswith("N06_WAIT_RESULT="))
        return json.loads(line.removeprefix("N06_WAIT_RESULT="))


@pytest.fixture(scope="module")
def ingested(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[dict[str, str], Path]]:
    docker = shutil.which("docker")
    assert docker is not None, "Test tích hợp cần docker; không bỏ qua."
    host_env = {**parse_dotenv_file(REPO_ROOT / ".env"), **os.environ}
    test_cfg = load_config(profile="test", env=host_env)
    bucket = test_cfg.object_store.bucket
    assert ensure_bucket(target_from_config(test_cfg, host_env), bucket).ready
    suffix = uuid.uuid4().hex[:10]
    topic, query = f"n06.test.wait.{suffix}", f"it-wait-{suffix}"
    env = {
        **host_env,
        "N06__KAFKA__EVENTS_TOPIC": topic,
        "N06__KAFKA__QUERY_NAME": query,
        "N06__OBJECT_STORE__BUCKET": bucket,
        "N06__SPARK__SHUFFLE_PARTITIONS": "4",
        "N06__SPARK__EVENT_LOG_ENABLED": "false",
    }
    root = tmp_path_factory.mktemp("wait_data")
    config = json.loads((CONTRACT_DIR / "config/generator.demo-faults.json").read_text("utf-8"))
    config.update(dataset_id=DATASET_ID, transaction_count=200, account_count=20)
    build_dataset(config, root, validate_outputs=False)
    source = root / "source"
    root.chmod(0o755)
    out = io.StringIO()
    code = main(
        ["publish", "--profile", "test", "--source-dir", str(source), "--topic", topic, "--json"],
        out=out,
    )
    assert code == 0, out.getvalue()
    try:
        assert run_stream(profile="demo", env=env, available_now=True) == 0
        yield env, source
    finally:
        cfg = load_config(profile="demo", env=env)
        runner = make_compose_psql_runner(
            cfg.postgres.control, docker_bin=docker,
            compose_file=REPO_ROOT / "docker-compose.yml", repo_root=REPO_ROOT,
        )  # fmt: skip
        runner(f"DELETE FROM ingest_batches WHERE query_name = '{query}';", False)
        admin = AdminClient({"bootstrap.servers": test_cfg.kafka.bootstrap_servers})
        for future in admin.delete_topics([topic]).values():
            future.result(timeout=30)
        for prefix in ("raw", "bronze", "checkpoints"):
            subprocess.run(
                [docker, "compose", "--profile", "core", "exec", "-T", "minio", "sh", "-c",
                 'rm -rf "/data/$1/$2/$3"', "sh", bucket, prefix, query],
                cwd=REPO_ROOT, check=True, capture_output=True,
            )  # fmt: skip


def _lines(source: Path) -> int:
    return sum(p.read_bytes().count(b"\n") for p in (source / "events").glob("*.jsonl"))


def _wait(env: dict[str, str], source: Path, **kwargs: Any) -> tuple[int, _Capture]:
    capture = _Capture()
    code = run_wait_ingestion(
        dataset_id=kwargs.pop("dataset_id", DATASET_ID),
        profile="demo",
        env=env,
        source_dir=source,
        timeout_seconds=kwargs.pop("timeout_seconds", 60),
        runner=capture,
        out=io.StringIO(),
        **kwargs,
    )
    return code, capture


def test_ready_when_every_source_line_is_in_committed_raw(
    ingested: tuple[dict[str, str], Path],
) -> None:
    env, source = ingested
    code, capture = _wait(env, source)
    assert code == 0, capture.stderr[-4000:]
    result = capture.result()
    assert result["ready"] is True and result["missing_lines"] == 0
    assert result["expected_lines"] == _lines(source) > 0
    assert result["committed_batches"] >= 1


def test_times_out_when_the_query_has_committed_nothing(
    ingested: tuple[dict[str, str], Path],
) -> None:
    env, source = ingested
    code, capture = _wait(env, source, query_name="it-wait-nothing", timeout_seconds=3)
    assert code == 14, capture.stderr[-4000:]
    result = capture.result()
    assert result["ready"] is False and result["committed_batches"] == 0
    assert result["missing_lines"] == _lines(source)
    assert result["parse_error_rows"] == 0


@pytest.mark.parametrize("extra", ["new-line", "one-more-copy"])
def test_times_out_when_one_line_or_one_copy_was_never_published(
    ingested: tuple[dict[str, str], Path], tmp_path: Path, extra: str
) -> None:
    """Nguồn có thêm một dòng mới, hoặc thêm một bản giống hệt dòng đã publish: thiếu đúng 1."""
    env, source = ingested
    changed = tmp_path / "source"
    shutil.copytree(source, changed)
    tmp_path.chmod(0o755)
    shard = sorted((changed / "events").glob("*.jsonl"))[0]
    data = shard.read_bytes()
    first = data.split(b"\n", 1)[0]
    added = first if extra == "one-more-copy" else first.replace(b'"', b' "', 1)
    data += added + b"\n"
    shard.write_bytes(data)
    for manifest_path in (changed / "manifests").glob("*.json"):
        manifest = json.loads(manifest_path.read_text("utf-8"))
        for item in manifest["files"]:
            if item["path"] == f"events/{shard.name}":
                item["sha256"] = hashlib.sha256(data).hexdigest()
                item["physical_rows"] = data.count(b"\n")
        manifest_path.write_text(json.dumps(manifest), "utf-8")

    code, capture = _wait(env, changed, timeout_seconds=3)

    assert code == 14, capture.stderr[-4000:]
    result = capture.result()
    assert result["missing_lines"] == 1 and result["expected_lines"] == _lines(source) + 1


def test_unknown_dataset_is_refused_without_waiting(
    ingested: tuple[dict[str, str], Path],
) -> None:
    env, source = ingested
    code, capture = _wait(env, source, dataset_id="n06-it-absent")
    assert code == 10, capture.stdout[-2000:]
    assert "N06_WAIT_RESULT" not in capture.stdout
