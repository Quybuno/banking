"""`PsqlCliControlStore` chạy bằng `psql` **trong image Spark** trên PostgreSQL thật (ADR-0012).

Test tạo database tạm, chạy một kịch bản Python trong container mới của service `spark` (mã gắn chỉ
đọc), rồi so giá trị đọc lại với hằng viết tay ở đây. Không đụng database control dùng chung.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from n06.cli import parse_dotenv_file

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["compose", "-f", str(REPO_ROOT / "docker-compose.yml"), "--profile", "core"]

# Giá trị khó: nháy đơn và kép, `;`, chú thích SQL, dấu `\` (cả chuỗi trông như escape), `$$`,
# thẻ dollar, cú pháp biến của psql, tab, xuống dòng, tiếng Việt.
NASTY = (
    "a'b\"c; DROP TABLE ingest_batches; -- \\ \\n \\0 \\x41 \\' $$ $q$ :'params' :j "
    "\t|\n| {}[] % đồng Việt Nam ½"
)

_SCRIPT = r"""
import json, os, subprocess
from datetime import date

from n06.config import load_config
from n06.control import connect_control_db, migrate_up
from n06.storage import CommitError, IngestBatchRecord, RunRecord, connect_control_store

nasty = json.loads(os.environ["N06_TEST_NASTY"])
cfg = load_config(profile="default", env=os.environ)
applied = migrate_up(connect_control_db(cfg.postgres.control))
store = connect_control_store(cfg.postgres.control)
out = {"store": type(store).__name__, "applied": applied,
       "psql": subprocess.run(["psql", "--version"], capture_output=True, text=True).stdout.strip()}


def batch(count):
    return IngestBatchRecord(
        query_name="q-it", batch_id=7, dataset_id=None, ingestion_date=date(2026, 1, 2),
        topic_offsets={nasty: {"0": [0, count]}}, record_count=count, raw_path=nasty,
        bronze_path=nasty + "/bronze", status="COMMITTED")


store.upsert_ingest_batch(batch(1))
store.upsert_ingest_batch(batch(2))  # cùng khóa: cập nhật, không thêm dòng
out["batches"] = [
    {**vars(b), "ingestion_date": b.ingestion_date.isoformat()}
    for b in store.list_committed_ingest_batches(query_name="q-it")
]
out["other_query"] = len(store.list_committed_ingest_batches(query_name=nasty))

run = RunRecord(
    run_id="run-it-1", dataset_id="ds-it", job_name=nasty, profile="test",
    business_date_from=None, business_date_to=date(2026, 1, 3), status="RUNNING", row_count=0,
    input_versions=[nasty, None, 3], config_hash=nasty, schema_version="1", rule_version="1",
    code_version="dev")
loaded = store.start_run(run)
out["run"] = {k: (v.isoformat() if isinstance(v, date) else v) for k, v in vars(loaded).items()}

# Lỗi SQL giữa transaction: `output_path` NULL vi phạm NOT NULL ở lệnh INSERT, SAU lệnh UPDATE runs.
try:
    store.commit_run_atomic(
        run_id="run-it-1", dataset_id="ds-it", layer="silver", entity="transactions",
        business_date=date(2026, 1, 3), output_path=None, manifest_path=None, row_count=5,
        semantic_hash="h1", target_status="SUCCEEDED")
    out["error"] = None
except CommitError as exc:
    out["error"] = str(exc)
after = store.get_run("run-it-1")
out["after_failure"] = [after.status, after.row_count, after.semantic_hash]

store.commit_run_atomic(
    run_id="run-it-1", dataset_id="ds-it", layer="silver", entity="transactions",
    business_date=date(2026, 1, 3), output_path=nasty, manifest_path=None, row_count=5,
    semantic_hash=nasty, target_status="SUCCEEDED")
snap = store.resolve_current_snapshot(
    dataset_id="ds-it", layer="silver", entity="transactions", business_date=date(2026, 1, 3))
out["snapshot"] = [snap.output_path, snap.manifest_path, snap.semantic_hash]

store.start_run(RunRecord(**{**vars(run), "run_id": "run-it-2"}))
store.fail_run(run_id="run-it-2", error_summary=nasty)
failed = store.get_run("run-it-2")
out["failed"] = [failed.status, failed.error_summary]
print("N06_RESULT=" + json.dumps(out))
"""


def _env() -> dict[str, str]:
    env_file = REPO_ROOT / ".env"
    dotenv = parse_dotenv_file(env_file) if env_file.is_file() else {}
    return {**dotenv, **os.environ}


@pytest.fixture
def database() -> Iterator[tuple[str, str]]:
    docker = shutil.which("docker")
    assert docker is not None, "Test tích hợp cần docker; không bỏ qua."
    name = f"n06_it_store_{uuid.uuid4().hex[:12]}"
    owner = _env().get("N06_POSTGRES_CONTROL_USER", "n06_control_user")

    def admin(sql: str) -> None:
        subprocess.run(
            [docker, *COMPOSE, "exec", "-T", "postgres", "sh", "-c",
             'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q -f -'],
            cwd=REPO_ROOT, input=sql, text=True, capture_output=True, check=True,
        )  # fmt: skip

    admin(f'CREATE DATABASE {name} OWNER "{owner}";')
    try:
        yield docker, name
    finally:
        admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE);")


def test_psql_control_store_in_spark_image(database: tuple[str, str]) -> None:
    docker, name = database
    env = {
        **_env(),
        "N06__POSTGRES__CONTROL__DATABASE": name,
        "N06_TEST_NASTY": json.dumps(NASTY),
    }
    command = [docker, *COMPOSE, "run", "--rm", "--no-deps", "-T"]
    for folder in ("src", "config", "contracts", "sql"):
        command += ["-v", f"{REPO_ROOT / folder}:/opt/n06/{folder}:ro"]
    command += ["-e", "PYTHONPATH=/opt/n06/src", "-e", "PYTHONDONTWRITEBYTECODE=1"]
    for key in sorted(env):
        if key.startswith("N06"):
            command += ["-e", key]
    command += ["spark", "python3", "-"]
    proc = subprocess.run(
        command, cwd=REPO_ROOT, env=env, input=_SCRIPT, text=True, capture_output=True, check=False
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    line = next(x for x in proc.stdout.splitlines() if x.startswith("N06_RESULT="))
    out = json.loads(line.removeprefix("N06_RESULT="))

    assert out["store"] == "PsqlCliControlStore" and "16.6" in out["psql"]
    assert out["applied"] == ["0001", "0002"]
    assert out["batches"] == [
        {
            "query_name": "q-it",
            "batch_id": 7,
            "dataset_id": None,
            "ingestion_date": "2026-01-02",
            "topic_offsets": {NASTY: {"0": [0, 2]}},
            "record_count": 2,
            "raw_path": NASTY,
            "bronze_path": NASTY + "/bronze",
            "status": "COMMITTED",
        }
    ]
    assert out["other_query"] == 0
    assert out["run"]["job_name"] == out["run"]["config_hash"] == NASTY
    assert out["run"]["input_versions"] == [NASTY, None, 3]
    assert out["run"]["business_date_from"] is None
    assert out["run"]["business_date_to"] == "2026-01-03"
    # Lỗi xảy ra ở INSERT và cả UPDATE trước đó bị rollback thật.
    assert out["error"] is not None and "output_path" in out["error"]
    assert out["after_failure"] == ["RUNNING", 0, None]
    assert out["snapshot"] == [NASTY, None, NASTY]
    assert out["failed"] == ["FAILED", NASTY]
