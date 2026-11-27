"""`n06 stream` trên Kafka, MinIO, PostgreSQL thật: tắt tiến trình giữa batch rồi chạy lại.

Kịch bản (đặc tả I.8.1 bước 5): (1) tắt sau khi ghi Raw và Bronze nhưng trước upsert; (2) chạy lại,
tắt ngay sau upsert, trước khi checkpoint ghi nhận; (3) chạy lại tới hết. Yêu cầu: không dòng
`ingest_batches` trùng, batch phát lại ghi đè đúng đường dẫn với đúng khoảng offset, và Raw của các
batch đã commit chứa đúng mỗi message một lần.

Expected tính ở đây từ byte của file nguồn và các message hỏng do test gửi; việc đọc Parquet dùng
Spark trong container.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import subprocess
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

from n06.cli import main
from n06.cli.init_local import parse_dotenv_file
from n06.cli.object_store import ensure_bucket, target_from_config
from n06.config import AppConfig, load_config
from n06.generator import build_dataset
from n06.ingestion.launcher import job_command
from n06.storage import PsqlCliControlStore, make_compose_psql_runner

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIR = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
PARTITIONS = 3
PER_BATCH = 700

_INSPECT = r"""
import json, os

from n06.config import load_config
from n06.ingestion.spark_session import build_spark, hadoop_path
from pyspark.sql import functions as F

request = json.loads(os.environ["N06_IT_REQUEST"])
spark = build_spark(load_config(profile="demo", env=os.environ), "it-inspect")
try:
    if request["action"] == "delete":
        for uri in request["uris"]:
            filesystem, path = hadoop_path(spark, uri)
            filesystem.delete(path, True)
        print("N06_IT_RESULT=" + json.dumps({"deleted": len(request["uris"])}))
    else:
        result = []
        for batch in request["batches"]:
            raw = spark.read.parquet(batch["raw_path"])
            raw_rows = raw.select(
                "topic", "partition", "offset", F.sha2("value", 256).alias("sha"),
                "ingested_at", "dataset_id", "ingestion_run_id").collect()
            filesystem, path = hadoop_path(spark, batch["bronze_path"])
            folders = sorted(s.getPath().getName() for s in filesystem.listStatus(path)
                             if s.isDirectory())
            bronze = {}
            for folder in folders:
                frame = spark.read.parquet(batch["bronze_path"] + folder + "/")
                extra = ["error_code"] if folder == "entity=parse_error" else []
                bronze[folder] = {
                    "columns": sorted(frame.columns),
                    "rows": [r.asDict() for r in frame.select(
                        "partition", "offset", "ingested_at", *extra).collect()],
                }
            result.append({
                "batch_id": batch["batch_id"],
                "raw_columns": sorted(raw.columns),
                "raw": [[r["partition"], r["offset"], r["sha"], str(r["ingested_at"]),
                         r["dataset_id"], r["ingestion_run_id"]] for r in raw_rows],
                "bronze": {k: {"columns": v["columns"],
                               "rows": [[x["partition"], x["offset"], str(x["ingested_at"]),
                                         x.get("error_code")] for x in v["rows"]]}
                           for k, v in bronze.items()},
            })
        print("N06_IT_RESULT=" + json.dumps(result))
finally:
    spark.stop()
"""


@dataclass
class Stack:
    docker: str
    env: dict[str, str]
    cfg: AppConfig
    topic: str
    query: str
    store: PsqlCliControlStore
    sql: Any

    def job(self, module: str, args: list[str]) -> subprocess.CompletedProcess[str]:
        command = job_command(
            self.cfg,
            docker_bin=self.docker,
            repo_root=REPO_ROOT,
            job_module=module,
            job_args=["--profile", "demo", *args],
            env_names=[k for k in self.env if k.startswith("N06")],
        )
        return subprocess.run(
            command, cwd=REPO_ROOT, env=self.env, capture_output=True, text=True,
            timeout=400, check=False,
        )  # fmt: skip

    def stream(self, *args: str) -> subprocess.CompletedProcess[str]:
        return self.job("n06.ingestion.stream_job", ["--available-now", *args])

    def spark(self, request: dict[str, Any], tmp: Path) -> Any:
        # Tên không được trùng module chuẩn: thư mục của kịch bản đứng đầu `sys.path`.
        script = tmp / "n06_it_read_batches.py"
        script.write_text(_INSPECT, encoding="utf-8")
        tmp.chmod(0o755)
        command = [
            self.docker, "compose", "-f", str(COMPOSE_FILE), "--profile", "core",
            "run", "--rm", "--no-deps", "-T",
        ]  # fmt: skip
        for folder in ("src", "config", "contracts", "sql"):
            command += ["-v", f"{REPO_ROOT / folder}:/opt/n06/{folder}:ro"]
        command += ["-v", f"{tmp}:/opt/n06/it:ro", "-e", "PYTHONPATH=/opt/n06/src"]
        env = {**self.env, "N06_IT_REQUEST": json.dumps(request)}
        for key in sorted(env):
            if key.startswith("N06"):
                command += ["-e", key]
        command += ["spark", "/opt/spark/bin/spark-submit", "--master", "local[2]"]
        command += ["--conf", "spark.ui.enabled=false", f"/opt/n06/it/{script.name}"]
        proc = subprocess.run(
            command, cwd=REPO_ROOT, env=env, capture_output=True, text=True,
            timeout=300, check=False,
        )  # fmt: skip
        # Bỏ dòng log và stack Java để thấy lỗi thật của kịch bản khi nó thất bại.
        problems = [
            x for x in (proc.stdout + proc.stderr).splitlines()
            if " INFO " not in x and " WARN " not in x and not x.startswith("\tat ")
        ]  # fmt: skip
        assert proc.returncode == 0, "\n".join(problems[:80])
        line = next(x for x in proc.stdout.splitlines() if x.startswith("N06_IT_RESULT="))
        return json.loads(line.removeprefix("N06_IT_RESULT="))


def _event_lines(source: Path) -> list[bytes]:
    lines: list[bytes] = []
    for path in sorted((source / "events").glob("*.jsonl")):
        lines += [x for x in path.read_bytes().split(b"\n") if x]
    return lines


@pytest.fixture(scope="module")
def stack() -> Iterator[Stack]:
    docker = shutil.which("docker")
    assert docker is not None, "Test tích hợp cần docker; không bỏ qua."
    dotenv = parse_dotenv_file(REPO_ROOT / ".env")
    suffix = uuid.uuid4().hex[:10]
    topic, query = f"n06.test.stream.{suffix}", f"it-stream-{suffix}"
    host_env = {**dotenv, **os.environ}
    test_cfg = load_config(profile="test", env=host_env)
    bucket = test_cfg.object_store.bucket
    assert ensure_bucket(target_from_config(test_cfg, host_env), bucket).ready
    env = {
        **host_env,
        "N06__KAFKA__EVENTS_TOPIC": topic,
        "N06__KAFKA__QUERY_NAME": query,
        "N06__KAFKA__MAX_OFFSETS_PER_TRIGGER": str(PER_BATCH),
        "N06__OBJECT_STORE__BUCKET": bucket,
        "N06__SPARK__SHUFFLE_PARTITIONS": "4",
        "N06__SPARK__EVENT_LOG_ENABLED": "false",
    }
    cfg = load_config(profile="demo", env=env)
    runner = make_compose_psql_runner(
        cfg.postgres.control, docker_bin=docker, compose_file=COMPOSE_FILE, repo_root=REPO_ROOT
    )
    store = PsqlCliControlStore(cfg.postgres.control, psql_runner=runner)
    value = Stack(docker, env, cfg, topic, query, store, runner)
    try:
        yield value
    finally:
        # Chỉ dọn tài nguyên của chính test này (tên có hậu tố ngẫu nhiên).
        runner(f"DELETE FROM ingest_batches WHERE query_name = '{query}';", False)
        admin = AdminClient({"bootstrap.servers": test_cfg.kafka.bootstrap_servers})
        for future in admin.delete_topics([topic]).values():
            future.result(timeout=30)


def test_kill_during_a_batch_then_restart(
    stack: Stack, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset = json.loads((CONTRACT_DIR / "config/generator.demo-faults.json").read_text("utf-8"))
    dataset.update(dataset_id="n06-it-stream", transaction_count=500, account_count=20)
    build_dataset(dataset, tmp_path / "data", validate_outputs=False)
    source = tmp_path / "data" / "source"
    lines = _event_lines(source)

    monkeypatch.setenv("N06__KAFKA__NUM_PARTITIONS", str(PARTITIONS))
    out = io.StringIO()
    publish = ["publish", "--profile", "test", "--source-dir", str(source), "--topic", stack.topic]
    assert main([*publish, "--json"], out=out) == 0, out.getvalue()
    # Message hỏng gửi thẳng vào topic: Raw phải giữ, Bronze phải đưa vào parse_error.
    bad: list[bytes | None] = [b"not json", None, b"\xff\xfe", b'{"schema_version":"9.9.9"}']
    producer = Producer(
        {"bootstrap.servers": load_config(profile="test", env=stack.env).kafka.bootstrap_servers}
    )
    for value in bad:
        producer.produce(stack.topic, value=value, key=b"bad")
    assert producer.flush(30) == 0
    total = len(lines) + len(bad)
    assert total > 2 * PER_BATCH  # cần ít nhất ba batch để có batch ở giữa

    def rows() -> dict[int, Any]:
        batches = stack.store.list_committed_ingest_batches(query_name=stack.query)
        assert len({b.batch_id for b in batches}) == len(batches)
        return {b.batch_id: b for b in batches}

    # (1) Tắt sau khi ghi output của batch 1, trước upsert.
    first = stack.stream("--crash-at", "before_upsert", "--crash-batch", "1")
    assert first.returncode != 0, first.stdout[-3000:]
    assert "tắt tiến trình tại before_upsert, batch_id=1" in first.stdout, first.stderr[-3000:]
    assert list(rows()) == [0]

    # (2) Chạy lại: batch 1 phát lại; tắt ngay sau upsert, trước khi checkpoint ghi nhận.
    second = stack.stream("--crash-at", "after_upsert", "--crash-batch", "1")
    assert second.returncode != 0, second.stdout[-3000:]
    assert "tắt tiến trình tại after_upsert, batch_id=1" in second.stdout, second.stderr[-3000:]
    after_second = rows()
    assert list(after_second) == [0, 1]

    # (3) Chạy lại tới hết: batch 1 phát lại lần nữa, upsert không thêm dòng.
    third = stack.stream()
    assert third.returncode == 0, third.stderr[-4000:]
    final = rows()
    assert list(final) == list(range(len(final))) and len(final) >= 3
    for batch_id, batch in after_second.items():
        again = final[batch_id]
        assert (again.topic_offsets, again.raw_path, again.bronze_path, again.record_count) == (
            batch.topic_offsets, batch.raw_path, batch.bronze_path, batch.record_count,
        )  # fmt: skip

    # Khoảng offset nối tiếp nhau từ 0, tổng đúng bằng số message.
    next_offset = {str(p): 0 for p in range(PARTITIONS)}
    for batch_id, batch in final.items():
        assert batch.dataset_id is None and batch.status == "COMMITTED"
        spans = batch.topic_offsets[stack.topic]
        assert sum(end - start for start, end in spans.values()) == batch.record_count
        for partition, (start, end) in spans.items():
            assert start == next_offset[partition]
            next_offset[partition] = end
        expected_tail = f"/{stack.query}/ingestion_date={batch.ingestion_date}/batch_id={batch_id}/"
        assert batch.raw_path.endswith(expected_tail) and batch.bronze_path.endswith(expected_tail)
    assert sum(next_offset.values()) == total

    request = {
        "action": "inspect",
        "batches": [
            {"batch_id": b.batch_id, "raw_path": b.raw_path, "bronze_path": b.bronze_path}
            for b in final.values()
        ],
    }
    (tmp_path / "script").mkdir()
    report = stack.spark(request, tmp_path / "script")

    coordinates: Counter[tuple[int, int]] = Counter()
    values: Counter[str | None] = Counter()
    error_codes: Counter[str] = Counter()
    for item in report:
        batch = final[item["batch_id"]]
        raw = item["raw"]
        assert len(raw) == batch.record_count  # ghi đè, không cộng dồn qua các lần phát lại
        assert item["raw_columns"] == sorted(
            ["key", "value", "topic", "partition", "offset", "kafka_timestamp", "ingested_at",
             "dataset_id", "ingestion_run_id"]
        )  # fmt: skip
        coordinates.update((row[0], row[1]) for row in raw)
        values.update(row[2] for row in raw)
        assert len({row[3] for row in raw}) == 1  # Q7: một `ingested_at` mỗi batch
        assert len({row[5] for row in raw}) == 1
        # G11: mỗi record Raw đúng một dòng Bronze, cùng `ingested_at`.
        bronze_rows = [row for entity in item["bronze"].values() for row in entity["rows"]]
        assert Counter((r[0], r[1]) for r in bronze_rows) == Counter((r[0], r[1]) for r in raw)
        assert {r[2] for r in bronze_rows} == {raw[0][3]}
        assert set(item["bronze"]) <= {
            "entity=transaction", "entity=ledger_entry", "entity=parse_error"
        }  # fmt: skip
        error_codes.update(
            r[3] for r in item["bronze"].get("entity=parse_error", {}).get("rows", [])
        )

    assert len(coordinates) == total and set(coordinates.values()) == {1}
    expected_values: Counter[str | None] = Counter(hashlib.sha256(x).hexdigest() for x in lines)
    expected_values.update(None if x is None else hashlib.sha256(x).hexdigest() for x in bad)
    assert values == expected_values  # byte nguyên dạng, kể cả replay do generator tiêm
    # Viết tay theo bốn message hỏng: không JSON, null, không UTF-8, sai phiên bản schema.
    assert error_codes == Counter(
        {"INVALID_JSON": 2, "SCHEMA_TYPE": 1, "UNSUPPORTED_SCHEMA_VERSION": 1}
    )

    prefixes = ("raw", "bronze", "checkpoints")
    bucket = stack.cfg.object_store.bucket
    uris = [f"s3a://{bucket}/{prefix}/{stack.query}/" for prefix in prefixes]
    assert stack.spark({"action": "delete", "uris": uris}, tmp_path / "script")["deleted"] == 3
