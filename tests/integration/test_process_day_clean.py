"""`n06 process-day` trên Kafka, MinIO, PostgreSQL và Spark thật (TASK T25, bản sạch).

Chuỗi: generate bản sạch nhỏ (quy mô lấy từ profile `test`) -> publish -> `stream --available-now`
-> `wait-ingestion` -> `ingest-statements` -> `process-day`. Expected tính ở đây, độc lập với mã
đang kiểm: khóa kỳ vọng từ byte file events nguồn, tổng từ `control_report.json` của dataset, đường
dẫn dựng lại bằng chuỗi. Đọc Parquet bằng Spark trong container (host không có PySpark).
"""

from __future__ import annotations

import hashlib
import http.client
import io
import json
import os
import shutil
import subprocess
import uuid
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from n06.cli import main
from n06.cli.init_local import parse_dotenv_file
from n06.cli.object_store import ensure_bucket, target_from_config
from n06.cli.statements import run_ingest_statements
from n06.config import AppConfig, load_config
from n06.generator import build_dataset
from n06.ingestion.launcher import job_command
from n06.storage import PsqlCliControlStore, make_compose_psql_runner
from n06.storage.object_store import (
    EMPTY_PAYLOAD_SHA256,
    ObjectStoreTarget,
    authorization_header,
    put_object_bytes,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIR = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
AS_OF_LATE = "2099-01-01T00:00:00Z"
PROCESS_DAY = "n06.spark_jobs.process_day_job"
SILVER_ENTITIES = ("transactions", "ledger_entries", "statements", "accounts")
GOLD_ENTITIES = ("fact_transactions", "fact_reconciliation")

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
        result = {"deleted": len(request["uris"])}
    else:
        result = {}
        for name, spec in request["frames"].items():
            frame = spark.read.parquet(*spec["paths"])
            if spec["kind"] == "transactions":
                rows = frame.groupBy("business_date", "currency", "status").agg(
                    F.count(F.lit(1)).alias("n"), F.sum("amount_minor").alias("total"),
                    F.countDistinct("transaction_id").alias("k")).collect()
                result[name] = [[str(r["business_date"]), r["currency"], r["status"],
                                 r["n"], r["total"], r["k"]] for r in rows]
            elif spec["kind"] == "ledger":
                rows = frame.groupBy("business_date", "currency", "side").agg(
                    F.count(F.lit(1)).alias("n"), F.sum("amount_minor").alias("total"),
                    F.countDistinct("entry_id").alias("k")).collect()
                result[name] = [[str(r["business_date"]), r["currency"], r["side"],
                                 r["n"], r["total"], r["k"]] for r in rows]
            elif spec["kind"] == "ids":
                rows = frame.agg(F.count(F.lit(1)).alias("n"),
                                 F.countDistinct(spec["key"]).alias("k")).collect()[0]
                result[name] = {"rows": rows["n"], "distinct": rows["k"],
                                "columns": frame.columns}
            elif spec["kind"] == "reconciliation":
                rows = frame.select("channel", "external_reference", "recon_status",
                                    F.col("business_date").cast("string").alias("d"),
                                    F.size("issue_codes").alias("issues")).collect()
                result[name] = [[r["channel"], r["external_reference"], r["recon_status"],
                                 r["d"], r["issues"]] for r in rows]
            else:
                result[name] = {"columns": frame.columns, "rows": frame.count()}
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
    dataset_id: str
    bucket: str
    store: PsqlCliControlStore
    sql: Any
    target: ObjectStoreTarget
    source: Path

    def job(
        self, module: str, args: list[str], *, timeout: int = 600
    ) -> subprocess.CompletedProcess[str]:
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
            timeout=timeout, check=False,
        )  # fmt: skip

    def process_day(self, *extra: str, as_of: str = AS_OF_LATE) -> subprocess.CompletedProcess[str]:
        return self.job(
            PROCESS_DAY,
            ["--dataset-id", self.dataset_id, "--as-of-time", as_of, *extra],
        )

    def spark(self, request: dict[str, Any], tmp: Path) -> Any:
        script = tmp / "n06_it_inspect_process_day.py"
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
            timeout=400, check=False,
        )  # fmt: skip
        problems = [
            x for x in (proc.stdout + proc.stderr).splitlines()
            if " INFO " not in x and " WARN " not in x and not x.startswith("\tat ")
        ]  # fmt: skip
        assert proc.returncode == 0, "\n".join(problems[:80])
        line = next(x for x in proc.stdout.splitlines() if x.startswith("N06_IT_RESULT="))
        return json.loads(line.removeprefix("N06_IT_RESULT="))


def _result(proc: subprocess.CompletedProcess[str], prefix: str) -> dict[str, Any]:
    lines = [x for x in proc.stdout.splitlines() if x.startswith(prefix)]
    assert lines, (proc.stdout[-3000:], proc.stderr[-3000:])
    return json.loads(lines[-1].removeprefix(prefix))


def _s3(target: ObjectStoreTarget, path: str, query: list[tuple[str, str]]) -> bytes:
    amz_date = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    headers = {
        "host": target.netloc,
        "x-amz-content-sha256": EMPTY_PAYLOAD_SHA256,
        "x-amz-date": amz_date,
    }
    headers["Authorization"] = authorization_header(
        access_key=target.access_key,
        secret_key=target.secret_key,
        region=target.region,
        amz_date=amz_date,
        method="GET",
        path=path,
        query=query,
        headers=headers,
        payload_sha256=EMPTY_PAYLOAD_SHA256,
    )
    qs = "&".join(f"{k}={v}" for k, v in sorted(query))
    conn = http.client.HTTPConnection(target.host, target.port, timeout=20)
    try:
        conn.request("GET", f"{path}?{qs}", headers=headers)
        response = conn.getresponse()
        body = response.read()
        assert response.status == 200, body[:300]
        return body
    finally:
        conn.close()


def list_keys(target: ObjectStoreTarget, bucket: str, prefix: str) -> list[str]:
    """Liệt kê khóa dưới `prefix` bằng ListObjectsV2 độc lập với mã đang kiểm."""
    body = _s3(target, f"/{bucket}", [("list-type", "2"), ("prefix", prefix)])
    return sorted(n.text for n in ET.fromstring(body).iter(f"{S3_NS}Key") if n.text)


def _lines(source: Path, subdir: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted((source / subdir).glob("*.jsonl")):
        records += [json.loads(x) for x in path.read_bytes().split(b"\n") if x]
    return records


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    docker = shutil.which("docker")
    assert docker is not None, "Test tích hợp cần docker; không bỏ qua."
    dotenv = parse_dotenv_file(REPO_ROOT / ".env")
    suffix = uuid.uuid4().hex[:10]
    topic, query = f"n06.test.pd.{suffix}", f"it-pd-{suffix}"
    dataset_id = f"n06-it-pd-{suffix}"
    host_env = {**dotenv, **os.environ}
    test_cfg = load_config(profile="test", env=host_env)
    bucket = test_cfg.object_store.bucket
    target = target_from_config(test_cfg, host_env)
    assert ensure_bucket(target, bucket).ready
    demo_prefixes = load_config(profile="demo", env=host_env).paths
    env = {
        **host_env,
        "N06__KAFKA__EVENTS_TOPIC": topic,
        "N06__KAFKA__QUERY_NAME": query,
        "N06__KAFKA__MAX_OFFSETS_PER_TRIGGER": "100",
        "N06__OBJECT_STORE__BUCKET": bucket,
        "N06__PATHS__STATEMENTS_PREFIX": demo_prefixes.statements_prefix,
        "N06__SPARK__SHUFFLE_PARTITIONS": "4",
        "N06__SPARK__EVENT_LOG_ENABLED": "false",
    }
    cfg = load_config(profile="demo", env=env)
    runner = make_compose_psql_runner(
        cfg.postgres.control, docker_bin=docker, compose_file=COMPOSE_FILE, repo_root=REPO_ROOT
    )
    store = PsqlCliControlStore(cfg.postgres.control, psql_runner=runner)

    # Quy mô, số tài khoản và khoảng ngày lấy từ profile `test` (không hardcode trong test).
    generator = json.loads((CONTRACT_DIR / "config/generator.demo-clean.json").read_text("utf-8"))
    generator.update(
        dataset_id=dataset_id,
        transaction_count=test_cfg.dataset.transaction_count,
        account_count=test_cfg.dataset.account_count,
        from_date=test_cfg.dataset.from_date,
        to_date=test_cfg.dataset.to_date,
    )
    out_dir = tmp_path_factory.mktemp("pd") / "data"
    build_dataset(generator, out_dir, validate_outputs=False)
    source = out_dir / "source"

    value = Stack(docker, env, cfg, topic, query, dataset_id, bucket, store, runner, target, source)
    try:
        yield value
    finally:
        # Chỉ dọn tài nguyên của chính test này (tên có hậu tố ngẫu nhiên).
        store_sql = (
            f"DELETE FROM current_snapshot WHERE dataset_id = '{dataset_id}';"
            f"DELETE FROM runs WHERE dataset_id = '{dataset_id}';"
            f"DELETE FROM ingest_batches WHERE query_name = '{query}';"
        )
        runner(store_sql, False)
        uris = [
            f"s3a://{bucket}/{prefix}"
            for prefix in (
                f"{cfg.paths.raw_prefix}/{query}/",
                f"{cfg.paths.bronze_prefix}/{query}/",
                f"{cfg.paths.checkpoints_prefix}/{query}/",
                f"{cfg.paths.silver_prefix}/{dataset_id}/",
                f"{cfg.paths.gold_prefix}/{dataset_id}/",
                f"{cfg.paths.statements_prefix}/{dataset_id}/",
            )
        ]
        scratch = tmp_path_factory.mktemp("pd-clean")
        value.spark({"action": "delete", "uris": uris}, scratch)
        admin = AdminClient({"bootstrap.servers": test_cfg.kafka.bootstrap_servers})
        for future in admin.delete_topics([topic]).values():
            future.result(timeout=30)


@pytest.fixture(scope="module")
def ingested(stack: Stack, tmp_path_factory: pytest.TempPathFactory) -> Stack:
    """publish -> stream --available-now -> wait-ingestion -> ingest-statements."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("N06__KAFKA__NUM_PARTITIONS", "3")
        out = io.StringIO()
        publish = ["publish", "--profile", "test", "--source-dir", str(stack.source)]
        assert main([*publish, "--topic", stack.topic, "--json"], out=out) == 0, out.getvalue()
    streamed = stack.job("n06.ingestion.stream_job", ["--available-now"], timeout=400)
    assert streamed.returncode == 0, streamed.stdout[-3000:] + streamed.stderr[-3000:]

    out = io.StringIO()
    code = main(
        [
            "wait-ingestion", "--profile", "demo", "--dataset-id", stack.dataset_id,
            "--source-dir", str(stack.source), "--query-name", stack.query,
            "--timeout-seconds", "300",
        ],
        out=out,
    )  # fmt: skip
    assert code == 0, out.getvalue()

    out = io.StringIO()
    code = run_ingest_statements(
        source_dir=stack.source,
        as_of_time=AS_OF_LATE,
        dataset_id=stack.dataset_id,
        profile="test",
        env_file=REPO_ROOT / ".env",
        env=stack.env,
        as_json=True,
        out=out,
    )
    assert code == 0, out.getvalue()
    payload = json.loads(out.getvalue())
    assert payload["accounts_status"] == "uploaded"
    return stack


@pytest.fixture(scope="module")
def first_run(ingested: Stack) -> dict[str, Any]:
    proc = ingested.process_day()
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    return _result(proc, "N06_PROCESS_DAY_RESULT=")


def _snapshots(stack: Stack, dates: list[str]) -> dict[tuple[str, str, str], Any]:
    from datetime import date

    found: dict[tuple[str, str, str], Any] = {}
    for layer, entities in (("silver", SILVER_ENTITIES), ("gold", GOLD_ENTITIES)):
        for entity in entities:
            for day in dates:
                snapshot = stack.store.resolve_current_snapshot(
                    dataset_id=stack.dataset_id,
                    layer=layer,
                    entity=entity,
                    business_date=date.fromisoformat(day),
                )
                if snapshot is not None:
                    found[(layer, entity, day)] = snapshot
    return found


def _expected_path(stack: Stack, layer: str, entity: str, run_id: str, day: str) -> str:
    prefix = getattr(stack.cfg.paths, f"{layer}_prefix")
    return (
        f"s3a://{stack.bucket}/{prefix}/{stack.dataset_id}/{entity}/"
        f"run_id={run_id}/business_date={day}"
    )


def test_every_expected_key_is_matched_and_totals_equal_control_report(
    stack: Stack, first_run: dict[str, Any], tmp_path: Path
) -> None:
    dates = first_run["business_dates"]
    assert dates, first_run
    snaps = _snapshots(stack, dates)

    def paths(layer: str, entity: str) -> list[str]:
        found = [snaps[key].output_path for key in sorted(snaps) if key[:2] == (layer, entity)]
        assert found, (layer, entity)
        return found

    answer = stack.spark(
        {
            "action": "read",
            "frames": {
                "tx": {"kind": "transactions", "paths": paths("silver", "transactions")},
                "ledger": {"kind": "ledger", "paths": paths("silver", "ledger_entries")},
                "recon": {"kind": "reconciliation", "paths": paths("gold", "fact_reconciliation")},
                "fact": {"kind": "transactions", "paths": paths("gold", "fact_transactions")},
                "fact_cols": {"kind": "columns", "paths": paths("gold", "fact_transactions")},
                "recon_cols": {"kind": "columns", "paths": paths("gold", "fact_reconciliation")},
                "stmts": {
                    "kind": "ids",
                    "key": "statement_record_id",
                    "paths": paths("silver", "statements"),
                },
                "accounts": {
                    "kind": "ids",
                    "key": "account_id",
                    "paths": paths("silver", "accounts"),
                },
            },
        },
        tmp_path,
    )

    # Tổng từ control_report của dataset (nguồn công bố, không phải đầu ra của mã đang kiểm).
    report = json.loads((stack.source / "control_report.json").read_text("utf-8"))
    expected_tx = {
        (t["business_date"], t["currency"], t["status_or_side"]): (
            t["record_count"], t["amount_minor_total"])
        for t in report["totals"] if t["entity"] == "transaction"
    }  # fmt: skip
    expected_ledger = {
        (t["business_date"], t["currency"], t["status_or_side"]): (
            t["record_count"], t["amount_minor_total"])
        for t in report["totals"] if t["entity"] == "ledger_entry"
    }  # fmt: skip
    got_tx = {(d, c, s): (n, total) for d, c, s, n, total, k in answer["tx"]}
    got_ledger = {(d, c, s): (n, total) for d, c, s, n, total, k in answer["ledger"]}
    assert got_tx == expected_tx
    assert got_ledger == expected_ledger
    # Mỗi dòng Silver là một khóa nghiệp vụ duy nhất.
    assert all(n == k for *_, n, _, k in answer["tx"])
    assert all(n == k for *_, n, _, k in answer["ledger"])

    # Khóa kỳ vọng tính từ byte file events nguồn (loại bản phát lại bằng tập).
    expected_keys = {
        (e["payload"]["channel"], e["payload"]["external_reference"])
        for e in _lines(stack.source, "events")
        if e["event_type"] == "TRANSACTION_CREATED"
    }
    assert expected_keys
    recon = answer["recon"]
    assert {(c, r) for c, r, *_ in recon} == expected_keys
    assert len(recon) == len(expected_keys)  # mỗi khóa đúng một dòng
    assert Counter(status for _, _, status, *_ in recon) == {"MATCHED": len(expected_keys)}
    assert all(issues == 0 for *_, issues in recon)
    assert first_run["reconciliation"] == {
        "total_keys": len(expected_keys),
        "matched_keys": len(expected_keys),
        "unclassified_keys": 0,
    }
    # Gold fact_transactions: cùng tổng với control_report, đúng cột theo ADR-0005 mục 2.1/2.3.
    assert {(d, c, s): (n, total) for d, c, s, n, total, k in answer["fact"]} == expected_tx
    assert answer["fact_cols"]["columns"] == [
        "dataset_id", "transaction_id", "business_date", "date_key", "external_reference",
        "account_from", "account_to", "channel", "transaction_type", "currency", "status",
        "amount_minor", "event_time", "replay_count", "source_pointer", "run_id",
    ]  # fmt: skip
    assert answer["recon_cols"]["columns"][:7] == [
        "dataset_id", "channel", "external_reference", "business_date", "date_key",
        "recon_status", "issue_codes",
    ]  # fmt: skip
    assert len(answer["recon_cols"]["columns"]) == 25

    # Silver sao kê và tài khoản so với byte nguồn (số dòng khác nhau, loại trùng theo ID).
    source_statements = [
        json.loads(line)["statement_record_id"]
        for path in sorted((stack.source / "statements").rglob("*.jsonl"))
        for line in path.read_bytes().split(b"\n")
        if line
    ]
    assert answer["stmts"]["rows"] == answer["stmts"]["distinct"] == len(set(source_statements))
    source_accounts = [
        line for line in (stack.source / "accounts.jsonl").read_bytes().split(b"\n") if line
    ]
    # Bản chụp tài khoản được ghi dưới mọi business_date của run (ADR-0002 điểm 5).
    assert answer["accounts"]["distinct"] == len(source_accounts)
    assert answer["accounts"]["rows"] == len(source_accounts) * len(dates)


def test_run_paths_current_snapshot_and_rerun_hash(
    stack: Stack, first_run: dict[str, Any], tmp_path: Path
) -> None:
    dates = first_run["business_dates"]
    run1 = first_run["run_id"]
    snaps = _snapshots(stack, dates)
    assert snaps and len(snaps) == first_run["committed_snapshots"]
    for (layer, entity, day), snapshot in snaps.items():
        assert snapshot.run_id == run1
        assert snapshot.output_path == _expected_path(stack, layer, entity, run1, day)
    run_row = stack.store.get_run(run1)
    assert run_row is not None and run_row.status == "SUCCEEDED"
    assert run_row.semantic_hash == first_run["semantic_hash"]

    # Chạy lại cùng input: run mới, đường dẫn mới, semantic hash không đổi.
    proc = stack.process_day()
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    second = _result(proc, "N06_PROCESS_DAY_RESULT=")
    assert second["run_id"] != run1
    assert second["semantic_hash"] == first_run["semantic_hash"]
    again = _snapshots(stack, dates)
    assert set(again) == set(snaps)
    for (layer, entity, day), snapshot in again.items():
        assert snapshot.run_id == second["run_id"]
        assert snapshot.output_path == _expected_path(stack, layer, entity, second["run_id"], day)
        # Run cũ vẫn còn nguyên, bất biến.
        old = _expected_path(stack, layer, entity, run1, day).removeprefix(f"s3a://{stack.bucket}/")
        assert list_keys(stack.target, stack.bucket, old + "/")


def test_failed_run_is_never_read(stack: Stack, first_run: dict[str, Any]) -> None:
    dates = first_run["business_dates"]
    before = {k: v.run_id for k, v in _snapshots(stack, dates).items()}
    run_id = f"run-it-fail-{uuid.uuid4().hex[:8]}"
    proc = stack.process_day("--run-id", run_id, "--fail-at", "after_write")
    assert proc.returncode == 14, proc.stdout[-3000:] + proc.stderr[-3000:]
    assert _result(proc, "N06_PROCESS_DAY_ERROR=")["code"] == "INJECTED_FAILURE"
    failed = stack.store.get_run(run_id)
    assert failed is not None and failed.status == "FAILED"
    # Object của run thất bại có thật nhưng current_snapshot không trỏ tới.
    prefix = f"{stack.cfg.paths.silver_prefix}/{stack.dataset_id}/transactions/run_id={run_id}/"
    assert list_keys(stack.target, stack.bucket, prefix)
    after = {k: v.run_id for k, v in _snapshots(stack, dates).items()}
    assert after == before
    assert run_id not in after.values()


@pytest.mark.parametrize(
    ("layer", "entity"), [("silver", "transactions"), ("gold", "fact_reconciliation")]
)
def test_run_id_that_already_has_objects_is_rejected_without_writing(
    stack: Stack, first_run: dict[str, Any], layer: str, entity: str
) -> None:
    run_id = f"run-it-used-{uuid.uuid4().hex[:8]}"
    tier = getattr(stack.cfg.paths, f"{layer}_prefix")
    stale = f"{tier}/{stack.dataset_id}/{entity}/run_id={run_id}/x.txt"
    put_object_bytes(stack.target, stack.bucket, stale, b"da co object")
    before = {k: v.run_id for k, v in _snapshots(stack, first_run["business_dates"]).items()}

    proc = stack.process_day("--run-id", run_id)
    assert proc.returncode == 14, proc.stdout[-3000:] + proc.stderr[-3000:]
    error = _result(proc, "N06_PROCESS_DAY_ERROR=")
    assert error["code"] == "COMMIT_ERROR" and "đã có object" in error["message"]

    # Không ghi thêm bất kỳ object nào: mọi gốc của run trống, trừ đúng object đã đặt sẵn.
    for other_layer, entities in (("silver", SILVER_ENTITIES), ("gold", GOLD_ENTITIES)):
        for other in entities:
            prefix = getattr(stack.cfg.paths, f"{other_layer}_prefix")
            root = f"{prefix}/{stack.dataset_id}/{other}/run_id={run_id}/"
            keys = list_keys(stack.target, stack.bucket, root)
            assert keys == ([stale] if (other_layer, other) == (layer, entity) else []), (
                other_layer,
                other,
                keys,
            )
    after = {k: v.run_id for k, v in _snapshots(stack, first_run["business_dates"]).items()}
    assert after == before
    run_row = stack.store.get_run(run_id)
    assert run_row is not None and run_row.status == "FAILED"


def test_business_date_filter_writes_only_the_selected_day_but_reconciles_everything(
    stack: Stack, first_run: dict[str, Any]
) -> None:
    dates = first_run["business_dates"]
    assert len(dates) >= 2
    before = {k: v.run_id for k, v in _snapshots(stack, dates).items()}
    proc = stack.process_day("--business-date", dates[0])
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    result = _result(proc, "N06_PROCESS_DAY_RESULT=")
    assert result["business_dates"] == [dates[0]]
    # Đối soát chạy trên toàn dataset (không bị khóa "chưa phân loại" giả do lọc ngày).
    assert result["reconciliation"]["unclassified_keys"] == 0
    assert result["reconciliation"]["total_keys"] == first_run["reconciliation"]["total_keys"]
    after = {k: v.run_id for k, v in _snapshots(stack, dates).items()}
    for key, run in after.items():
        if key[2] == dates[0]:
            assert run == result["run_id"]
        else:
            assert run == before[key]  # ngày không chọn giữ nguyên run cũ


# --- cổng chất lượng và control totals (T32) -------------------------------------------------
def test_quality_metrics_match_every_control_report_row(
    ingested: Stack, first_run: dict[str, Any]
) -> None:
    report = json.loads((ingested.source / "control_report.json").read_text("utf-8"))
    metrics = ingested.store.list_quality_metrics(run_id=first_run["run_id"])
    found = {(m.layer, m.entity, m.business_date.isoformat(), m.metric_name): m for m in metrics}

    for row in report["totals"]:
        suffix = f"{row['currency']}.{row['status_or_side']}"
        key = (row["entity"], row["business_date"])
        assert found[("silver", *key, f"record_count.{suffix}")].metric_value == row["record_count"]
        assert (
            found[("silver", *key, f"distinct_keys.{suffix}")].metric_value == (row["record_count"])
        )
        assert (
            found[("silver", *key, f"amount_minor_total.{suffix}")].metric_value
            == (row["amount_minor_total"])
        )
        for name in ("control_count_diff", "control_amount_diff"):
            diff = found[("control", *key, f"{name}.{suffix}")]
            assert (diff.metric_value, diff.status) == (0, "PASS")
    assert first_run["quality"]["control_groups_checked"] == len(report["totals"])
    assert first_run["quality"]["control_mismatches"] == 0

    # Đếm vật lý độc lập từ file nguồn; bản sạch: không bản ghi nào bị loại.
    envelopes = _lines(ingested.source, "events")
    transactions = [e for e in envelopes if "entry_id" not in e["payload"]]
    physical = sum(
        m.metric_value
        for m in metrics
        if (m.layer, m.entity, m.metric_name) == ("bronze", "transaction", "physical_records")
    )
    assert physical == len(transactions)
    blocked = [m for m in metrics if m.metric_name == "publish_blocked"]
    assert [(m.metric_value, m.status) for m in blocked] == [(0, "PASS")]
    assert first_run["quality"]["rejected_records"] == 0


def test_unexplained_control_report_mismatch_blocks_publish(
    ingested: Stack, first_run: dict[str, Any], tmp_path: Path
) -> None:
    """Nguồn khai nhiều hơn dữ liệu 1 đơn vị tiền: run bị chặn, current_snapshot giữ nguyên."""
    source = tmp_path / "source"
    shutil.copytree(ingested.source, source)
    report_path = source / "control_report.json"
    report = json.loads(report_path.read_text("utf-8"))
    old_sha = hashlib.sha256(report_path.read_bytes()).hexdigest()
    report["totals"][0]["amount_minor_total"] += 1
    report_path.write_bytes((json.dumps(report, separators=(",", ":")) + "\n").encode("utf-8"))
    new_sha = hashlib.sha256(report_path.read_bytes()).hexdigest()
    for manifest in (source / "manifests").glob("*.json"):
        text = manifest.read_text("utf-8")
        if old_sha in text:
            manifest.write_text(text.replace(old_sha, new_sha), encoding="utf-8")

    # Prefix sao kê riêng: object của lần nạp sạch là bất biến, không ghi đè.
    prefix = f"{ingested.cfg.paths.statements_prefix}-bad-{uuid.uuid4().hex[:8]}"
    env = {**ingested.env, "N06__PATHS__STATEMENTS_PREFIX": prefix}
    bad = replace(ingested, env=env, cfg=load_config(profile="demo", env=env))
    out = io.StringIO()
    code = run_ingest_statements(
        source_dir=source,
        as_of_time=AS_OF_LATE,
        dataset_id=ingested.dataset_id,
        profile="test",
        env_file=REPO_ROOT / ".env",
        env=env,
        as_json=True,
        out=out,
    )
    assert code == 0, out.getvalue()
    days = first_run["business_dates"]
    before = _snapshots(ingested, days)
    run_id = f"run-it-blocked-{uuid.uuid4().hex[:8]}"
    try:
        proc = bad.process_day("--run-id", run_id)
        assert proc.returncode != 0, proc.stdout[-3000:]
        assert _result(proc, "N06_PROCESS_DAY_ERROR=")["code"] == "QUALITY_GATE_BLOCKED"
        assert _snapshots(ingested, days) == before
        run = ingested.store.get_run(run_id)
        assert run is not None and run.status == "FAILED"
        metrics = ingested.store.list_quality_metrics(run_id=run_id)
        failed = {m.metric_name.split(".")[0] for m in metrics if m.status == "FAIL"}
        assert {"control_amount_diff", "control_total_mismatches", "publish_blocked"} <= failed
    finally:
        ingested.spark(
            {"action": "delete", "uris": [f"s3a://{ingested.bucket}/{prefix}/"]}, tmp_path
        )
