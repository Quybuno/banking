"""Biến đổi Spark của `process-day` trên dữ liệu nhỏ dựng tay, chạy trong container Spark thật.

Phủ các nhánh mà bộ dữ liệu sạch không chạm tới: gộp replay, xung đột khóa và `event_id`, lệch
envelope, lệch ngày nghiệp vụ, loại trùng sao kê, sao kê chưa nhìn thấy tại `as_of_time`, khóa chưa
phân loại, tính bất biến của hash nội dung. Expected viết tay trong test; kịch bản chỉ in số liệu.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

_SCRIPT = r"""
import json
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from n06.spark_jobs import receipts, transforms

spark = (
    SparkSession.builder.master("local[1]").appName("it-transforms")
    .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2").getOrCreate()
)
DS = "ds-one"
OUT = {}


def ts(text):
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def tx_row(event_id, tx, offset, *, amount=100, event_time="2026-01-01T05:00:00Z",
           business_date="2026-01-01", env_ds=DS, ds=DS, env_time=None, partition=0):
    return {
        "topic": "t", "partition": partition, "offset": offset,
        "kafka_timestamp": ts("2026-01-01T05:00:01Z"), "ingested_at": ts("2026-01-01T05:00:02Z"),
        "event_id": event_id, "envelope_dataset_id": env_ds, "dataset_id": ds,
        "envelope_event_time": env_time or event_time, "event_time": event_time,
        "transaction_id": tx, "external_reference": "REF-" + tx.upper(),
        "account_from": "a", "account_to": "b", "amount_minor": amount, "currency": "VND",
        "status": "SUCCESS", "transaction_type": "INTERNAL_TRANSFER",
        "channel": "SIM_CHANNEL_A", "business_date": business_date,
    }


bronze = spark.createDataFrame([
    tx_row("e1", "t1", 0), tx_row("e1", "t1", 5), tx_row("e2", "t1", 9),   # replay, cùng nội dung
    tx_row("e3", "t2", 1, amount=100), tx_row("e4", "t2", 2, amount=101),  # cùng khóa khác nội dung
    tx_row("e9", "t3", 3), tx_row("e9", "t4", 4),   # cùng event_id khác khóa
    tx_row("e5", "t5", 6, env_ds="other-ds"),       # envelope lệch dataset
    tx_row("e6", "t6", 7, env_time="2026-01-01T06:00:00Z"),   # envelope lệch event_time
    tx_row("e7", "t7", 8, ds="other-ds", env_ds="other-ds"),  # dataset khác: bị loại
])
grouped, source = transforms.dedup_event_bronze(
    bronze, dataset_id=DS, key="transaction_id", payload=transforms.TRANSACTION_PAYLOAD)
rows = {r["transaction_id"]: r.asDict() for r in grouped.collect()}
OUT["dedup_keys"] = sorted(rows)
t1 = rows["t1"]
OUT["t1"] = [t1["variants"], t1["replay_count"], t1["event_ids"], t1["first_event_id"],
             t1["source_pointers"]]
conflicting = grouped.where(F.col("variants") > 1).collect()
OUT["conflict_keys"] = sorted(r["transaction_id"] for r in conflicting)
OUT["event_id_conflicts"] = [r["event_id"] for r in transforms.event_id_conflicts(source).collect()]
OUT["envelope_mismatch"] = sorted(
    r["event_id"] for r in transforms.envelope_mismatch(source, DS).collect())

silver = transforms.silver_transactions(grouped.where(F.col("variants") == 1), run_id="run-x")
OUT["silver_columns"] = silver.columns
CHECKED = ("amount_minor", "event_time", "business_date", "replay_count")
OUT["silver_types"] = {f.name: f.dataType.simpleString() for f in silver.schema.fields
                        if f.name in CHECKED}

edge = spark.createDataFrame([
    tx_row("e1", "g1", 0, event_time="2026-01-01T16:59:59Z", business_date="2026-01-01"),
    tx_row("e2", "g2", 1, event_time="2026-01-01T17:00:00Z", business_date="2026-01-01"),
    tx_row("e3", "g3", 2, event_time="2026-01-01T17:00:00Z", business_date="2026-01-02"),
])
edge_g, _ = transforms.dedup_event_bronze(
    edge, dataset_id=DS, key="transaction_id", payload=transforms.TRANSACTION_PAYLOAD)
edge_s = transforms.silver_transactions(edge_g, run_id="run-x")
bad_dates = transforms.invalid_event_rows(edge_s, "Asia/Ho_Chi_Minh").collect()
OUT["bad_business_date"] = sorted(r["transaction_id"] for r in bad_dates)


# --- sao kê ---------------------------------------------------------------------------------
def stmt(sid, ref, amount=100, channel="SIM_CHANNEL_A", status="SUCCESS", **extra):
    record = {
        "schema_version": "1.0.0", "dataset_id": DS, "statement_record_id": sid,
        "external_reference": ref, "channel": channel, "amount_minor": amount, "currency": "VND",
        "status": status, "event_time": "2026-01-01T05:01:00Z", "business_date": "2026-01-01",
        "available_at": "2026-01-01T05:02:00Z",
    }
    record.update(extra)
    return json.dumps(record, sort_keys=True)


def make_receipts(files):
    result, uris, data = [], {}, []
    for order, (available, lines) in enumerate(files):
        path = f"statements/f{order}.jsonl"
        uris[path] = f"s3a://b/s/{path}"
        data.append((uris[path], bytearray(("\n".join(lines) + "\n").encode())))
        result.append(receipts.Receipt(
            DS, order, ts(available), available, path, "m", "0" * 64, len(lines)))
    frame = spark.createDataFrame(data, "path string, content binary")
    return result, uris, frame


def silver_statements(files):
    rs, uris, frame = make_receipts(files)
    flat, invalid = transforms.silver_statements(spark, frame, rs, uris, dataset_id=DS)
    return flat, invalid, frame


flat, invalid, frame = silver_statements([
    ("2026-01-01T18:00:00Z",
     [stmt("s1", "REF-T1"), stmt("s1", "REF-T1"), stmt("s2", "REF-T2", 200)]),
    ("2026-01-03T00:00:00Z",
     [stmt("s3", "REF-T3"), stmt("s4", "REF-T4"), stmt("s5", "REF-T4")]),
])
final = transforms.finish_silver_statements(flat, run_id="run-x")
got = {r["statement_record_id"]: r.asDict() for r in final.collect()}
OUT["stmt_ids"] = sorted(got)
OUT["stmt_s1"] = [got["s1"]["delivery_order"], got["s1"]["source_line"], got["s1"]["source_file"]]
OUT["stmt_s3_file_available_at"] = got["s3"]["file_available_at"].strftime("%Y-%m-%dT%H:%M:%SZ")
OUT["stmt_invalid"] = invalid.count()
same_key = final.where(F.col("external_reference") == "REF-T4").collect()
OUT["stmt_same_key_kept"] = sorted(r["statement_record_id"] for r in same_key)
stats = {r["path"]: [r["sha256"], r["rows"]] for r in transforms.file_stats(frame).collect()}
OUT["file_rows"] = sorted(v[1] for v in stats.values())

flat2, invalid2, _ = silver_statements([
    ("2026-01-01T18:00:00Z", [stmt("s1", "REF-T1"), stmt("s1", "REF-T1", 999), "{not json}",
                              stmt("s6", "REF-T6", channel="SIM_CHANNEL_Z"),
                              stmt("s7", "REF-T7", amount=0)]),
])
s1_rows = flat2.where(F.col("statement_record_id") == "s1").collect()
OUT["stmt_conflict_variants"] = [r["variants"] for r in s1_rows]
OUT["stmt_invalid_rows"] = invalid2.count()
OUT["stmt_ids_with_invalid_input"] = sorted(r["statement_record_id"] for r in flat2.collect())

empty = spark.createDataFrame([("s3a://b/e", bytearray(b""))], "path string, content binary")
OUT["empty_file_rows"] = [r["rows"] for r in transforms.file_stats(empty).collect()]

# --- đối soát MATCHED -----------------------------------------------------------------------
def internal(tx, ref, amount=100, status="SUCCESS", currency="VND", date="2026-01-01"):
    return {
        "dataset_id": DS, "transaction_id": tx, "external_reference": ref,
        "channel": "SIM_CHANNEL_A", "currency": currency, "status": status,
        "amount_minor": amount, "business_date": datetime.strptime(date, "%Y-%m-%d").date(),
        "source_pointers": ["t:0:" + tx],
    }


tx_df = spark.createDataFrame([
    internal("a", "REF-A"), internal("b", "REF-B", 200), internal("c", "REF-C"),
    internal("d", "REF-D"), internal("e", "REF-E"), internal("f", "REF-F", status="FAILED"),
    internal("g1", "REF-G"), internal("g2", "REF-G"),
])
st_rows = []
for sid, ref, amount, delivery, available, status in [
    ("x1", "REF-A", 100, 0, "2026-01-01T18:00:00Z", "SUCCESS"),
    ("x2", "REF-B", 201, 0, "2026-01-01T18:00:00Z", "SUCCESS"),
    ("x3", "REF-C", 100, 1, "2026-01-03T00:00:00Z", "SUCCESS"),   # chưa nhìn thấy ở mốc sớm
    ("x4", "REF-D", 100, 0, "2026-01-01T18:00:00Z", "SUCCESS"),
    ("x5", "REF-D", 100, 0, "2026-01-01T18:00:00Z", "SUCCESS"),   # hai sao kê độc lập cùng khóa
    ("x6", "REF-F", 100, 0, "2026-01-01T18:00:00Z", "FAILED"),
    ("x7", "REF-G", 100, 0, "2026-01-01T18:00:00Z", "SUCCESS"),   # hai giao dịch cùng khóa
    ("x8", "REF-Z", 100, 0, "2026-01-01T18:00:00Z", "SUCCESS"),   # chỉ có sao kê
]:
    st_rows.append({
        "dataset_id": DS, "statement_record_id": sid, "external_reference": ref,
        "channel": "SIM_CHANNEL_A", "currency": "VND", "status": status, "amount_minor": amount,
        "file_available_at": ts(available), "source_file": "f" + str(delivery),
        "source_sha256": "0" * 64, "source_line": 1,
    })
st_all = spark.createDataFrame(st_rows)


def reconcile(as_of):
    gold, summary = transforms.reconcile_matched(
        tx_df, st_all, as_of_time=ts(as_of), run_id="run-x")
    s = summary.collect()[0]
    return gold, [s["total_keys"], s["matched_keys"]]


gold_early, summary_early = reconcile("2026-01-02T00:00:00Z")
gold_late, summary_late = reconcile("2026-01-03T00:00:00Z")   # đúng mốc: có mặt
OUT["recon_early"] = [summary_early, sorted(r["external_reference"] for r in gold_early.collect())]
OUT["recon_late"] = [summary_late, sorted(r["external_reference"] for r in gold_late.collect())]
row = gold_early.where(F.col("external_reference") == "REF-A").collect()[0].asDict()
OUT["recon_row"] = {k: (str(v) if v is not None else None) for k, v in row.items()
                    if k in ("recon_status", "issue_codes", "internal_amount_minor",
                             "statement_amount_minor", "anchor_event_time",
                             "discrepancy_age_seconds", "first_seen_at", "resolved_at",
                             "internal_record_count", "statement_record_count", "date_key",
                             "as_of_time", "last_evaluated_at")}
OUT["recon_columns"] = gold_early.columns
OUT["failed_matched_as_success"] = "REF-F" in OUT["recon_early"][1]

# --- hash nội dung --------------------------------------------------------------------------
base = silver.withColumn("run_id", F.lit("run-A"))
OUT["digest_stable"] = [
    transforms.entity_digest(base) == transforms.entity_digest(
        base.orderBy(F.col("transaction_id").desc()).withColumn("run_id", F.lit("run-B"))),
    transforms.entity_digest(base) == transforms.entity_digest(
        base.withColumn("amount_minor", F.col("amount_minor") + 1)),
]
print("N06_IT_RESULT=" + json.dumps(OUT, default=str))
spark.stop()
"""


def _run(tmp_path: Path) -> dict[str, Any]:
    docker = shutil.which("docker")
    assert docker is not None, "Test tích hợp cần docker; không bỏ qua."
    script = tmp_path / "n06_it_transforms.py"
    script.write_text(_SCRIPT, encoding="utf-8")
    tmp_path.chmod(0o755)
    command = [
        docker, "compose", "-f", str(COMPOSE_FILE), "--profile", "core",
        "run", "--rm", "--no-deps", "-T",
    ]  # fmt: skip
    for folder in ("src", "config", "contracts", "sql"):
        command += ["-v", f"{REPO_ROOT / folder}:/opt/n06/{folder}:ro"]
    command += ["-v", f"{tmp_path}:/opt/n06/it:ro", "-e", "PYTHONPATH=/opt/n06/src"]
    command += ["spark", "/opt/spark/bin/spark-submit", "--master", "local[1]"]
    command += ["--conf", "spark.ui.enabled=false", f"/opt/n06/it/{script.name}"]
    proc = subprocess.run(
        command, cwd=REPO_ROOT, capture_output=True, text=True, timeout=400, check=False
    )
    problems = [
        x for x in (proc.stdout + proc.stderr).splitlines()
        if " INFO " not in x and " WARN " not in x and not x.startswith("\tat ")
    ]  # fmt: skip
    assert proc.returncode == 0, "\n".join(problems[:80])
    line = next(x for x in proc.stdout.splitlines() if x.startswith("N06_IT_RESULT="))
    return json.loads(line.removeprefix("N06_IT_RESULT="))


@pytest.fixture(scope="module")
def result(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return _run(tmp_path_factory.mktemp("transforms"))


def test_replays_are_merged_with_lineage_and_conflicts_are_flagged_not_chosen(
    result: dict[str, Any],
) -> None:
    # `t7` thuộc dataset khác nên không có; `t5` và `t6` có mặt để lộ ra ở envelope_mismatch.
    assert result["dedup_keys"] == ["t1", "t2", "t3", "t4", "t5", "t6"]
    variants, replay_count, event_ids, first_event_id, pointers = result["t1"]
    assert (variants, replay_count, first_event_id) == (1, 3, "e1")
    assert event_ids == ["e1", "e2"]
    assert pointers == ["t:0:0", "t:0:5", "t:0:9"]
    assert result["conflict_keys"] == ["t2"]
    assert result["event_id_conflicts"] == ["e9"]  # cùng event_id nhưng khác khóa nghiệp vụ
    assert result["envelope_mismatch"] == ["e5", "e6"]


def test_silver_transactions_columns_types_and_business_date_check(result: dict[str, Any]) -> None:
    assert result["silver_columns"] == [
        "dataset_id", "transaction_id", "external_reference", "channel", "account_from",
        "account_to", "amount_minor", "currency", "status", "transaction_type", "event_time",
        "business_date", "first_event_id", "event_ids", "replay_count", "source_pointers",
        "first_kafka_timestamp", "first_ingested_at", "run_id",
    ]  # fmt: skip
    assert result["silver_types"] == {
        "amount_minor": "bigint",
        "event_time": "timestamp",
        "business_date": "date",
        "replay_count": "int",
    }
    # 16:59:59Z vẫn là ngày 01/01 giờ Việt Nam; 17:00:00Z đã sang 02/01.
    assert result["bad_business_date"] == ["g2"]


def test_statement_dedup_receipt_columns_and_invalid_rows(result: dict[str, Any]) -> None:
    assert result["stmt_ids"] == ["s1", "s2", "s3", "s4", "s5"]  # s1 gộp từ hai dòng giống nhau
    assert result["stmt_s1"] == [0, 1, "statements/f0.jsonl"]  # dòng nhỏ nhất của biên nhận đầu
    assert result["stmt_s3_file_available_at"] == "2026-01-03T00:00:00Z"
    assert result["stmt_invalid"] == 0
    # Hai statement_record_id cho cùng match key được giữ lại để T33 phát hiện.
    assert result["stmt_same_key_kept"] == ["s4", "s5"]
    assert result["file_rows"] == [3, 3]
    assert result["stmt_conflict_variants"] == [2]  # s1 hai nội dung khác nhau
    assert result["stmt_invalid_rows"] == 3  # JSON hỏng, channel ngoài enum, amount = 0
    # Dòng sai (s6 kênh lạ, s7 amount = 0) chỉ đi quarantine, không vào Silver/đối soát.
    assert result["stmt_ids_with_invalid_input"] == ["s1"]
    assert result["empty_file_rows"] == [0]  # file rỗng có 0 dòng chứ không "không có"


def test_matched_only_reconciliation_visibility_and_unclassified_keys(
    result: dict[str, Any],
) -> None:
    # Khóa: A, B, C, D, E, F, G, Z. Mốc sớm: C chưa có sao kê nhìn thấy.
    early_summary, early_keys = result["recon_early"]
    assert early_summary == [8, 2]
    assert early_keys == ["REF-A", "REF-F"]  # FAILED/FAILED được đối soát như SUCCESS
    # Tại đúng mốc `file_available_at` thì sao kê đã có mặt (inclusive).
    late_summary, late_keys = result["recon_late"]
    assert late_summary == [8, 3]
    assert late_keys == ["REF-A", "REF-C", "REF-F"]
    assert result["failed_matched_as_success"] is True
    row = result["recon_row"]
    assert row == {
        "recon_status": "MATCHED",
        "issue_codes": "[]",
        "internal_amount_minor": "100",
        "statement_amount_minor": "100",
        "internal_record_count": "1",
        "statement_record_count": "1",
        "anchor_event_time": None,
        "discrepancy_age_seconds": None,
        "first_seen_at": None,
        "resolved_at": None,
        "date_key": "20260101",
        "as_of_time": "2026-01-02 00:00:00",
        "last_evaluated_at": "2026-01-02 00:00:00",
    }
    assert result["recon_columns"] == [
        "dataset_id", "channel", "external_reference", "business_date", "date_key",
        "recon_status", "issue_codes", "transaction_id", "internal_amount_minor",
        "internal_currency", "internal_status", "statement_amount_minor",
        "statement_currency", "statement_status", "internal_record_count",
        "statement_record_count", "anchor_event_time", "discrepancy_age_seconds", "as_of_time",
        "last_evaluated_at", "first_seen_at", "resolved_at", "recon_rule_version",
        "source_pointers", "run_id",
    ]  # fmt: skip


def test_content_digest_ignores_row_order_and_run_id_but_not_content(
    result: dict[str, Any],
) -> None:
    same_content, changed_amount = result["digest_stable"]
    assert same_content is True
    assert changed_amount is False
