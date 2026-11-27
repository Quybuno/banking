"""Kịch bản PySpark chạy trong container `spark` cho `test_recon_spark.py` (TASK T33).

Đọc một JSON từ stdin, tạo DataFrame theo đúng cột Silver của ADR-0004b, gọi
`n06.reconciliation.reconcile` và in `N06_RECON_RESULT=<json>`. Không chứa kỳ vọng nào: mọi giá trị
kỳ vọng nằm ở phía test (host). Không nằm trong `src/` nên được phép nhận dữ liệu fixture.
"""

from __future__ import annotations

import glob
import json
import os
import sys
from datetime import UTC, date, datetime

spark_home = os.environ.get("SPARK_HOME", "/opt/spark")
sys.path.insert(0, os.path.join(spark_home, "python"))
for py4j_zip in glob.glob(os.path.join(spark_home, "python", "lib", "py4j-*.zip")):
    sys.path.insert(0, py4j_zip)

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from n06.reconciliation import ReconciliationError, ReconciliationSettings  # noqa: E402
from n06.reconciliation.reconcile import (  # noqa: E402
    RESULT_COLUMNS,
    quality_counters,
    reconcile,
    reconcile_with_diagnostics,
)

S = StringType()
L = LongType()
TS = TimestampType()
D = DateType()
STR_ARRAY = ArrayType(S)


def _schema(*fields: tuple[str, object]) -> StructType:
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


# Silver theo ADR-0004b mục 3.1, 3.3, 3.5 (đủ cột, kể cả cột đối soát không đọc).
TRANSACTIONS = _schema(
    ("dataset_id", S), ("transaction_id", S), ("external_reference", S), ("channel", S),
    ("account_from", S), ("account_to", S), ("amount_minor", L), ("currency", S), ("status", S),
    ("transaction_type", S), ("event_time", TS), ("business_date", D), ("first_event_id", S),
    ("event_ids", STR_ARRAY), ("replay_count", IntegerType()), ("source_pointers", STR_ARRAY),
    ("first_kafka_timestamp", TS), ("first_ingested_at", TS), ("run_id", S),
)  # fmt: skip
STATEMENTS = _schema(
    ("dataset_id", S), ("statement_record_id", S), ("external_reference", S), ("channel", S),
    ("amount_minor", L), ("currency", S), ("status", S), ("event_time", TS),
    ("business_date", D), ("available_at", TS), ("file_available_at", TS),
    ("delivery_order", IntegerType()), ("source_file", S), ("source_sha256", S),
    ("source_line", L), ("run_id", S),
)  # fmt: skip
KEY_CONFLICTS = _schema(
    ("dataset_id", S), ("business_date", D), ("side_origin", S), ("entity", S),
    ("business_key", S), ("channel", S), ("external_reference", S), ("conflict_code", S),
    ("issue_codes", STR_ARRAY), ("source_pointers", STR_ARRAY), ("run_id", S),
)  # fmt: skip

TIMESTAMP_RESULT_COLUMNS = (
    "anchor_event_time",
    "as_of_time",
    "last_evaluated_at",
    "first_seen_at",
    "resolved_at",
)


def parse_ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def build(spark: SparkSession, schema: StructType, rows: list[dict]) -> DataFrame:
    converted = []
    for row in rows:
        values = []
        for field in schema.fields:
            raw = row.get(field.name)
            if raw is None:
                values.append(None)
            elif isinstance(field.dataType, TimestampType):
                values.append(parse_ts(raw))
            elif isinstance(field.dataType, DateType):
                values.append(date.fromisoformat(raw))
            else:
                values.append(raw)
        converted.append(tuple(values))
    return spark.createDataFrame(converted, schema)


def collect(df: DataFrame) -> list[dict]:
    """Chuyển thời gian thành chuỗi UTC (phiên chạy ở UTC) để so sánh không phụ thuộc máy."""
    columns = []
    for name, dtype in df.dtypes:
        col = F.col(name)
        if dtype == "timestamp":
            col = F.date_format(col, "yyyy-MM-dd'T'HH:mm:ss'Z'")
        elif dtype == "date":
            col = col.cast("string")
        columns.append(col.alias(name))
    ordered = df.select(*columns).orderBy("dataset_id", "channel", "external_reference")
    return [row.asDict(recursive=True) for row in ordered.collect()]


def run_scenario(spark: SparkSession, scenario: dict) -> dict:
    settings = ReconciliationSettings(**scenario["settings"])
    transactions = build(spark, TRANSACTIONS, scenario.get("transactions", []))
    statements = build(spark, STATEMENTS, scenario.get("statements", []))
    key_conflicts = build(spark, KEY_CONFLICTS, scenario.get("key_conflicts", []))
    as_of = parse_ts(scenario["as_of_time"])
    kwargs = {
        "as_of_time": as_of,
        "settings": settings,
        "run_id": "run-test",
        "input_snapshot_id": "snap-test",
    }
    out: dict = {"runs": []}
    for _ in range(scenario.get("repeat", 1)):
        result = reconcile(transactions, statements, key_conflicts, **kwargs)
        out["runs"].append(collect(result))
    result = reconcile(transactions, statements, key_conflicts, **kwargs)
    out["columns"] = result.columns
    out["dtypes"] = dict(result.dtypes)
    wide = reconcile_with_diagnostics(transactions, statements, key_conflicts, **kwargs)
    out["counters"] = quality_counters(wide)
    out["cutoff"] = {
        row["external_reference"]: row["cutoff_time"]
        for row in collect(
            wide.select("dataset_id", "channel", "external_reference", "cutoff_time")
        )
    }
    return out


def run_error_cases(spark: SparkSession, payload: dict) -> dict:
    settings = ReconciliationSettings(**payload["settings"])
    as_of = parse_ts(payload["as_of_time"])
    tx = build(spark, TRANSACTIONS, payload["transactions"])
    st = build(spark, STATEMENTS, [])
    kc = build(spark, KEY_CONFLICTS, [])
    base = {"settings": settings, "run_id": "r", "input_snapshot_id": "s"}
    errors: dict[str, str] = {}

    def attempt(name: str, fn) -> None:
        try:
            fn()
            errors[name] = ""
        except ReconciliationError as exc:
            errors[name] = str(exc)

    attempt("naive_as_of", lambda: reconcile(tx, st, kc, as_of_time=datetime(2026, 1, 3), **base))
    attempt(
        "missing_column", lambda: reconcile(tx.drop("currency"), st, kc, as_of_time=as_of, **base)
    )
    attempt(
        "float_money",
        lambda: reconcile(
            tx.withColumn("amount_minor", F.col("amount_minor").cast("double")),
            st,
            kc,
            as_of_time=as_of,
            **base,
        ),
    )
    attempt(
        "empty_run_id", lambda: reconcile(tx, st, kc, as_of_time=as_of, **{**base, "run_id": " "})
    )
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    try:
        attempt("non_utc_session", lambda: reconcile(tx, st, kc, as_of_time=as_of, **base))
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    return errors


def main() -> None:
    payload = json.loads(sys.stdin.read())
    spark = (
        SparkSession.builder.appName(payload["app_name"])
        .master(payload["master"])
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(payload["shuffle_partitions"]))
        .getOrCreate()
    )
    try:
        out: dict = {
            "result_columns": list(RESULT_COLUMNS),
            "scenarios": {s["name"]: run_scenario(spark, s) for s in payload["scenarios"]},
            "errors": run_error_cases(spark, payload["error_cases"]),
        }
        print("N06_RECON_RESULT=" + json.dumps(out, sort_keys=True))
    finally:
        spark.stop()


main()
