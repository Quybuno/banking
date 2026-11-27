"""Job `n06 wait-ingestion`: chờ tới khi mọi dòng events của một dataset đã nằm trong Raw đã commit.

Thuật toán của ADR-0004 mục 9: so đa tập `sha256` byte của dòng nguồn (không gồm LF) với đa tập
`sha256(value)` của Raw thuộc các batch có trong `ingest_batches` của query đang chờ.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from n06.config import QUERY_NAME_PATTERN, AppConfig, ConfigValidationError, load_config
from n06.ingestion import layout, records
from n06.ingestion.source_lines import dataset_event_files
from n06.ingestion.spark_session import build_spark, hadoop_path
from n06.producer.errors import SourceDataError
from n06.storage import ControlStore, IngestBatchRecord, connect_control_store

CONFIG_ERROR_EXIT = 10
NOT_READY_EXIT = 14


def expected_lines(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Đa tập `(h, expected)` của các dòng nguồn. `lineSep` là LF nên `\\r` được giữ nguyên."""
    return (
        spark.read.option("lineSep", "\n")
        .text(paths)
        .select(F.sha2(F.col("value").cast("binary"), 256).alias("h"))
        .groupBy("h")
        .agg(F.count(F.lit(1)).alias("expected"))
    )


def missing_lines(expected: DataFrame, spark: SparkSession, raw_paths: list[str]) -> int:
    """Số dòng nguồn (kể cả bản lặp) chưa có đủ bản giống từng byte trong Raw đã commit."""
    if raw_paths:
        observed = (
            spark.read.parquet(*raw_paths)
            .where(F.col("value").isNotNull())
            .select(F.sha2(F.col("value"), 256).alias("h"))
            .groupBy("h")
            .agg(F.count(F.lit(1)).alias("observed"))
        )
        joined = expected.join(observed, "h", "left")
    else:
        joined = expected.withColumn("observed", F.lit(0))
    shortfall = F.greatest(F.col("expected") - F.coalesce(F.col("observed"), F.lit(0)), F.lit(0))
    return int(joined.agg(F.coalesce(F.sum(shortfall), F.lit(0))).first()[0])


def parse_error_rows(spark: SparkSession, batches: list[IngestBatchRecord]) -> int:
    paths = []
    for batch in batches:
        uri = layout.bronze_entity_uri(batch.bronze_path, records.PARSE_ERROR)
        filesystem, path = hadoop_path(spark, uri)
        if filesystem.exists(path):
            paths.append(uri)
    return spark.read.parquet(*paths).count() if paths else 0


def wait(
    cfg: AppConfig,
    spark: SparkSession,
    store: ControlStore,
    *,
    query_name: str,
    source_paths: list[str],
    total: int,
    timeout_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    expected = expected_lines(spark, source_paths).persist()
    deadline = clock() + timeout_seconds
    missing = total
    batches: list[IngestBatchRecord] = []
    while True:
        batches = store.list_committed_ingest_batches(query_name=query_name)
        try:
            missing = missing_lines(expected, spark, [batch.raw_path for batch in batches])
        except AnalysisException as exc:  # batch đang được phát lại: thư mục tạm thời dở dang
            print(f"N06_WAIT chưa đọc được Raw ({type(exc).__name__}), thử lại", flush=True)
        if missing == 0 or clock() >= deadline:
            break
        sleep(min(cfg.kafka.trigger_interval_seconds, max(deadline - clock(), 0)))
    return {
        "ready": missing == 0,
        "query_name": query_name,
        "expected_lines": total,
        "missing_lines": missing,
        "committed_batches": len(batches),
        "parse_error_rows": None if missing == 0 else parse_error_rows(spark, batches),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="n06-wait-ingestion-job")
    parser.add_argument("--profile")
    parser.add_argument("--query-name")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--source-dir", required=True, type=Path)
    parser.add_argument("--timeout-seconds", required=True, type=float)
    args = parser.parse_args(argv)
    try:
        cfg = load_config(profile=args.profile, env=os.environ)
    except ConfigValidationError as exc:
        print(f"[wait-ingestion] Lỗi: {exc.public_message}", file=sys.stderr)
        return CONFIG_ERROR_EXIT
    query_name = args.query_name or cfg.kafka.query_name
    if not QUERY_NAME_PATTERN.fullmatch(query_name):
        print("[wait-ingestion] Lỗi: query_name không đúng mẫu.", file=sys.stderr)
        return CONFIG_ERROR_EXIT
    try:
        files, total = dataset_event_files(args.source_dir, args.dataset_id)
    except SourceDataError as exc:
        print(f"[wait-ingestion] Lỗi nguồn: {exc}", file=sys.stderr)
        return CONFIG_ERROR_EXIT
    spark = None
    try:
        store = connect_control_store(cfg.postgres.control)
        spark = build_spark(cfg, "wait-ingestion")
        result = wait(
            cfg,
            spark,
            store,
            query_name=query_name,
            source_paths=[item.path.resolve().as_uri() for item in files],
            total=total,
            timeout_seconds=args.timeout_seconds,
        )
    except Exception as exc:
        print(f"[wait-ingestion] Job thất bại ({type(exc).__name__}): {exc}", file=sys.stderr)
        return NOT_READY_EXIT
    finally:
        if spark is not None:
            spark.stop()
    print("N06_WAIT_RESULT=" + json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["ready"] else NOT_READY_EXIT


if __name__ == "__main__":
    sys.exit(main())
