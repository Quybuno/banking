"""Job `n06 stream`: một query `foreachBatch` ghi Raw rồi Bronze theo `batch_id`.

Giao thức (đặc tả I.8.1 streaming, ADR-0002 mục 2, ADR-0004):
1. persist microbatch; 2. ghi đè đúng thư mục Raw và Bronze của `batch_id`;
3. sau khi mọi output đã ghi, upsert một dòng `ingest_batches`; chỉ khi đó callback mới trả về.
Chạy trong image Spark bằng `spark-submit` (xem `n06.ingestion.launcher`).
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from n06.config import QUERY_NAME_PATTERN, AppConfig, ConfigValidationError, load_config
from n06.control import connect_control_db, migrate_up
from n06.ingestion import layout, records
from n06.ingestion.spark_session import build_spark, hadoop_path
from n06.storage import ControlStore, IngestBatchRecord, connect_control_store

# Điểm tắt tiến trình dùng cho kiểm thử phục hồi (`--crash-at`).
CRASH_POINTS = ("after_raw", "before_upsert", "after_upsert")
CONFIG_ERROR_EXIT = 10
SERVICE_ERROR_EXIT = 14

_ERROR_COLUMNS = ("entity", "business_key", "error_code", "error_detail", "issue_codes")
_BRONZE_COMMON = ("topic", "partition", "offset", "kafka_timestamp", "ingested_at")


def _parsed_schema() -> StructType:
    names = ["route", "raw_dataset_id"]
    names += [column for column, _ in records.ENVELOPE_COLUMNS]
    names += list(records.TRANSACTION_COLUMNS)
    names += [c for c in records.LEDGER_COLUMNS if c not in records.TRANSACTION_COLUMNS]
    names += list(_ERROR_COLUMNS)

    def field(name: str) -> StructField:
        if name in records.INTEGER_COLUMNS:
            return StructField(name, LongType(), True)
        if name == "issue_codes":
            return StructField(name, ArrayType(StringType()), True)
        return StructField(name, StringType(), True)

    return StructType([field(name) for name in names])


def _classify(value: bytearray | None) -> dict[str, object]:
    route, dataset_id, columns = records.classify_record(value)
    return {"route": route, "raw_dataset_id": dataset_id, **columns}


@dataclass(frozen=True)
class BatchContext:
    cfg: AppConfig
    query_name: str
    ingestion_run_id: str
    store: ControlStore
    clock: Callable[[], datetime]
    crash_at: str | None = None
    crash_batch: int = 0

    def maybe_crash(self, point: str, batch_id: int) -> None:
        if self.crash_at == point and batch_id == self.crash_batch:
            print(f"N06_STREAM tắt tiến trình tại {point}, batch_id={batch_id}", flush=True)
            os.kill(os.getppid(), signal.SIGKILL)  # JVM của spark-submit
            os._exit(86)


def _bronze_columns(route: str) -> list[Column]:
    common = [F.col(name) for name in _BRONZE_COMMON] + [F.col("ingestion_run_id")]
    if route == records.PARSE_ERROR:
        own = [F.col("p.raw_dataset_id").alias("dataset_id")]
        own += [F.col(f"p.{name}").alias(name) for name in _ERROR_COLUMNS]
    else:
        names = [column for column, _ in records.ENVELOPE_COLUMNS]
        names += list(
            records.TRANSACTION_COLUMNS if route == records.TRANSACTION else records.LEDGER_COLUMNS
        )
        own = [F.col(f"p.{name}").alias(name) for name in names]
    return common + own


def process_batch(batch: DataFrame, batch_id: int, ctx: BatchContext) -> None:
    """Callback của `foreachBatch`. Ném ngoại lệ thì Spark không ghi nhận batch và phát lại."""
    spark = batch.sparkSession
    ingested_at = ctx.clock()  # Q7: một giá trị cho cả batch, chung Raw và Bronze
    parse = F.udf(_classify, _parsed_schema())
    source = (
        batch.select(
            "key",
            "value",
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
        )  # fmt: skip
        .withColumn("p", parse(F.col("value")))
        .withColumn("ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("ingestion_run_id", F.lit(ctx.ingestion_run_id))
        .persist()
    )
    try:
        ranges = (
            source.groupBy("topic", "partition")
            .agg(
                F.min("offset").alias("first"),
                F.max("offset").alias("last"),
                F.count(F.lit(1)).alias("rows"),
                F.expr("unix_micros(min(kafka_timestamp))").alias("min_us"),
            )
            .collect()  # một dòng cho mỗi topic-partition
        )
        if not ranges:
            return  # G9: batch rỗng không ghi file và không tạo dòng
        record_count = sum(row["rows"] for row in ranges)
        earliest = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
            microseconds=min(row["min_us"] for row in ranges)
        )
        ingestion_date = layout.ingestion_date_of(earliest, ctx.cfg.business_timezone)
        raw_uri = layout.raw_batch_uri(ctx.cfg, ctx.query_name, ingestion_date, batch_id)
        bronze_uri = layout.bronze_batch_uri(ctx.cfg, ctx.query_name, ingestion_date, batch_id)

        source.select(
            "key", "value", "topic", "partition", "offset", "kafka_timestamp", "ingested_at",
            F.col("p.raw_dataset_id").alias("dataset_id"), "ingestion_run_id",
        ).write.mode("overwrite").parquet(raw_uri)  # fmt: skip
        ctx.maybe_crash("after_raw", batch_id)

        # Q6: ghi đè cả thư mục batch để lần phát lại không sót thư mục entity cũ.
        filesystem, path = hadoop_path(spark, bronze_uri)
        if filesystem.exists(path) and not filesystem.delete(path, True):
            raise RuntimeError(f"Không xóa được thư mục Bronze cũ: {bronze_uri}")
        routes = {row["route"] for row in source.select("p.route").distinct().collect()}
        for route in sorted(routes):
            source.where(F.col("p.route") == route).select(*_bronze_columns(route)).write.mode(
                "overwrite"
            ).parquet(layout.bronze_entity_uri(bronze_uri, route))
        ctx.maybe_crash("before_upsert", batch_id)

        offsets = layout.topic_offsets(
            (row["topic"], row["partition"], row["first"], row["last"]) for row in ranges
        )
        ctx.store.upsert_ingest_batch(
            IngestBatchRecord(
                query_name=ctx.query_name,
                batch_id=batch_id,
                dataset_id=None,  # ADR-0004 Q1
                ingestion_date=ingestion_date,
                topic_offsets=offsets,
                record_count=record_count,
                raw_path=raw_uri,
                bronze_path=bronze_uri,
                status="COMMITTED",
            )
        )
        ctx.maybe_crash("after_upsert", batch_id)
        print(
            "N06_STREAM_BATCH="
            + json.dumps(
                {
                    "batch_id": batch_id,
                    "record_count": record_count,
                    "ingestion_date": ingestion_date.isoformat(),
                    "topic_offsets": offsets,
                    "raw_path": raw_uri,
                },
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        source.unpersist()


def run(cfg: AppConfig, query_name: str, args: argparse.Namespace) -> None:
    migrate_up(connect_control_db(cfg.postgres.control))
    store = connect_control_store(cfg.postgres.control)
    spark = build_spark(cfg, "stream")
    try:
        context = BatchContext(
            cfg=cfg,
            query_name=query_name,
            ingestion_run_id=f"stream-{uuid.uuid4().hex}",
            store=store,
            clock=lambda: datetime.now(UTC),
            crash_at=args.crash_at,
            crash_batch=args.crash_batch,
        )
        reader = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
            .option("subscribe", cfg.kafka.events_topic)
            .option("startingOffsets", "earliest")  # G17; chỉ áp cho checkpoint mới
            .option("maxOffsetsPerTrigger", cfg.kafka.max_offsets_per_trigger)
            .option("groupIdPrefix", cfg.kafka.consumer_group)
            .load()
        )
        writer = (
            reader.writeStream.queryName(query_name)
            .option("checkpointLocation", layout.checkpoint_uri(cfg, query_name))
            .foreachBatch(lambda frame, batch_id: process_batch(frame, batch_id, context))
        )
        if args.available_now:
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime=f"{cfg.kafka.trigger_interval_seconds} seconds")
        print(f"N06_STREAM_STARTED query_name={query_name}", flush=True)
        writer.start().awaitTermination()
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="n06-stream-job")
    parser.add_argument("--profile")
    parser.add_argument("--query-name")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--crash-at", choices=CRASH_POINTS, help="chỉ dùng cho kiểm thử")
    parser.add_argument("--crash-batch", type=int, default=0, help="chỉ dùng cho kiểm thử")
    args = parser.parse_args(argv)
    try:
        cfg = load_config(profile=args.profile, env=os.environ)
    except ConfigValidationError as exc:
        print(f"[stream] Lỗi: {exc.public_message}", file=sys.stderr)
        return CONFIG_ERROR_EXIT
    query_name = args.query_name or cfg.kafka.query_name
    if not QUERY_NAME_PATTERN.fullmatch(query_name):
        print("[stream] Lỗi: query_name không đúng mẫu.", file=sys.stderr)
        return CONFIG_ERROR_EXIT
    try:
        run(cfg, query_name, args)
    except Exception as exc:  # lỗi hạ tầng làm job thất bại, không nuốt
        print(f"[stream] Job thất bại ({type(exc).__name__}): {exc}", file=sys.stderr)
        return SERVICE_ERROR_EXIT
    return 0


if __name__ == "__main__":
    sys.exit(main())
