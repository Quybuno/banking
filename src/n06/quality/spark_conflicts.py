"""Hàm Spark SQL tách `EVENT_ID_CONFLICT`, `BUSINESS_KEY_CONFLICT` và `silver.key_conflicts`.

Tuân thủ đặc tả I.3 (không Python UDF), I.6, I.7.1, II.8 và ADR-0004b §3.5 (TASK T31).
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DateType, StringType, StructField, StructType

from n06.quality.conflicts import SIDE_INTERNAL, SIDE_STATEMENT
from n06.quality.quarantine import STAGE_SILVER_DEDUP

RECORD_SEPARATOR = "\x1f"

KEY_CONFLICT_SCHEMA = StructType(
    [
        StructField("dataset_id", StringType(), False),
        StructField("business_date", DateType(), False),
        StructField("side_origin", StringType(), False),
        StructField("entity", StringType(), False),
        StructField("business_key", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("external_reference", StringType(), True),
        StructField("conflict_code", StringType(), False),
        StructField("issue_codes", ArrayType(StringType()), False),
        StructField("source_pointers", ArrayType(StringType()), False),
        StructField("run_id", StringType(), False),
    ]
)


def empty_key_conflicts_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], schema=KEY_CONFLICT_SCHEMA)


def _text(columns: Sequence[str]) -> F.Column:
    return F.concat_ws(RECORD_SEPARATOR, *[F.col(name).cast("string") for name in columns])


def split_event_conflicts_spark(
    bronze_valid: DataFrame,
    *,
    dataset_id: str,
    entity: str,
    key_col: str,
    payload_cols: Sequence[str],
    run_id: str,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Tách bản ghi sạch (đã gộp replay) và bản ghi xung đột khóa từ Bronze sự kiện đã qua T30.

    `bronze_valid` cần có cột `_raw_path`.
    Trả `(clean_grouped_df, clean_source_df, quarantine_df, key_conflicts_df)`:
    - `EVENT_ID_CONFLICT` ưu tiên cao hơn `BUSINESS_KEY_CONFLICT` (Phần II mục 8).
    - Bản ghi sạch gộp cả event replay lẫn business replay (T009), giữ nguyên nhiều `transaction_id`
      cho cùng `(channel, external_reference)` (T010).
    """
    source = (
        bronze_valid.where(F.col("dataset_id") == F.lit(dataset_id))
        .withColumn("_payload_hash", F.sha2(_text(payload_cols), 256))
        .withColumn("_event_hash", F.sha2(_text([key_col, *payload_cols]), 256))
        .withColumn(
            "_pointer",
            F.concat_ws(
                ":",
                F.col("topic"),
                F.col("partition").cast("string"),
                F.col("offset").cast("string"),
            ),
        )
    )

    bad_eids = (
        source.groupBy("event_id")
        .agg(F.countDistinct("_event_hash").alias("_ev_vars"))
        .where(F.col("_ev_vars") > 1)
        .select(F.col("event_id").alias("_bad_eid"))
    )
    bad_bks = (
        source.groupBy("dataset_id", key_col)
        .agg(F.countDistinct("_payload_hash").alias("_bk_vars"))
        .where(F.col("_bk_vars") > 1)
        .select(F.col("dataset_id").alias("_bad_ds"), F.col(key_col).alias("_bad_bk"))
    )

    annotated = (
        source.join(bad_eids, source["event_id"] == bad_eids["_bad_eid"], "left")
        .join(
            bad_bks,
            (source["dataset_id"] == bad_bks["_bad_ds"]) & (source[key_col] == bad_bks["_bad_bk"]),
            "left",
        )
        .withColumn("_has_ev", F.col("_bad_eid").isNotNull())
        .withColumn("_has_bk", F.col("_bad_bk").isNotNull())
        .withColumn(
            "_issues",
            F.array_compact(
                F.array(
                    F.when(F.col("_has_ev"), F.lit("EVENT_ID_CONFLICT")),
                    F.when(F.col("_has_bk"), F.lit("BUSINESS_KEY_CONFLICT")),
                )
            ),
        )
    )

    clean_source = annotated.where(F.size("_issues") == 0)
    clean_grouped = clean_source.groupBy("dataset_id", key_col).agg(
        F.lit(1).cast("long").alias("variants"),
        *[F.first(name).alias(name) for name in payload_cols if name != key_col],
        F.min("event_id").alias("first_event_id"),
        F.sort_array(F.collect_set("event_id")).alias("event_ids"),
        F.count(F.lit(1)).cast("int").alias("replay_count"),
        F.sort_array(F.collect_list("_pointer")).alias("source_pointers"),
        F.min("kafka_timestamp").alias("first_kafka_timestamp"),
        F.min("ingested_at").alias("first_ingested_at"),
    )

    conflicted = annotated.where(F.size("_issues") > 0)
    quarantine_df = conflicted.select(
        F.col("dataset_id").cast("string").alias("dataset_id"),
        F.to_date(F.col("business_date"), "yyyy-MM-dd").alias("business_date"),
        F.lit(run_id).alias("run_id"),
        F.lit(STAGE_SILVER_DEDUP).alias("stage"),
        F.lit(entity).alias("entity"),
        F.col(key_col).cast("string").alias("business_key"),
        F.element_at("_issues", 1).alias("error_code"),
        F.col("_issues").alias("issue_codes"),
        F.concat(F.lit(f"Xung đột khóa {entity}: "), F.concat_ws(", ", "_issues")).alias(
            "error_detail"
        ),
        F.col("_pointer").alias("source_pointer"),
        F.col("_raw_path").cast("string").alias("raw_reference"),
    )

    ch_col = (
        F.col("channel").cast("string")
        if "channel" in bronze_valid.columns
        else F.lit(None).cast("string")
    )
    ext_col = (
        F.col("external_reference").cast("string")
        if "external_reference" in bronze_valid.columns
        else F.lit(None).cast("string")
    )

    kc_base = conflicted.select(
        F.col("dataset_id").cast("string").alias("dataset_id"),
        F.to_date(F.col("business_date"), "yyyy-MM-dd").alias("business_date"),
        F.col(key_col).cast("string").alias("business_key"),
        ch_col.alias("channel"),
        ext_col.alias("external_reference"),
        F.col("_has_ev"),
        F.col("_has_bk"),
        F.col("_pointer"),
    )
    key_conflicts_df = (
        kc_base.groupBy("dataset_id", "business_key", "channel", "external_reference")
        .agg(
            F.min("business_date").alias("business_date"),
            F.max(F.col("_has_ev").cast("int")).alias("_any_ev"),
            F.max(F.col("_has_bk").cast("int")).alias("_any_bk"),
            F.sort_array(F.collect_list("_pointer")).alias("source_pointers"),
        )
        .withColumn(
            "issue_codes",
            F.array_compact(
                F.array(
                    F.when(F.col("_any_ev") > 0, F.lit("EVENT_ID_CONFLICT")),
                    F.when(F.col("_any_bk") > 0, F.lit("BUSINESS_KEY_CONFLICT")),
                )
            ),
        )
        .select(
            "dataset_id",
            "business_date",
            F.lit(SIDE_INTERNAL).alias("side_origin"),
            F.lit(entity).alias("entity"),
            "business_key",
            "channel",
            "external_reference",
            F.element_at("issue_codes", 1).alias("conflict_code"),
            "issue_codes",
            "source_pointers",
            F.lit(run_id).alias("run_id"),
        )
    )
    return clean_grouped, clean_source, quarantine_df, key_conflicts_df


def split_statement_conflicts_spark(
    flat_statements: DataFrame,
    *,
    run_id: str,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Tách `statement_record_id` xung đột nội dung (`variants > 1`) khỏi Silver sao kê.

    Giữ nguyên nhiều `statement_record_id` khác nhau cho cùng match key (T011).
    """
    clean_flat = flat_statements.where(F.col("variants") == 1)
    conflicted = flat_statements.where(F.col("variants") > 1)

    ptr_col = F.concat_ws(
        ":",
        F.col("source_file"),
        F.col("source_sha256"),
        F.col("source_line").cast("string"),
    )
    quarantine_df = conflicted.select(
        F.col("dataset_id").cast("string").alias("dataset_id"),
        F.to_date(F.col("business_date"), "yyyy-MM-dd").alias("business_date"),
        F.lit(run_id).alias("run_id"),
        F.lit(STAGE_SILVER_DEDUP).alias("stage"),
        F.lit("statement").alias("entity"),
        F.col("statement_record_id").cast("string").alias("business_key"),
        F.lit("BUSINESS_KEY_CONFLICT").alias("error_code"),
        F.array(F.lit("BUSINESS_KEY_CONFLICT")).alias("issue_codes"),
        F.lit("Xung đột khóa sao kê: cùng statement_record_id khác nội dung").alias("error_detail"),
        ptr_col.alias("source_pointer"),
        F.col("source_file").cast("string").alias("raw_reference"),
    )
    key_conflicts_df = conflicted.select(
        F.col("dataset_id").cast("string").alias("dataset_id"),
        F.to_date(F.col("business_date"), "yyyy-MM-dd").alias("business_date"),
        F.lit(SIDE_STATEMENT).alias("side_origin"),
        F.lit("statement").alias("entity"),
        F.col("statement_record_id").cast("string").alias("business_key"),
        F.col("channel").cast("string").alias("channel"),
        F.col("external_reference").cast("string").alias("external_reference"),
        F.lit("BUSINESS_KEY_CONFLICT").alias("conflict_code"),
        F.array(F.lit("BUSINESS_KEY_CONFLICT")).alias("issue_codes"),
        F.array(ptr_col).alias("source_pointers"),
        F.lit(run_id).alias("run_id"),
    )
    return clean_flat, quarantine_df, key_conflicts_df
