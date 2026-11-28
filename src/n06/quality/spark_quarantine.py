"""Hàm Spark cho Quarantine (TASK T30, ADR-0004b §3.6 & Q1).

Dùng hoàn toàn Spark SQL functions (không Python UDF cho biến đổi nghiệp vụ, đặc tả I.3) để:
1. Chuyển Bronze `parse_error` -> `quarantine.records` (`stage = 'BRONZE_PARSE'`,
   `business_date = NULL`, phân vùng theo `ingestion_date` của batch Raw).
2. Kiểm tra nghiệp vụ Silver cho `transactions` (5 lỗi theo thứ tự ưu tiên Phần II mục 8:
   `ENVELOPE_PAYLOAD_MISMATCH` -> `BUSINESS_DATE_MISMATCH` -> `SAME_ACCOUNT_TRANSFER` ->
   `UNKNOWN_ACCOUNT` -> `ACCOUNT_CURRENCY_MISMATCH`), tách bản ghi sạch và bản ghi quarantine.
3. Kiểm tra nghiệp vụ Silver cho `ledger_entries` (4 lỗi đơn lẻ + 3 lỗi nhóm `LEDGER_INCOMPLETE`,
   `LEDGER_UNBALANCED`, `LEDGER_TRANSACTION_MISMATCH`), tách bản ghi sạch và bản ghi quarantine.
4. Gom bản ghi sao kê / tài khoản không hợp lệ vào đúng schema `quarantine.records`.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DateType, StringType, StructField, StructType

from n06.quality.quarantine import STAGE_BRONZE_PARSE, STAGE_SILVER_BUSINESS

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"

QUARANTINE_SCHEMA = StructType(
    [
        StructField("dataset_id", StringType(), True),
        StructField("business_date", DateType(), True),
        StructField("run_id", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("entity", StringType(), True),
        StructField("business_key", StringType(), True),
        StructField("error_code", StringType(), False),
        StructField("issue_codes", ArrayType(StringType()), False),
        StructField("error_detail", StringType(), False),
        StructField("source_pointer", StringType(), False),
        StructField("raw_reference", StringType(), False),
    ]
)


def empty_quarantine_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], schema=QUARANTINE_SCHEMA)


def _pointer_col() -> Column:
    return F.concat_ws(
        ":", F.col("topic"), F.col("partition").cast("string"), F.col("offset").cast("string")
    )


def _envelope_mismatch_cond(dataset_id: str) -> Column:
    ev_ts = F.to_timestamp(F.col("event_time"), TS_FORMAT)
    emit_ts = F.to_timestamp(F.col("emitted_at"), TS_FORMAT)
    return (
        F.col("envelope_dataset_id").isNull()
        | (F.col("envelope_dataset_id") != F.lit(dataset_id))
        | F.col("dataset_id").isNull()
        | (F.col("dataset_id") != F.lit(dataset_id))
        | F.col("envelope_event_time").isNull()
        | (F.col("envelope_event_time") != F.col("event_time"))
        | (emit_ts.isNotNull() & ev_ts.isNotNull() & (emit_ts < ev_ts))
    )


def _business_date_mismatch_cond(timezone: str) -> Column:
    ev_ts = F.to_timestamp(F.col("event_time"), TS_FORMAT)
    biz_dt = F.to_date(F.col("business_date"), "yyyy-MM-dd")
    local_dt = F.to_date(F.from_utc_timestamp(ev_ts, timezone))
    return ev_ts.isNull() | biz_dt.isNull() | local_dt.isNull() | (local_dt != biz_dt)


def bronze_parse_errors_to_quarantine(
    parse_errors: DataFrame,
    *,
    dataset_id: str,
    run_id: str,
    raw_path: str,
) -> DataFrame:
    """Chuyển Bronze `entity=parse_error` của một batch sang schema `quarantine.records`.

    Theo ADR-0004b Q1: cột `business_date` là `NULL` (caller phân vùng bằng `batch.ingestion_date`).
    """
    filtered = parse_errors.where(
        F.col("dataset_id").isNull() | (F.col("dataset_id") == F.lit(dataset_id))
    )
    return filtered.select(
        F.col("dataset_id").cast("string").alias("dataset_id"),
        F.lit(None).cast("date").alias("business_date"),
        F.lit(run_id).alias("run_id"),
        F.lit(STAGE_BRONZE_PARSE).alias("stage"),
        F.col("entity").cast("string").alias("entity"),
        F.col("business_key").cast("string").alias("business_key"),
        F.col("error_code").cast("string").alias("error_code"),
        F.coalesce(F.col("issue_codes"), F.array(F.col("error_code"))).alias("issue_codes"),
        F.coalesce(F.col("error_detail"), F.col("error_code")).alias("error_detail"),
        _pointer_col().alias("source_pointer"),
        F.lit(raw_path).alias("raw_reference"),
    )


def validate_bronze_transactions_spark(
    bronze_tx: DataFrame,
    accounts_df: DataFrame,
    *,
    dataset_id: str,
    business_timezone: str,
    run_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Kiểm tra 5 lỗi nghiệp vụ trên Bronze `transaction` (phải có cột `_raw_path`).

    Trả `(valid_bronze_df, quarantine_df)` theo đúng thứ tự ưu tiên Phần II mục 8:
    ENVELOPE_PAYLOAD_MISMATCH -> BUSINESS_DATE_MISMATCH -> SAME_ACCOUNT_TRANSFER ->
    UNKNOWN_ACCOUNT -> ACCOUNT_CURRENCY_MISMATCH.
    """
    acc_from = F.broadcast(
        accounts_df.select(
            F.col("account_id").alias("_af_id"),
            F.col("currency").alias("_af_cur"),
        )
    )
    acc_to = F.broadcast(
        accounts_df.select(
            F.col("account_id").alias("_at_id"),
            F.col("currency").alias("_at_cur"),
        )
    )

    scoped = bronze_tx.where(
        (F.col("dataset_id") == F.lit(dataset_id))
        | (F.col("envelope_dataset_id") == F.lit(dataset_id))
    )
    enriched = scoped.join(acc_from, F.col("account_from") == F.col("_af_id"), "left").join(
        acc_to, F.col("account_to") == F.col("_at_id"), "left"
    )

    env_err = _envelope_mismatch_cond(dataset_id)
    date_err = _business_date_mismatch_cond(business_timezone)
    same_acc_err = (
        F.col("account_from").isNotNull()
        & F.col("account_to").isNotNull()
        & (F.col("account_from") == F.col("account_to"))
    )
    unknown_acc_err = F.col("_af_id").isNull() | F.col("_at_id").isNull()
    cur_err = (
        ~unknown_acc_err
        & F.col("currency").isNotNull()
        & ((F.col("_af_cur") != F.col("currency")) | (F.col("_at_cur") != F.col("currency")))
    )

    issue_array = F.array_compact(
        F.array(
            F.when(env_err, F.lit("ENVELOPE_PAYLOAD_MISMATCH")),
            F.when(date_err, F.lit("BUSINESS_DATE_MISMATCH")),
            F.when(same_acc_err, F.lit("SAME_ACCOUNT_TRANSFER")),
            F.when(unknown_acc_err, F.lit("UNKNOWN_ACCOUNT")),
            F.when(cur_err, F.lit("ACCOUNT_CURRENCY_MISMATCH")),
        )
    )
    annotated = enriched.withColumn("_issues", issue_array)

    valid_cols = bronze_tx.columns
    valid_df = annotated.where(F.size("_issues") == 0).select(*valid_cols)

    quarantine_df = annotated.where(F.size("_issues") > 0).select(
        F.coalesce(F.col("dataset_id"), F.col("envelope_dataset_id")).alias("dataset_id"),
        F.to_date(F.col("business_date"), "yyyy-MM-dd").alias("business_date"),
        F.lit(run_id).alias("run_id"),
        F.lit(STAGE_SILVER_BUSINESS).alias("stage"),
        F.lit("transaction").alias("entity"),
        F.col("transaction_id").cast("string").alias("business_key"),
        F.element_at("_issues", 1).alias("error_code"),
        F.col("_issues").alias("issue_codes"),
        F.concat(F.lit("Vi phạm nghiệp vụ transaction: "), F.concat_ws(", ", "_issues")).alias(
            "error_detail"
        ),
        _pointer_col().alias("source_pointer"),
        F.col("_raw_path").cast("string").alias("raw_reference"),
    )
    return valid_df, quarantine_df


def validate_bronze_ledger_spark(
    bronze_ledger: DataFrame,
    accounts_df: DataFrame,
    *,
    dataset_id: str,
    business_timezone: str,
    run_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Kiểm tra 4 lỗi nghiệp vụ đơn lẻ trên Bronze `ledger_entry` (phải có cột `_raw_path`)."""
    acc = F.broadcast(
        accounts_df.select(
            F.col("account_id").alias("_la_id"),
            F.col("currency").alias("_la_cur"),
        )
    )
    scoped = bronze_ledger.where(
        (F.col("dataset_id") == F.lit(dataset_id))
        | (F.col("envelope_dataset_id") == F.lit(dataset_id))
    )
    enriched = scoped.join(acc, F.col("ledger_account_id") == F.col("_la_id"), "left")

    env_err = _envelope_mismatch_cond(dataset_id)
    date_err = _business_date_mismatch_cond(business_timezone)
    unknown_acc_err = F.col("_la_id").isNull()
    cur_err = (
        ~unknown_acc_err & F.col("currency").isNotNull() & (F.col("_la_cur") != F.col("currency"))
    )

    issue_array = F.array_compact(
        F.array(
            F.when(env_err, F.lit("ENVELOPE_PAYLOAD_MISMATCH")),
            F.when(date_err, F.lit("BUSINESS_DATE_MISMATCH")),
            F.when(unknown_acc_err, F.lit("UNKNOWN_ACCOUNT")),
            F.when(cur_err, F.lit("ACCOUNT_CURRENCY_MISMATCH")),
        )
    )
    annotated = enriched.withColumn("_issues", issue_array)

    valid_cols = bronze_ledger.columns
    valid_df = annotated.where(F.size("_issues") == 0).select(*valid_cols)

    quarantine_df = annotated.where(F.size("_issues") > 0).select(
        F.coalesce(F.col("dataset_id"), F.col("envelope_dataset_id")).alias("dataset_id"),
        F.to_date(F.col("business_date"), "yyyy-MM-dd").alias("business_date"),
        F.lit(run_id).alias("run_id"),
        F.lit(STAGE_SILVER_BUSINESS).alias("stage"),
        F.lit("ledger_entry").alias("entity"),
        F.col("entry_id").cast("string").alias("business_key"),
        F.element_at("_issues", 1).alias("error_code"),
        F.col("_issues").alias("issue_codes"),
        F.concat(F.lit("Vi phạm nghiệp vụ ledger_entry: "), F.concat_ws(", ", "_issues")).alias(
            "error_detail"
        ),
        _pointer_col().alias("source_pointer"),
        F.col("_raw_path").cast("string").alias("raw_reference"),
    )
    return valid_df, quarantine_df


def validate_ledger_groups_spark(
    silver_tx: DataFrame,
    silver_ledger: DataFrame,
    *,
    run_id: str,
    raw_reference: str,
) -> tuple[DataFrame, DataFrame]:
    """Kiểm nhóm bút toán sổ cái ở Silver: `LEDGER_INCOMPLETE`, `LEDGER_UNBALANCED`,
    `LEDGER_TRANSACTION_MISMATCH`.

    Trả `(valid_silver_ledger_df, quarantine_df)`.
    """
    agg = silver_ledger.groupBy("dataset_id", "transaction_id").agg(
        F.sum(F.when(F.col("side") == "DEBIT", 1).otherwise(0)).alias("debit_cnt"),
        F.sum(F.when(F.col("side") == "CREDIT", 1).otherwise(0)).alias("credit_cnt"),
        F.sum(F.when(F.col("side") == "DEBIT", F.col("amount_minor")).otherwise(0)).alias(
            "debit_sum"
        ),
        F.sum(F.when(F.col("side") == "CREDIT", F.col("amount_minor")).otherwise(0)).alias(
            "credit_sum"
        ),
        F.min(F.when(F.col("side") == "DEBIT", F.col("ledger_account_id"))).alias("debit_acc"),
        F.min(F.when(F.col("side") == "CREDIT", F.col("ledger_account_id"))).alias("credit_acc"),
        F.min("currency").alias("le_cur"),
        F.max("currency").alias("le_cur_max"),
        F.min("event_time").alias("le_ev_min"),
        F.max("event_time").alias("le_ev_max"),
        F.min("business_date").alias("le_bd_min"),
        F.max("business_date").alias("le_bd_max"),
        F.min(F.element_at("source_pointers", 1)).alias("le_ptr"),
    )

    tx_proj = silver_tx.select(
        F.col("dataset_id").alias("tx_ds"),
        F.col("transaction_id").alias("tx_id"),
        F.col("status").alias("tx_status"),
        F.col("account_from").alias("tx_from"),
        F.col("account_to").alias("tx_to"),
        F.col("amount_minor").alias("tx_amt"),
        F.col("currency").alias("tx_cur"),
        F.col("event_time").alias("tx_ev"),
        F.col("business_date").alias("tx_bd"),
        F.element_at("source_pointers", 1).alias("tx_ptr"),
    )

    joined = tx_proj.join(
        agg,
        (F.col("tx_ds") == agg["dataset_id"]) & (F.col("tx_id") == agg["transaction_id"]),
        "full_outer",
    )

    d_cnt = F.coalesce(F.col("debit_cnt"), F.lit(0))
    c_cnt = F.coalesce(F.col("credit_cnt"), F.lit(0))

    incomplete_err = (F.col("tx_status") == "SUCCESS") & ((d_cnt != 1) | (c_cnt != 1))
    unbalanced_err = (
        (F.col("tx_status") == "SUCCESS")
        & (d_cnt >= 1)
        & (c_cnt >= 1)
        & (F.col("debit_sum") != F.col("credit_sum"))
    )
    orphan_ledger = F.col("tx_id").isNull()
    failed_with_ledger = (F.col("tx_status") == "FAILED") & ((d_cnt + c_cnt) > 0)
    field_mismatch = (
        (F.col("tx_status") == "SUCCESS")
        & (d_cnt == 1)
        & (c_cnt == 1)
        & (F.col("debit_sum") == F.col("credit_sum"))
        & (
            (F.col("debit_acc") != F.col("tx_from"))
            | (F.col("credit_acc") != F.col("tx_to"))
            | (F.col("debit_sum") != F.col("tx_amt"))
            | (F.col("le_cur") != F.col("tx_cur"))
            | (F.col("le_cur_max") != F.col("tx_cur"))
            | (F.col("le_ev_min") != F.col("tx_ev"))
            | (F.col("le_ev_max") != F.col("tx_ev"))
            | (F.col("le_bd_min") != F.col("tx_bd"))
            | (F.col("le_bd_max") != F.col("tx_bd"))
        )
    )
    mismatch_err = orphan_ledger | failed_with_ledger | field_mismatch

    issues = F.array_compact(
        F.array(
            F.when(incomplete_err, F.lit("LEDGER_INCOMPLETE")),
            F.when(unbalanced_err, F.lit("LEDGER_UNBALANCED")),
            F.when(mismatch_err, F.lit("LEDGER_TRANSACTION_MISMATCH")),
        )
    )
    bad_groups = joined.withColumn("_issues", issues).where(F.size("_issues") > 0)

    bad_keys = bad_groups.select(
        F.coalesce(F.col("tx_ds"), agg["dataset_id"]).alias("_bad_ds"),
        F.coalesce(F.col("tx_id"), agg["transaction_id"]).alias("_bad_tx"),
    ).distinct()

    valid_ledger = silver_ledger.join(
        bad_keys,
        (silver_ledger["dataset_id"] == bad_keys["_bad_ds"])
        & (silver_ledger["transaction_id"] == bad_keys["_bad_tx"]),
        "left_anti",
    )

    quarantine_df = bad_groups.select(
        F.coalesce(F.col("tx_ds"), agg["dataset_id"]).alias("dataset_id"),
        F.coalesce(F.col("tx_bd"), F.col("le_bd_min")).alias("business_date"),
        F.lit(run_id).alias("run_id"),
        F.lit(STAGE_SILVER_BUSINESS).alias("stage"),
        F.lit("ledger_entry").alias("entity"),
        F.coalesce(F.col("tx_id"), agg["transaction_id"]).alias("business_key"),
        F.element_at("_issues", 1).alias("error_code"),
        F.col("_issues").alias("issue_codes"),
        F.concat(F.lit("Vi phạm nhóm sổ cái: "), F.concat_ws(", ", "_issues")).alias(
            "error_detail"
        ),
        F.coalesce(F.col("le_ptr"), F.col("tx_ptr"), F.lit("unknown")).alias("source_pointer"),
        F.lit(raw_reference).alias("raw_reference"),
    )
    return valid_ledger, quarantine_df


def partition_quarantine_by_date(
    df: DataFrame, *, fallback_date: date
) -> list[tuple[date, DataFrame, int]]:
    """Tách DataFrame `quarantine.records` theo ngày phân vùng (dùng `fallback_date` khi
    `business_date IS NULL` theo ADR-0004b Q1, giữ nguyên cột `business_date` là NULL trong file).
    """
    with_part = df.withColumn(
        "_part_date", F.coalesce(F.col("business_date"), F.lit(fallback_date).cast("date"))
    )
    counts = {
        row["_part_date"]: int(row["count"])
        for row in with_part.groupBy("_part_date").count().collect()
    }
    cols = list(QUARANTINE_SCHEMA.fieldNames())
    return [
        (
            part_date,
            with_part.where(F.col("_part_date") == F.lit(part_date)).select(*cols),
            counts[part_date],
        )
        for part_date in sorted(counts)
    ]
