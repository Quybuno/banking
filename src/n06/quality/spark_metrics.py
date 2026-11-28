"""Số liệu chất lượng bằng Spark SQL thuần (TASK T32): control totals, đếm tầng, khóa bị loại."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from n06.quality.metrics import PARSE_ERROR_ENTITY, ControlTotalRow, LayerCounts, RejectedKey

BIGINT_MAX = 2**63 - 1


def compute_group_totals_spark(
    silver: DataFrame, *, entity: str, key_col: str, group_col: str
) -> list[ControlTotalRow]:
    """Count, distinct keys và tổng tiền theo `(business_date, currency, <group_col>)`."""
    rows = (
        silver.groupBy("business_date", "currency", group_col)
        .agg(
            F.count(F.lit(1)).alias("record_count"),
            F.countDistinct(key_col).alias("distinct_keys"),
            # Cộng bằng decimal(38,0): tổng long tràn âm thầm khi tắt ANSI.
            F.sum(F.col("amount_minor").cast(DecimalType(38, 0))).alias("total"),
        )
        .collect()
    )
    out: list[ControlTotalRow] = []
    for row in rows:
        total = int(row["total"] or 0)
        if total > BIGINT_MAX:
            raise OverflowError(f"Tổng tiền {entity} {row['business_date']} vượt BIGINT: {total}")
        out.append(
            ControlTotalRow(
                entity=entity,
                business_date=row["business_date"],
                currency=str(row["currency"]),
                status_or_side=str(row[group_col]),
                record_count=int(row["record_count"]),
                amount_minor_total=total,
                distinct_keys=int(row["distinct_keys"]),
            )
        )
    return sorted(out, key=lambda r: r.group_key())


def compute_control_totals_spark(
    silver_transactions: DataFrame,
    silver_ledger_entries: DataFrame,
) -> list[ControlTotalRow]:
    """Control totals tách riêng hai entity nội bộ."""
    return compute_group_totals_spark(
        silver_ledger_entries, entity="ledger_entry", key_col="entry_id", group_col="side"
    ) + compute_group_totals_spark(
        silver_transactions, entity="transaction", key_col="transaction_id", group_col="status"
    )


def _kafka_pointer() -> Column:
    return F.concat_ws(
        ":", F.col("topic"), F.col("partition").cast("string"), F.col("offset").cast("string")
    )


def statement_pointer() -> Column:
    return F.concat_ws(
        ":", F.col("source_file"), F.col("source_sha256"), F.col("source_line").cast("string")
    )


def layer_counts_spark(
    physical: DataFrame,
    silver: DataFrame,
    quarantine: DataFrame | None,
    *,
    entity: str,
    key_col: str,
    pointer: Column,
    fallback_date: date,
    event_col: str | None = None,
) -> list[LayerCounts]:
    """Đếm theo `business_date` từ bản ghi vật lý (Bronze hoặc dòng sao kê) tới Silver.

    Một bản ghi vật lý bị tính là "bị loại" khi khóa của nó không có trong Silver, hoặc chính dòng
    đó đã vào quarantine. Phần còn lại là bản ghi nghiệp vụ duy nhất cộng replay.
    """
    silver_keys = silver.select(F.col(key_col).alias("_silver_key")).distinct()
    flagged = physical.withColumn(
        "_date",
        F.coalesce(F.to_date(F.col("business_date"), "yyyy-MM-dd"), F.lit(fallback_date)),
    ).withColumn("_ptr", pointer)
    flagged = flagged.join(silver_keys, F.col(key_col) == F.col("_silver_key"), "left")
    is_rejected = F.col("_silver_key").isNull()
    if quarantine is not None:
        pointers = (
            quarantine.where(F.col("entity") == entity)
            .select(F.col("source_pointer").alias("_q_ptr"))
            .distinct()
        )
        flagged = flagged.join(pointers, F.col("_ptr") == F.col("_q_ptr"), "left")
        is_rejected = is_rejected | F.col("_q_ptr").isNotNull()
    aggregates = [
        F.count(F.lit(1)).alias("physical"),
        F.sum(is_rejected.cast("long")).alias("rejected"),
    ]
    if event_col is not None:
        aggregates.append(F.countDistinct(event_col).alias("events"))
    seen = {r["_date"]: r for r in flagged.groupBy("_date").agg(*aggregates).collect()}
    unique = {
        r["business_date"]: int(r["count"])
        for r in silver.groupBy("business_date").count().collect()
    }
    out: list[LayerCounts] = []
    for day in sorted(set(seen) | set(unique)):
        row = seen.get(day)
        out.append(
            LayerCounts(
                entity=entity,
                business_date=day,
                physical_records=int(row["physical"]) if row else 0,
                rejected_records=int(row["rejected"]) if row else 0,
                unique_business_records=unique.get(day, 0),
                unique_events=(int(row["events"]) if row else 0) if event_col else None,
            )
        )
    return out


def parse_error_counts(parse_errors: Sequence[tuple[date, int]]) -> list[LayerCounts]:
    """Record không đọc được: vật lý và bị loại, gắn ngày ingestion (ADR-0004b Q1)."""
    by_date: dict[date, int] = {}
    for day, count in parse_errors:
        by_date[day] = by_date.get(day, 0) + count
    return [
        LayerCounts(PARSE_ERROR_ENTITY, day, count, count, 0)
        for day, count in sorted(by_date.items())
    ]


def rejected_keys_spark(
    physical: DataFrame, silver: DataFrame, *, entity: str, key_col: str, group_col: str
) -> list[RejectedKey]:
    """Khóa có trong Bronze nhưng không vào Silver, kèm khoảng số tiền theo từng nhóm đối chiếu."""
    missing = physical.where(F.col(key_col).isNotNull()).join(
        silver.select(key_col).distinct(), key_col, "left_anti"
    )
    rows = (
        missing.withColumn("_date", F.to_date(F.col("business_date"), "yyyy-MM-dd"))
        .where(
            F.col("_date").isNotNull()
            & F.col("currency").isNotNull()
            & F.col(group_col).isNotNull()
            & F.col("amount_minor").isNotNull()
        )
        .groupBy(key_col, "_date", "currency", group_col)
        .agg(F.min("amount_minor").alias("low"), F.max("amount_minor").alias("high"))
        .collect()
    )
    return [
        RejectedKey(
            entity=entity,
            business_date=row["_date"].isoformat(),
            currency=str(row["currency"]),
            status_or_side=str(row[group_col]),
            business_key=str(row[key_col]),
            min_amount_minor=int(row["low"]),
            max_amount_minor=int(row["high"]),
        )
        for row in rows
    ]
