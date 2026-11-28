"""Tính control totals bằng Spark SQL thuần cho `transactions` và `ledger_entries` (TASK T32)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from n06.quality.metrics import ControlTotalRow


def compute_control_totals_spark(
    silver_transactions: DataFrame,
    silver_ledger_entries: DataFrame,
) -> list[ControlTotalRow]:
    """Tính control totals tách riêng 2 entity bằng Spark SQL (số nguyên `int`, không float)."""
    tx_rows = (
        silver_transactions.groupBy("business_date", "currency", "status")
        .agg(
            F.count(F.lit(1)).alias("record_count"),
            F.sum(F.col("amount_minor").cast("long")).alias("amount_minor_total"),
        )
        .collect()
    )
    le_rows = (
        silver_ledger_entries.groupBy("business_date", "currency", "side")
        .agg(
            F.count(F.lit(1)).alias("record_count"),
            F.sum(F.col("amount_minor").cast("long")).alias("amount_minor_total"),
        )
        .collect()
    )

    out: list[ControlTotalRow] = []
    for row in tx_rows:
        out.append(
            ControlTotalRow(
                entity="transaction",
                business_date=row["business_date"],
                currency=str(row["currency"]),
                status_or_side=str(row["status"]),
                record_count=int(row["record_count"]),
                amount_minor_total=int(row["amount_minor_total"] or 0),
            )
        )
    for row in le_rows:
        out.append(
            ControlTotalRow(
                entity="ledger_entry",
                business_date=row["business_date"],
                currency=str(row["currency"]),
                status_or_side=str(row["side"]),
                record_count=int(row["record_count"]),
                amount_minor_total=int(row["amount_minor_total"] or 0),
            )
        )
    return sorted(out, key=lambda r: r.group_key())
