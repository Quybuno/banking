"""Đối soát hai phía theo match key tại một `as_of_time` (TASK T33; ADR-0006, spec Phần I mục 7).

Hàm nhận DataFrame Silver đúng cột của ADR-0004b (transactions, statements, key_conflicts), tham số
`as_of_time` và cấu hình; trả một dòng cho mỗi `(dataset_id, channel, external_reference)` theo cột
của `fact_reconciliation` (ADR-0005 mục 2.3). Không đọc đồng hồ hệ thống, không đọc ground truth.

Cardinality (S336): mỗi phía được gom về đúng một dòng cho mỗi match key (kèm số bản ghi độc lập)
TRƯỚC khi full outer join, nên phép join là 1:1 và không thể nhân dòng. Khóa có nhiều bản ghi độc
lập ở một phía không đi qua phép so thuộc tính mà là `DUPLICATE_OR_CONFLICT` (ADR-0006 D2).

Module này cần PySpark; `n06.reconciliation` (rules, settings) thì không.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from n06.reconciliation.rules import (
    COUNTERPART_QUARANTINED,
    ISSUE_CODE_ORDER,
    KEY_CONFLICT_COLUMNS,
    KEY_CONFLICT_ENTITIES,
    MONEY_COLUMN,
    MONEY_SPARK_TYPE,
    SOURCE_CONFLICT_CODES,
    STATEMENT_COLUMNS,
    TRANSACTION_COLUMNS,
    ReconStatus,
)
from n06.reconciliation.settings import ReconciliationError, ReconciliationSettings

MATCH_KEY: tuple[str, ...] = ("dataset_id", "channel", "external_reference")

RESULT_COLUMNS: tuple[str, ...] = (
    "dataset_id",
    "channel",
    "external_reference",
    "business_date",
    "date_key",
    "recon_status",
    "issue_codes",
    "transaction_id",
    "internal_amount_minor",
    "internal_currency",
    "internal_status",
    "statement_amount_minor",
    "statement_currency",
    "statement_status",
    "internal_record_count",
    "statement_record_count",
    "anchor_event_time",
    "discrepancy_age_seconds",
    "as_of_time",
    "last_evaluated_at",
    "first_seen_at",
    "resolved_at",
    "recon_rule_version",
    "source_pointers",
    "run_id",
    "input_snapshot_id",
)
"""Cột đầu ra: đúng ADR-0005 mục 2.3, thêm `input_snapshot_id` (spec S361 yêu cầu lưu ở P0)."""


def _empty_strings() -> Column:
    """Mảng chuỗi rỗng; tạo khi gọi hàm, vì `F.array()` cần SparkContext đang hoạt động."""
    return F.array().cast("array<string>")


def _require_columns(df: DataFrame, name: str, required: Sequence[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ReconciliationError(f"{name} thiếu cột Silver bắt buộc: {', '.join(missing)}.")
    if MONEY_COLUMN in required:
        actual = dict(df.dtypes)[MONEY_COLUMN]
        if actual != MONEY_SPARK_TYPE:
            raise ReconciliationError(
                f"{name}.{MONEY_COLUMN} phải là {MONEY_SPARK_TYPE} (số nguyên), nhận {actual}."
            )


def _normalize_as_of(as_of_time: datetime) -> datetime:
    if not isinstance(as_of_time, datetime) or as_of_time.tzinfo is None:
        raise ReconciliationError("as_of_time phải là datetime có múi giờ (UTC).")
    return as_of_time.astimezone(UTC)


def _require_utc_session(df: DataFrame) -> None:
    """Cutoff tính bằng biểu thức Spark nên `spark.sql.session.timeZone` phải là UTC."""
    tz = df.sparkSession.conf.get("spark.sql.session.timeZone")
    if tz not in ("UTC", "Etc/UTC", "GMT", "Z"):
        raise ReconciliationError(
            f"spark.sql.session.timeZone phải là UTC để tính cutoff, nhận {tz!r}."
        )


def _cutoff_column(business_date: Column, settings: ReconciliationSettings) -> Column:
    """Cutoff = 00:00 ngày kế tiếp theo `business_timezone` (đổi sang UTC) + allowed_lateness."""
    next_day_wall = F.to_timestamp(
        F.concat(F.date_format(F.date_add(business_date, 1), "yyyy-MM-dd"), F.lit(" 00:00:00"))
    )
    next_day_utc = F.to_utc_timestamp(next_day_wall, F.lit(settings.business_timezone))
    return (next_day_utc.cast("long") + F.lit(settings.allowed_lateness_seconds)).cast("timestamp")


def _internal_side(transactions: DataFrame, settings: ReconciliationSettings) -> DataFrame:
    tx = transactions.where(
        F.col("channel").isin(*settings.channels) & F.col("status").isin(*settings.statuses)
    )
    return tx.groupBy(*MATCH_KEY).agg(
        F.countDistinct("transaction_id").alias("n_int"),
        F.min(F.struct("event_time", "transaction_id", "amount_minor", "currency", "status")).alias(
            "t"
        ),
        F.min("event_time").alias("t_event"),
        F.min("business_date").alias("t_bd"),
        F.array_sort(F.array_distinct(F.flatten(F.collect_list("source_pointers")))).alias(
            "t_ptrs"
        ),
    )


def _statement_side(
    statements: DataFrame, as_of: Column, settings: ReconciliationSettings
) -> DataFrame:
    visible = statements.where(
        F.col("channel").isin(*settings.channels)
        & F.col("status").isin(*settings.statuses)
        & (F.col("file_available_at") <= as_of)
    )
    # Loại bản sao cùng statement_record_id TRƯỚC khi gom theo match key (S336); lấy bản có
    # `file_available_at` sớm nhất, hòa thì theo delivery_order, file, dòng để kết quả xác định.
    deduped = (
        visible.groupBy("dataset_id", "statement_record_id")
        .agg(
            F.min(
                F.struct(
                    "file_available_at",
                    "delivery_order",
                    "source_file",
                    "source_line",
                    "source_sha256",
                    "channel",
                    "external_reference",
                    "amount_minor",
                    "currency",
                    "status",
                    "event_time",
                    "business_date",
                )
            ).alias("r")
        )
        .select("dataset_id", "statement_record_id", "r.*")
    )
    pointer = F.concat_ws(
        ":", F.col("source_file"), F.col("source_sha256"), F.col("source_line").cast("string")
    )
    return deduped.groupBy(*MATCH_KEY).agg(
        F.countDistinct("statement_record_id").alias("n_st"),
        F.min(
            F.struct("event_time", "statement_record_id", "amount_minor", "currency", "status")
        ).alias("s"),
        F.min("event_time").alias("s_event"),
        F.min("business_date").alias("s_bd"),
        F.array_sort(F.array_distinct(F.collect_list(pointer))).alias("s_ptrs"),
    )


def _key_conflict_side(key_conflicts: DataFrame, settings: ReconciliationSettings) -> DataFrame:
    """Chỉ dùng dòng còn khôi phục được match key; không bịa key (S334, S898)."""
    usable = key_conflicts.where(
        F.col("entity").isin(*KEY_CONFLICT_ENTITIES)
        & F.col("channel").isNotNull()
        & F.col("external_reference").isNotNull()
        & F.col("channel").isin(*settings.channels)
    )
    is_source_conflict = F.col("conflict_code").isin(*SOURCE_CONFLICT_CODES)
    return usable.groupBy(*MATCH_KEY).agg(
        (F.max(is_source_conflict.cast("int")) == 1).alias("has_conflict"),
        (F.max((~is_source_conflict).cast("int")) == 1).alias("has_quarantine"),
        F.array_sort(
            F.array_distinct(F.collect_list(F.when(is_source_conflict, F.col("conflict_code"))))
        ).alias("conflict_codes"),
        F.min("business_date").alias("kc_bd"),
        F.array_sort(F.array_distinct(F.flatten(F.collect_list("source_pointers")))).alias(
            "kc_ptrs"
        ),
    )


def _issue_code_conditions() -> dict[str, Column]:
    """Điều kiện sinh từng mã `issue_codes` (ADR-0006 D6, Q1, Q8); thứ tự theo ISSUE_CODE_ORDER."""
    status_col = F.col("recon_status")
    conditions: dict[str, Column] = {
        ReconStatus.DUPLICATE_OR_CONFLICT.value: F.col("is_duplicate"),
        ReconStatus.CURRENCY_MISMATCH.value: F.col("is_currency_diff"),
        ReconStatus.AMOUNT_MISMATCH.value: F.col("is_amount_diff"),
        ReconStatus.STATUS_MISMATCH.value: F.col("is_status_diff"),
        ReconStatus.PENDING.value: status_col == ReconStatus.PENDING.value,
        ReconStatus.MISSING_EXTERNAL.value: status_col == ReconStatus.MISSING_EXTERNAL.value,
        ReconStatus.MISSING_INTERNAL.value: status_col == ReconStatus.MISSING_INTERNAL.value,
        COUNTERPART_QUARANTINED: F.col("has_quarantine")
        & (status_col != ReconStatus.MATCHED.value),
    }
    for code in SOURCE_CONFLICT_CODES:
        conditions[code] = F.array_contains(F.col("conflict_codes"), code)
    if set(conditions) != set(ISSUE_CODE_ORDER):
        raise ReconciliationError("ISSUE_CODE_ORDER không khớp tập điều kiện mã vấn đề.")
    return conditions


def reconcile_with_diagnostics(
    transactions: DataFrame,
    statements: DataFrame,
    key_conflicts: DataFrame,
    *,
    as_of_time: datetime,
    settings: ReconciliationSettings,
    run_id: str,
    input_snapshot_id: str,
) -> DataFrame:
    """Như `reconcile` nhưng giữ thêm `cutoff_time`, `business_date_disagrees`, `age_clamped`."""
    if not run_id.strip() or not input_snapshot_id.strip():
        raise ReconciliationError("run_id và input_snapshot_id không được rỗng.")
    _require_columns(transactions, "transactions", TRANSACTION_COLUMNS)
    _require_columns(statements, "statements", STATEMENT_COLUMNS)
    _require_columns(key_conflicts, "key_conflicts", KEY_CONFLICT_COLUMNS)
    _require_utc_session(transactions)
    as_of = F.lit(_normalize_as_of(as_of_time))

    # Mỗi phía đã có đúng một dòng cho mỗi match key, nên hai phép full outer join là 1:1.
    joined = (
        _internal_side(transactions, settings)
        .join(_statement_side(statements, as_of, settings), list(MATCH_KEY), "full_outer")
        .join(_key_conflict_side(key_conflicts, settings), list(MATCH_KEY), "full_outer")
    )
    n_int = F.coalesce(F.col("n_int"), F.lit(0))
    n_st = F.coalesce(F.col("n_st"), F.lit(0))
    has_conflict = F.coalesce(F.col("has_conflict"), F.lit(False))
    has_quarantine = F.coalesce(F.col("has_quarantine"), F.lit(False))
    # Khóa chỉ có bản ghi bị quarantine, không còn bản ghi hợp lệ nào, không phải khóa đối soát.
    kept = joined.where((n_int > 0) | (n_st > 0) | has_conflict)

    # Ngày nghiệp vụ của khóa (ADR-0006 Q3): ngày nhỏ nhất của giao dịch nếu có, rồi của sao kê.
    business_date = F.coalesce(F.col("t_bd"), F.col("s_bd"), F.col("kc_bd"))
    duplicate = (n_int > 1) | (n_st > 1) | has_conflict
    comparable = (n_int == 1) & (n_st == 1) & ~duplicate
    stage1 = kept.select(
        *MATCH_KEY,
        business_date.alias("business_date"),
        (
            F.col("t_bd").isNotNull() & F.col("s_bd").isNotNull() & (F.col("t_bd") != F.col("s_bd"))
        ).alias("business_date_disagrees"),
        _cutoff_column(business_date, settings).alias("cutoff_time"),
        n_int.alias("n_int"),
        n_st.alias("n_st"),
        has_quarantine.alias("has_quarantine"),
        F.coalesce(F.col("conflict_codes"), _empty_strings()).alias("conflict_codes"),
        F.col("t"),
        F.col("s"),
        F.coalesce(F.col("t_event"), F.col("s_event")).alias("anchor_candidate"),
        F.array_sort(
            F.array_distinct(
                F.concat(
                    F.coalesce(F.col("t_ptrs"), _empty_strings()),
                    F.coalesce(F.col("s_ptrs"), _empty_strings()),
                    F.coalesce(F.col("kc_ptrs"), _empty_strings()),
                )
            )
        ).alias("source_pointers"),
        duplicate.alias("is_duplicate"),
        (comparable & ~F.col("t.currency").eqNullSafe(F.col("s.currency"))).alias(
            "is_currency_diff"
        ),
        (comparable & ~F.col("t.amount_minor").eqNullSafe(F.col("s.amount_minor"))).alias(
            "is_amount_diff"
        ),
        (comparable & ~F.col("t.status").eqNullSafe(F.col("s.status"))).alias("is_status_diff"),
        comparable.alias("is_comparable"),
    )

    # Bảng ưu tiên mục 7.2, xét từ hàng 1; điều kiện đầu tiên đúng quyết định trạng thái.
    overdue = as_of >= F.col("cutoff_time")
    status = (
        F.when(F.col("is_duplicate"), F.lit(ReconStatus.DUPLICATE_OR_CONFLICT.value))
        .when(F.col("is_currency_diff"), F.lit(ReconStatus.CURRENCY_MISMATCH.value))
        .when(F.col("is_amount_diff"), F.lit(ReconStatus.AMOUNT_MISMATCH.value))
        .when(F.col("is_status_diff"), F.lit(ReconStatus.STATUS_MISMATCH.value))
        .when(F.col("is_comparable"), F.lit(ReconStatus.MATCHED.value))
        .when(~overdue, F.lit(ReconStatus.PENDING.value))
        .when(F.col("n_int") == 1, F.lit(ReconStatus.MISSING_EXTERNAL.value))
        .otherwise(F.lit(ReconStatus.MISSING_INTERNAL.value))
    )
    stage2 = stage1.withColumn("recon_status", status)

    # Tuổi sai lệch (S362): chỉ khóa khác MATCHED; không âm, số âm bị kẹp về 0 và được đếm.
    matched = F.col("recon_status") == ReconStatus.MATCHED.value
    raw_age = as_of.cast("long") - F.col("anchor_candidate").cast("long")
    stage3 = (
        stage2.withColumn(
            "anchor_event_time", F.when(~matched, F.col("anchor_candidate")).cast("timestamp")
        )
        .withColumn(
            "discrepancy_age_seconds",
            F.when(~matched & F.col("anchor_candidate").isNotNull(), F.greatest(raw_age, F.lit(0))),
        )
        .withColumn("age_clamped", ~matched & (raw_age < 0))
    )

    conditions = _issue_code_conditions()
    stage4 = stage3.withColumn(
        "issue_codes",
        F.filter(
            F.array(*[F.when(conditions[code], F.lit(code)) for code in ISSUE_CODE_ORDER]),
            lambda item: item.isNotNull(),
        ),
    )

    amount_int = F.col("n_int") == 1
    amount_st = F.col("n_st") == 1
    return stage4.select(
        "dataset_id",
        "channel",
        "external_reference",
        "business_date",
        F.date_format("business_date", "yyyyMMdd").cast("int").alias("date_key"),
        "recon_status",
        "issue_codes",
        F.when(amount_int, F.col("t.transaction_id")).alias("transaction_id"),
        F.when(amount_int, F.col("t.amount_minor")).alias("internal_amount_minor"),
        F.when(amount_int, F.col("t.currency")).alias("internal_currency"),
        F.when(amount_int, F.col("t.status")).alias("internal_status"),
        F.when(amount_st, F.col("s.amount_minor")).alias("statement_amount_minor"),
        F.when(amount_st, F.col("s.currency")).alias("statement_currency"),
        F.when(amount_st, F.col("s.status")).alias("statement_status"),
        F.col("n_int").cast("int").alias("internal_record_count"),
        F.col("n_st").cast("int").alias("statement_record_count"),
        "anchor_event_time",
        F.col("discrepancy_age_seconds").cast("long").alias("discrepancy_age_seconds"),
        as_of.alias("as_of_time"),
        as_of.alias("last_evaluated_at"),  # ADR-0006 Q9: dùng as_of_time, không dùng đồng hồ máy
        F.lit(None).cast("timestamp").alias("first_seen_at"),  # P1 (T61)
        F.lit(None).cast("timestamp").alias("resolved_at"),  # P1 (T61)
        F.lit(settings.rule_version).alias("recon_rule_version"),
        "source_pointers",
        F.lit(run_id).alias("run_id"),
        F.lit(input_snapshot_id).alias("input_snapshot_id"),
        "cutoff_time",
        "business_date_disagrees",
        "age_clamped",
    )


def reconcile(
    transactions: DataFrame,
    statements: DataFrame,
    key_conflicts: DataFrame,
    *,
    as_of_time: datetime,
    settings: ReconciliationSettings,
    run_id: str,
    input_snapshot_id: str,
) -> DataFrame:
    """Kết quả đối soát hiện tại của mọi match key tại `as_of_time` (cột `RESULT_COLUMNS`).

    Mỗi lần gọi tính lại từ đầu chỉ từ đầu vào và `as_of_time`; cùng đầu vào cho cùng kết quả.
    """
    wide = reconcile_with_diagnostics(
        transactions,
        statements,
        key_conflicts,
        as_of_time=as_of_time,
        settings=settings,
        run_id=run_id,
        input_snapshot_id=input_snapshot_id,
    )
    return wide.select(*RESULT_COLUMNS)


def quality_counters(wide: DataFrame) -> dict[str, int]:
    """Thống kê chất lượng của một lần đối soát (ADR-0006 Q3, Q6) từ `reconcile_with_diagnostics`.

    `business_date_disagreements`: khóa có hai phía mang `business_date` khác nhau.
    `age_clamped`: khóa có `as_of_time` sớm hơn `anchor_event_time` nên tuổi bị kẹp về 0.
    """
    aggregations = [
        F.count(F.lit(1)).alias("keys"),
        F.sum(F.col("business_date_disagrees").cast("int")).alias("business_date_disagreements"),
        F.sum(F.col("age_clamped").cast("int")).alias("age_clamped"),
    ] + [
        F.sum((F.col("recon_status") == status.value).cast("int")).alias(status.value)
        for status in ReconStatus
    ]
    row = wide.agg(*aggregations).collect()[0].asDict()
    return {name: int(value or 0) for name, value in row.items()}
