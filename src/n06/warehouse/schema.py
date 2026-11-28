"""Mô tả bảng mart theo ADR-0005: cột, kiểu, khóa chính, phạm vi thay thế khi publish.

Danh sách này phải khớp migration `sql/migrations/mart/0001_init_mart_schema.up.sql`; test
integration so khớp với `information_schema` của PostgreSQL thật.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

BIGINT_MIN: Final = -(2**63)
BIGINT_MAX: Final = 2**63 - 1
INT_MIN: Final = -(2**31)
INT_MAX: Final = 2**31 - 1
SMALLINT_MIN: Final = -(2**15)
SMALLINT_MAX: Final = 2**15 - 1

INTEGER_RANGES: Final[dict[str, tuple[int, int]]] = {
    "BIGINT": (BIGINT_MIN, BIGINT_MAX),
    "INT": (INT_MIN, INT_MAX),
    "SMALLINT": (SMALLINT_MIN, SMALLINT_MAX),
}


@dataclass(frozen=True)
class ColumnSpec:
    """Một cột: tên, kiểu PostgreSQL (viết hoa) và khả năng NULL."""

    name: str
    sql_type: str
    nullable: bool = False


@dataclass(frozen=True)
class TableSpec:
    """Một bảng mart: danh sách cột có thứ tự, khóa chính và loại bảng."""

    name: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    kind: str  # "dim" (upsert) hoặc "fact" (thay thế theo phạm vi ngày)
    dataset_scoped: bool  # có cột dataset_id cần khớp dataset đang publish

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(col.name for col in self.columns)

    @property
    def non_key_columns(self) -> tuple[str, ...]:
        return tuple(name for name in self.column_names if name not in self.primary_key)


def _c(name: str, sql_type: str, *, null: bool = False) -> ColumnSpec:
    return ColumnSpec(name=name, sql_type=sql_type, nullable=null)


DIM_DATE: Final = TableSpec(
    name="dim_date",
    columns=(
        _c("date_key", "INT"),
        _c("calendar_date", "DATE"),
        _c("year", "SMALLINT"),
        _c("quarter", "SMALLINT"),
        _c("month", "SMALLINT"),
        _c("day", "SMALLINT"),
        _c("day_of_week", "SMALLINT"),
    ),
    primary_key=("date_key",),
    kind="dim",
    dataset_scoped=False,
)

DIM_ACCOUNT: Final = TableSpec(
    name="dim_account",
    columns=(
        _c("dataset_id", "TEXT"),
        _c("account_id", "TEXT"),
        _c("account_type", "TEXT"),
        _c("currency", "TEXT"),
        _c("opening_balance_minor", "BIGINT"),
        _c("opening_business_date", "DATE"),
        _c("run_id", "TEXT"),
    ),
    primary_key=("dataset_id", "account_id"),
    kind="dim",
    dataset_scoped=True,
)

DIM_CHANNEL: Final = TableSpec(
    name="dim_channel",
    columns=(_c("channel", "TEXT"), _c("channel_label", "TEXT")),
    primary_key=("channel",),
    kind="dim",
    dataset_scoped=False,
)

DIM_TRANSACTION_TYPE: Final = TableSpec(
    name="dim_transaction_type",
    columns=(_c("transaction_type", "TEXT"), _c("description", "TEXT")),
    primary_key=("transaction_type",),
    kind="dim",
    dataset_scoped=False,
)

FACT_TRANSACTIONS: Final = TableSpec(
    name="fact_transactions",
    columns=(
        _c("dataset_id", "TEXT"),
        _c("transaction_id", "TEXT"),
        _c("business_date", "DATE"),
        _c("date_key", "INT"),
        _c("external_reference", "TEXT"),
        _c("account_from", "TEXT"),
        _c("account_to", "TEXT"),
        _c("channel", "TEXT"),
        _c("transaction_type", "TEXT"),
        _c("currency", "TEXT"),
        _c("status", "TEXT"),
        _c("amount_minor", "BIGINT"),
        _c("event_time", "TIMESTAMPTZ"),
        _c("replay_count", "INT"),
        _c("source_pointer", "TEXT"),
        _c("run_id", "TEXT"),
    ),
    primary_key=("dataset_id", "transaction_id"),
    kind="fact",
    dataset_scoped=True,
)

FACT_DAILY_BALANCES: Final = TableSpec(
    name="fact_daily_balances",
    columns=(
        _c("dataset_id", "TEXT"),
        _c("account_id", "TEXT"),
        _c("business_date", "DATE"),
        _c("currency", "TEXT"),
        _c("date_key", "INT"),
        _c("opening_balance_minor", "BIGINT"),
        _c("debit_minor", "BIGINT"),
        _c("credit_minor", "BIGINT"),
        _c("closing_balance_minor", "BIGINT"),
        _c("ledger_entry_count", "INT"),
        _c("run_id", "TEXT"),
    ),
    primary_key=("dataset_id", "account_id", "business_date", "currency"),
    kind="fact",
    dataset_scoped=True,
)

FACT_RECONCILIATION: Final = TableSpec(
    name="fact_reconciliation",
    columns=(
        _c("dataset_id", "TEXT"),
        _c("channel", "TEXT"),
        _c("external_reference", "TEXT"),
        _c("business_date", "DATE"),
        _c("date_key", "INT"),
        _c("recon_status", "TEXT"),
        _c("issue_codes", "TEXT[]"),
        _c("transaction_id", "TEXT", null=True),
        _c("internal_amount_minor", "BIGINT", null=True),
        _c("internal_currency", "TEXT", null=True),
        _c("internal_status", "TEXT", null=True),
        _c("statement_amount_minor", "BIGINT", null=True),
        _c("statement_currency", "TEXT", null=True),
        _c("statement_status", "TEXT", null=True),
        _c("internal_record_count", "INT"),
        _c("statement_record_count", "INT"),
        _c("anchor_event_time", "TIMESTAMPTZ", null=True),
        _c("discrepancy_age_seconds", "BIGINT", null=True),
        _c("as_of_time", "TIMESTAMPTZ"),
        _c("last_evaluated_at", "TIMESTAMPTZ"),
        _c("first_seen_at", "TIMESTAMPTZ", null=True),
        _c("resolved_at", "TIMESTAMPTZ", null=True),
        _c("recon_rule_version", "TEXT"),
        _c("source_pointers", "TEXT[]"),
        _c("run_id", "TEXT"),
        _c("input_snapshot_id", "TEXT", null=True),
    ),
    primary_key=("dataset_id", "channel", "external_reference"),
    kind="fact",
    dataset_scoped=True,
)

# Thứ tự publish: dimension trước, fact sau (fact tham chiếu dim_date, dim_channel, dim_*type).
DIMENSION_TABLES: Final = (DIM_DATE, DIM_CHANNEL, DIM_TRANSACTION_TYPE, DIM_ACCOUNT)
FACT_TABLES: Final = (FACT_TRANSACTIONS, FACT_DAILY_BALANCES, FACT_RECONCILIATION)
ALL_TABLES: Final = DIMENSION_TABLES + FACT_TABLES
TABLES_BY_NAME: Final = {table.name: table for table in ALL_TABLES}
