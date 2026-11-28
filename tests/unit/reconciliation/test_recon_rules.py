"""Test hằng số quy tắc đối soát (TASK T33). Expected chép tay từ bảng 7.2 (spec S341–S350)."""

from __future__ import annotations

from n06.reconciliation import (
    COUNTERPART_QUARANTINED,
    ISSUE_CODE_ORDER,
    RECON_RULE_VERSION,
    STATUS_PRIORITY,
    ReconStatus,
)
from n06.reconciliation.rules import (
    KEY_CONFLICT_COLUMNS,
    MONEY_COLUMN,
    MONEY_SPARK_TYPE,
    SOURCE_CONFLICT_CODES,
    STATEMENT_COLUMNS,
    TRANSACTION_COLUMNS,
)

# Bảng mục 7.2 chép tay, ưu tiên 1 đến 8.
SPEC_TABLE_7_2 = [
    "DUPLICATE_OR_CONFLICT",
    "CURRENCY_MISMATCH",
    "AMOUNT_MISMATCH",
    "STATUS_MISMATCH",
    "MATCHED",
    "PENDING",
    "MISSING_EXTERNAL",
    "MISSING_INTERNAL",
]

# Mười bảy mã lỗi hợp đồng ở Phần II mục 8 (S878-S896), chép tay.
CONTRACT_ERROR_CODES = {
    "INVALID_JSON",
    "UNSUPPORTED_SCHEMA_VERSION",
    "MISSING_REQUIRED_FIELD",
    "SCHEMA_TYPE",
    "UNSUPPORTED_CURRENCY",
    "AMOUNT_OUT_OF_RANGE",
    "SCHEMA_VALUE",
    "BUSINESS_DATE_MISMATCH",
    "SAME_ACCOUNT_TRANSFER",
    "UNKNOWN_ACCOUNT",
    "ACCOUNT_CURRENCY_MISMATCH",
    "ENVELOPE_PAYLOAD_MISMATCH",
    "EVENT_ID_CONFLICT",
    "BUSINESS_KEY_CONFLICT",
    "LEDGER_INCOMPLETE",
    "LEDGER_UNBALANCED",
    "LEDGER_TRANSACTION_MISMATCH",
}


def test_status_priority_matches_spec_table_exactly() -> None:
    assert [s.value for s in STATUS_PRIORITY] == SPEC_TABLE_7_2
    assert len(set(STATUS_PRIORITY)) == 8
    assert {s.value for s in ReconStatus} == set(SPEC_TABLE_7_2)


def test_issue_code_order_is_status_codes_then_extra_then_source_conflicts() -> None:
    assert list(ISSUE_CODE_ORDER) == [
        "DUPLICATE_OR_CONFLICT",
        "CURRENCY_MISMATCH",
        "AMOUNT_MISMATCH",
        "STATUS_MISMATCH",
        "PENDING",
        "MISSING_EXTERNAL",
        "MISSING_INTERNAL",
        "COUNTERPART_QUARANTINED",
        "BUSINESS_KEY_CONFLICT",
        "EVENT_ID_CONFLICT",
    ]
    assert "MATCHED" not in ISSUE_CODE_ORDER  # MATCHED có issue_codes rỗng (ADR-0005)
    assert len(set(ISSUE_CODE_ORDER)) == len(ISSUE_CODE_ORDER)


def test_counterpart_quarantined_is_not_a_contract_error_code() -> None:
    assert len(CONTRACT_ERROR_CODES) == 17
    assert COUNTERPART_QUARANTINED not in CONTRACT_ERROR_CODES
    assert set(SOURCE_CONFLICT_CODES) <= CONTRACT_ERROR_CODES


def test_required_columns_cover_adr_0004b_names() -> None:
    assert {"transaction_id", "source_pointers", "business_date"} <= set(TRANSACTION_COLUMNS)
    assert {"file_available_at", "statement_record_id", "source_sha256"} <= set(STATEMENT_COLUMNS)
    assert {"conflict_code", "entity", "channel", "external_reference"} <= set(KEY_CONFLICT_COLUMNS)
    assert (MONEY_COLUMN, MONEY_SPARK_TYPE) == ("amount_minor", "bigint")  # tiền là số nguyên


def test_rule_version_is_semver() -> None:
    major, minor, patch = RECON_RULE_VERSION.split(".")
    assert all(part.isdigit() for part in (major, minor, patch))
