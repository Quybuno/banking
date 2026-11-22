"""Bảng mã lỗi ổn định và thứ tự ưu tiên theo hợp đồng dữ liệu 1.0.0 (TASK T10).

Tham chiếu: docs/spec.md Phần II mục 8.
Thứ tự ưu tiên:
INVALID_JSON -> UNSUPPORTED_SCHEMA_VERSION -> MISSING_REQUIRED_FIELD -> SCHEMA_TYPE ->
UNSUPPORTED_CURRENCY -> AMOUNT_OUT_OF_RANGE -> SCHEMA_VALUE -> kiểm tra semantic.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    """17 mã lỗi chuẩn của hợp đồng dữ liệu N06 v1.0.0 (Phần II mục 8)."""

    INVALID_JSON = "INVALID_JSON"
    UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    SCHEMA_TYPE = "SCHEMA_TYPE"
    UNSUPPORTED_CURRENCY = "UNSUPPORTED_CURRENCY"
    AMOUNT_OUT_OF_RANGE = "AMOUNT_OUT_OF_RANGE"
    SCHEMA_VALUE = "SCHEMA_VALUE"
    ENVELOPE_PAYLOAD_MISMATCH = "ENVELOPE_PAYLOAD_MISMATCH"
    BUSINESS_DATE_MISMATCH = "BUSINESS_DATE_MISMATCH"
    SAME_ACCOUNT_TRANSFER = "SAME_ACCOUNT_TRANSFER"
    UNKNOWN_ACCOUNT = "UNKNOWN_ACCOUNT"
    ACCOUNT_CURRENCY_MISMATCH = "ACCOUNT_CURRENCY_MISMATCH"
    EVENT_ID_CONFLICT = "EVENT_ID_CONFLICT"
    BUSINESS_KEY_CONFLICT = "BUSINESS_KEY_CONFLICT"
    LEDGER_INCOMPLETE = "LEDGER_INCOMPLETE"
    LEDGER_UNBALANCED = "LEDGER_UNBALANCED"
    LEDGER_TRANSACTION_MISMATCH = "LEDGER_TRANSACTION_MISMATCH"


# Thứ tự ưu tiên tuyệt đối từ cao xuống thấp (đặc tả Phần II mục 8)
ERROR_PRIORITY: tuple[ErrorCode, ...] = (
    ErrorCode.INVALID_JSON,
    ErrorCode.UNSUPPORTED_SCHEMA_VERSION,
    ErrorCode.MISSING_REQUIRED_FIELD,
    ErrorCode.SCHEMA_TYPE,
    ErrorCode.UNSUPPORTED_CURRENCY,
    ErrorCode.AMOUNT_OUT_OF_RANGE,
    ErrorCode.SCHEMA_VALUE,
    ErrorCode.ENVELOPE_PAYLOAD_MISMATCH,
    ErrorCode.BUSINESS_DATE_MISMATCH,
    ErrorCode.SAME_ACCOUNT_TRANSFER,
    ErrorCode.UNKNOWN_ACCOUNT,
    ErrorCode.ACCOUNT_CURRENCY_MISMATCH,
    ErrorCode.EVENT_ID_CONFLICT,
    ErrorCode.BUSINESS_KEY_CONFLICT,
    ErrorCode.LEDGER_INCOMPLETE,
    ErrorCode.LEDGER_UNBALANCED,
    ErrorCode.LEDGER_TRANSACTION_MISMATCH,
)

_PRIORITY_RANK: dict[str, int] = {code.value: idx for idx, code in enumerate(ERROR_PRIORITY)}


def sort_issue_codes(codes: Iterable[ErrorCode | str]) -> tuple[ErrorCode, ...]:
    """Loại trùng và sắp xếp danh sách mã lỗi theo đúng thứ tự ưu tiên ở Phần II mục 8."""
    normalized: set[ErrorCode] = {ErrorCode(str(c)) for c in codes}
    ordered = sorted(normalized, key=lambda c: _PRIORITY_RANK[c.value])
    return tuple(ordered)


def select_primary_error(codes: Iterable[ErrorCode | str]) -> ErrorCode | None:
    """Chọn mã lỗi chính (primary_error) có mức ưu tiên cao nhất từ tập mã lỗi."""
    ordered = sort_issue_codes(codes)
    return ordered[0] if ordered else None


@dataclass(frozen=True)
class ValidationIssue:
    """Chi tiết một lỗi phát hiện được trên bản ghi."""

    code: ErrorCode
    field_path: str
    message: str


@dataclass(frozen=True)
class ValidationResult:
    """Kết quả kiểm tra một bản ghi hoặc một nhóm bản ghi."""

    is_valid: bool
    primary_error: ErrorCode | None
    issue_codes: tuple[ErrorCode, ...]
    issues: tuple[ValidationIssue, ...]
    record: dict[str, Any] | None = None
    model: Any | None = None

    @classmethod
    def ok(
        cls,
        *,
        record: dict[str, Any] | None = None,
        model: Any | None = None,
    ) -> ValidationResult:
        """Tạo kết quả hợp lệ (không có lỗi)."""
        return cls(
            is_valid=True,
            primary_error=None,
            issue_codes=(),
            issues=(),
            record=record,
            model=model,
        )

    @classmethod
    def from_issues(
        cls,
        issues: Iterable[ValidationIssue],
        *,
        record: dict[str, Any] | None = None,
        model: Any | None = None,
    ) -> ValidationResult:
        """Tạo kết quả từ danh sách `ValidationIssue`, tự động sắp xếp và chọn `primary_error`."""
        issue_list = list(issues)
        if not issue_list:
            return cls.ok(record=record, model=model)
        ordered_issues = tuple(
            sorted(
                issue_list,
                key=lambda item: (_PRIORITY_RANK[item.code.value], item.field_path),
            )
        )
        ordered_codes = sort_issue_codes(item.code for item in ordered_issues)
        return cls(
            is_valid=False,
            primary_error=ordered_codes[0],
            issue_codes=ordered_codes,
            issues=ordered_issues,
            record=record,
            model=None,
        )
