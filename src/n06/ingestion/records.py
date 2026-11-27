"""Định tuyến một record Kafka vào đúng một entity Bronze (ADR-0004: G1, G2, Q4, Q5, Q8).

Module thuần, không nhập PySpark: job Spark gọi `classify_record` trong một UDF (G22).
Bước parse chỉ kiểm JSON Schema của contract; kiểm tra ngữ nghĩa thuộc Silver.
"""

from __future__ import annotations

import json
from typing import Any

from n06.config import DATASET_ID_PATTERN
from n06.domain import parse_and_validate_json_schema, validate_event_envelope
from n06.domain.errors import ErrorCode, ValidationIssue, sort_issue_codes
from n06.domain.schema_validator import check_format

TRANSACTION = "transaction"
LEDGER_ENTRY = "ledger_entry"
PARSE_ERROR = "parse_error"
_ROUTES = {"TRANSACTION_CREATED": TRANSACTION, "LEDGER_ENTRY_CREATED": LEDGER_ENTRY}
_BUSINESS_KEY_FIELD = {TRANSACTION: "transaction_id", LEDGER_ENTRY: "entry_id"}
MAX_ERROR_DETAIL_CHARS = 4000

# Cột envelope (G2: tên trùng với payload có tiền tố `envelope_`) và cột payload theo entity.
ENVELOPE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("envelope_schema_version", "schema_version"),
    ("event_id", "event_id"),
    ("event_type", "event_type"),
    ("envelope_dataset_id", "dataset_id"),
    ("source_batch_id", "source_batch_id"),
    ("envelope_event_time", "event_time"),
    ("emitted_at", "emitted_at"),
)
TRANSACTION_COLUMNS: tuple[str, ...] = (
    "schema_version",
    "dataset_id",
    "transaction_id",
    "external_reference",
    "account_from",
    "account_to",
    "amount_minor",
    "currency",
    "status",
    "transaction_type",
    "channel",
    "event_time",
    "business_date",
)
LEDGER_COLUMNS: tuple[str, ...] = (
    "schema_version",
    "dataset_id",
    "entry_id",
    "transaction_id",
    "ledger_account_id",
    "side",
    "amount_minor",
    "currency",
    "event_time",
    "business_date",
)
INTEGER_COLUMNS = frozenset({"amount_minor"})


def _clean(text: str) -> str:
    """Chuỗi ghi được ra Parquet: surrogate lẻ (từ `\\udxxx` trong JSON) được thay thế."""
    return text.encode("utf-8", errors="replace").decode("utf-8")


def _load_object(text: str) -> dict[str, Any] | None:
    try:
        value = json.loads(text)
    except (ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _dataset_id(record: dict[str, Any] | None) -> str | None:
    """Q4: `envelope.dataset_id` nếu là chuỗi khớp mẫu của contract; không lấy từ payload."""
    value = record.get("dataset_id") if record is not None else None
    if isinstance(value, str) and DATASET_ID_PATTERN.fullmatch(value):
        return value
    return None


def _entity_and_key(record: dict[str, Any] | None) -> tuple[str | None, str | None]:
    """Q5: suy `entity` theo `event_type`, `business_key` là UUID đọc được; không tạo key giả."""
    if record is None:
        return None, None
    event_type = record.get("event_type")
    entity = _ROUTES.get(event_type) if isinstance(event_type, str) else None
    if entity is None:
        return None, None
    payload = record.get("payload")
    key = payload.get(_BUSINESS_KEY_FIELD[entity]) if isinstance(payload, dict) else None
    if isinstance(key, str) and check_format(key, "uuid"):
        return entity, _clean(key)
    return entity, None


def _error(
    issues: list[ValidationIssue], record: dict[str, Any] | None
) -> tuple[str, dict[str, Any]]:
    codes = sort_issue_codes(issue.code for issue in issues)
    detail = "; ".join(f"{i.code.value}@{i.field_path}: {i.message}" for i in issues)
    entity, business_key = _entity_and_key(record)
    return PARSE_ERROR, {
        "entity": entity,
        "business_key": business_key,
        "error_code": codes[0].value,
        "error_detail": _clean(detail)[:MAX_ERROR_DETAIL_CHARS],
        "issue_codes": [code.value for code in codes],
    }


def _single_issue(code: ErrorCode, message: str) -> list[ValidationIssue]:
    return [ValidationIssue(code=code, field_path="$", message=message)]


def classify_record(value: bytes | bytearray | None) -> tuple[str, str | None, dict[str, Any]]:
    """Trả `(entity Bronze, dataset_id của Raw, các cột riêng của entity)`.

    Không bao giờ ném: một record xấu không được làm hỏng batch (G1).
    """
    if value is None:  # tombstone; `bytes(None)` sẽ ném `TypeError`
        route, columns = _error(
            _single_issue(ErrorCode.SCHEMA_TYPE, "Record không có value (null)."), None
        )
        return route, None, columns
    try:
        text = bytes(value).decode("utf-8")
    except UnicodeDecodeError as exc:
        route, columns = _error(
            _single_issue(ErrorCode.INVALID_JSON, f"Value không phải UTF-8: {exc.reason}"), None
        )
        return route, None, columns
    try:
        result = parse_and_validate_json_schema(text, "event_envelope")
    except (ValueError, RecursionError) as exc:  # số quá dài, lồng quá sâu
        route, columns = _error(
            _single_issue(ErrorCode.INVALID_JSON, f"Không phân tích được ({type(exc).__name__})."),
            None,
        )
        return route, None, columns

    record = result.record if result.record is not None else _load_object(text)
    dataset_id = _dataset_id(record)
    if not result.is_valid or record is None:
        issues = list(result.issues)
        issues.extend(_envelope_mismatch(text))
        route, columns = _error(issues, record)
        return route, dataset_id, columns

    route = _ROUTES[record["event_type"]]
    payload = record["payload"]
    columns = {name: _clean(record[source]) for name, source in ENVELOPE_COLUMNS}
    for name in TRANSACTION_COLUMNS if route == TRANSACTION else LEDGER_COLUMNS:
        field = payload[name]
        if name in INTEGER_COLUMNS:
            if type(field) is not int:  # tiền là số nguyên; không ép từ float
                route, columns = _error(
                    _single_issue(ErrorCode.SCHEMA_TYPE, f"{name} phải là số nguyên."), record
                )
                return route, dataset_id, columns
            columns[name] = field
        else:
            columns[name] = _clean(field)
    return route, dataset_id, columns


def _envelope_mismatch(text: str) -> list[ValidationIssue]:
    """Q8: chỉ lấy `ENVELOPE_PAYLOAD_MISMATCH` từ validator domain; lỗi của lời gọi bị bỏ qua."""
    try:
        domain = validate_event_envelope(text)
        return [i for i in domain.issues if i.code == ErrorCode.ENVELOPE_PAYLOAD_MISMATCH]
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError):
        return []
