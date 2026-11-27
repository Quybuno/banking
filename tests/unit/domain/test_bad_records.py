"""Bộ kiểm T10 không bao giờ ném trên record xấu: mỗi record cho đúng một kết quả có mã lỗi.

Record hợp lệ mẫu lấy từ fixture của hợp đồng; mã lỗi kỳ vọng viết tay theo bảng mã Phần II mục 8.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from n06.domain import (
    parse_and_validate_json_schema,
    validate_event_envelope,
    validate_ledger_entry_record,
    validate_transaction_record,
)

EVENTS = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "N06_INPUT_CONTRACT_V1"
    / "fixtures"
    / "acceptance"
    / "events.jsonl"
)


def _valid(event_type: str) -> dict[str, Any]:
    for line in EVENTS.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record["event_type"] == event_type:
            return record
    raise AssertionError(event_type)


VALIDATORS: list[Callable[[Any], Any]] = [
    lambda raw: parse_and_validate_json_schema(raw, "event_envelope"),
    validate_event_envelope,
    validate_transaction_record,
    validate_ledger_entry_record,
]
BROKEN_UTF8_INSIDE_STRING = (
    json.dumps(_valid("TRANSACTION_CREATED")).encode().replace(b"REF-", b"REF-\xff", 1)
)
UNPARSEABLE = {
    "integer-of-5000-digits": '{"a": ' + "9" * 5000 + "}",
    "deeply-nested-array": "[" * 200_000,
    "deeply-nested-inside-object": '{"payload": ' + "[" * 100_000 + "]" * 100_000 + "}",
    "invalid-utf8-bytes": b"\xff\xfe{",
    # Trước đây byte hỏng bị thay bằng U+FFFD rồi record đi tiếp với mã khác.
    "invalid-utf8-inside-a-string": BROKEN_UTF8_INSIDE_STRING,
    "invalid-utf8-bytearray": bytearray(BROKEN_UTF8_INSIDE_STRING),
}


@pytest.mark.parametrize("validate", VALIDATORS)
@pytest.mark.parametrize("raw", UNPARSEABLE.values(), ids=UNPARSEABLE.keys())
def test_unparseable_record_is_invalid_json_not_an_exception(
    validate: Callable[[Any], Any], raw: Any
) -> None:
    result = validate(raw)
    assert result.is_valid is False
    assert result.primary_error == "INVALID_JSON", result.issues


@pytest.mark.parametrize("validate", VALIDATORS)
def test_null_value_is_a_type_error_not_an_exception(validate: Callable[[Any], Any]) -> None:
    result = validate(None)
    assert result.is_valid is False and result.primary_error == "SCHEMA_TYPE"


@pytest.mark.parametrize("kind", [bytes, bytearray, memoryview])
def test_valid_record_is_accepted_from_any_bytes_like(kind: type) -> None:
    raw = kind(json.dumps(_valid("TRANSACTION_CREATED")).encode())
    result = parse_and_validate_json_schema(raw, "event_envelope")
    assert result.is_valid is True, result.issues


@pytest.mark.parametrize("validate", VALIDATORS)
def test_mapping_with_unprintable_integer_does_not_raise(validate: Callable[[Any], Any]) -> None:
    result = validate({"schema_version": 10**5000})
    assert result.is_valid is False


@pytest.mark.parametrize("event_type", ["TRANSACTION_CREATED", "LEDGER_ENTRY_CREATED"])
def test_event_time_beyond_the_business_calendar_is_a_date_mismatch(event_type: str) -> None:
    """23:59 UTC ngày 31/12/9999 đổi sang giờ Việt Nam vượt năm 9999."""
    record = _valid(event_type)
    for target in (record, record["payload"]):
        target["event_time"] = "9999-12-31T23:59:59Z"
    record["emitted_at"] = "9999-12-31T23:59:59Z"
    record["payload"]["business_date"] = "9999-12-31"

    result = validate_event_envelope(record)

    assert result.is_valid is False
    assert result.primary_error == "BUSINESS_DATE_MISMATCH", result.issues
