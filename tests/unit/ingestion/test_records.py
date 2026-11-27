"""Định tuyến record vào entity Bronze (ADR-0004: G1, G2, Q4, Q5, Q8). Expected viết tay."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from n06.ingestion.records import classify_record

EVENTS = (
    Path(__file__).resolve().parents[3]
    / "contracts/N06_INPUT_CONTRACT_V1/fixtures/acceptance/events.jsonl"
)
UUID = "aaaaaaaa-1111-4222-8333-444444444444"


def _line(event_type: str) -> str:
    for line in EVENTS.read_text(encoding="utf-8").splitlines():
        if json.loads(line)["event_type"] == event_type:
            return line
    raise AssertionError(event_type)


def _event(event_type: str = "TRANSACTION_CREATED") -> dict[str, Any]:
    return json.loads(_line(event_type))


@pytest.mark.parametrize(
    ("event_type", "route", "key_column"),
    [
        ("TRANSACTION_CREATED", "transaction", "transaction_id"),
        ("LEDGER_ENTRY_CREATED", "ledger_entry", "entry_id"),
    ],
)
def test_valid_record_goes_to_its_entity_with_envelope_and_payload_columns(
    event_type: str, route: str, key_column: str
) -> None:
    event = _event(event_type)
    got_route, dataset_id, columns = classify_record(_line(event_type).encode())

    assert got_route == route and dataset_id == event["dataset_id"]
    # G2: trường envelope trùng tên payload mang tiền tố `envelope_`.
    assert columns["envelope_dataset_id"] == event["dataset_id"]
    assert columns["envelope_event_time"] == event["event_time"]
    assert columns["envelope_schema_version"] == event["schema_version"]
    assert columns["event_id"] == event["event_id"]
    assert columns[key_column] == event["payload"][key_column]
    assert columns["amount_minor"] == event["payload"]["amount_minor"]
    assert type(columns["amount_minor"]) is int
    expected_names = {
        "envelope_schema_version", "event_id", "event_type", "envelope_dataset_id",
        "source_batch_id", "envelope_event_time", "emitted_at",
    } | set(event["payload"])  # fmt: skip
    assert set(columns) == expected_names


def _error(value: bytes | None) -> tuple[str | None, dict[str, Any]]:
    route, dataset_id, columns = classify_record(value)
    assert route == "parse_error"
    assert set(columns) == {"entity", "business_key", "error_code", "error_detail", "issue_codes"}
    assert columns["error_code"] == columns["issue_codes"][0]
    return dataset_id, columns


@pytest.mark.parametrize(
    ("value", "code"),
    [
        (None, "SCHEMA_TYPE"),
        (b"\xff\xfe", "INVALID_JSON"),
        (b"not json", "INVALID_JSON"),
        (b"[1,2]", "SCHEMA_TYPE"),
        (b'{"a":' + b"9" * 5000 + b"}", "INVALID_JSON"),
        (b"[" * 200_000, "INVALID_JSON"),
        (b"{}", "MISSING_REQUIRED_FIELD"),
    ],
)
def test_bad_value_is_a_parse_error_without_invented_identifiers(
    value: bytes | None, code: str
) -> None:
    dataset_id, columns = _error(value)
    assert columns["error_code"] == code
    assert dataset_id is None and columns["entity"] is None and columns["business_key"] is None


def test_readable_but_invalid_record_keeps_dataset_entity_and_key() -> None:
    event = _event()
    event["payload"]["amount_minor"] = -5
    dataset_id, columns = _error(json.dumps(event).encode())
    assert columns["error_code"] == "AMOUNT_OUT_OF_RANGE"
    assert dataset_id == event["dataset_id"]
    assert columns["entity"] == "transaction"
    assert columns["business_key"] == event["payload"]["transaction_id"]


def test_dataset_id_comes_from_the_envelope_only_and_must_match_the_contract() -> None:
    event = _event()
    event["dataset_id"] = "Khong Hop Le"  # payload vẫn hợp lệ nhưng không được lấy thay
    dataset_id, _ = _error(json.dumps(event).encode())
    assert dataset_id is None


@pytest.mark.parametrize("key", ["khong-phai-uuid", 12, None])
def test_business_key_is_only_a_readable_uuid(key: object) -> None:
    event = _event("LEDGER_ENTRY_CREATED")
    event["payload"]["entry_id"] = key
    _, columns = _error(json.dumps(event).encode())
    assert columns["entity"] == "ledger_entry" and columns["business_key"] is None


def test_unknown_event_type_has_no_entity() -> None:
    event = _event()
    event["event_type"] = "SOMETHING_ELSE"
    _, columns = _error(json.dumps(event).encode())
    assert columns["entity"] is None and columns["business_key"] is None


def test_envelope_payload_mismatch_is_added_to_issue_codes() -> None:
    """Q8: event_type nói transaction nhưng payload là ledger entry."""
    event = _event()
    event["payload"] = _event("LEDGER_ENTRY_CREATED")["payload"]
    _, columns = _error(json.dumps(event).encode())
    assert "ENVELOPE_PAYLOAD_MISMATCH" in columns["issue_codes"]
    assert columns["error_code"] != "ENVELOPE_PAYLOAD_MISMATCH"  # mã schema ưu tiên cao hơn


def test_lone_surrogate_is_replaced_so_the_row_can_be_written() -> None:
    raw = _line("TRANSACTION_CREATED").replace('"event_type"', '"x\\ud800": 1, "event_type"')
    _, columns = _error(raw.encode())
    columns["error_detail"].encode("utf-8")  # không ném


def test_year_9999_record_is_routed_without_raising() -> None:
    event = _event()
    for target in (event, event["payload"]):
        target["event_time"] = "9999-12-31T23:59:59Z"
    event["emitted_at"] = "9999-12-31T23:59:59Z"
    event["payload"]["business_date"] = "9999-12-31"
    # Hợp lệ theo schema nên vào entity (G1: ngữ nghĩa thuộc Silver).
    assert classify_record(json.dumps(event).encode())[0] == "transaction"
