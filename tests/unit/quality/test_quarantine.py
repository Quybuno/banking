"""Unit tests cho Quarantine (TASK T30, ADR-0004b §3.6, Phần II mục 8).

Kiểm tra:
1. Toàn bộ 17 ca `record_cases.jsonl` trong `contracts/N06_INPUT_CONTRACT_V1/fixtures/negative/`
   đi qua `classify_raw_event_for_quarantine` ra đúng `expected_error` (đọc trực tiếp từ file
   negative, không sửa expected).
2. `malformed_payloads.txt`, UTF-8 hỏng và tombstone null đều đi vào quarantine (`BRONZE_PARSE`),
   không ném ngoại lệ.
3. `unbalanced_ledger.jsonl` (`LEDGER_UNBALANCED`) và `missing_ledger_side.jsonl`
   (`LEDGER_INCOMPLETE`) đi qua `check_ledger_groups_for_quarantine` ra đúng mã lỗi.
4. Bản ghi vi phạm nhiều lỗi cùng lúc giữ đủ `issue_codes` và chọn `error_code` theo thứ tự ưu
   tiên Phần II mục 8.
5. Dòng sao kê và tài khoản hỏng đi vào quarantine đúng mã, kèm `source_pointer` và
   `raw_reference`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from n06.domain import CONTRACT_ROOT, Account, LedgerEntry, Transaction
from n06.quality import (
    QUARANTINE_COLUMNS,
    STAGE_BRONZE_PARSE,
    STAGE_SILVER_BUSINESS,
    check_ledger_groups_for_quarantine,
    classify_account_line,
    classify_raw_event_for_quarantine,
    classify_statement_line,
)

ACCEPTANCE_DIR = CONTRACT_ROOT / "fixtures" / "acceptance"
NEGATIVE_DIR = CONTRACT_ROOT / "fixtures" / "negative"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _acceptance_accounts() -> dict[str, Account]:
    return {
        raw["account_id"]: Account.from_dict(raw)
        for raw in _read_jsonl(ACCEPTANCE_DIR / "accounts.jsonl")
    }


def _wrap_tx_envelope(tx: dict[str, Any], *, event_id: str) -> bytes:
    env = {
        "schema_version": "1.0.0",
        "event_id": event_id,
        "event_type": "TRANSACTION_CREATED",
        "dataset_id": tx.get("dataset_id", "n06-acceptance-v1"),
        "source_batch_id": "11111111-1111-5111-8111-111111111111",
        "event_time": tx.get("event_time", "2026-01-01T01:00:01Z"),
        "emitted_at": "2026-01-01T01:00:05Z",
        "payload": tx,
    }
    return json.dumps(env).encode("utf-8")


def test_negative_record_cases_all_17_produce_exact_expected_error() -> None:
    accounts = _acceptance_accounts()
    cases = _read_jsonl(NEGATIVE_DIR / "record_cases.jsonl")
    assert len(cases) == 17

    for idx, case in enumerate(cases):
        raw_bytes = _wrap_tx_envelope(
            case["input"],
            event_id=f"00000000-0000-5000-8000-{idx + 1:012d}",
        )
        valid, q = classify_raw_event_for_quarantine(
            raw_bytes,
            run_id="run-neg-1",
            source_pointer=f"n06.events:0:{idx}",
            raw_reference="s3a://n06-lakehouse/raw/q/ingestion_date=2026-01-01/batch_id=0/",
            accounts=accounts,
        )
        assert valid is None, f"Case {case['case_id']} không được coi là hợp lệ"
        assert q is not None
        assert q.error_code == case["expected_error"], (
            f"Case {case['case_id']}: nhận {q.error_code}, kỳ vọng {case['expected_error']} "
            f"(issues={q.issue_codes})"
        )
        assert q.error_code in q.issue_codes
        assert q.source_pointer == f"n06.events:0:{idx}"
        assert q.raw_reference.startswith("s3a://")
        assert q.run_id == "run-neg-1"
        assert set(q.as_dict()) == set(QUARANTINE_COLUMNS)


def test_malformed_payloads_utf8_and_null_route_to_quarantine() -> None:
    expected = json.loads((NEGATIVE_DIR / "expected_cases.json").read_text(encoding="utf-8"))
    raw_lines = (NEGATIVE_DIR / "malformed_payloads.txt").read_bytes().splitlines()
    assert raw_lines

    for idx, line in enumerate(raw_lines):
        valid, q = classify_raw_event_for_quarantine(
            line,
            run_id="run-mal-1",
            source_pointer=f"n06.events:0:{idx}",
            raw_reference="s3a://n06-lakehouse/raw/q/ingestion_date=2026-01-01/batch_id=0/",
        )
        assert valid is None
        assert q is not None
        assert q.stage == STAGE_BRONZE_PARSE
        assert q.error_code == expected["malformed_payloads"]
        assert q.business_date is None

    # UTF-8 hỏng và tombstone null không bao giờ ném ngoại lệ
    valid_bad, q_bad = classify_raw_event_for_quarantine(
        b"\xff\xfe\xfd",
        run_id="run-mal-1",
        source_pointer="n06.events:0:99",
        raw_reference="s3a://n06-lakehouse/raw/q/ingestion_date=2026-01-01/batch_id=0/",
    )
    assert valid_bad is None and q_bad is not None and q_bad.error_code == "INVALID_JSON"

    valid_null, q_null = classify_raw_event_for_quarantine(
        None,
        run_id="run-mal-1",
        source_pointer="n06.events:0:100",
        raw_reference="s3a://n06-lakehouse/raw/q/ingestion_date=2026-01-01/batch_id=0/",
    )
    assert valid_null is None and q_null is not None and q_null.error_code == "SCHEMA_TYPE"


def test_negative_ledger_group_cases_unbalanced_and_incomplete() -> None:
    expected = json.loads((NEGATIVE_DIR / "expected_cases.json").read_text(encoding="utf-8"))
    txs = [Transaction.from_dict(raw) for raw in _read_jsonl(ACCEPTANCE_DIR / "transactions.jsonl")]
    tx_by_id = {tx.transaction_id: tx for tx in txs}

    for filename, case_key in (
        ("unbalanced_ledger.jsonl", "unbalanced_ledger"),
        ("missing_ledger_side.jsonl", "missing_ledger_side"),
    ):
        entries = [LedgerEntry.from_dict(raw) for raw in _read_jsonl(NEGATIVE_DIR / filename)]
        target_tx_ids = {e.transaction_id for e in entries}
        target_txs = [tx_by_id[tid] for tid in target_tx_ids]
        q_list = check_ledger_groups_for_quarantine(target_txs, entries, run_id="run-lg-1")
        assert len(q_list) == len(target_txs)
        assert all(q.error_code == expected[case_key] for q in q_list)
        assert all(q.stage == STAGE_SILVER_BUSINESS for q in q_list)


def test_multiple_semantic_errors_respect_priority_order() -> None:
    accounts = _acceptance_accounts()
    # Vừa sai business_date vừa same_account_transfer -> ưu tiên BUSINESS_DATE_MISMATCH trước.
    bad_tx = {
        "schema_version": "1.0.0",
        "dataset_id": "n06-acceptance-v1",
        "transaction_id": "52a6c7e3-a44d-52a1-9ffb-ae06d7bf6522",
        "external_reference": "REF-MULTI",
        "account_from": "9a583596-945e-58a9-8a0c-ae06d161569",
        "account_to": "9a583596-945e-58a9-8a0c-ae06d161569",
        "amount_minor": 100,
        "currency": "VND",
        "status": "SUCCESS",
        "transaction_type": "INTERNAL_TRANSFER",
        "channel": "SIM_CHANNEL_A",
        "event_time": "2026-01-01T01:00:01Z",
        "business_date": "2026-01-02",
    }
    # Dùng UUID hợp lệ để qua được Bronze schema parse, vào Silver business validation.
    bad_tx["account_from"] = "9a583596-945e-58a9-8a0c-ae060d161569"
    bad_tx["account_to"] = "9a583596-945e-58a9-8a0c-ae060d161569"
    raw_bytes = _wrap_tx_envelope(bad_tx, event_id="00000000-0000-5000-8000-000000000999")
    valid, q = classify_raw_event_for_quarantine(
        raw_bytes,
        run_id="run-multi",
        source_pointer="n06.events:0:5",
        raw_reference="s3a://bucket/raw/b0/",
        accounts=accounts,
    )
    assert valid is None and q is not None
    assert q.stage == STAGE_SILVER_BUSINESS
    assert q.error_code == "BUSINESS_DATE_MISMATCH"
    assert q.issue_codes == ("BUSINESS_DATE_MISMATCH", "SAME_ACCOUNT_TRANSFER")


def test_statement_and_account_quarantine_lines() -> None:
    v_s, q_s = classify_statement_line(
        "{not-json",
        dataset_id="ds-1",
        run_id="run-1",
        source_pointer="stmt.jsonl:abc:1",
        raw_reference="s3a://bucket/statements/ds-1/stmt.jsonl",
    )
    assert v_s is None and q_s is not None
    assert q_s.error_code == "INVALID_JSON"
    assert q_s.stage == STAGE_BRONZE_PARSE
    assert q_s.entity == "statement"

    v_a, q_a = classify_account_line(
        json.dumps({"schema_version": "9.0.0", "dataset_id": "ds-1"}),
        dataset_id="ds-1",
        run_id="run-1",
        source_pointer="accounts.jsonl:abc:2",
        raw_reference="s3a://bucket/statements/ds-1/accounts.jsonl",
    )
    assert v_a is None and q_a is not None
    assert q_a.error_code == "UNSUPPORTED_SCHEMA_VERSION"
    assert q_a.entity == "account"
