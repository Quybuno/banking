"""Unit tests cho domain models, schema validator và business validator (TASK T10).

Kiểm tra các điều kiện hoàn thành:
1. Validator dùng JSON Schema Draft 2020-12 có bật format checks (`uuid`, `date`, `date-time`);
   schema đọc trực tiếp từ `contracts/N06_INPUT_CONTRACT_V1/schemas/`, không sao chép vào `src/`.
2. Toàn bộ bộ acceptance (`contracts/N06_INPUT_CONTRACT_V1/fixtures/acceptance/`) đều PASS 100%
   qua cả schema validator lẫn business validator (kể cả replay dedup và nhóm ledger 2 vế cân bằng).
3. Toàn bộ negative suite (`contracts/N06_INPUT_CONTRACT_V1/fixtures/negative/`) ra đúng mã lỗi
   mong đợi lấy trực tiếp từ `record_cases.jsonl` và `expected_cases.json` trong `contracts/`.
4. Khi một bản ghi vi phạm nhiều lỗi cùng lúc, validator lưu đủ tất cả `issue_codes` và trả về
   `primary_error` theo đúng thứ tự ưu tiên ở Phần II mục 8.
5. Domain models từ chối `float` và `bool` cho các trường số nguyên tiền tệ (`amount_minor`).
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest

from n06.domain import (
    CONTRACT_ROOT,
    DEFAULT_SCHEMAS_DIR,
    DRAFT_2020_12_META_URI,
    ERROR_PRIORITY,
    Account,
    ConflictTracker,
    ContractSchemaRegistry,
    ControlReport,
    ErrorCode,
    EventEnvelope,
    ExternalStatement,
    LedgerEntry,
    SourceManifest,
    StatementDelivery,
    Transaction,
    validate_account_record,
    validate_control_report_data,
    validate_event_envelope,
    validate_generator_config_data,
    validate_ledger_entry_record,
    validate_record,
    validate_statement_record,
    validate_transaction_ledger_group,
    validate_transaction_record,
)

ACCEPTANCE_DIR = CONTRACT_ROOT / "fixtures" / "acceptance"
NEGATIVE_DIR = CONTRACT_ROOT / "fixtures" / "negative"
CONFIG_DIR = CONTRACT_ROOT / "config"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _load_acceptance_accounts() -> dict[str, Account]:
    accounts: dict[str, Account] = {}
    for raw in _read_jsonl(ACCEPTANCE_DIR / "accounts.jsonl"):
        res = validate_account_record(raw)
        assert res.is_valid, f"Account acceptance phải hợp lệ: {res.issues}"
        assert isinstance(res.model, Account)
        accounts[res.model.account_id] = res.model
    return accounts


def test_schema_registry_loads_draft_2020_12_from_contracts_only() -> None:
    """Xác nhận schema đọc từ `contracts/` và không có file `.schema.json` nào trong `src/`."""
    src_root = CONTRACT_ROOT.parents[1] / "src"
    copied_schemas = list(src_root.rglob("*.schema.json"))
    assert copied_schemas == [], f"Cấm sao chép schema vào src/: {copied_schemas}"

    reg = ContractSchemaRegistry()
    assert reg.schemas_dir == DEFAULT_SCHEMAS_DIR

    expected_schema_files = {
        "account.schema.json",
        "control_report.schema.json",
        "event_envelope.schema.json",
        "generator_config.schema.json",
        "ledger_entry.schema.json",
        "source_manifest.schema.json",
        "statement.schema.json",
        "statement_delivery.schema.json",
        "transaction.schema.json",
    }
    for name in expected_schema_files:
        schema = reg.get_schema(name)
        assert schema["$schema"] == DRAFT_2020_12_META_URI


def test_acceptance_suite_all_records_and_relationships_pass() -> None:
    """Toàn bộ bản ghi trong bộ acceptance hợp lệ (accounts, txs, ledgers, events, statements)."""
    manifest_raw = json.loads((ACCEPTANCE_DIR / "source_manifest.json").read_text(encoding="utf-8"))
    manifest_res = validate_record(manifest_raw, "source_manifest")
    assert manifest_res.is_valid
    assert isinstance(manifest_res.model, SourceManifest)
    allowed_batches = {manifest_res.model.source_batch_id}

    control_raw = json.loads((ACCEPTANCE_DIR / "control_report.json").read_text(encoding="utf-8"))
    control_res = validate_control_report_data(control_raw)
    assert control_res.is_valid
    assert isinstance(control_res.model, ControlReport)
    assert len(control_res.model.totals) == 7

    for profile in ("demo-clean", "demo-faults", "bench-skew"):
        cfg_raw = json.loads((CONFIG_DIR / f"generator.{profile}.json").read_text(encoding="utf-8"))
        cfg_res = validate_generator_config_data(cfg_raw)
        assert cfg_res.is_valid, f"generator.{profile}.json lỗi: {cfg_res.issues}"

    accounts = _load_acceptance_accounts()
    assert len(accounts) == 2

    transactions: dict[str, Transaction] = {}
    for raw_tx in _read_jsonl(ACCEPTANCE_DIR / "transactions.jsonl"):
        tx_res = validate_transaction_record(raw_tx, accounts=accounts)
        assert tx_res.is_valid, f"Transaction lỗi: {tx_res.issues}"
        assert isinstance(tx_res.model, Transaction)
        transactions[tx_res.model.transaction_id] = tx_res.model
    assert len(transactions) == 12

    ledgers_by_tx: dict[str, list[LedgerEntry]] = defaultdict(list)
    for raw_le in _read_jsonl(ACCEPTANCE_DIR / "ledger_entries.jsonl"):
        le_res = validate_ledger_entry_record(raw_le, accounts=accounts, transactions=transactions)
        assert le_res.is_valid, f"LedgerEntry lỗi: {le_res.issues}"
        assert isinstance(le_res.model, LedgerEntry)
        ledgers_by_tx[le_res.model.transaction_id].append(le_res.model)

    for tx_id, tx_obj in transactions.items():
        grp_res = validate_transaction_ledger_group(tx_obj, ledgers_by_tx[tx_id])
        assert grp_res.is_valid, f"Ledger group của tx {tx_id} lỗi: {grp_res.issues}"

    # Kiểm tra toàn bộ 36 physical events trong events.jsonl (có cả event replay & business replay)
    tracker = ConflictTracker()
    raw_events = _read_jsonl(ACCEPTANCE_DIR / "events.jsonl")
    assert len(raw_events) == 36
    for raw_ev in raw_events:
        ev_res = validate_event_envelope(
            raw_ev,
            accounts=accounts,
            allowed_source_batch_ids=allowed_batches,
        )
        assert ev_res.is_valid, f"EventEnvelope lỗi: {ev_res.issues}"
        assert isinstance(ev_res.model, EventEnvelope)

        conflict_res = tracker.check_and_register_envelope(ev_res.model)
        assert conflict_res.is_valid, (
            f"Phát hiện xung đột sai trên acceptance: {conflict_res.issues}"
        )
    assert tracker.conflicts == []

    # Kiểm tra toàn bộ statements và statement_deliveries
    for raw_st in _read_jsonl(ACCEPTANCE_DIR / "statements.jsonl"):
        st_res = validate_statement_record(raw_st)
        assert st_res.is_valid, f"Statement lỗi: {st_res.issues}"
        assert isinstance(st_res.model, ExternalStatement)

    for raw_deliv in _read_jsonl(ACCEPTANCE_DIR / "statement_deliveries.jsonl"):
        deliv_res = validate_record(raw_deliv, "statement_delivery")
        assert deliv_res.is_valid, f"StatementDelivery lỗi: {deliv_res.issues}"
        assert isinstance(deliv_res.model, StatementDelivery)


def test_negative_record_cases_match_contract_expected_errors() -> None:
    """Mỗi ca trong `fixtures/negative/record_cases.jsonl` phải trả về đúng `expected_error`."""
    accounts = _load_acceptance_accounts()
    cases = _read_jsonl(NEGATIVE_DIR / "record_cases.jsonl")
    assert len(cases) == 17

    for case in cases:
        case_id = case["case_id"]
        entity = case["entity"]
        expected_error = case["expected_error"]
        raw_input = case["input"]

        result = validate_record(raw_input, entity, accounts=accounts)
        assert not result.is_valid, f"Negative case '{case_id}' phải không hợp lệ"
        assert result.primary_error is not None
        assert result.primary_error.value == expected_error, (
            f"Ca '{case_id}': kỳ vọng {expected_error!r}, nhận {result.primary_error.value!r} "
            f"(issues={result.issues})"
        )


def test_negative_scenario_files_match_expected_cases_json() -> None:
    """Kiểm tra 5 file kịch bản lỗi cô lập trong `fixtures/negative/` với `expected_cases.json`."""
    expected_cases = json.loads((NEGATIVE_DIR / "expected_cases.json").read_text(encoding="utf-8"))
    accounts = _load_acceptance_accounts()
    txs = {
        t["transaction_id"]: Transaction.from_dict(t)
        for t in _read_jsonl(ACCEPTANCE_DIR / "transactions.jsonl")
    }
    tx_t001 = txs["52a6c7e3-a44d-52a1-9ffb-ae06d7bf6522"]

    # 1. malformed_payloads.txt -> INVALID_JSON
    malformed_lines = [
        line
        for line in (NEGATIVE_DIR / "malformed_payloads.txt")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert len(malformed_lines) >= 2
    for raw_line in malformed_lines:
        res = validate_record(raw_line, "event_envelope", accounts=accounts)
        assert not res.is_valid
        assert res.primary_error is not None
        assert res.primary_error.value == expected_cases["malformed_payloads"]

    # 2. event_id_conflict.jsonl -> EVENT_ID_CONFLICT
    tracker_event = ConflictTracker()
    ev_conflict_rows = _read_jsonl(NEGATIVE_DIR / "event_id_conflict.jsonl")
    assert len(ev_conflict_rows) == 2
    first_res = tracker_event.check_and_register_envelope(ev_conflict_rows[0])
    assert first_res.is_valid
    second_res = tracker_event.check_and_register_envelope(ev_conflict_rows[1])
    assert not second_res.is_valid
    assert second_res.primary_error is not None
    assert second_res.primary_error.value == expected_cases["event_id_conflict"]
    assert len(tracker_event.conflicts) == 1
    assert tracker_event.conflicts[0].match_key is not None
    assert tracker_event.conflicts[0].match_key.external_reference == "REF-T001"

    # 3. business_key_conflict.jsonl -> BUSINESS_KEY_CONFLICT
    tracker_biz = ConflictTracker()
    biz_conflict_rows = _read_jsonl(NEGATIVE_DIR / "business_key_conflict.jsonl")
    assert len(biz_conflict_rows) == 2
    first_biz = tracker_biz.check_and_register_envelope(biz_conflict_rows[0])
    assert first_biz.is_valid
    second_biz = tracker_biz.check_and_register_envelope(biz_conflict_rows[1])
    assert not second_biz.is_valid
    assert second_biz.primary_error is not None
    assert second_biz.primary_error.value == expected_cases["business_key_conflict"]
    assert len(tracker_biz.conflicts) == 1
    assert tracker_biz.conflicts[0].match_key is not None
    assert tracker_biz.conflicts[0].match_key.external_reference == "REF-T001"

    # 4. unbalanced_ledger.jsonl -> LEDGER_UNBALANCED (trong context transaction T001)
    unbalanced_rows = _read_jsonl(NEGATIVE_DIR / "unbalanced_ledger.jsonl")
    unbalanced_res = validate_transaction_ledger_group(tx_t001, unbalanced_rows)
    assert not unbalanced_res.is_valid
    assert unbalanced_res.primary_error is not None
    assert unbalanced_res.primary_error.value == expected_cases["unbalanced_ledger"]

    # 5. missing_ledger_side.jsonl -> LEDGER_INCOMPLETE (trong context transaction T001)
    missing_side_rows = _read_jsonl(NEGATIVE_DIR / "missing_ledger_side.jsonl")
    missing_side_res = validate_transaction_ledger_group(tx_t001, missing_side_rows)
    assert not missing_side_res.is_valid
    assert missing_side_res.primary_error is not None
    assert missing_side_res.primary_error.value == expected_cases["missing_ledger_side"]


def test_multi_violation_error_priority_ordering() -> None:
    """Khi bản ghi vi phạm nhiều lỗi cùng lúc, primary_error và issue_codes đúng thứ tự mục 8."""
    accounts = _load_acceptance_accounts()
    base_tx = _read_jsonl(ACCEPTANCE_DIR / "transactions.jsonl")[0]

    # Tổ hợp 1: UNSUPPORTED_SCHEMA_VERSION + MISSING_REQUIRED_FIELD + AMOUNT_OUT_OF_RANGE + SAME_ACC
    rec_1 = dict(base_tx)
    rec_1["schema_version"] = "2.0.0"
    del rec_1["channel"]
    rec_1["amount_minor"] = 0
    rec_1["account_to"] = rec_1["account_from"]
    res_1 = validate_transaction_record(rec_1, accounts=accounts)
    assert not res_1.is_valid
    assert res_1.primary_error == ErrorCode.UNSUPPORTED_SCHEMA_VERSION
    assert res_1.issue_codes == (
        ErrorCode.UNSUPPORTED_SCHEMA_VERSION,
        ErrorCode.MISSING_REQUIRED_FIELD,
        ErrorCode.AMOUNT_OUT_OF_RANGE,
        ErrorCode.SAME_ACCOUNT_TRANSFER,
    )

    # Tổ hợp 2: MISSING_REQUIRED_FIELD + SCHEMA_TYPE + UNSUPPORTED_CURRENCY + AMOUNT_OUT_OF_RANGE
    rec_2 = dict(base_tx)
    del rec_2["external_reference"]
    rec_2["status"] = 12345  # sai kiểu (int thay vì string)
    rec_2["currency"] = "EUR"
    rec_2["amount_minor"] = -50
    res_2 = validate_transaction_record(rec_2, accounts=accounts)
    assert not res_2.is_valid
    assert res_2.primary_error == ErrorCode.MISSING_REQUIRED_FIELD
    assert res_2.issue_codes == (
        ErrorCode.MISSING_REQUIRED_FIELD,
        ErrorCode.SCHEMA_TYPE,
        ErrorCode.UNSUPPORTED_CURRENCY,
        ErrorCode.AMOUNT_OUT_OF_RANGE,
    )

    # Tổ hợp 3: SCHEMA_TYPE ưu tiên hơn UNSUPPORTED_CURRENCY và AMOUNT_OUT_OF_RANGE
    rec_3 = dict(base_tx)
    rec_3["external_reference"] = None
    rec_3["currency"] = "JPY"
    rec_3["amount_minor"] = 0
    res_3 = validate_transaction_record(rec_3, accounts=accounts)
    assert res_3.primary_error == ErrorCode.SCHEMA_TYPE
    assert res_3.issue_codes[:3] == (
        ErrorCode.SCHEMA_TYPE,
        ErrorCode.UNSUPPORTED_CURRENCY,
        ErrorCode.AMOUNT_OUT_OF_RANGE,
    )

    # Tổ hợp 4: UNSUPPORTED_CURRENCY ưu tiên hơn AMOUNT_OUT_OF_RANGE và SCHEMA_VALUE
    rec_4 = dict(base_tx)
    rec_4["currency"] = "GBP"
    rec_4["amount_minor"] = 0
    rec_4["status"] = "UNKNOWN_STATUS"
    res_4 = validate_transaction_record(rec_4, accounts=accounts)
    assert res_4.primary_error == ErrorCode.UNSUPPORTED_CURRENCY
    assert res_4.issue_codes[:3] == (
        ErrorCode.UNSUPPORTED_CURRENCY,
        ErrorCode.AMOUNT_OUT_OF_RANGE,
        ErrorCode.SCHEMA_VALUE,
    )

    # Tổ hợp 5: AMOUNT_OUT_OF_RANGE ưu tiên hơn SCHEMA_VALUE và BUSINESS_DATE_MISMATCH
    rec_5 = dict(base_tx)
    rec_5["amount_minor"] = 0
    rec_5["transaction_id"] = "not-a-valid-uuid"
    rec_5["business_date"] = "2026-01-09"
    res_5 = validate_transaction_record(rec_5, accounts=accounts)
    assert res_5.primary_error == ErrorCode.AMOUNT_OUT_OF_RANGE
    assert res_5.issue_codes[:3] == (
        ErrorCode.AMOUNT_OUT_OF_RANGE,
        ErrorCode.SCHEMA_VALUE,
        ErrorCode.BUSINESS_DATE_MISMATCH,
    )

    # Tổ hợp 6: Lỗi semantic: BUSINESS_DATE_MISMATCH > SAME_ACCOUNT_TRANSFER > UNKNOWN_ACCOUNT
    rec_6 = dict(base_tx)
    rec_6["business_date"] = "2026-01-05"
    rec_6["account_from"] = "e29d2fbb-4643-51db-b304-b11ac9866147"
    rec_6["account_to"] = "e29d2fbb-4643-51db-b304-b11ac9866147"
    res_6 = validate_transaction_record(rec_6, accounts=accounts)
    assert res_6.primary_error == ErrorCode.BUSINESS_DATE_MISMATCH
    assert res_6.issue_codes == (
        ErrorCode.BUSINESS_DATE_MISMATCH,
        ErrorCode.SAME_ACCOUNT_TRANSFER,
        ErrorCode.UNKNOWN_ACCOUNT,
    )

    # Xác nhận bảng ERROR_PRIORITY có đủ 17 mã lỗi duy nhất
    assert len(ERROR_PRIORITY) == 17
    assert len(set(ERROR_PRIORITY)) == 17


def test_domain_models_reject_float_and_bool_for_money() -> None:
    """Quy tắc cứng: Tiền là số nguyên (`amount_minor`), cấm dùng `float` hoặc `bool`."""
    raw_tx = dict(_read_jsonl(ACCEPTANCE_DIR / "transactions.jsonl")[0])

    raw_float = {**raw_tx, "amount_minor": 100.0}
    res_float = validate_transaction_record(raw_float)
    assert not res_float.is_valid
    assert res_float.primary_error == ErrorCode.SCHEMA_TYPE

    with pytest.raises(TypeError):
        Transaction.from_dict(raw_float)

    raw_bool = {**raw_tx, "amount_minor": True}
    with pytest.raises(TypeError):
        Transaction.from_dict(raw_bool)
