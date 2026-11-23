"""Tác động của từng loại lỗi lên events và sao kê; mọi record sau tiêm vẫn qua T10 (TASK T13)."""

from __future__ import annotations

from collections import Counter
from typing import Any

from n06.domain import ConflictTracker, validate_event_envelope, validate_statement_record
from n06.generator import build_statement_rows
from n06.generator.params import DatasetParams

from .conftest import read_jsonl, read_jsonl_dir


def _load(summary: Any) -> tuple[dict[str, Any], dict[str, str], list[dict[str, Any]]]:
    txs = {
        t["transaction_id"]: t
        for t in read_jsonl_dir(summary.ground_truth_dir / "canonical", "transactions_part_*.jsonl")
    }
    fault_of = {
        r["transaction_id"]: r["fault_type"]
        for r in read_jsonl(summary.ground_truth_dir / "fault_assignment.jsonl")
    }
    events = read_jsonl_dir(summary.source_dir / "events", "events_batch_*.jsonl")
    return txs, fault_of, events


def test_event_effects_per_fault(built: Any) -> None:
    _, summary = built
    txs, fault_of, events = _load(summary)
    tx_events: dict[str, list[dict[str, Any]]] = {}
    ledger_events: Counter[str] = Counter()
    for env in events:
        assert env["emitted_at"] == env["event_time"]
        if env["event_type"] == "TRANSACTION_CREATED":
            tx_events.setdefault(env["payload"]["transaction_id"], []).append(env)
        else:
            ledger_events[env["payload"]["transaction_id"]] += 1

    for tx_id, tx in txs.items():
        fault = fault_of.get(tx_id)
        got = tx_events.get(tx_id, [])
        if fault == "missing_internal":
            assert got == [] and ledger_events[tx_id] == 0
            continue
        expected_n = 2 if fault in ("event_replay", "business_replay") else 1
        assert len(got) == expected_n
        assert all(e["payload"] == tx for e in got)
        assert ledger_events[tx_id] == (2 if tx["status"] == "SUCCESS" else 0)
        if fault == "event_replay":
            assert got[0] == got[1]
        if fault == "business_replay":
            assert got[0]["event_id"] != got[1]["event_id"]


def test_after_injection_all_records_pass_t10_and_replays_are_not_conflicts(built: Any) -> None:
    _, summary = built
    accounts = {a["account_id"]: a for a in read_jsonl(summary.source_dir / "accounts.jsonl")}
    tracker = ConflictTracker()
    events = read_jsonl_dir(summary.source_dir / "events", "events_batch_*.jsonl")
    for env in events:
        assert validate_event_envelope(env, accounts=accounts).is_valid
        assert tracker.check_and_register_envelope(env).is_valid
    assert tracker.conflicts == []
    for path in (summary.source_dir / "statements").glob("*/*.jsonl"):
        for row in read_jsonl(path):
            assert validate_statement_record(row).is_valid


def test_amount_mismatch_uses_minus_one_at_contract_maximum(make_config: Any) -> None:
    params = DatasetParams.from_config(make_config())
    tx = {
        "external_reference": "REF-X",
        "channel": "SIM_CHANNEL_A",
        "amount_minor": 10**12,
        "currency": "VND",
        "status": "SUCCESS",
        "event_time": "2026-01-01T01:00:00Z",
        "business_date": "2026-01-01",
    }
    (row, _), *_ = build_statement_rows(tx, 0, "amount_mismatch", params)
    assert row["amount_minor"] == 10**12 - 1
    (row, _), *_ = build_statement_rows(
        {**tx, "amount_minor": 10**12 - 1}, 0, "amount_mismatch", params
    )
    assert row["amount_minor"] == 10**12


def test_currency_mismatch_swaps_both_ways_without_conversion(make_config: Any) -> None:
    params = DatasetParams.from_config(make_config())
    tx = {
        "external_reference": "REF-X",
        "channel": "SIM_CHANNEL_B",
        "amount_minor": 12345,
        "currency": "USD",
        "status": "FAILED",
        "event_time": "2026-01-01T01:00:00Z",
        "business_date": "2026-01-01",
    }
    (row, _), *_ = build_statement_rows(tx, 5, "currency_mismatch", params)
    assert (row["currency"], row["amount_minor"]) == ("VND", 12345)
