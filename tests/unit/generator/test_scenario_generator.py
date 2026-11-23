"""Unit tests cho generator kịch bản gốc (TASK T12).

Expected checksum, số dư và thứ tự được tính độc lập trong test,
không gọi ngược hàm sinh đang kiểm thử để tạo expected.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from n06.domain import (
    validate_account_record,
    validate_ledger_entry_record,
    validate_transaction_ledger_group,
    validate_transaction_record,
)
from n06.generator import (
    external_sort_jsonl,
    generate_base_scenario,
    select_account_pair,
)


def _base_config(
    *,
    dataset_id: str = "n06-t12-unit",
    tx_count: int = 20,
    account_count: int = 5,
    opening_balance: int = 100_000,
    amount_min: int = 1_000,
    amount_max: int = 5_000,
    chunk_rows: int = 7,
    account_selection: str = "UNIFORM_DISTINCT_PAIR",
    hot_account_count: int = 0,
    hot_account_basis_points: int = 0,
    from_date: str = "2026-01-01",
    to_date: str = "2026-01-03",
) -> dict[str, Any]:
    return {
        "contract_version": "1.0.0",
        "profile": "demo-clean",
        "seed": 42,
        "dataset_id": dataset_id,
        "business_timezone": "Asia/Ho_Chi_Minh",
        "from_date": from_date,
        "to_date": to_date,
        "transaction_count": tx_count,
        "account_count": account_count,
        "currency": "VND",
        "currency_exponents": {"VND": 0, "USD": 2},
        "opening_balance_minor_per_account": opening_balance,
        "amount_min_minor": amount_min,
        "amount_max_minor": amount_max,
        "amount_distribution": "UNIFORM_INTEGER",
        "success_basis_points": 9000,
        "channel_weights_basis_points": {"SIM_CHANNEL_A": 7000, "SIM_CHANNEL_B": 3000},
        "account_selection": account_selection,
        "hot_account_count": hot_account_count,
        "hot_account_basis_points": hot_account_basis_points,
        "time_distribution": "UNIFORM_SECONDS_WITHIN_BUSINESS_DAY",
        "allowed_lateness_seconds": 86400,
        "amount_tolerance_minor": 0,
        "seed_namespace": "ef2331f9-2c98-5cc5-b955-09c647d87cb0",
        "faults": {
            "policy": "EXACT_DISJOINT_COUNTS_V1",
            "event_replay_basis_points": 0,
            "business_replay_basis_points": 0,
            "missing_internal_basis_points": 0,
            "missing_external_basis_points": 0,
            "statement_duplicate_basis_points": 0,
            "amount_mismatch_basis_points": 0,
            "currency_mismatch_basis_points": 0,
            "status_mismatch_basis_points": 0,
            "late_within_deadline_basis_points": 0,
            "late_after_deadline_basis_points": 0,
        },
        "generation_chunk_rows": chunk_rows,
        "normal_statement_delay_seconds": 60,
        "statement_file_window_seconds": 3600,
        "late_within_delay_after_day_end_seconds": 43200,
        "late_after_delay_after_day_end_seconds": 108000,
        "insufficient_funds_policy": "MARK_FAILED",
        "notes": "T12 unit test config",
    }


def _independent_checksum(output_dir: Path) -> str:
    """Tính độc lập SHA-256 trên các file JSONL xuất ra theo thứ tự chuẩn."""
    hasher = hashlib.sha256()
    for line in (output_dir / "accounts.jsonl").read_text(encoding="utf-8").splitlines():
        if line.strip():
            hasher.update(line.encode("utf-8"))
            hasher.update(b"\n")

    tx_files = sorted(output_dir.glob("transactions_part_*.jsonl"))
    le_files = sorted(output_dir.glob("ledger_entries_part_*.jsonl"))

    txs = [
        json.loads(line)
        for path in tx_files
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    les_by_tx: dict[str, list[dict[str, Any]]] = {}
    for path in le_files:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            le = json.loads(line)
            les_by_tx.setdefault(le["transaction_id"], []).append(le)

    for tx in txs:
        raw_tx = json.dumps(tx, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        hasher.update(raw_tx.encode("utf-8"))
        hasher.update(b"\n")
        for le in les_by_tx.get(tx["transaction_id"], []):
            raw_le = json.dumps(le, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
            hasher.update(raw_le.encode("utf-8"))
            hasher.update(b"\n")
    return hasher.hexdigest()


def test_external_sort_multi_pass_tie_breaks_by_ordinal() -> None:
    records = [
        {"event_time": "2026-01-01T02:00:00Z", "ordinal": 5, "v": "e"},
        {"event_time": "2026-01-01T01:00:00Z", "ordinal": 3, "v": "b"},
        {"event_time": "2026-01-01T01:00:00Z", "ordinal": 1, "v": "a"},
        {"event_time": "2026-01-01T01:30:00Z", "ordinal": 2, "v": "c"},
        {"event_time": "2026-01-01T02:00:00Z", "ordinal": 4, "v": "d"},
    ]
    sorted_items = list(external_sort_jsonl(records, chunk_rows=2, max_open_files=2))
    assert [item["v"] for item in sorted_items] == ["a", "b", "c", "d", "e"]


def test_account_selection_strategies_never_self_transfer() -> None:
    for ordinal in range(50):
        f_u, t_u = select_account_pair(
            contract_version="1.0.0",
            seed=42,
            ordinal=ordinal,
            account_count=10,
            account_selection="UNIFORM_DISTINCT_PAIR",
            hot_account_count=0,
            hot_account_basis_points=0,
        )
        assert 0 <= f_u < 10
        assert 0 <= t_u < 10
        assert f_u != t_u

    hot_from_count = 0
    for ordinal in range(200):
        f_h, t_h = select_account_pair(
            contract_version="1.0.0",
            seed=42,
            ordinal=ordinal,
            account_count=20,
            account_selection="HOT_ACCOUNT_V1",
            hot_account_count=2,
            hot_account_basis_points=8000,
        )
        assert f_h != t_h
        if f_h < 2:
            hot_from_count += 1
    assert hot_from_count > 120


def test_chunk_size_invariance_and_independent_checksum_and_balances(tmp_path: Path) -> None:
    dir_small = tmp_path / "chunk_3"
    dir_large = tmp_path / "chunk_100"

    summary_small = generate_base_scenario(_base_config(chunk_rows=3), dir_small)
    summary_large = generate_base_scenario(_base_config(chunk_rows=100), dir_large)

    expected_small = _independent_checksum(dir_small)
    expected_large = _independent_checksum(dir_large)

    assert summary_small.business_checksum == expected_small
    assert summary_large.business_checksum == expected_large
    assert summary_small.business_checksum == summary_large.business_checksum
    assert summary_small.success_count == summary_large.success_count
    assert summary_small.failed_count == summary_large.failed_count
    assert summary_small.ledger_entry_count == summary_small.success_count * 2
    assert len(summary_small.transaction_files) > 1
    assert len(summary_large.transaction_files) == 1


def test_insufficient_funds_marks_failed_without_ledger_and_passes_t10(tmp_path: Path) -> None:
    out_dir = tmp_path / "low_balance"
    # Số dư đầu kỳ nhỏ (10_000) với 30 giao dịch [3_000..5_000] chắc chắn phát sinh FAILED
    cfg = _base_config(
        tx_count=30,
        account_count=3,
        opening_balance=10_000,
        amount_min=3_000,
        amount_max=5_000,
        chunk_rows=5,
        from_date="2026-01-01",
        to_date="2026-01-01",
    )
    cfg["success_basis_points"] = 10_000
    summary = generate_base_scenario(cfg, out_dir)
    assert summary.insufficient_funds_failed_count > 0
    assert summary.failed_count == summary.insufficient_funds_failed_count

    accounts = {
        acc["account_id"]: acc
        for acc in (
            json.loads(line)
            for line in (out_dir / "accounts.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    }
    balances = {acc_id: acc["opening_balance_minor"] for acc_id, acc in accounts.items()}
    for acc in accounts.values():
        assert validate_account_record(acc).is_valid

    txs = [
        json.loads(line)
        for p in sorted(out_dir.glob("transactions_part_*.jsonl"))
        for line in p.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    les_by_tx: dict[str, list[dict[str, Any]]] = {}
    for p in sorted(out_dir.glob("ledger_entries_part_*.jsonl")):
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.strip():
                le = json.loads(line)
                les_by_tx.setdefault(le["transaction_id"], []).append(le)

    prev_time = ""
    for tx in txs:
        assert tx["event_time"] >= prev_time
        prev_time = tx["event_time"]
        assert validate_transaction_record(tx, accounts=accounts).is_valid
        entries = les_by_tx.get(tx["transaction_id"], [])
        for le in entries:
            assert validate_ledger_entry_record(le, accounts=accounts).is_valid
        assert validate_transaction_ledger_group(tx, entries).is_valid

        if tx["status"] == "SUCCESS":
            assert len(entries) == 2
            balances[tx["account_from"]] -= tx["amount_minor"]
            balances[tx["account_to"]] += tx["amount_minor"]
            assert balances[tx["account_from"]] >= 0
        else:
            assert entries == []
