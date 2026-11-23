"""Integration tests cho generator kịch bản gốc (TASK T12)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from n06.generator import generate_base_scenario


def _verify_disk_outputs_independently(out_dir: Path) -> tuple[str, int, int]:
    """Đọc lại toàn bộ JSONL từ đĩa, kiểm tra cân bằng sổ cái và tính checksum độc lập."""
    accounts = {
        acc["account_id"]: int(acc["opening_balance_minor"])
        for acc in (
            json.loads(line)
            for line in (out_dir / "accounts.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    }

    txs = [
        json.loads(line)
        for path in sorted(out_dir.glob("transactions_part_*.jsonl"))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    les_by_tx: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(out_dir.glob("ledger_entries_part_*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                le = json.loads(line)
                les_by_tx.setdefault(le["transaction_id"], []).append(le)

    total_debit = 0
    total_credit = 0
    hasher = hashlib.sha256()

    for line in (out_dir / "accounts.jsonl").read_text(encoding="utf-8").splitlines():
        if line.strip():
            hasher.update(line.encode("utf-8"))
            hasher.update(b"\n")

    for tx in txs:
        hasher.update(
            json.dumps(tx, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
        )
        hasher.update(b"\n")
        entries = les_by_tx.get(tx["transaction_id"], [])
        if tx["status"] == "SUCCESS":
            assert len(entries) == 2
            sides = {e["side"]: e for e in entries}
            assert set(sides) == {"DEBIT", "CREDIT"}
            assert sides["DEBIT"]["ledger_account_id"] == tx["account_from"]
            assert sides["CREDIT"]["ledger_account_id"] == tx["account_to"]
            assert sides["DEBIT"]["amount_minor"] == tx["amount_minor"]
            assert sides["CREDIT"]["amount_minor"] == tx["amount_minor"]
            accounts[tx["account_from"]] -= tx["amount_minor"]
            accounts[tx["account_to"]] += tx["amount_minor"]
            total_debit += sides["DEBIT"]["amount_minor"]
            total_credit += sides["CREDIT"]["amount_minor"]
        else:
            assert entries == []

        for le in entries:
            hasher.update(
                json.dumps(le, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
            hasher.update(b"\n")

    assert all(bal >= 0 for bal in accounts.values())
    assert total_debit == total_credit
    return hasher.hexdigest(), total_debit, len(txs)


def test_generate_base_scenario_uniform_and_hot_account_across_chunk_sizes(
    tmp_path: Path,
) -> None:
    base_cfg: dict[str, Any] = {
        "contract_version": "1.0.0",
        "profile": "demo-clean",
        "seed": 99,
        "dataset_id": "n06-t12-integration",
        "business_timezone": "Asia/Ho_Chi_Minh",
        "from_date": "2026-01-01",
        "to_date": "2026-01-03",
        "transaction_count": 300,
        "account_count": 30,
        "currency": "VND",
        "currency_exponents": {"VND": 0, "USD": 2},
        "opening_balance_minor_per_account": 500_000,
        "amount_min_minor": 100,
        "amount_max_minor": 2_000,
        "amount_distribution": "UNIFORM_INTEGER",
        "success_basis_points": 9500,
        "channel_weights_basis_points": {"SIM_CHANNEL_A": 7000, "SIM_CHANNEL_B": 3000},
        "account_selection": "UNIFORM_DISTINCT_PAIR",
        "hot_account_count": 0,
        "hot_account_basis_points": 0,
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
        "generation_chunk_rows": 37,
        "normal_statement_delay_seconds": 60,
        "statement_file_window_seconds": 3600,
        "late_within_delay_after_day_end_seconds": 43200,
        "late_after_delay_after_day_end_seconds": 108000,
        "insufficient_funds_policy": "MARK_FAILED",
        "notes": "T12 integration test config",
    }

    s1 = generate_base_scenario(base_cfg, tmp_path / "u_37")
    cfg_u200 = {**base_cfg, "generation_chunk_rows": 200}
    s2 = generate_base_scenario(cfg_u200, tmp_path / "u_200")

    cs1, deb1, count1 = _verify_disk_outputs_independently(tmp_path / "u_37")
    cs2, deb2, count2 = _verify_disk_outputs_independently(tmp_path / "u_200")
    assert count1 == count2 == 300
    assert deb1 == deb2
    assert s1.business_checksum == cs1 == cs2 == s2.business_checksum

    hot_cfg = {
        **base_cfg,
        "profile": "bench-skew",
        "account_selection": "HOT_ACCOUNT_V1",
        "hot_account_count": 3,
        "hot_account_basis_points": 6000,
        "generation_chunk_rows": 41,
    }
    sh1 = generate_base_scenario(hot_cfg, tmp_path / "h_41")
    sh2 = generate_base_scenario({**hot_cfg, "generation_chunk_rows": 300}, tmp_path / "h_300")
    csh1, _, _ = _verify_disk_outputs_independently(tmp_path / "h_41")
    csh2, _, _ = _verify_disk_outputs_independently(tmp_path / "h_300")
    assert sh1.business_checksum == csh1 == csh2 == sh2.business_checksum
