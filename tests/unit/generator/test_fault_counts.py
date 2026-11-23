"""Số lỗi theo công thức floor và chọn lỗi không chồng lấn (TASK T13, Phần II mục 5.3).

Expected được tính lại độc lập trong test (hashlib/json chuẩn), không gọi hàm đang kiểm thử.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest

from n06.generator import FaultConfigError, build_dataset, compute_fault_counts

from .conftest import read_jsonl, read_jsonl_dir

ORDER = [
    "event_replay",
    "business_replay",
    "missing_internal",
    "missing_external",
    "statement_duplicate",
    "amount_mismatch",
    "currency_mismatch",
    "status_mismatch",
    "late_within_deadline",
    "late_after_deadline",
]


def _expected_count(eligible: int, bp: int) -> int:
    return eligible * bp // 10_000


def _hash(contract_version: str, seed: int, transaction_id: str) -> str:
    payload = json.dumps(
        [contract_version, seed, "fault-order", transaction_id],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@pytest.mark.parametrize("eligible", [0, 1, 99, 100, 101, 199, 200, 9_999, 10_000, 123_457])
@pytest.mark.parametrize("bp", [0, 1, 100, 250, 3333])
def test_floor_formula_for_many_totals(eligible: int, bp: int) -> None:
    counts = compute_fault_counts(
        eligible, {name: bp for name in ORDER[:3]} | {n: 0 for n in ORDER[3:]}
    )
    for name in ORDER[:3]:
        assert counts[name] == _expected_count(eligible, bp)


def test_boundary_where_floor_is_zero() -> None:
    bp = {name: 100 for name in ORDER}
    assert compute_fault_counts(99, bp) == dict.fromkeys(ORDER, 0)
    assert compute_fault_counts(100, bp) == dict.fromkeys(ORDER, 1)
    assert compute_fault_counts(199, bp) == dict.fromkeys(ORDER, 1)
    assert compute_fault_counts(200, bp) == dict.fromkeys(ORDER, 2)


def test_rejects_basis_points_sum_over_limit() -> None:
    bp = {name: 0 for name in ORDER} | {"event_replay": 6000, "business_replay": 4001}
    with pytest.raises(FaultConfigError):
        compute_fault_counts(1000, bp)


def test_total_count_never_exceeds_eligible() -> None:
    bp = {name: 1000 for name in ORDER}
    for eligible in range(0, 300):
        assert sum(compute_fault_counts(eligible, bp).values()) <= eligible


def test_dataset_fault_counts_and_membership_match_independent_computation(
    built: Any,
) -> None:
    cfg, summary = built
    canonical = summary.ground_truth_dir / "canonical"
    txs = read_jsonl_dir(canonical, "transactions_part_*.jsonl")
    eligible = [t for t in txs if t["status"] == "SUCCESS"]
    ranked = sorted(
        eligible,
        key=lambda t: (
            _hash(cfg["contract_version"], cfg["seed"], t["transaction_id"]),
            t["transaction_id"],
        ),
    )
    expected: dict[str, str] = {}
    start = 0
    expected_counts: dict[str, int] = {}
    for name in ORDER:
        count = _expected_count(len(eligible), cfg["faults"][f"{name}_basis_points"])
        expected_counts[name] = count
        for tx in ranked[start : start + count]:
            expected[tx["transaction_id"]] = name
        start += count

    assert summary.eligible_success_count == len(eligible)
    assert summary.fault_counts == expected_counts
    assert all(c > 0 for c in expected_counts.values())

    assigned = {
        row["transaction_id"]: row["fault_type"]
        for row in read_jsonl(summary.ground_truth_dir / "fault_assignment.jsonl")
    }
    assert assigned == expected
    # Mỗi giao dịch tối đa một lỗi và chỉ SUCCESS mới bị tiêm.
    success_ids = {t["transaction_id"] for t in eligible}
    assert set(assigned) <= success_ids

    doc = json.loads((summary.ground_truth_dir / "fault_counts.json").read_text("utf-8"))
    assert doc["counts"] == expected_counts
    assert doc["eligible_success_count"] == len(eligible)
    assert doc["zero_count_faults"] == []


def test_zero_count_faults_are_reported(tmp_path: Any, make_config: Any) -> None:
    cfg = make_config(transaction_count=40)
    summary = build_dataset(cfg, tmp_path)
    assert summary.eligible_success_count < 100
    doc = json.loads((summary.ground_truth_dir / "fault_counts.json").read_text("utf-8"))
    assert doc["zero_count_faults"] == ORDER
    assert doc["injected_total"] == 0
    assert read_jsonl(summary.ground_truth_dir / "fault_assignment.jsonl") == []
    # Không lỗi nào được tiêm: số event = 1 transaction + 2 ledger cho mỗi SUCCESS, 1 cho FAILED.
    txs = read_jsonl_dir(summary.ground_truth_dir / "canonical", "transactions_part_*.jsonl")
    expected_events = sum(3 if t["status"] == "SUCCESS" else 1 for t in txs)
    assert summary.event_record_count == expected_events
    assert summary.statement_record_count == len(txs)


def test_non_default_basis_points_use_floor(tmp_path: Any, make_config: Any) -> None:
    cfg = make_config(
        faults={"event_replay_basis_points": 333, "late_after_deadline_basis_points": 77}
    )
    summary = build_dataset(cfg, tmp_path)
    s = summary.eligible_success_count
    assert summary.fault_counts["event_replay"] == s * 333 // 10_000
    assert summary.fault_counts["late_after_deadline"] == s * 77 // 10_000
