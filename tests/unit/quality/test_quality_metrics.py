"""Unit tests cho Quality Metrics, Control Totals và ngưỡng chặn publish (TASK T32)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from n06.domain import CONTRACT_ROOT
from n06.quality import (
    ControlTotalRow,
    QualityThresholds,
    compare_control_totals,
    compute_control_totals,
    evaluate_quality_gate,
)

ACCEPTANCE_DIR = CONTRACT_ROOT / "fixtures" / "acceptance"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def test_acceptance_control_totals_match_every_row_of_control_report() -> None:
    txs = _read_jsonl(ACCEPTANCE_DIR / "transactions.jsonl")
    ledgers = _read_jsonl(ACCEPTANCE_DIR / "ledger_entries.jsonl")
    control_report = json.loads(
        (ACCEPTANCE_DIR / "control_report.json").read_text(encoding="utf-8")
    )

    actual = compute_control_totals(txs, ledgers)
    comparisons = compare_control_totals(actual, control_report)

    assert len(comparisons) == 7
    assert all(c.matched for c in comparisons), [c for c in comparisons if not c.matched]
    # Không cộng gộp transaction và ledger_entry thành một tổng
    assert {c.entity for c in comparisons} == {"transaction", "ledger_entry"}


def test_compare_control_totals_detects_count_amount_missing_and_extra_groups() -> None:
    expected = {
        "schema_version": "1.0.0",
        "dataset_id": "ds-1",
        "totals": [
            {
                "entity": "transaction",
                "business_date": "2026-01-01",
                "currency": "VND",
                "status_or_side": "SUCCESS",
                "record_count": 2,
                "amount_minor_total": 300,
            },
            {
                "entity": "transaction",
                "business_date": "2026-01-01",
                "currency": "VND",
                "status_or_side": "FAILED",
                "record_count": 1,
                "amount_minor_total": 50,
            },
        ],
    }
    actual = [
        ControlTotalRow("transaction", date(2026, 1, 1), "VND", "SUCCESS", 2, 299),
        ControlTotalRow("ledger_entry", date(2026, 1, 1), "VND", "DEBIT", 1, 100),
    ]
    comps = {c.status_or_side: c for c in compare_control_totals(actual, expected)}
    assert comps["SUCCESS"].matched is False and comps["SUCCESS"].amount_diff == -1
    assert comps["FAILED"].matched is False and comps["FAILED"].count_diff == -1
    assert comps["DEBIT"].matched is False and comps["DEBIT"].count_diff == 1


def test_quality_gate_blocks_publish_when_thresholds_exceeded() -> None:
    eval_fail = evaluate_quality_gate(
        run_id="run-1",
        dataset_id="ds-1",
        business_date=date(2026, 1, 1),
        physical_records=120,
        unique_business_records=90,
        quarantine_records=10,
        conflict_records=2,
        thresholds=QualityThresholds(max_quarantine_rate=0.05, max_conflict_count=0),
    )
    assert eval_fail.should_block_publish is True
    assert len(eval_fail.blocking_reasons) == 2
    by_name = {m.metric_name: m for m in eval_fail.metrics}
    assert by_name["quarantine_records"].status == "FAIL"
    assert by_name["quarantine_records"].details["quarantine_rate_bps"] == 1000
    assert by_name["conflict_records"].status == "FAIL"

    # Khi nâng ngưỡng theo policy demo có lỗi chủ đích -> WARN nhưng không chặn publish
    eval_warn = evaluate_quality_gate(
        run_id="run-2",
        dataset_id="ds-1",
        business_date=date(2026, 1, 1),
        physical_records=120,
        unique_business_records=90,
        quarantine_records=10,
        conflict_records=2,
        thresholds=QualityThresholds(max_quarantine_rate=0.2, max_conflict_count=5),
    )
    assert eval_warn.should_block_publish is False
    by_name_w = {m.metric_name: m for m in eval_warn.metrics}
    assert by_name_w["quarantine_records"].status == "WARN"
    assert by_name_w["conflict_records"].status == "WARN"


def test_compute_control_totals_rejects_float_amounts() -> None:
    with pytest.raises(TypeError, match="int"):
        compute_control_totals(
            [
                {
                    "business_date": "2026-01-01",
                    "currency": "VND",
                    "status": "SUCCESS",
                    "amount_minor": 10.5,
                }
            ],
            [],
        )
