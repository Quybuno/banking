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
    LayerCounts,
    QualityThresholds,
    RejectedKey,
    compare_control_totals,
    compute_control_totals,
    evaluate_quality_gate,
    is_explained,
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


DAY = date(2026, 1, 1)


def _report(*rows: tuple[str, str, int, int]) -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "dataset_id": "ds-1",
        "totals": [
            {
                "entity": entity,
                "business_date": DAY.isoformat(),
                "currency": "VND",
                "status_or_side": status,
                "record_count": count,
                "amount_minor_total": amount,
            }
            for entity, status, count, amount in rows
        ],
    }


def _total(count: int, amount: int) -> ControlTotalRow:
    return ControlTotalRow("transaction", DAY, "VND", "SUCCESS", count, amount, count)


def _rejected(key: str, low: int, high: int) -> RejectedKey:
    return RejectedKey("transaction", DAY.isoformat(), "VND", "SUCCESS", key, low, high)


def _gate(**kwargs: Any) -> Any:
    return evaluate_quality_gate(run_id="run-1", dataset_id="ds-1", default_date=DAY, **kwargs)


def _metrics(evaluation: Any) -> dict[tuple[str, str, str], Any]:
    keys = [(m.layer, m.entity, m.business_date, m.metric_name) for m in evaluation.metrics]
    assert len(keys) == len(set(keys)), "metric trùng khóa unique của quality_metrics"
    return {(m.layer, m.entity, m.metric_name): m for m in evaluation.metrics}


@pytest.mark.parametrize(
    ("count", "amount", "rejected", "explained"),
    [
        (3, 600, [], True),  # khớp
        (2, 400, [_rejected("a", 200, 200)], True),  # thiếu đúng một khóa bị loại
        (2, 400, [_rejected("a", 150, 250)], True),  # xung đột: tiền thiếu nằm trong khoảng
        (2, 400, [_rejected("a", 100, 150)], False),  # tiền thiếu ngoài khoảng
        (2, 400, [], False),  # thiếu mà không có khóa bị loại
        (1, 200, [_rejected("a", 200, 200)], False),  # thiếu nhiều hơn số khóa bị loại
        (3, 599, [_rejected("a", 1, 1)], False),  # đủ bản ghi nhưng lệch tiền
        (4, 800, [_rejected("a", 200, 200)], False),  # Silver thừa
    ],
)
def test_mismatch_is_explained_only_by_rejected_keys_of_that_group(
    count: int, amount: int, rejected: list[RejectedKey], explained: bool
) -> None:
    report = _report(("transaction", "SUCCESS", 3, 600))
    comparison = compare_control_totals([_total(count, amount)], report)[0]
    assert is_explained(comparison, rejected) is explained


def test_gate_saves_totals_counts_and_control_rows_and_passes_clean_data() -> None:
    result = _gate(
        layer_counts=[LayerCounts("transaction", DAY, 5, 0, 3, unique_events=4)],
        totals=[_total(3, 600)],
        control_report=_report(("transaction", "SUCCESS", 3, 600)),
    )
    found = _metrics(result)
    assert result.should_block_publish is False
    assert found[("bronze", "transaction", "physical_records")].metric_value == 5
    assert found[("bronze", "transaction", "unique_events")].metric_value == 4
    assert found[("bronze", "transaction", "replay_records")].metric_value == 2
    assert found[("silver", "transaction", "unique_business_records")].metric_value == 3
    assert found[("silver", "transaction", "record_count.VND.SUCCESS")].metric_value == 3
    assert found[("silver", "transaction", "distinct_keys.VND.SUCCESS")].metric_value == 3
    assert found[("silver", "transaction", "amount_minor_total.VND.SUCCESS")].metric_value == 600
    assert found[("control", "transaction", "control_count_diff.VND.SUCCESS")].status == "PASS"
    assert found[("control", "control_totals", "control_total_mismatches")].metric_value == 0
    assert found[("run", "all", "publish_blocked")].metric_value == 0


def test_default_thresholds_block_a_single_rejected_record_without_rounding() -> None:
    result = _gate(
        layer_counts=[LayerCounts("transaction", DAY, 1_000_000, 1, 999_999)],
        control_report=None,
    )
    found = _metrics(result)
    assert result.should_block_publish is True
    assert found[("run", "all", "quarantine_rate_bps")].metric_value == 0  # làm tròn xuống
    assert found[("run", "all", "quarantine_rate_bps")].status == "FAIL"
    assert found[("control", "control_totals", "control_report_missing")].status == "WARN"


def test_demo_policy_warns_on_explained_errors_and_blocks_unexplained_ones() -> None:
    policy = QualityThresholds(max_quarantine_rate_bps=100, max_conflict_count=5)
    common: dict[str, Any] = {
        "layer_counts": [LayerCounts("transaction", DAY, 1000, 2, 998)],
        "conflicts_by_date": {DAY: 2},
        "quarantine_by_date": {DAY: 2},
        "control_report": _report(("transaction", "SUCCESS", 3, 600)),
        "totals": [_total(2, 400)],
        "thresholds": policy,
    }
    explained = _gate(rejected_keys=[_rejected("a", 200, 200)], **common)
    found = _metrics(explained)
    assert explained.should_block_publish is False
    assert found[("control", "transaction", "control_count_diff.VND.SUCCESS")].status == "WARN"
    assert found[("silver", "key_conflicts", "conflict_records")].status == "WARN"
    assert found[("quarantine", "records", "quarantine_records")].metric_value == 2

    unexplained = _gate(rejected_keys=[], **common)
    assert unexplained.should_block_publish is True
    assert len(unexplained.unexplained) == 1
    assert len(unexplained.blocking_reasons) == 1
    blocked = _metrics(unexplained)[("run", "all", "publish_blocked")]
    assert blocked.metric_value == 1 and blocked.details["reasons"]


def test_conflicts_over_threshold_block_publish() -> None:
    result = _gate(
        conflicts_by_date={DAY: 6},
        thresholds=QualityThresholds(max_conflict_count=5),
    )
    assert result.should_block_publish is True


def test_business_date_filter_limits_the_comparison() -> None:
    report = _report(("transaction", "SUCCESS", 3, 600))
    assert compare_control_totals([], report, business_dates={date(2026, 1, 2)}) == []
    assert len(compare_control_totals([], report, business_dates={DAY})) == 1


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
