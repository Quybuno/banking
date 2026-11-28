"""Quality metrics, control totals so với `control_report.json` và ngưỡng chặn publish (TASK T32).

Tuân thủ đặc tả Phần I mục 6 (dòng 320–327) và bảng `quality_metrics` (ADR-0003):
- Tách bạch các đếm: physical records, unique events, unique business records, quarantine, conflict.
- Tính count và `amount_minor_total` (số nguyên `int`, không float) theo
  `(entity, business_date, currency, status_or_side)`, tách riêng `transaction` và `ledger_entry`.
- Đối chiếu từng dòng với `control_report.json` của nguồn.
- Đánh giá ngưỡng chặn publish (`QualityThresholds`); lỗi nghiêm trọng chặn publish Gold.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from n06.storage.control_store import QualityMetricRecord


@dataclass(frozen=True)
class ControlTotalRow:
    """Một dòng tổng đối soát khớp `control_report.schema.json`."""

    entity: str  # "transaction" | "ledger_entry"
    business_date: date
    currency: str
    status_or_side: str  # "SUCCESS" | "FAILED" | "DEBIT" | "CREDIT"
    record_count: int
    amount_minor_total: int

    def group_key(self) -> tuple[str, str, str, str]:
        return (self.entity, self.business_date.isoformat(), self.currency, self.status_or_side)

    def as_dict(self) -> dict[str, object]:
        return {
            "entity": self.entity,
            "business_date": self.business_date.isoformat(),
            "currency": self.currency,
            "status_or_side": self.status_or_side,
            "record_count": self.record_count,
            "amount_minor_total": self.amount_minor_total,
        }


@dataclass(frozen=True)
class ControlTotalComparison:
    """Kết quả đối chiếu một nhóm `(entity, business_date, currency, status_or_side)`."""

    entity: str
    business_date: str
    currency: str
    status_or_side: str
    expected_count: int
    actual_count: int
    expected_amount_minor: int
    actual_amount_minor: int

    @property
    def count_diff(self) -> int:
        return self.actual_count - self.expected_count

    @property
    def amount_diff(self) -> int:
        return self.actual_amount_minor - self.expected_amount_minor

    @property
    def matched(self) -> bool:
        return self.count_diff == 0 and self.amount_diff == 0


@dataclass(frozen=True)
class QualityThresholds:
    """Ngưỡng chất lượng cấu hình được cho cổng chặn publish Gold (spec I.6 dòng 327)."""

    max_quarantine_rate: float = 0.0
    max_conflict_count: int = 0
    fail_on_control_total_mismatch: bool = True

    def __post_init__(self) -> None:
        if not 0.0 <= self.max_quarantine_rate <= 1.0:
            raise ValueError("max_quarantine_rate phải nằm trong [0.0, 1.0]")
        if self.max_conflict_count < 0:
            raise ValueError("max_conflict_count phải >= 0")


@dataclass(frozen=True)
class QualityEvaluation:
    """Kết quả đánh giá chất lượng và cổng chặn publish cho một run."""

    metrics: tuple[QualityMetricRecord, ...]
    comparisons: tuple[ControlTotalComparison, ...] = field(default_factory=tuple)
    should_block_publish: bool = False
    blocking_reasons: tuple[str, ...] = field(default_factory=tuple)


def _to_date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _require_int(value: object, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name}: tiền phải là int, nhận {type(value).__name__}")
    return value


def compute_control_totals(
    transactions: Sequence[Mapping[str, Any]],
    ledger_entries: Sequence[Mapping[str, Any]],
) -> list[ControlTotalRow]:
    """Tính control totals từ Silver `transactions` và `ledger_entries` (tách riêng 2 entity)."""
    buckets: dict[tuple[str, date, str, str], list[int]] = defaultdict(lambda: [0, 0])

    for tx in transactions:
        key = (
            "transaction",
            _to_date(tx["business_date"]),
            str(tx["currency"]),
            str(tx["status"]),
        )
        buckets[key][0] += 1
        buckets[key][1] += _require_int(tx["amount_minor"], "transaction.amount_minor")

    for le in ledger_entries:
        key = (
            "ledger_entry",
            _to_date(le["business_date"]),
            str(le["currency"]),
            str(le["side"]),
        )
        buckets[key][0] += 1
        buckets[key][1] += _require_int(le["amount_minor"], "ledger_entry.amount_minor")

    rows = [
        ControlTotalRow(
            entity=entity,
            business_date=biz_date,
            currency=currency,
            status_or_side=status_or_side,
            record_count=counts[0],
            amount_minor_total=counts[1],
        )
        for (entity, biz_date, currency, status_or_side), counts in buckets.items()
    ]
    return sorted(rows, key=lambda r: r.group_key())


def parse_control_report_totals(control_report: Mapping[str, Any]) -> list[ControlTotalRow]:
    """Đọc danh sách `ControlTotalRow` từ dict `control_report.json`."""
    rows: list[ControlTotalRow] = []
    for item in control_report.get("totals", []):
        rows.append(
            ControlTotalRow(
                entity=str(item["entity"]),
                business_date=_to_date(item["business_date"]),
                currency=str(item["currency"]),
                status_or_side=str(item["status_or_side"]),
                record_count=_require_int(item["record_count"], "control_report.record_count"),
                amount_minor_total=_require_int(
                    item["amount_minor_total"], "control_report.amount_minor_total"
                ),
            )
        )
    return sorted(rows, key=lambda r: r.group_key())


def compare_control_totals(
    actual_rows: Sequence[ControlTotalRow],
    expected_control_report: Mapping[str, Any],
) -> list[ControlTotalComparison]:
    """So khớp từng nhóm `(entity, business_date, currency, status_or_side)` với control report."""
    actual_map = {r.group_key(): r for r in actual_rows}
    expected_map = {r.group_key(): r for r in parse_control_report_totals(expected_control_report)}

    comparisons: list[ControlTotalComparison] = []
    for key in sorted(set(actual_map) | set(expected_map)):
        entity, biz_date, currency, status_or_side = key
        exp = expected_map.get(key)
        act = actual_map.get(key)
        comparisons.append(
            ControlTotalComparison(
                entity=entity,
                business_date=biz_date,
                currency=currency,
                status_or_side=status_or_side,
                expected_count=exp.record_count if exp else 0,
                actual_count=act.record_count if act else 0,
                expected_amount_minor=exp.amount_minor_total if exp else 0,
                actual_amount_minor=act.amount_minor_total if act else 0,
            )
        )
    return comparisons


def evaluate_quality_gate(
    *,
    run_id: str,
    dataset_id: str,
    business_date: date,
    physical_records: int,
    unique_business_records: int,
    quarantine_records: int,
    conflict_records: int,
    actual_totals: Sequence[ControlTotalRow] = (),
    expected_control_report: Mapping[str, Any] | None = None,
    thresholds: QualityThresholds | None = None,
) -> QualityEvaluation:
    """Tính toàn bộ `quality_metrics` và quyết định có chặn publish Gold hay không."""
    policy = thresholds or QualityThresholds()
    total_considered = max(1, unique_business_records + quarantine_records)
    quarantine_rate = quarantine_records / total_considered
    quarantine_rate_bps = round(quarantine_rate * 10_000)

    blocking: list[str] = []

    q_status = "PASS"
    if quarantine_rate > policy.max_quarantine_rate:
        q_status = "FAIL"
        blocking.append(
            f"Tỷ lệ quarantine {quarantine_rate:.4f} vượt ngưỡng {policy.max_quarantine_rate:.4f}"
        )
    elif quarantine_records > 0:
        q_status = "WARN"

    c_status = "PASS"
    if conflict_records > policy.max_conflict_count:
        c_status = "FAIL"
        blocking.append(
            f"Số xung đột khóa ({conflict_records}) vượt ngưỡng {policy.max_conflict_count}"
        )
    elif conflict_records > 0:
        c_status = "WARN"

    comparisons: list[ControlTotalComparison] = []
    mismatch_count = 0
    ct_status = "PASS"
    if expected_control_report is not None:
        comparisons = compare_control_totals(actual_totals, expected_control_report)
        mismatches = [c for c in comparisons if not c.matched]
        mismatch_count = len(mismatches)
        if mismatch_count > 0:
            if policy.fail_on_control_total_mismatch:
                ct_status = "FAIL"
                blocking.append(
                    f"Lệch control_report ở {mismatch_count}/{len(comparisons)} nhóm tổng"
                )
            else:
                ct_status = "WARN"

    metrics: list[QualityMetricRecord] = [
        QualityMetricRecord(
            run_id=run_id,
            dataset_id=dataset_id,
            layer="silver",
            entity="all",
            business_date=business_date,
            metric_name="physical_records",
            metric_value=physical_records,
            status="PASS",
        ),
        QualityMetricRecord(
            run_id=run_id,
            dataset_id=dataset_id,
            layer="silver",
            entity="all",
            business_date=business_date,
            metric_name="unique_business_records",
            metric_value=unique_business_records,
            status="PASS",
        ),
        QualityMetricRecord(
            run_id=run_id,
            dataset_id=dataset_id,
            layer="quarantine",
            entity="records",
            business_date=business_date,
            metric_name="quarantine_records",
            metric_value=quarantine_records,
            status=q_status,
            details={"quarantine_rate_bps": quarantine_rate_bps},
        ),
        QualityMetricRecord(
            run_id=run_id,
            dataset_id=dataset_id,
            layer="silver",
            entity="key_conflicts",
            business_date=business_date,
            metric_name="conflict_records",
            metric_value=conflict_records,
            status=c_status,
        ),
    ]
    if expected_control_report is not None:
        metrics.append(
            QualityMetricRecord(
                run_id=run_id,
                dataset_id=dataset_id,
                layer="silver",
                entity="control_totals",
                business_date=business_date,
                metric_name="control_total_mismatches",
                metric_value=mismatch_count,
                status=ct_status,
                details={
                    "groups_checked": len(comparisons),
                    "mismatched_groups": [
                        {
                            "entity": c.entity,
                            "business_date": c.business_date,
                            "currency": c.currency,
                            "status_or_side": c.status_or_side,
                            "count_diff": c.count_diff,
                            "amount_diff": c.amount_diff,
                        }
                        for c in comparisons
                        if not c.matched
                    ],
                },
            )
        )

    return QualityEvaluation(
        metrics=tuple(metrics),
        comparisons=tuple(comparisons),
        should_block_publish=bool(blocking),
        blocking_reasons=tuple(blocking),
    )
