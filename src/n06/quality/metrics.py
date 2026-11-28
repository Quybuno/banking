"""Quality metrics, control totals so với `control_report.json` và cổng chặn publish (TASK T32).

Tuân thủ đặc tả Phần I mục 6 ("Kiểm tra chất lượng và control totals"), bảng `quality_metrics`
(ADR-0003) và ADR-0014:
- tách bạch theo từng `business_date`: physical records, unique event, unique business record,
  bản ghi bị loại, replay; luôn có `physical = unique_business + replay + rejected`;
- count, distinct keys, `amount_minor_total` (số nguyên) theo `(entity, business_date, currency,
  status_or_side)`, không cộng ledger với transaction;
- đối chiếu từng dòng với control report; chênh lệch phải giải thích được bằng bản ghi bị loại;
- lỗi vượt ngưỡng cấu hình thì chặn publish.

Module thuần Python: không nhập PySpark.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from n06.storage.control_store import QualityMetricRecord

CONTROL_ENTITIES = ("transaction", "ledger_entry")
PARSE_ERROR_ENTITY = "parse_error"
BPS = 10_000


@dataclass(frozen=True)
class ControlTotalRow:
    """Một dòng tổng; năm trường đầu và `amount_minor_total` khớp `control_report.schema.json`."""

    entity: str  # "transaction" | "ledger_entry" | "statement"
    business_date: date
    currency: str
    status_or_side: str  # "SUCCESS" | "FAILED" | "DEBIT" | "CREDIT"
    record_count: int
    amount_minor_total: int
    distinct_keys: int | None = None

    def group_key(self) -> tuple[str, str, str, str]:
        return (self.entity, self.business_date.isoformat(), self.currency, self.status_or_side)


@dataclass(frozen=True)
class LayerCounts:
    """Đếm của một entity trong một `business_date`, từ bản ghi vật lý tới Silver."""

    entity: str
    business_date: date
    physical_records: int
    rejected_records: int
    unique_business_records: int
    unique_events: int | None = None  # sao kê và parse_error không có event_id

    @property
    def replay_records(self) -> int:
        """Bản ghi được chấp nhận nhưng gộp vào một bản ghi nghiệp vụ khác (replay/duplicate)."""
        return self.physical_records - self.rejected_records - self.unique_business_records


@dataclass(frozen=True)
class RejectedKey:
    """Một khóa nghiệp vụ có trong Bronze nhưng không vào Silver, theo một nhóm đối chiếu."""

    entity: str
    business_date: str
    currency: str
    status_or_side: str
    business_key: str
    min_amount_minor: int
    max_amount_minor: int


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

    def group_key(self) -> tuple[str, str, str, str]:
        return (self.entity, self.business_date, self.currency, self.status_or_side)


@dataclass(frozen=True)
class QualityThresholds:
    """Ngưỡng cổng chất lượng; giá trị lấy từ mục `[quality]` của cấu hình."""

    max_quarantine_rate_bps: int = 0
    max_conflict_count: int = 0
    fail_on_unexplained_control_mismatch: bool = True

    def __post_init__(self) -> None:
        if not 0 <= self.max_quarantine_rate_bps <= BPS:
            raise ValueError("max_quarantine_rate_bps phải nằm trong [0, 10000]")
        if self.max_conflict_count < 0:
            raise ValueError("max_conflict_count phải >= 0")


@dataclass(frozen=True)
class QualityEvaluation:
    """Kết quả đánh giá chất lượng và cổng chặn publish cho một run."""

    metrics: tuple[QualityMetricRecord, ...]
    comparisons: tuple[ControlTotalComparison, ...] = field(default_factory=tuple)
    unexplained: tuple[ControlTotalComparison, ...] = field(default_factory=tuple)
    physical_records: int = 0
    rejected_records: int = 0
    should_block_publish: bool = False
    blocking_reasons: tuple[str, ...] = field(default_factory=tuple)

    @property
    def mismatches(self) -> tuple[ControlTotalComparison, ...]:
        return tuple(c for c in self.comparisons if not c.matched)


def _to_date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _require_int(value: object, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name}: tiền và số đếm phải là int, nhận {type(value).__name__}")
    return value


def compute_control_totals(
    transactions: Sequence[Mapping[str, Any]],
    ledger_entries: Sequence[Mapping[str, Any]],
) -> list[ControlTotalRow]:
    """Control totals của bản ghi Python (test và fixture); pipeline dùng bản Spark."""
    buckets: dict[tuple[str, date, str, str], list[int]] = defaultdict(lambda: [0, 0])
    for entity, rows, group_field in (
        ("transaction", transactions, "status"),
        ("ledger_entry", ledger_entries, "side"),
    ):
        for row in rows:
            key = (
                entity,
                _to_date(row["business_date"]),
                str(row["currency"]),
                str(row[group_field]),
            )
            buckets[key][0] += 1
            buckets[key][1] += _require_int(row["amount_minor"], f"{entity}.amount_minor")
    totals = [ControlTotalRow(*key, counts[0], counts[1]) for key, counts in buckets.items()]
    return sorted(totals, key=lambda r: r.group_key())


def parse_control_report_totals(control_report: Mapping[str, Any]) -> list[ControlTotalRow]:
    """Đọc danh sách `ControlTotalRow` từ dict `control_report.json`."""
    rows = [
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
        for item in control_report.get("totals", [])
    ]
    return sorted(rows, key=lambda r: r.group_key())


def compare_control_totals(
    actual_rows: Sequence[ControlTotalRow],
    expected_control_report: Mapping[str, Any],
    *,
    business_dates: Collection[date] = (),
) -> list[ControlTotalComparison]:
    """So từng nhóm của `transaction` và `ledger_entry`; `business_dates` rỗng là mọi ngày."""

    def in_scope(row: ControlTotalRow) -> bool:
        return row.entity in CONTROL_ENTITIES and (
            not business_dates or row.business_date in business_dates
        )

    actual_map = {r.group_key(): r for r in actual_rows if in_scope(r)}
    expected_map = {
        r.group_key(): r
        for r in parse_control_report_totals(expected_control_report)
        if in_scope(r)
    }
    comparisons: list[ControlTotalComparison] = []
    for key in sorted(set(actual_map) | set(expected_map)):
        exp = expected_map.get(key)
        act = actual_map.get(key)
        comparisons.append(
            ControlTotalComparison(
                *key,
                expected_count=exp.record_count if exp else 0,
                actual_count=act.record_count if act else 0,
                expected_amount_minor=exp.amount_minor_total if exp else 0,
                actual_amount_minor=act.amount_minor_total if act else 0,
            )
        )
    return comparisons


def is_explained(comparison: ControlTotalComparison, rejected: Sequence[RejectedKey]) -> bool:
    """Chênh lệch có giải thích được bằng khóa bị loại của đúng nhóm đó không (ADR-0014).

    Chỉ chấp nhận Silver THIẾU so với nguồn: số khóa thiếu không vượt số khóa bị loại của nhóm và
    số tiền thiếu nằm trong khoảng mà đúng ngần ấy khóa bị loại có thể tạo ra. Thừa bản ghi hoặc
    lệch tiền khi đủ số bản ghi không bao giờ là "giải thích được".
    """
    if comparison.matched:
        return True
    missing = -comparison.count_diff
    shortfall = -comparison.amount_diff
    group = [
        r
        for r in rejected
        if (r.entity, r.business_date, r.currency, r.status_or_side) == comparison.group_key()
    ]
    if missing <= 0 or missing > len(group):
        return False
    lowest = sum(sorted(r.min_amount_minor for r in group)[:missing])
    highest = sum(sorted((r.max_amount_minor for r in group), reverse=True)[:missing])
    return lowest <= shortfall <= highest


def evaluate_quality_gate(
    *,
    run_id: str,
    dataset_id: str,
    default_date: date,
    layer_counts: Sequence[LayerCounts] = (),
    totals: Sequence[ControlTotalRow] = (),
    quarantine_by_date: Mapping[date, int] | None = None,
    conflicts_by_date: Mapping[date, int] | None = None,
    control_report: Mapping[str, Any] | None = None,
    rejected_keys: Sequence[RejectedKey] = (),
    business_dates: Collection[date] = (),
    thresholds: QualityThresholds | None = None,
) -> QualityEvaluation:
    """Sinh toàn bộ `quality_metrics` của run và quyết định có chặn publish hay không.

    `default_date` là ngày gắn cho metric phạm vi cả run (`layer='run'`).
    """
    policy = thresholds or QualityThresholds()
    metrics: list[QualityMetricRecord] = []

    def add(
        layer: str,
        entity: str,
        day: date,
        name: str,
        value: int,
        status: str = "PASS",
        details: dict[str, Any] | None = None,
    ) -> None:
        metrics.append(
            QualityMetricRecord(
                run_id=run_id,
                dataset_id=dataset_id,
                layer=layer,
                entity=entity,
                business_date=day,
                metric_name=name,
                metric_value=value,
                status=status,
                details=details or {},
            )
        )

    for c in layer_counts:
        day = c.business_date
        add("bronze", c.entity, day, "physical_records", c.physical_records)
        if c.unique_events is not None:
            add("bronze", c.entity, day, "unique_events", c.unique_events)
        add("bronze", c.entity, day, "replay_records", c.replay_records)
        add(
            "bronze",
            c.entity,
            day,
            "rejected_records",
            c.rejected_records,
            "WARN" if c.rejected_records else "PASS",
        )
        if c.entity != PARSE_ERROR_ENTITY:
            add("silver", c.entity, day, "unique_business_records", c.unique_business_records)

    for t in totals:
        suffix = f"{t.currency}.{t.status_or_side}"
        add("silver", t.entity, t.business_date, f"record_count.{suffix}", t.record_count)
        add(
            "silver",
            t.entity,
            t.business_date,
            f"amount_minor_total.{suffix}",
            t.amount_minor_total,
        )
        if t.distinct_keys is not None:
            add("silver", t.entity, t.business_date, f"distinct_keys.{suffix}", t.distinct_keys)

    for day, count in sorted((quarantine_by_date or {}).items()):
        add("quarantine", "records", day, "quarantine_records", count, "WARN" if count else "PASS")

    blocking: list[str] = []

    physical = sum(c.physical_records for c in layer_counts)
    rejected = sum(c.rejected_records for c in layer_counts)
    rate_bps = rejected * BPS // physical if physical else 0
    # So bằng số nguyên, không làm tròn: 1 bản ghi trên ngưỡng 0 vẫn phải bị chặn.
    over_rate = rejected * BPS > policy.max_quarantine_rate_bps * physical
    if over_rate:
        blocking.append(
            f"Bản ghi bị loại {rejected}/{physical} vượt ngưỡng "
            f"{policy.max_quarantine_rate_bps} bps"
        )
    rate_status = "FAIL" if over_rate else "WARN" if rejected else "PASS"
    add("run", "all", default_date, "physical_records", physical)
    add("run", "all", default_date, "rejected_records", rejected, rate_status)
    add("run", "all", default_date, "quarantine_rate_bps", rate_bps, rate_status)

    conflicts = conflicts_by_date or {}
    conflict_total = sum(conflicts.values())
    over_conflicts = conflict_total > policy.max_conflict_count
    if over_conflicts:
        blocking.append(f"Xung đột khóa {conflict_total} vượt ngưỡng {policy.max_conflict_count}")
    for day, count in sorted(conflicts.items()):
        add(
            "silver",
            "key_conflicts",
            day,
            "conflict_records",
            count,
            "FAIL" if over_conflicts else "WARN" if count else "PASS",
        )
    add(
        "run",
        "all",
        default_date,
        "conflict_records",
        conflict_total,
        "FAIL" if over_conflicts else "WARN" if conflict_total else "PASS",
    )

    comparisons: list[ControlTotalComparison] = []
    unexplained: list[ControlTotalComparison] = []
    if control_report is None:
        add("control", "control_totals", default_date, "control_report_missing", 1, "WARN")
    else:
        comparisons = compare_control_totals(totals, control_report, business_dates=business_dates)
        unexplained = [c for c in comparisons if not is_explained(c, rejected_keys)]
        bad_status = "FAIL" if policy.fail_on_unexplained_control_mismatch else "WARN"
        by_date: dict[str, list[ControlTotalComparison]] = defaultdict(list)
        for c in comparisons:
            by_date[c.business_date].append(c)
            status = "PASS" if c.matched else bad_status if c in unexplained else "WARN"
            suffix = f"{c.currency}.{c.status_or_side}"
            detail = {**asdict(c), "explained": c not in unexplained}
            day = _to_date(c.business_date)
            add(
                "control",
                c.entity,
                day,
                f"control_count_diff.{suffix}",
                c.count_diff,
                status,
                detail,
            )
            add(
                "control",
                c.entity,
                day,
                f"control_amount_diff.{suffix}",
                c.amount_diff,
                status,
                detail,
            )
        for day_text, rows in sorted(by_date.items()):
            wrong = [c for c in rows if not c.matched]
            not_explained = sum(1 for c in wrong if c in unexplained)
            add(
                "control",
                "control_totals",
                _to_date(day_text),
                "control_total_mismatches",
                len(wrong),
                bad_status if not_explained else "WARN" if wrong else "PASS",
                {"groups_checked": len(rows), "unexplained_groups": not_explained},
            )
        if unexplained and policy.fail_on_unexplained_control_mismatch:
            blocking.append(
                f"Lệch control report không giải thích được ở "
                f"{len(unexplained)}/{len(comparisons)} nhóm"
            )

    add(
        "run",
        "all",
        default_date,
        "publish_blocked",
        int(bool(blocking)),
        "FAIL" if blocking else "PASS",
        {"reasons": blocking, "thresholds": asdict(policy)},
    )
    return QualityEvaluation(
        metrics=tuple(metrics),
        comparisons=tuple(comparisons),
        unexplained=tuple(unexplained),
        physical_records=physical,
        rejected_records=rejected,
        should_block_publish=bool(blocking),
        blocking_reasons=tuple(blocking),
    )
