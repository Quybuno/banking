"""Chọn lỗi với số lượng chính xác và không chồng lấn (Phần II mục 5.3 — TASK T13)."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from n06.generator.determinism import canonical_json
from n06.generator.external_sort import event_time_ordinal_key, external_sort_jsonl
from n06.generator.params import FaultConfigError

# Thứ tự cắt đoạn bắt buộc của mục 5.3.
FAULT_ORDER: tuple[str, ...] = (
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
)

BASIS_POINT_DENOMINATOR = 10_000


@dataclass(frozen=True)
class FaultPlan:
    eligible_count: int
    counts: dict[str, int]

    @property
    def zero_count_faults(self) -> list[str]:
        return [name for name in FAULT_ORDER if self.counts[name] == 0]


def fault_basis_points(config: Mapping[str, Any]) -> dict[str, int]:
    faults = config["faults"]
    return {name: int(faults[f"{name}_basis_points"]) for name in FAULT_ORDER}


def compute_fault_counts(eligible_count: int, basis_points: Mapping[str, int]) -> dict[str, int]:
    """`count = floor(S * bp / 10000)` bằng số nguyên; kiểm tra tổng bp và tổng count."""
    if eligible_count < 0:
        raise FaultConfigError(f"Số giao dịch eligible phải >= 0, nhận {eligible_count}")
    if sum(basis_points[name] for name in FAULT_ORDER) > BASIS_POINT_DENOMINATOR:
        raise FaultConfigError("Tổng basis_points của các lỗi vượt quá 10000.")
    counts = {
        name: (eligible_count * basis_points[name]) // BASIS_POINT_DENOMINATOR
        for name in FAULT_ORDER
    }
    if sum(counts.values()) > eligible_count:
        raise FaultConfigError("Tổng số lỗi vượt số giao dịch eligible.")
    return counts


def fault_rank_key(contract_version: str, seed: int, transaction_id: str) -> str:
    """SHA-256 hex của canonical JSON `[contract_version, seed, 'fault-order', transaction_id]`."""
    payload = canonical_json([contract_version, seed, "fault-order", transaction_id])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def fault_for_rank(rank: int, counts: Mapping[str, int]) -> str | None:
    """Đoạn liên tiếp theo `FAULT_ORDER`: rank thuộc đoạn nào thì nhận lỗi đó."""
    start = 0
    for name in FAULT_ORDER:
        end = start + counts[name]
        if rank < end:
            return name
        start = end
    return None


def plan_faults(eligible_count: int, config: Mapping[str, Any]) -> FaultPlan:
    counts = compute_fault_counts(eligible_count, fault_basis_points(config))
    return FaultPlan(eligible_count=eligible_count, counts=counts)


def iter_fault_labels(
    eligible: Callable[[], Iterator[dict[str, Any]]],
    *,
    contract_version: str,
    seed: int,
    plan: FaultPlan,
    chunk_rows: int,
) -> Iterator[dict[str, Any]]:
    """Sinh nhãn lỗi theo thứ tự `(event_time, ordinal)`.

    `eligible()` trả luồng dict có `ordinal`, `transaction_id`, `event_time` của các giao dịch
    SUCCESS. Hai lượt external sort: theo hash để xếp hạng, rồi theo thời gian để nối lại.
    """

    def ranked_input() -> Iterator[dict[str, Any]]:
        for rec in eligible():
            yield {
                "hash": fault_rank_key(contract_version, seed, rec["transaction_id"]),
                "transaction_id": rec["transaction_id"],
                "ordinal": rec["ordinal"],
                "event_time": rec["event_time"],
            }

    def labelled() -> Iterator[dict[str, Any]]:
        by_hash = external_sort_jsonl(
            ranked_input(),
            key_fn=lambda r: (r["hash"], r["transaction_id"]),
            chunk_rows=chunk_rows,
        )
        for rank, rec in enumerate(by_hash):
            fault = fault_for_rank(rank, plan.counts)
            if fault is not None:
                yield {
                    "event_time": rec["event_time"],
                    "ordinal": rec["ordinal"],
                    "transaction_id": rec["transaction_id"],
                    "fault_type": fault,
                }

    return external_sort_jsonl(labelled(), key_fn=event_time_ordinal_key, chunk_rows=chunk_rows)
