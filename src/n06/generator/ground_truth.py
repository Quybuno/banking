"""Ground truth tách khỏi cây dữ liệu pipeline được đọc (Phần II mục 5.3 — TASK T13).

Không lưu trạng thái đối soát và không lưu số dư: oracle tính từ ledger đã công bố.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from n06.generator.determinism import canonical_json
from n06.generator.fault import FAULT_ORDER, FaultPlan


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(canonical_json(value))
        fh.write("\n")


def stream_sha256_and_rows(paths: Iterable[Path]) -> dict[str, Any]:
    """sha256 của nối các file theo thứ tự và tổng số dòng; không phụ thuộc cách chia part."""
    digest = hashlib.sha256()
    rows = 0
    for path in paths:
        with path.open("rb") as fh:
            while block := fh.read(1 << 20):
                digest.update(block)
                rows += block.count(b"\n")
    return {"sha256": digest.hexdigest(), "rows": rows}


def fault_counts_document(
    *,
    contract_version: str,
    dataset_id: str,
    seed: int,
    transaction_count: int,
    plan: FaultPlan,
    basis_points: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "schema_version": contract_version,
        "dataset_id": dataset_id,
        "seed": seed,
        "transaction_count": transaction_count,
        "eligible_success_count": plan.eligible_count,
        "basis_points": {name: basis_points[name] for name in FAULT_ORDER},
        "counts": {name: plan.counts[name] for name in FAULT_ORDER},
        "injected_total": sum(plan.counts.values()),
        "zero_count_faults": plan.zero_count_faults,
    }
