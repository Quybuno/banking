"""Ground truth tách khỏi source/ và nhãn lỗi không rò vào dữ liệu pipeline đọc (TASK T13)."""

from __future__ import annotations

import json
from typing import Any

from .conftest import read_jsonl

FORBIDDEN = [
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
    "fault",
    "ground_truth",
    "canonical",
    "expected_",
    "kind",
]


def test_no_fault_label_or_ground_truth_path_under_source(built: Any) -> None:
    _, summary = built
    files = [p for p in summary.source_dir.rglob("*") if p.is_file()]
    assert files
    for path in files:
        text = path.read_text("utf-8")
        for token in FORBIDDEN:
            assert token not in text, f"{path} chứa {token!r}"
        assert "ground_truth" not in path.as_posix().split("source/", 1)[1]


def test_ground_truth_outside_source_tree(built: Any) -> None:
    _, summary = built
    assert summary.source_dir.parent == summary.ground_truth_dir.parent
    assert not summary.ground_truth_dir.is_relative_to(summary.source_dir)
    assert not summary.source_dir.is_relative_to(summary.ground_truth_dir)
    names = {p.name for p in summary.ground_truth_dir.iterdir()}
    assert {
        "fault_assignment.jsonl",
        "statement_rows.jsonl",
        "fault_counts.json",
        "canonical",
        "canonical_index.json",
    } <= names


def test_ground_truth_has_no_reconciliation_status_or_balances(built: Any) -> None:
    _, summary = built
    for path in summary.ground_truth_dir.glob("*.json*"):
        text = path.read_text("utf-8")
        for token in ("MATCHED", "PENDING", "MISSING_EXTERNAL", "balance"):
            assert token not in text


def test_canonical_is_written_once_and_indexed(built: Any) -> None:
    _, summary = built
    index = json.loads((summary.ground_truth_dir / "canonical_index.json").read_text("utf-8"))
    assert index["business_checksum"] == summary.business_checksum
    for kind, pattern in (
        ("transactions", "transactions_part_*.jsonl"),
        ("ledger_entries", "ledger_entries_part_*.jsonl"),
    ):
        rows = sum(
            len(read_jsonl(p)) for p in summary.ground_truth_dir.joinpath("canonical").glob(pattern)
        )
        assert index[kind]["rows"] == rows
    # Nguồn chỉ có accounts (nhỏ) giống hệt bản canonical; không có transactions/ledger sao chép.
    assert not list(summary.source_dir.rglob("transactions*"))
    assert not list(summary.source_dir.rglob("ledger_entries*"))
