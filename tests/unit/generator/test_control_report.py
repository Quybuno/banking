"""Control report: quy tắc dòng, replay tính một lần, khớp fixture byte-by-byte (TASK T13)."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from n06.domain import validate_control_report_data
from n06.generator import ControlTotals, build_control_report_from_envelopes

from .conftest import ACCEPTANCE_DIR, read_jsonl, read_jsonl_dir


def _fixture_events() -> list[dict[str, Any]]:
    return read_jsonl(ACCEPTANCE_DIR / "events.jsonl")


def _canonical_line(report: dict[str, Any]) -> str:
    return json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"


def test_fixture_report_matches_byte_by_byte() -> None:
    events = _fixture_events()
    report = build_control_report_from_envelopes(events, "1.0.0", "n06-acceptance-v1")
    expected = (ACCEPTANCE_DIR / "control_report.json").read_text("utf-8")
    assert _canonical_line(report) == expected
    assert validate_control_report_data(report).is_valid


def test_fixture_replay_counted_once_and_missing_internal_absent() -> None:
    events = _fixture_events()
    # Số liệu vật lý của fixture theo đặc tả mục 6: 36 dòng, 35 event_id, 34 bản ghi nghiệp vụ.
    assert len(events) == 36
    assert len({e["event_id"] for e in events}) == 35
    business = {
        (e["event_type"], e["payload"].get("transaction_id"), e["payload"].get("entry_id"))
        for e in events
    }
    assert len(business) == 34
    # Có đúng hai replay tường minh: một cùng event_id, một khác event_id cùng business ID.
    id_counts = Counter(e["event_id"] for e in events)
    assert [c for c in id_counts.values() if c > 1] == [2]
    tx_ids = Counter(
        e["payload"]["transaction_id"] for e in events if e["event_type"] == "TRANSACTION_CREATED"
    )
    assert sorted(c for c in tx_ids.values() if c > 1) == [2, 2]
    # REF-X012 không có trong events của fixture (thiếu internal), không cần lọc thủ công.
    assert "REF-X012" not in (ACCEPTANCE_DIR / "events.jsonl").read_text("utf-8")

    naive = ControlTotals()
    for env in events:
        naive.add(
            "transaction" if env["event_type"] == "TRANSACTION_CREATED" else "ledger_entry",
            env["payload"],
        )
    naive_rows = {
        (r["entity"], r["business_date"], r["status_or_side"]): r["record_count"]
        for r in naive.to_report("1.0.0", "n06-acceptance-v1")["totals"]
    }
    deduped = {
        (r["entity"], r["business_date"], r["status_or_side"]): r["record_count"]
        for r in build_control_report_from_envelopes(events, "1.0.0", "n06-acceptance-v1")["totals"]
    }
    # Nếu không khử replay thì SUCCESS ngày 01/01 là 12 thay vì 10.
    assert naive_rows[("transaction", "2026-01-01", "SUCCESS")] == 12
    assert deduped[("transaction", "2026-01-01", "SUCCESS")] == 10


def test_row_rules_sorted_unique_no_zero_rows(built: Any) -> None:
    cfg, summary = built
    text = (summary.source_dir / "control_report.json").read_text("utf-8")
    assert text.endswith("\n") and text.count("\n") == 1
    report = json.loads(text)
    assert validate_control_report_data(report).is_valid
    keys = [
        (r["entity"], r["business_date"], r["currency"], r["status_or_side"])
        for r in report["totals"]
    ]
    assert keys == sorted(keys) and len(keys) == len(set(keys))
    assert all(r["record_count"] >= 1 for r in report["totals"])
    assert {r["status_or_side"] for r in report["totals"] if r["entity"] == "ledger_entry"} <= {
        "DEBIT",
        "CREDIT",
    }
    assert {r["status_or_side"] for r in report["totals"] if r["entity"] == "transaction"} <= {
        "SUCCESS",
        "FAILED",
    }


def test_generated_report_equals_independent_totals(built: Any) -> None:
    cfg, summary = built
    txs = read_jsonl_dir(summary.ground_truth_dir / "canonical", "transactions_part_*.jsonl")
    ledger = read_jsonl_dir(summary.ground_truth_dir / "canonical", "ledger_entries_part_*.jsonl")
    missing = {
        r["transaction_id"]
        for r in read_jsonl(summary.ground_truth_dir / "fault_assignment.jsonl")
        if r["fault_type"] == "missing_internal"
    }
    expected: dict[tuple[str, str, str, str], list[int]] = {}
    for t in txs:
        if t["transaction_id"] in missing:
            continue
        slot = expected.setdefault(
            ("transaction", t["business_date"], t["currency"], t["status"]), [0, 0]
        )
        slot[0] += 1
        slot[1] += t["amount_minor"]
    for e in ledger:
        if e["transaction_id"] in missing:
            continue
        slot = expected.setdefault(
            ("ledger_entry", e["business_date"], e["currency"], e["side"]), [0, 0]
        )
        slot[0] += 1
        slot[1] += e["amount_minor"]
    report = json.loads((summary.source_dir / "control_report.json").read_text("utf-8"))
    got = {
        (r["entity"], r["business_date"], r["currency"], r["status_or_side"]): [
            r["record_count"],
            r["amount_minor_total"],
        ]
        for r in report["totals"]
    }
    assert got == expected
    # Báo cáo dựng lại từ events thực sự đã ghi (có replay) cũng phải bằng đúng báo cáo này.
    events = read_jsonl_dir(summary.source_dir / "events", "events_batch_*.jsonl")
    assert (
        build_control_report_from_envelopes(events, cfg["contract_version"], cfg["dataset_id"])
        == report
    )
