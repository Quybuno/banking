"""Mốc D, cửa sổ file sao kê, lịch giao và biến đổi lỗi sao kê (TASK T13)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from n06.generator import deadline_d, format_utc, statement_window

from .conftest import read_jsonl, read_jsonl_dir


def _independent_d(business_date: str) -> str:
    """D = 00:00 giờ Việt Nam (UTC+7, không DST) của ngày sau business_date, tính bằng số học."""
    day = datetime.strptime(business_date, "%Y-%m-%d")
    return (day + timedelta(days=1) - timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.mark.parametrize(
    ("business_date", "expected"),
    [
        ("2026-01-01", "2026-01-01T17:00:00Z"),  # ví dụ trong đặc tả
        ("2026-01-02", "2026-01-02T17:00:00Z"),
        ("2025-12-31", "2025-12-31T17:00:00Z"),  # qua năm
        ("2026-02-28", "2026-02-28T17:00:00Z"),  # sau đó là 1/3 (năm thường)
        ("2028-02-28", "2028-02-28T17:00:00Z"),  # năm nhuận: ngày sau là 29/2
        ("2028-02-29", "2028-02-29T17:00:00Z"),
        ("2026-12-31", "2026-12-31T17:00:00Z"),
    ],
)
def test_d_is_local_midnight_of_next_day_in_utc(business_date: str, expected: str) -> None:
    assert format_utc(deadline_d(business_date, "Asia/Ho_Chi_Minh")) == expected
    assert _independent_d(business_date) == expected


def test_d_has_no_fractional_seconds_and_z_suffix() -> None:
    text = format_utc(deadline_d("2026-03-05", "Asia/Ho_Chi_Minh"))
    assert len(text) == 20 and text.endswith("Z") and "." not in text


def test_d_is_not_utc_midnight() -> None:
    assert format_utc(deadline_d("2026-01-01", "Asia/Ho_Chi_Minh")) != "2026-01-02T00:00:00Z"


def test_d_around_midnight_event_times() -> None:
    """Giao dịch 16:59:59Z (23:59:59 VN) và 17:00:00Z (00:00:00 VN ngày sau) có D khác nhau."""
    from n06.domain import derive_business_date

    before, after = "2026-01-01T16:59:59Z", "2026-01-01T17:00:00Z"
    assert derive_business_date(before) == "2026-01-01"
    assert derive_business_date(after) == "2026-01-02"
    assert (
        format_utc(deadline_d(derive_business_date(before), "Asia/Ho_Chi_Minh"))
        == "2026-01-01T17:00:00Z"
    )
    assert (
        format_utc(deadline_d(derive_business_date(after), "Asia/Ho_Chi_Minh"))
        == "2026-01-02T17:00:00Z"
    )


@pytest.mark.parametrize(
    ("available_at", "window", "w", "file_available_at"),
    [
        ("2026-01-01T00:00:00Z", 3600, "2026-01-01T01:00:00Z", None),
        ("2026-01-01T00:59:59Z", 3600, "2026-01-01T01:00:00Z", None),
        ("2026-01-01T01:00:00Z", 3600, "2026-01-01T02:00:00Z", None),
        ("2026-12-31T23:59:59Z", 3600, "2027-01-01T00:00:00Z", None),
    ],
)
def test_window_end_is_strictly_after_available_at(
    available_at: str, window: int, w: str, file_available_at: Any
) -> None:
    index, file_at = statement_window(available_at, window)
    assert file_at == w
    assert file_at > available_at
    assert (
        index
        == int(datetime.fromisoformat(available_at.replace("Z", "+00:00")).timestamp()) // window
    )


def test_every_delivery_is_after_row_availability_and_matches_window(built: Any) -> None:
    cfg, summary = built
    window = cfg["statement_file_window_seconds"]
    rows = read_jsonl(summary.ground_truth_dir / "statement_rows.jsonl")
    deliveries = read_jsonl(summary.source_dir / "statement_deliveries.jsonl")
    assert len(rows) == len(deliveries) == summary.statement_record_count
    by_id = {d["statement_record_id"]: d for d in deliveries}
    for row in rows:
        avail = datetime.strptime(row["available_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        file_at = datetime.strptime(row["file_available_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=UTC
        )
        assert file_at > avail
        assert int(file_at.timestamp()) == (int(avail.timestamp()) // window + 1) * window
        delivery = by_id[row["statement_record_id"]]
        assert delivery["file_available_at"] == row["file_available_at"]
        assert delivery["file_path"] == row["file_path"]


def test_one_file_per_window_and_channel_sorted_rows(built: Any) -> None:
    cfg, summary = built
    window = cfg["statement_file_window_seconds"]
    seen_keys = set()
    for path in sorted((summary.source_dir / "statements").glob("*/*.jsonl")):
        rows = read_jsonl(path)
        channel = path.parent.name
        assert {r["channel"] for r in rows} == {channel}
        windows = {
            int(
                datetime.strptime(r["available_at"], "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=UTC)
                .timestamp()
            )
            // window
            for r in rows
        }
        assert len(windows) == 1
        key = (channel, windows.pop())
        assert key not in seen_keys
        seen_keys.add(key)
        keys = [(r["available_at"], r["statement_record_id"]) for r in rows]
        assert keys == sorted(keys)


def test_delivery_order_by_file_available_at_then_channel(built: Any) -> None:
    _, summary = built
    deliveries = read_jsonl(summary.source_dir / "statement_deliveries.jsonl")
    per_file: dict[str, tuple[str, str, int]] = {}
    for d in deliveries:
        channel = d["file_path"].split("/")[1]
        info = (d["file_available_at"], channel, d["delivery_order"])
        assert per_file.setdefault(d["file_path"], info) == info
    ordered = sorted(per_file.values(), key=lambda t: (t[0], t[1]))
    assert [t[2] for t in ordered] == list(range(len(ordered)))
    assert [d["delivery_order"] for d in deliveries] == sorted(
        d["delivery_order"] for d in deliveries
    )


def test_statement_transformations_match_fault_labels(built: Any) -> None:
    cfg, summary = built
    txs = {
        t["transaction_id"]: t
        for t in read_jsonl_dir(summary.ground_truth_dir / "canonical", "transactions_part_*.jsonl")
    }
    fault_of = {
        r["transaction_id"]: r["fault_type"]
        for r in read_jsonl(summary.ground_truth_dir / "fault_assignment.jsonl")
    }
    gt_rows = read_jsonl(summary.ground_truth_dir / "statement_rows.jsonl")
    statements = {
        r["statement_record_id"]: r
        for r in read_jsonl_dir(summary.source_dir / "statements", "*/*.jsonl")
    }
    by_tx: dict[str, list[dict[str, Any]]] = {}
    for gt in gt_rows:
        by_tx.setdefault(gt["transaction_id"], []).append(gt)

    for tx_id, tx in txs.items():
        fault = fault_of.get(tx_id)
        rows = by_tx.get(tx_id, [])
        if fault == "missing_external":
            assert rows == []
            continue
        assert len(rows) == (2 if fault == "statement_duplicate" else 1)
        for gt in rows:
            st = statements[gt["statement_record_id"]]
            assert st["external_reference"] == tx["external_reference"]
            assert st["channel"] == tx["channel"]
            assert (
                st["event_time"] == tx["event_time"] and st["business_date"] == tx["business_date"]
            )
            amount_ok = st["amount_minor"] == tx["amount_minor"] + (
                1 if fault == "amount_mismatch" else 0
            )
            assert amount_ok
            other = {"VND": "USD", "USD": "VND"}[tx["currency"]]
            assert st["currency"] == (other if fault == "currency_mismatch" else tx["currency"])
            assert st["status"] == ("FAILED" if fault == "status_mismatch" else tx["status"])
            if fault in ("late_within_deadline", "late_after_deadline"):
                delay_key = (
                    "late_within_delay_after_day_end_seconds"
                    if fault == "late_within_deadline"
                    else "late_after_delay_after_day_end_seconds"
                )
                d = datetime.strptime(_independent_d(tx["business_date"]), "%Y-%m-%dT%H:%M:%SZ")
                expected = (d + timedelta(seconds=cfg[delay_key])).strftime("%Y-%m-%dT%H:%M:%SZ")
                assert st["available_at"] == expected
                cutoff = d + timedelta(seconds=cfg["allowed_lateness_seconds"])
                got = datetime.strptime(st["available_at"], "%Y-%m-%dT%H:%M:%SZ")
                file_at = datetime.strptime(gt["file_available_at"], "%Y-%m-%dT%H:%M:%SZ")
                if fault == "late_within_deadline":
                    assert got < cutoff and file_at < cutoff
                else:
                    assert got >= cutoff
            else:
                et = datetime.strptime(tx["event_time"], "%Y-%m-%dT%H:%M:%SZ")
                expected = (et + timedelta(seconds=cfg["normal_statement_delay_seconds"])).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                assert st["available_at"] == expected
            assert st["available_at"] >= tx["event_time"]
        if fault == "statement_duplicate":
            a, b = (statements[g["statement_record_id"]] for g in rows)
            assert a["statement_record_id"] != b["statement_record_id"]
            assert {k: v for k, v in a.items() if k != "statement_record_id"} == {
                k: v for k, v in b.items() if k != "statement_record_id"
            }
            assert rows[0]["file_path"] == rows[1]["file_path"]
