"""`n06 process-day` trên dữ liệu có lỗi chủ đích (profile `demo-faults`), so với oracle T14.

Chuỗi thật: generate -> publish -> stream -> wait-ingestion -> ingest-statements -> process-day.
Kỳ vọng do `tests/harness/oracle.py` tính từ byte file nguồn đã công bố (events, sao kê, lịch giao),
không dùng mã đối soát đang kiểm và không đọc ground truth của generator.
"""

from __future__ import annotations

import contextlib
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from test_process_day_clean import REPO_ROOT, Stack, _lines, _result, build_stack, ingest

if str(REPO_ROOT) not in sys.path:  # để import được `tests.harness`
    sys.path.insert(0, str(REPO_ROOT))

from tests.harness.oracle import compare_against_oracle, evaluate_snapshot  # noqa: E402

from n06.reconciliation import ReconStatus  # noqa: E402

# Đủ lớn để mỗi loại lỗi (1 % mỗi loại) rơi vào nhiều ngày; dataset 01/01..03/01.
OVERRIDES = {
    "transaction_count": 600,
    "from_date": "2026-01-01",
    "to_date": "2026-01-03",
    "max_offsets": "1000",
}
# Sau cutoff của ngày 01/01 (02/01 17:00Z), trước cutoff của ngày 02/01: có cả PENDING và MISSING.
AS_OF_MID = "2026-01-02T18:00:00Z"
AS_OF_LATE = "2026-01-10T00:00:00Z"


@pytest.fixture(scope="module")
def faulty(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    with contextlib.contextmanager(build_stack)(
        tmp_path_factory, "generator.demo-faults.json", dict(OVERRIDES)
    ) as stack:
        yield ingest(stack)


def _run(stack: Stack, as_of: str, tmp: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    proc = stack.process_day(as_of=as_of)
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    result = _result(proc, "N06_PROCESS_DAY_RESULT=")
    paths = [
        f"s3a://{stack.bucket}/{stack.cfg.paths.gold_prefix}/{stack.dataset_id}/"
        f"fact_reconciliation/run_id={result['run_id']}/business_date={day}"
        for day in result["business_dates"]
    ]
    answer = stack.spark(
        {"action": "read", "frames": {"r": {"kind": "reconciliation_full", "paths": paths}}}, tmp
    )
    return result, answer["r"]


@pytest.fixture(scope="module", params=[AS_OF_MID, AS_OF_LATE], ids=["mid", "late"])
def run(
    request: pytest.FixtureRequest, faulty: Stack, tmp_path_factory: pytest.TempPathFactory
) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    as_of = str(request.param)
    result, rows = _run(faulty, as_of, tmp_path_factory.mktemp("pd-faults"))
    return as_of, result, rows


def _source_records(source: Path) -> tuple[list[dict], list[dict], list[dict]]:
    transactions = [
        {**e["payload"], "dataset_id": e["dataset_id"]}
        for e in _lines(source, "events")
        if e["event_type"] == "TRANSACTION_CREATED"
    ]
    statements = [
        json.loads(line)
        for path in sorted((source / "statements").rglob("*.jsonl"))
        for line in path.read_bytes().split(b"\n")
        if line
    ]
    deliveries = [
        json.loads(line)
        for line in (source / "statement_deliveries.jsonl").read_bytes().split(b"\n")
        if line
    ]
    return transactions, statements, deliveries


def test_every_key_matches_the_independent_oracle(
    faulty: Stack, run: tuple[str, dict[str, Any], list[dict[str, Any]]]
) -> None:
    as_of, result, rows = run
    transactions, statements, deliveries = _source_records(faulty.source)
    expected = evaluate_snapshot(
        transactions=transactions,
        statements=statements,
        deliveries=deliveries,
        as_of_time=as_of,
    )
    report = compare_against_oracle(expected, rows)
    assert report.passed, report.to_dict()["mismatches"][:10]
    counts = {k: v for k, v in result["reconciliation"]["status_counts"].items() if v}
    assert counts == {k: v for k, v in expected.counts_by_status.items() if v}


def test_each_injected_error_shows_up_as_a_status(
    run: tuple[str, dict[str, Any], list[dict[str, Any]]],
) -> None:
    as_of, result, rows = run
    seen = {r["recon_status"] for r in rows}
    expected = {s.value for s in ReconStatus}
    if as_of == AS_OF_LATE:
        expected.discard(ReconStatus.PENDING.value)  # mọi khóa đã quá hạn
    assert seen == expected, result["reconciliation"]
    # Khóa lệch thuộc tính giữ đủ giá trị hai phía để trình bày.
    for row in rows:
        if row["recon_status"] == ReconStatus.AMOUNT_MISMATCH.value:
            assert row["internal_amount_minor"] != row["statement_amount_minor"]
        if row["recon_status"] == ReconStatus.CURRENCY_MISMATCH.value:
            assert row["internal_currency"] != row["statement_currency"]
        if row["recon_status"] == ReconStatus.STATUS_MISMATCH.value:
            assert row["internal_status"] != row["statement_status"]


def test_replays_and_injected_faults_do_not_break_the_control_report(
    faulty: Stack, run: tuple[str, dict[str, Any], list[dict[str, Any]]]
) -> None:
    _, result, _ = run
    quality = result["quality"]
    assert quality["control_groups_checked"] > 0
    assert (quality["control_mismatches"], quality["rejected_records"]) == (0, 0)
    # Lỗi chủ đích có replay: đếm vật lý lớn hơn số bản ghi nghiệp vụ.
    metrics = faulty.store.list_quality_metrics(run_id=result["run_id"])
    replay = sum(
        m.metric_value
        for m in metrics
        if m.metric_name == "replay_records" and m.entity == "transaction"
    )
    assert replay > 0
