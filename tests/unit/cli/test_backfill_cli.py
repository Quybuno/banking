"""Unit test cho lệnh `n06 backfill` (TASK T42)."""

from __future__ import annotations

import io
import json

from n06.cli import main
from n06.cli.backfill import expand_date_range, run_backfill
from n06.cli.doctor import ExitCode


def test_expand_date_range_inclusive() -> None:
    assert expand_date_range("2026-01-01", "2026-01-03") == (
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
    )
    assert expand_date_range("2026-01-02", "2026-01-02") == ("2026-01-02",)


def test_backfill_rejects_invalid_date_range_without_calling_jobs() -> None:
    called: list[str] = []
    buf = io.StringIO()
    code = run_backfill(
        dataset_id="ds-01",
        from_date="2026-01-05",
        to_date="2026-01-01",
        as_of_time="2026-01-06T00:00:00Z",
        as_json=True,
        process_day_fn=lambda **kw: called.append("process-day") or 0,
        load_mart_fn=lambda **kw: called.append("load-mart") or 0,
        out=buf,
    )
    assert code == int(ExitCode.CONFIG_ERROR)
    assert called == []
    payload = json.loads(buf.getvalue())
    assert payload["ok"] is False


def test_backfill_stops_when_process_day_fails_or_blocked() -> None:
    called: list[str] = []
    buf = io.StringIO()
    code = run_backfill(
        dataset_id="ds-01",
        from_date="2026-01-01",
        to_date="2026-01-02",
        as_of_time="2026-01-05T00:00:00Z",
        as_json=True,
        process_day_fn=lambda **kw: (called.append("process-day"), int(ExitCode.SERVICE_ERROR))[1],
        load_mart_fn=lambda **kw: (called.append("load-mart"), 0)[1],
        out=buf,
    )
    assert code == int(ExitCode.SERVICE_ERROR)
    assert called == ["process-day"]
    payload = json.loads(buf.getvalue())
    assert payload["stage_failed"] == "process-day"
    assert payload["load_mart_exit_code"] is None


def test_backfill_runs_process_day_then_load_mart_and_supports_mart_only() -> None:
    seen_dates: list[tuple[str, ...]] = []
    seen_mart: list[tuple[str, str]] = []
    buf = io.StringIO()

    def fake_pday(**kw):  # type: ignore[no-untyped-def]
        seen_dates.append(tuple(kw["business_dates"]))
        return 0

    def fake_mart(**kw):  # type: ignore[no-untyped-def]
        seen_mart.append((kw["from_date"], kw["to_date"]))
        return 0

    code = run_backfill(
        dataset_id="ds-01",
        from_date="2026-01-01",
        to_date="2026-01-03",
        as_of_time="2026-01-05T00:00:00Z",
        as_json=True,
        process_day_fn=fake_pday,
        load_mart_fn=fake_mart,
        out=buf,
    )
    assert code == 0
    assert seen_dates == [("2026-01-01", "2026-01-02", "2026-01-03")]
    assert seen_mart == [("2026-01-01", "2026-01-03")]

    # Retry chỉ chạy load-mart (--mart-only) không gọi lại process-day
    buf2 = io.StringIO()
    code2 = run_backfill(
        dataset_id="ds-01",
        from_date="2026-01-01",
        to_date="2026-01-03",
        as_of_time="2026-01-05T00:00:00Z",
        mart_only=True,
        as_json=True,
        process_day_fn=fake_pday,
        load_mart_fn=fake_mart,
        out=buf2,
    )
    assert code2 == 0
    assert len(seen_dates) == 1
    assert len(seen_mart) == 2


def test_cli_help_includes_backfill() -> None:
    buf = io.StringIO()
    code = main(["backfill", "--help"], out=buf)
    assert code == 0
    assert "--from-date" in buf.getvalue()
    assert "--to-date" in buf.getvalue()
