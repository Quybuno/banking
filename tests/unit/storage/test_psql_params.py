"""Giá trị đi vào `psql` qua stdin, không qua argv và không ghép vào câu lệnh (ADR-0012)."""

from __future__ import annotations

import json
import subprocess
from datetime import date
from typing import Any

import pytest

from n06.config import PostgresDbConfig
from n06.storage import IngestBatchRecord, PsqlCliControlStore, QualityMetricRecord, RunRecord
from n06.storage import control_store as module

CONFIG = PostgresDbConfig(host="db", port=5432, database="d", user="u", password="pw-9917")
MARKER = "giá-trị-đánh-dấu"


def _capture(
    monkeypatch: pytest.MonkeyPatch,
    stdout_by_call: list[str] | None = None,
) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        idx = len(calls)
        calls.append({"argv": argv, **kwargs})
        out = stdout_by_call[idx] if stdout_by_call and idx < len(stdout_by_call) else "[]"
        return subprocess.CompletedProcess(argv, 0, out, "")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    return calls


def _assert_stdin_only(calls: list[dict[str, Any]]) -> None:
    for call in calls:
        assert not any(MARKER in arg or "pw-9917" in arg for arg in call["argv"])
        first_line, _, statements = call["input"].partition("\n")
        assert first_line.startswith("\\set params '") and MARKER in first_line
        assert MARKER not in statements  # câu lệnh SQL không chứa giá trị
        assert call["env"]["PGCLIENTENCODING"] == "UTF8"
        assert call["encoding"] == "utf-8"


def test_values_travel_on_stdin_only(monkeypatch: pytest.MonkeyPatch) -> None:
    run_row = json.dumps(
        {
            "run_id": MARKER,
            "dataset_id": MARKER,
            "job_name": MARKER,
            "profile": "test",
            "business_date_from": None,
            "business_date_to": "2026-01-01",
            "status": "RUNNING",
            "row_count": 0,
            "input_versions": [MARKER],
            "config_hash": MARKER,
            "schema_version": "1",
            "rule_version": "1",
            "code_version": "dev",
            "semantic_hash": None,
            "error_summary": None,
        },
        ensure_ascii=False,
    )
    calls = _capture(monkeypatch, ["", "[]", "", run_row, "", "", "", "", "[]", "", "", "[]"])
    store = PsqlCliControlStore(CONFIG)
    store.upsert_ingest_batch(
        IngestBatchRecord(
            query_name=MARKER,
            batch_id=1,
            dataset_id=None,
            ingestion_date=date(2026, 1, 1),
            topic_offsets={},
            record_count=0,
            raw_path=MARKER,
            bronze_path=MARKER,
            status="COMMITTED",
        )  # fmt: skip
    )
    store.list_committed_ingest_batches(query_name=MARKER)
    store.start_run(
        RunRecord(
            run_id=MARKER,
            dataset_id=MARKER,
            job_name=MARKER,
            profile="test",
            business_date_from=None,
            business_date_to=date(2026, 1, 1),
            status="RUNNING",
            row_count=0,
            input_versions=[MARKER],
            config_hash=MARKER,
            schema_version="1",
            rule_version="1",
            code_version="dev",
        )
    )
    store.commit_run_atomic(
        run_id=MARKER,
        dataset_id=MARKER,
        layer=MARKER,
        entity=MARKER,
        business_date=date(2026, 1, 1),
        output_path=MARKER,
        manifest_path=MARKER,
        row_count=1,
        semantic_hash=MARKER,
        target_status="SUCCEEDED",
    )
    store.fail_run(run_id=MARKER, error_summary=MARKER)
    store.get_run(MARKER)
    store.resolve_current_snapshot(
        dataset_id=MARKER,
        layer=MARKER,
        entity=MARKER,
        business_date=date(2026, 1, 1),
    )
    store.list_current_snapshots(
        dataset_id=MARKER,
        layer=MARKER,
        entity=MARKER,
        business_date_from=date(2026, 1, 1),
        business_date_to=date(2026, 1, 2),
    )
    store.mark_run_mart_published(run_id=MARKER)
    store.upsert_quality_metrics(
        [
            QualityMetricRecord(
                run_id=MARKER,
                dataset_id=MARKER,
                layer="silver",
                entity="all",
                business_date=date(2026, 1, 1),
                metric_name=MARKER,
                metric_value=1,
                status="PASS",
                details={"note": MARKER},
            )
        ]
    )
    store.list_quality_metrics(run_id=MARKER)

    assert len(calls) == 12
    _assert_stdin_only(calls)


def test_backslash_and_quote_are_doubled_for_the_psql_meta_command() -> None:
    script = module._with_params("SELECT 1;", {"a": "x'y\\z\n", "b": None})
    # Viết tay: JSON là {"a": "x'y\\z\n", "b": null}; mỗi `\` và `'` được nhân đôi cho `\set`.
    expected = '\\set params \'{"a": "x\'\'y\\\\\\\\z\\\\n", "b": null}\'\nSELECT 1;'
    assert script == expected


def test_custom_runner_receives_the_parameters_inside_the_script() -> None:
    seen: list[tuple[str, bool]] = []

    def runner(sql: str, tuples_only: bool) -> str:
        seen.append((sql, tuples_only))
        return ""

    assert PsqlCliControlStore(CONFIG, psql_runner=runner).get_run(MARKER) is None
    sql, tuples_only = seen[0]
    assert tuples_only is True and sql.startswith("\\set params '") and MARKER in sql
