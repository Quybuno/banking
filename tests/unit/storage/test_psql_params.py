"""Giá trị đi vào `psql` qua stdin, không qua argv và không ghép vào câu lệnh (ADR-0012)."""

from __future__ import annotations

import subprocess
from datetime import date
from typing import Any

import pytest

from n06.config import PostgresDbConfig
from n06.storage import IngestBatchRecord, PsqlCliControlStore
from n06.storage import control_store as module

CONFIG = PostgresDbConfig(host="db", port=5432, database="d", user="u", password="pw-9917")
MARKER = "giá-trị-đánh-dấu"


def _capture(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append({"argv": argv, **kwargs})
        return subprocess.CompletedProcess(argv, 0, "[]", "")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    return calls


def test_values_travel_on_stdin_only(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _capture(monkeypatch)
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

    assert len(calls) == 2
    for call in calls:
        assert not any(MARKER in arg or "pw-9917" in arg for arg in call["argv"])
        first_line, _, statements = call["input"].partition("\n")
        assert first_line.startswith("\\set params '") and MARKER in first_line
        assert MARKER not in statements  # câu lệnh SQL không chứa giá trị
        assert call["env"]["PGCLIENTENCODING"] == "UTF8"


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
