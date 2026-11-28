"""Unit tests cho phân biệt replay, duplicate nghiệp vụ và `key_conflicts` (TASK T31).

Kiểm tra trực tiếp trên bộ acceptance (T009, T010, T011) và bộ negative
(`event_id_conflict.jsonl`, `business_key_conflict.jsonl`) của hợp đồng 1.0.0.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from n06.domain import CONTRACT_ROOT
from n06.quality import (
    KEY_CONFLICT_COLUMNS,
    SIDE_INTERNAL,
    SIDE_STATEMENT,
    STAGE_SILVER_DEDUP,
    dedup_and_detect_event_conflicts,
    dedup_and_detect_statement_conflicts,
)

ACCEPTANCE_DIR = CONTRACT_ROOT / "fixtures" / "acceptance"
NEGATIVE_DIR = CONTRACT_ROOT / "fixtures" / "negative"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def test_acceptance_t010_event_replay_and_t011_business_replay() -> None:
    events = [
        ev
        for ev in _read_jsonl(ACCEPTANCE_DIR / "events.jsonl")
        if ev["event_type"] == "TRANSACTION_CREATED"
    ]
    outcome = dedup_and_detect_event_conflicts(
        events,
        entity="transaction",
        run_id="run-t31",
        raw_reference="s3a://n06-lakehouse/raw/q/b0/",
    )
    assert outcome.quarantine_records == ()
    assert outcome.key_conflicts == ()
    # Bộ acceptance có 12 giao dịch logic sau khi gộp T010 (event replay) & T011 (business replay).
    assert len(outcome.valid_records) == 12
    assert outcome.event_replay_count == 1
    assert outcome.business_replay_count == 1

    # T010 (REF-T010): event replay — cùng event_id -> 1 dòng, replay_count=2, 1 event_id.
    t010 = [r for r in outcome.valid_records if r["external_reference"] == "REF-T010"]
    assert len(t010) == 1
    assert t010[0]["replay_count"] == 2
    assert len(t010[0]["event_ids"]) == 1

    # T011 (REF-T011): business replay — khác event_id -> 1 dòng, replay_count=2, 2 event_ids.
    t011 = [r for r in outcome.valid_records if r["external_reference"] == "REF-T011"]
    assert len(t011) == 1
    assert t011[0]["replay_count"] == 2
    assert len(t011[0]["event_ids"]) == 2


def test_acceptance_t009_statement_duplicate_keeps_distinct_statement_ids() -> None:
    statements = _read_jsonl(ACCEPTANCE_DIR / "statements.jsonl")
    outcome = dedup_and_detect_statement_conflicts(statements, run_id="run-t31")
    assert outcome.quarantine_records == ()
    assert outcome.key_conflicts == ()

    # T009 (REF-T009): 2 statement_record_id khác nhau cùng match key -> GIỮ CẢ HAI cho T33.
    t009 = [r for r in outcome.valid_records if r["external_reference"] == "REF-T009"]
    assert len(t009) == 2
    assert t009[0]["statement_record_id"] != t009[1]["statement_record_id"]


def test_negative_event_id_conflict_prioritizes_event_id_conflict_and_emits_key_conflict() -> None:
    events = _read_jsonl(NEGATIVE_DIR / "event_id_conflict.jsonl")
    outcome = dedup_and_detect_event_conflicts(
        events,
        entity="transaction",
        run_id="run-t31",
        raw_reference="s3a://n06-lakehouse/raw/q/b0/",
    )
    assert outcome.valid_records == ()
    assert len(outcome.quarantine_records) == 2
    assert all(q.stage == STAGE_SILVER_DEDUP for q in outcome.quarantine_records)
    assert all(q.error_code == "EVENT_ID_CONFLICT" for q in outcome.quarantine_records)

    assert len(outcome.key_conflicts) == 1
    kc = outcome.key_conflicts[0]
    assert set(kc.as_dict()) == set(KEY_CONFLICT_COLUMNS)
    assert kc.side_origin == SIDE_INTERNAL
    assert kc.entity == "transaction"
    assert kc.conflict_code == "EVENT_ID_CONFLICT"
    assert kc.channel == "SIM_CHANNEL_A"
    assert kc.external_reference == "REF-T001"
    assert len(kc.source_pointers) == 2


def test_negative_business_key_conflict_emits_quarantine_and_key_conflict() -> None:
    events = _read_jsonl(NEGATIVE_DIR / "business_key_conflict.jsonl")
    outcome = dedup_and_detect_event_conflicts(
        events,
        entity="transaction",
        run_id="run-t31",
        raw_reference="s3a://n06-lakehouse/raw/q/b0/",
    )
    assert outcome.valid_records == ()
    assert len(outcome.quarantine_records) == 2
    assert all(q.error_code == "BUSINESS_KEY_CONFLICT" for q in outcome.quarantine_records)

    assert len(outcome.key_conflicts) == 1
    kc = outcome.key_conflicts[0]
    assert kc.side_origin == SIDE_INTERNAL
    assert kc.conflict_code == "BUSINESS_KEY_CONFLICT"
    assert kc.issue_codes == ("BUSINESS_KEY_CONFLICT",)
    assert kc.channel == "SIM_CHANNEL_A"
    assert kc.external_reference == "REF-T001"


def test_statement_same_id_different_payload_is_business_key_conflict() -> None:
    base = _read_jsonl(ACCEPTANCE_DIR / "statements.jsonl")[0]
    variant = dict(base)
    variant["amount_minor"] = int(base["amount_minor"]) + 1
    outcome = dedup_and_detect_statement_conflicts([base, variant], run_id="run-t31")
    assert outcome.valid_records == ()
    assert len(outcome.quarantine_records) == 2
    assert all(q.error_code == "BUSINESS_KEY_CONFLICT" for q in outcome.quarantine_records)
    assert len(outcome.key_conflicts) == 1
    assert outcome.key_conflicts[0].side_origin == SIDE_STATEMENT
    assert outcome.key_conflicts[0].entity == "statement"
    assert outcome.key_conflicts[0].conflict_code == "BUSINESS_KEY_CONFLICT"
