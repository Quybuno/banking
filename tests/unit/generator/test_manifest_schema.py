"""Mọi file đầu ra đúng schema trong contracts/; manifest đúng sha256/physical_rows (TASK T13)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from n06.domain import (
    parse_and_validate_json_schema,
    validate_control_report_data,
    validate_event_envelope,
    validate_statement_record,
)
from n06.generator import deterministic_uuid

from .conftest import read_jsonl, read_jsonl_dir


def _ok(value: Any, entity: str) -> None:
    res = parse_and_validate_json_schema(value, entity)
    assert res.is_valid, res.issues


def _uuid(cfg: dict[str, Any], ordinal: int, discriminator: str) -> str:
    return deterministic_uuid(
        seed_namespace=cfg["seed_namespace"],
        contract_version=cfg["contract_version"],
        dataset_id=cfg["dataset_id"],
        seed=cfg["seed"],
        entity="source_batch",
        ordinal=ordinal,
        discriminator=discriminator,
    )


def test_all_outputs_validate_against_contract_schemas(built: Any) -> None:
    _, summary = built
    src = summary.source_dir
    for path in sorted((src / "manifests").glob("*.json")):
        _ok(json.loads(path.read_text("utf-8")), "source_manifest")
    assert validate_control_report_data(
        json.loads((src / "control_report.json").read_text("utf-8"))
    ).is_valid
    for row in read_jsonl(src / "statement_deliveries.jsonl"):
        _ok(row, "statement_delivery")
    for row in read_jsonl_dir(src / "statements", "*/*.jsonl"):
        assert validate_statement_record(row).is_valid
    accounts = {a["account_id"]: a for a in read_jsonl(src / "accounts.jsonl")}
    for env in read_jsonl_dir(src / "events", "*.jsonl"):
        assert validate_event_envelope(env, accounts=accounts).is_valid


def test_manifest_hashes_and_rows_match_files_independently(built: Any) -> None:
    _, summary = built
    src = summary.source_dir
    listed: set[str] = set()
    for path in sorted((src / "manifests").glob("*.json")):
        manifest = json.loads(path.read_text("utf-8"))
        paths = [f["path"] for f in manifest["files"]]
        assert paths == sorted(paths)
        for item in manifest["files"]:
            data = (src / item["path"]).read_bytes()
            assert hashlib.sha256(data).hexdigest() == item["sha256"]
            assert data.count(b"\n") == item["physical_rows"]
            listed.add(item["path"])
    # Mọi file nguồn (trừ lịch giao và manifest) phải thuộc đúng một manifest.
    actual = {
        p.relative_to(src).as_posix()
        for p in src.rglob("*")
        if p.is_file() and "manifests" not in p.parts and p.name != "statement_deliveries.jsonl"
    }
    assert listed == actual


def test_events_manifest_ordinal_zero_contents_and_ids(built: Any) -> None:
    cfg, summary = built
    src = summary.source_dir
    manifest = json.loads(
        (src / "manifests" / "events_batch_00000.manifest.json").read_text("utf-8")
    )
    assert [f["path"] for f in manifest["files"]] == [
        "accounts.jsonl",
        "control_report.json",
        "events/events_batch_00000.jsonl",
    ]
    assert manifest["source_batch_id"] == _uuid(cfg, 0, "original")
    envelope = read_jsonl(src / "events" / "events_batch_00000.jsonl")[0]
    assert envelope["source_batch_id"] == manifest["source_batch_id"]
    # ledger_entries.jsonl và statement_deliveries.jsonl không nằm trong manifest.
    text = json.dumps(manifest)
    assert "ledger_entries" not in text and "statement_deliveries" not in text


def test_statement_manifest_source_batch_ids(built: Any) -> None:
    cfg, summary = built
    window = cfg["statement_file_window_seconds"]
    for path in sorted((summary.source_dir / "manifests").glob("statements_*.json")):
        manifest = json.loads(path.read_text("utf-8"))
        assert len(manifest["files"]) == 1
        rel = Path(manifest["files"][0]["path"])
        channel = rel.parent.name
        w = int(rel.stem.rsplit("_w", 1)[1])
        assert manifest["source_batch_id"] == _uuid(cfg, w, f"statement:{channel}")
        first = read_jsonl(summary.source_dir / rel)[0]
        from datetime import UTC, datetime

        epoch = int(
            datetime.strptime(first["available_at"], "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=UTC)
            .timestamp()
        )
        assert epoch // window == w
