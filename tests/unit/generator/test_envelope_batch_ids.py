"""Mọi envelope phải có source_batch_id nằm trong manifest nguồn (spec Phần II mục 3) — T13."""

from __future__ import annotations

import json
from typing import Any

from n06.domain import validate_event_envelope

from .conftest import read_jsonl


def _manifest_ids_by_events_file(summary: Any) -> dict[str, str]:
    result: dict[str, str] = {}
    for path in sorted((summary.source_dir / summary.layout.manifests_dirname).glob("events_*")):
        manifest = json.loads(path.read_text("utf-8"))
        for item in manifest["files"]:
            if item["path"].startswith(summary.layout.events_dirname + "/"):
                result[item["path"]] = manifest["source_batch_id"]
    return result


def test_every_envelope_batch_id_is_in_manifest_and_matches_its_file(built_multishard: Any) -> None:
    _, summary = built_multishard
    by_file = _manifest_ids_by_events_file(summary)
    assert len(by_file) == 2  # hai logical shard
    assert len(set(by_file.values())) == 2
    allowed = set(by_file.values())
    accounts = {
        a["account_id"]: a
        for a in read_jsonl(summary.source_dir / summary.layout.accounts_filename)
    }
    checked = 0
    for rel, batch_id in by_file.items():
        for env in read_jsonl(summary.source_dir / rel):
            assert env["source_batch_id"] == batch_id, rel
            res = validate_event_envelope(env, accounts=accounts, allowed_source_batch_ids=allowed)
            assert res.is_valid, res.issues
            checked += 1
    assert checked == summary.event_record_count


def test_envelope_with_foreign_batch_id_is_rejected_by_validator(built: Any) -> None:
    _, summary = built
    first = read_jsonl(summary.source_dir / "events" / "events_batch_00000.jsonl")[0]
    accounts = {a["account_id"]: a for a in read_jsonl(summary.source_dir / "accounts.jsonl")}
    res = validate_event_envelope(
        first, accounts=accounts, allowed_source_batch_ids={"00000000-0000-5000-8000-000000000000"}
    )
    assert not res.is_valid
