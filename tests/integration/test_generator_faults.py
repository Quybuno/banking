"""Integration test T13: dựng bộ dữ liệu có lỗi nhiều shard từ cấu hình `demo-faults` thu nhỏ."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from n06.domain import ConflictTracker, validate_event_envelope
from n06.generator import build_control_report_from_envelopes, build_dataset

CONTRACT_DIR = Path(__file__).resolve().parents[2] / "contracts" / "N06_INPUT_CONTRACT_V1"


def _read(path: Path) -> list[dict[str, Any]]:
    return [json.loads(x) for x in path.read_text("utf-8").splitlines() if x.strip()]


def test_multi_shard_dataset_with_exact_fault_counts(tmp_path: Path) -> None:
    cfg = json.loads((CONTRACT_DIR / "config" / "generator.demo-faults.json").read_text("utf-8"))
    cfg.update(dataset_id="n06-t13-integration", transaction_count=12_000, account_count=50)
    summary = build_dataset(cfg, tmp_path)

    # Công thức floor viết riêng cho test.
    expected = {
        name: summary.eligible_success_count * cfg["faults"][f"{name}_basis_points"] // 10_000
        for name in (
            k.removesuffix("_basis_points") for k in cfg["faults"] if k.endswith("_basis_points")
        )
    }
    assert summary.fault_counts == expected
    assert all(v > 0 for v in expected.values())

    # 12.000 giao dịch: hai logical shard độc lập generation_chunk_rows.
    assert summary.event_shard_count == 2
    manifests = sorted((summary.source_dir / "manifests").glob("events_batch_*.manifest.json"))
    assert [m.name for m in manifests] == [
        "events_batch_00000.manifest.json",
        "events_batch_00001.manifest.json",
    ]
    second = json.loads(manifests[1].read_text("utf-8"))
    assert [f["path"] for f in second["files"]] == ["events/events_batch_00001.jsonl"]

    accounts = {a["account_id"]: a for a in _read(summary.source_dir / "accounts.jsonl")}
    tracker = ConflictTracker()
    envelopes: list[dict[str, Any]] = []
    for shard in (0, 1):
        for env in _read(summary.source_dir / "events" / f"events_batch_{shard:05d}.jsonl"):
            assert validate_event_envelope(env, accounts=accounts).is_valid
            assert tracker.check_and_register_envelope(env).is_valid
            envelopes.append(env)
    assert tracker.conflicts == []
    assert len(envelopes) == summary.event_record_count

    # Mỗi event thuộc đúng shard theo ordinal nên source_batch_id trong file là duy nhất.
    for shard in (0, 1):
        ids = {
            e["source_batch_id"]
            for e in _read(summary.source_dir / "events" / f"events_batch_{shard:05d}.jsonl")
        }
        assert len(ids) == 1

    report = json.loads((summary.source_dir / "control_report.json").read_text("utf-8"))
    assert (
        build_control_report_from_envelopes(envelopes, cfg["contract_version"], cfg["dataset_id"])
        == report
    )

    stmt_files = list((summary.source_dir / "statements").glob("*/*.jsonl"))
    assert len(stmt_files) == summary.statement_file_count
    assert sum(len(_read(p)) for p in stmt_files) == summary.statement_record_count
