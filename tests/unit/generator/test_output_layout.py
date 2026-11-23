"""Bố cục đầu ra là một đối tượng duy nhất, truyền được vào build_dataset (TASK T13)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from n06.generator import DEFAULT_LAYOUT, FaultConfigError, OutputLayout, build_dataset

CUSTOM = OutputLayout(
    source_dirname="pipeline_in",
    ground_truth_dirname="private_answers",
    events_dirname="kafka_events",
    statements_dirname="channel_files",
    manifests_dirname="batch_manifests",
    accounts_filename="acct.jsonl",
    control_report_filename="totals.json",
    deliveries_filename="delivery_plan.jsonl",
    events_file_template="ev_{shard:03d}.jsonl",
    events_manifest_template="ev_{shard:03d}.manifest.json",
    statements_file_template="st_{window}.jsonl",
    statements_manifest_template="st_{channel}_{window}.manifest.json",
    canonical_dirname="base",
    fault_assignment_filename="faults.jsonl",
    statement_rows_filename="rows.jsonl",
    fault_counts_filename="counts.json",
    canonical_index_filename="base_index.json",
)


def _tree_content_hashes(root: Path, patterns: list[str]) -> list[str]:
    import hashlib

    return sorted(
        hashlib.sha256(p.read_bytes()).hexdigest()
        for pattern in patterns
        for p in root.glob(pattern)
    )


def test_default_layout_is_single_definition() -> None:
    assert OutputLayout() == DEFAULT_LAYOUT
    assert DEFAULT_LAYOUT.events_relpath(3) == "events/events_batch_00003.jsonl"
    assert (
        DEFAULT_LAYOUT.statements_relpath("SIM_CHANNEL_A", 7)
        == "statements/SIM_CHANNEL_A/statements_w000000007.jsonl"
    )


def test_custom_layout_changes_every_name(tmp_path: Path, make_config: Any) -> None:
    cfg = make_config(transaction_count=300)
    summary = build_dataset(cfg, tmp_path / "custom", layout=CUSTOM)
    assert summary.layout == CUSTOM
    src = tmp_path / "custom" / "pipeline_in"
    truth = tmp_path / "custom" / "private_answers"
    assert summary.source_dir == src and summary.ground_truth_dir == truth
    assert (src / "kafka_events" / "ev_000.jsonl").is_file()
    assert (src / "acct.jsonl").is_file() and (src / "totals.json").is_file()
    assert (src / "delivery_plan.jsonl").is_file()
    assert (src / "batch_manifests" / "ev_000.manifest.json").is_file()
    assert list((src / "channel_files").glob("*/st_*.jsonl"))
    assert list((src / "batch_manifests").glob("st_*.manifest.json"))
    for name in ("faults.jsonl", "rows.jsonl", "counts.json", "base_index.json"):
        assert (truth / name).is_file()
    assert (truth / "base").is_dir()
    # Không còn tên mặc định nào.
    assert not (tmp_path / "custom" / "source").exists()
    assert not (tmp_path / "custom" / "ground_truth").exists()
    manifest = json.loads((src / "batch_manifests" / "ev_000.manifest.json").read_text("utf-8"))
    assert [f["path"] for f in manifest["files"]] == [
        "acct.jsonl",
        "kafka_events/ev_000.jsonl",
        "totals.json",
    ]
    # Tên ground truth không rò vào bất kỳ file nào dưới source/.
    for path in src.rglob("*"):
        if path.is_file():
            text = path.read_text("utf-8")
            assert "private_answers" not in text and "faults.jsonl" not in text


def test_layout_changes_only_names_not_content(tmp_path: Path, make_config: Any) -> None:
    cfg = make_config(transaction_count=300)
    build_dataset(cfg, tmp_path / "a")
    build_dataset(cfg, tmp_path / "b", layout=CUSTOM)
    default_events = _tree_content_hashes(tmp_path / "a" / "source", ["events/*.jsonl"])
    custom_events = _tree_content_hashes(tmp_path / "b" / "pipeline_in", ["kafka_events/*.jsonl"])
    assert default_events == custom_events
    default_stmt = _tree_content_hashes(tmp_path / "a" / "source", ["statements/*/*.jsonl"])
    custom_stmt = _tree_content_hashes(tmp_path / "b" / "pipeline_in", ["channel_files/*/*.jsonl"])
    assert default_stmt == custom_stmt
    a = (tmp_path / "a" / "ground_truth" / "fault_counts.json").read_text("utf-8")
    b = (tmp_path / "b" / "private_answers" / "counts.json").read_text("utf-8")
    assert a == b


@pytest.mark.parametrize(
    "overrides",
    [
        {"events_dirname": ""},
        {"events_dirname": "a/b"},
        {"accounts_filename": ".."},
        {"manifests_dirname": "x\\y"},
        {"ground_truth_dirname": "source"},
    ],
)
def test_invalid_layout_is_rejected(overrides: dict[str, str]) -> None:
    with pytest.raises(FaultConfigError, match="Bố cục"):
        OutputLayout(**overrides)
