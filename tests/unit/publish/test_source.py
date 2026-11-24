"""Đọc nguồn events theo manifest và kiểm toàn vẹn."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from n06.producer.errors import SourceDataError
from n06.producer.source import discover_event_files, verify_event_files

from .conftest import ledger_event, line_of, tx_event

ShardFactory = Callable[..., Path]


def _two_shards() -> list[list[bytes]]:
    return [
        [
            line_of(tx_event("t1", "a", "b", event_id="e1")),
            line_of(tx_event("t1", "a", "b", event_id="e1")),  # replay cùng event_id
            line_of(ledger_event("t1", "DEBIT", "a")),
        ],
        [line_of(tx_event("t2", "b", "a"))],
    ]


def test_only_events_files_are_selected_in_shard_order(make_source: ShardFactory) -> None:
    root = make_source(_two_shards())
    source = discover_event_files(root)
    assert [f.relative_path for f in source.files] == [
        "events/events_batch_00000.jsonl",
        "events/events_batch_00001.jsonl",
    ]


def test_expected_messages_is_sum_of_event_rows_only(make_source: ShardFactory) -> None:
    root = make_source(_two_shards())
    source = discover_event_files(root)
    # Manifest 0 còn liệt kê accounts.jsonl (2 dòng) và control_report.json (1 dòng): không tính.
    manifest_0 = json.loads(
        (root / "manifests" / "events_batch_00000.manifest.json").read_text("utf-8")
    )
    assert sum(f["physical_rows"] for f in manifest_0["files"]) == 3 + 2 + 1
    assert source.expected_messages == 3 + 1  # replay vẫn được tính


def test_verify_passes_on_intact_files(make_source: ShardFactory) -> None:
    verify_event_files(discover_event_files(make_source(_two_shards())))


def test_modified_content_is_rejected(make_source: ShardFactory) -> None:
    root = make_source(_two_shards())
    path = root / "events" / "events_batch_00001.jsonl"
    path.write_bytes(path.read_bytes().replace(b"t2", b"t9"))
    with pytest.raises(SourceDataError, match="không khớp manifest"):
        verify_event_files(discover_event_files(root))


def test_missing_row_is_rejected(make_source: ShardFactory) -> None:
    root = make_source(_two_shards())
    path = root / "events" / "events_batch_00000.jsonl"
    lines = path.read_bytes().splitlines(keepends=True)
    path.write_bytes(b"".join(lines[:-1]))
    with pytest.raises(SourceDataError, match="số dòng"):
        verify_event_files(discover_event_files(root))


def test_event_file_not_in_any_manifest_is_rejected(make_source: ShardFactory) -> None:
    root = make_source(_two_shards())
    (root / "events" / "events_batch_00009.jsonl").write_bytes(b"{}\n")
    with pytest.raises(SourceDataError, match="không có trong manifest"):
        discover_event_files(root)


def test_file_in_two_manifests_is_rejected(make_source: ShardFactory) -> None:
    root = make_source(_two_shards())
    manifest = (root / "manifests" / "events_batch_00001.manifest.json").read_text("utf-8")
    (root / "manifests" / "events_batch_copy.manifest.json").write_text(manifest, "utf-8")
    with pytest.raises(SourceDataError, match="nhiều manifest"):
        discover_event_files(root)


def test_invalid_manifest_schema_is_rejected(make_source: ShardFactory) -> None:
    root = make_source(_two_shards())
    path = root / "manifests" / "events_batch_00000.manifest.json"
    manifest = json.loads(path.read_text("utf-8"))
    manifest["files"][0]["sha256"] = "xyz"
    path.write_text(json.dumps(manifest), "utf-8")
    with pytest.raises(SourceDataError, match="schema"):
        discover_event_files(root)


def test_no_events_in_manifests_is_rejected(make_source: ShardFactory) -> None:
    root = make_source(_two_shards())
    with pytest.raises(SourceDataError, match="Không có file events"):
        discover_event_files(root, events_dirname="other")


def test_default_directory_names_match_generator_layout() -> None:
    """Mặc định của publisher phải khớp bố cục T13 (chống lệch khi một bên đổi tên)."""
    from n06.generator import DEFAULT_LAYOUT
    from n06.producer.source import (
        DEFAULT_EVENTS_DIRNAME,
        DEFAULT_MANIFESTS_DIRNAME,
        DEFAULT_SOURCE_DIRNAME,
    )

    assert DEFAULT_SOURCE_DIRNAME == DEFAULT_LAYOUT.source_dirname
    assert DEFAULT_MANIFESTS_DIRNAME == DEFAULT_LAYOUT.manifests_dirname
    assert DEFAULT_EVENTS_DIRNAME == DEFAULT_LAYOUT.events_dirname


def test_producer_package_does_not_touch_ground_truth_or_generator() -> None:
    root = Path(__file__).resolve().parents[3] / "src" / "n06" / "producer"
    for path in root.glob("*.py"):
        text = path.read_text("utf-8")
        for forbidden in ("ground_truth", "n06.generator", "tests.harness", "expected_results"):
            assert forbidden not in text, f"{path.name} chứa {forbidden!r}"
