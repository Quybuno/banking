"""Thứ tự phát theo chuỗi đường dẫn phải trùng thứ tự theo số shard (đệm 0, 5 chữ số)."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from pathlib import Path

from n06.generator import DEFAULT_LAYOUT
from n06.producer.source import discover_event_files

SHARDS = [0, 1, 9, 10, 11, 99, 100, 999, 1000, 9999, 10000, 99999]


def _write_source(root: Path, shard_numbers: list[int], relpath) -> None:  # type: ignore[no-untyped-def]
    (root / "manifests").mkdir(parents=True)
    for number in shard_numbers:
        rel = relpath(number)
        data = f'{{"shard":{number}}}\n'.encode()
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        manifest = {
            "schema_version": "1.0.0",
            "dataset_id": "n06-unit-test",
            "source_batch_id": str(uuid.uuid4()),
            "files": [
                {"path": rel, "sha256": hashlib.sha256(data).hexdigest(), "physical_rows": 1}
            ],
        }
        (root / "manifests" / f"m_{number:07d}.manifest.json").write_text(json.dumps(manifest))


def _shard_number(relative_path: str) -> int:
    match = re.search(r"(\d+)\.jsonl$", relative_path)
    assert match, relative_path
    return int(match.group(1))


def test_string_order_equals_numeric_shard_order_around_digit_boundaries(tmp_path: Path) -> None:
    # Dùng đúng tên file mà T13 sinh (DEFAULT_LAYOUT), tạo theo thứ tự đảo để loại trùng hợp.
    _write_source(tmp_path, list(reversed(SHARDS)), DEFAULT_LAYOUT.events_relpath)
    source = discover_event_files(tmp_path)
    numbers = [_shard_number(f.relative_path) for f in source.files]
    assert numbers == SHARDS  # 9 trước 10, 99 trước 100, 9999 trước 10000, 99999 là cuối
    assert numbers == sorted(numbers)


def test_generator_names_sort_numerically_for_every_five_digit_shard() -> None:
    names = [DEFAULT_LAYOUT.events_relpath(n) for n in range(100_000)]
    assert names == sorted(names)
    assert DEFAULT_LAYOUT.events_relpath(9) == "events/events_batch_00009.jsonl"
    assert DEFAULT_LAYOUT.events_relpath(99_999) == "events/events_batch_99999.jsonl"


def test_unpadded_names_are_still_ordered_by_shard_number(tmp_path: Path) -> None:
    """Sắp xếp tự nhiên: tên không đệm 0 vẫn theo số shard (trước đây ra [10, 9])."""
    _write_source(tmp_path, [9, 10], lambda n: f"events/batch_{n}.jsonl")
    order = [_shard_number(f.relative_path) for f in discover_event_files(tmp_path).files]
    assert order == [9, 10]


def test_six_digit_shards_are_ordered_after_five_digit_shards(tmp_path: Path) -> None:
    """Từ shard 100000 (10^9 giao dịch) sắp theo chuỗi sẽ sai im lặng; sắp tự nhiên thì đúng."""
    shards = [0, 99_998, 99_999, 100_000, 100_001, 1_000_000]
    _write_source(tmp_path, list(reversed(shards)), DEFAULT_LAYOUT.events_relpath)
    order = [_shard_number(f.relative_path) for f in discover_event_files(tmp_path).files]
    assert order == shards


def test_plain_string_order_of_generator_names_is_wrong_past_five_digits() -> None:
    """Lý do cần sắp tự nhiên: theo chuỗi, shard 100000 đứng trước shard 99999."""
    five = DEFAULT_LAYOUT.events_relpath(99_999)
    six = DEFAULT_LAYOUT.events_relpath(100_000)
    assert six < five
