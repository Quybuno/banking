"""generation_chunk_rows không đổi kết quả khi dữ liệu có nhiều logical shard (TASK T13)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from n06.generator import build_dataset

from .conftest import digest_tree


def test_two_shards_two_chunk_sizes_same_hash_for_all_outputs(
    tmp_path: Path, built_multishard: Any, make_config: Any
) -> None:
    cfg_small, summary_small = built_multishard
    assert cfg_small["generation_chunk_rows"] == 997
    assert summary_small.event_shard_count == 2
    cfg_big = make_config(transaction_count=10_500, generation_chunk_rows=10_000)
    summary_big = build_dataset(cfg_big, tmp_path / "big", validate_outputs=False)
    assert summary_big.event_shard_count == 2
    small = digest_tree(summary_small.source_dir.parent)
    big = digest_tree(tmp_path / "big")
    assert len(small) > 10
    assert small == big
    assert summary_small.fault_counts == summary_big.fault_counts
    assert summary_small.business_checksum == summary_big.business_checksum
