"""Cùng seed cho cùng dữ liệu; đổi generation_chunk_rows không đổi kết quả (TASK T13)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from n06.generator import build_dataset


def _digest(root: Path) -> dict[str, str]:
    """Hash mọi file; part file canonical (tên phụ thuộc chunk) được hash theo luồng nối."""
    out: dict[str, str] = {}
    for sub in ("source", "ground_truth"):
        for path in sorted((root / sub).rglob("*")):
            if (
                not path.is_file()
                or "canonical" in path.relative_to(root).parts[1:2]
                and "_part_" in path.name
            ):
                continue
            out[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    for kind in ("transactions", "ledger_entries"):
        data = b"".join(
            p.read_bytes()
            for p in sorted((root / "ground_truth" / "canonical").glob(f"{kind}_part_*.jsonl"))
        )
        out[f"canonical/{kind}"] = hashlib.sha256(data).hexdigest()
    return out


@pytest.mark.parametrize("chunk_rows", [7, 50, 10_000])
def test_chunk_rows_does_not_change_any_output(
    tmp_path: Path, make_config: Any, chunk_rows: int
) -> None:
    ref_cfg = make_config(transaction_count=400, generation_chunk_rows=10_000)
    build_dataset(ref_cfg, tmp_path / "ref")
    cfg = make_config(transaction_count=400, generation_chunk_rows=chunk_rows)
    build_dataset(cfg, tmp_path / "run")
    assert _digest(tmp_path / "run") == _digest(tmp_path / "ref")


def test_same_seed_same_output_different_seed_differs(tmp_path: Path, make_config: Any) -> None:
    cfg = make_config(transaction_count=400)
    build_dataset(cfg, tmp_path / "a")
    build_dataset(cfg, tmp_path / "b")
    build_dataset(make_config(transaction_count=400, seed=43), tmp_path / "c")
    assert _digest(tmp_path / "a") == _digest(tmp_path / "b")
    assert _digest(tmp_path / "a") != _digest(tmp_path / "c")
