"""Fixture dùng chung cho test T13 (tiêm lỗi, sao kê, manifest, control report, ground truth).

Cấu hình thu nhỏ dựng ngay từ file mẫu trong `contracts/` (chỉ đọc), không thêm file vào `config/`.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from n06.generator import DatasetSummary, build_dataset

CONTRACT_DIR = Path(__file__).resolve().parents[3] / "contracts" / "N06_INPUT_CONTRACT_V1"
ACCEPTANCE_DIR = CONTRACT_DIR / "fixtures" / "acceptance"


@pytest.fixture(scope="session")
def make_config() -> Callable[..., dict[str, Any]]:
    base = json.loads((CONTRACT_DIR / "config" / "generator.demo-faults.json").read_text("utf-8"))

    def factory(**overrides: Any) -> dict[str, Any]:
        cfg = json.loads(json.dumps(base))
        cfg.update(
            dataset_id="n06-t13-unit",
            transaction_count=3000,
            account_count=20,
            generation_chunk_rows=500,
        )
        for key, value in overrides.items():
            if key == "faults":
                cfg["faults"].update(value)
            else:
                cfg[key] = value
        return cfg

    return factory


@pytest.fixture(scope="session")
def built(
    tmp_path_factory: pytest.TempPathFactory, make_config: Callable[..., dict[str, Any]]
) -> tuple[dict[str, Any], DatasetSummary]:
    cfg = make_config()
    summary = build_dataset(cfg, tmp_path_factory.mktemp("t13_built"))
    return cfg, summary


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text("utf-8").splitlines() if line.strip()]


def read_jsonl_dir(directory: Path, pattern: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(directory.glob(pattern)):
        rows.extend(read_jsonl(path))
    return rows
