"""Manifest của source batch: path, sha256, physical_rows (Phần II mục 4)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from n06.generator.determinism import canonical_json


def file_sha256_and_rows(path: Path) -> tuple[str, int]:
    """sha256 của toàn bộ byte và số dòng vật lý (đếm LF), đọc theo khối."""
    digest = hashlib.sha256()
    rows = 0
    with path.open("rb") as fh:
        while block := fh.read(1 << 20):
            digest.update(block)
            rows += block.count(b"\n")
    return digest.hexdigest(), rows


def build_manifest(
    *,
    contract_version: str,
    dataset_id: str,
    source_batch_id: str,
    root: Path,
    relative_paths: Iterable[str],
) -> dict[str, Any]:
    files = []
    for rel in sorted(relative_paths):
        sha, rows = file_sha256_and_rows(root / rel)
        files.append({"path": rel, "sha256": sha, "physical_rows": rows})
    return {
        "schema_version": contract_version,
        "dataset_id": dataset_id,
        "source_batch_id": source_batch_id,
        "files": files,
    }


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(canonical_json(manifest))
        fh.write("\n")
