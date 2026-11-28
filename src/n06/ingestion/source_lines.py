"""File events nguồn của một dataset, kiểm đúng như bước của producer (ADR-0004 mục 9 bước 1).

Module thuần. Không kiểm key của producer (phần đó thuộc bước publish).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from n06.producer.errors import SourceDataError
from n06.producer.source import DEFAULT_MANIFESTS_DIRNAME, EventFile, discover_event_files


def dataset_event_files(source_dir: Path, dataset_id: str) -> tuple[list[EventFile], int]:
    """`(file events của dataset, tổng số dòng)`; ném `SourceDataError` nếu thiếu hoặc lệch."""
    source = discover_event_files(source_dir)
    listed: set[str] = set()
    for manifest_path in sorted((source_dir / DEFAULT_MANIFESTS_DIRNAME).glob("*.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["dataset_id"] == dataset_id:
            listed.update(str(item["path"]) for item in manifest["files"])
    files = [item for item in source.files if item.relative_path in listed]
    if not files:
        raise SourceDataError(f"Nguồn không có file events nào của dataset {dataset_id}.")
    total = 0
    for item in files:
        digest = hashlib.sha256()
        rows = 0
        try:
            with item.path.open("rb") as handle:
                for line_number, line in enumerate(handle, start=1):
                    digest.update(line)
                    if not line.endswith(b"\n"):
                        raise SourceDataError(
                            f"Dòng cuối thiếu ký tự LF tại {item.relative_path} dòng {line_number}."
                        )
                    rows += 1
        except OSError as exc:
            raise SourceDataError(
                f"Không đọc được file events {item.relative_path} ({type(exc).__name__})."
            ) from exc
        if digest.hexdigest() != item.sha256 or rows != item.physical_rows:
            raise SourceDataError(f"File events {item.relative_path} không khớp manifest.")
        total += rows
    if total == 0:
        raise SourceDataError(f"Dataset {dataset_id} không có dòng events nào.")
    return files, total
