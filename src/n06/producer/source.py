"""Đọc nguồn events theo manifest và kiểm tra toàn vẹn trước khi gửi (Phần II mục 4, ADR-0010).

Chỉ đọc thư mục manifest và thư mục events dưới thư mục nguồn; không đọc gì khác.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from n06.domain import parse_and_validate_json_schema
from n06.producer.errors import SourceDataError

# Tên mặc định của bố cục do generator T13 sinh; đều ghi đè được bằng tham số CLI.
DEFAULT_SOURCE_DIRNAME = "source"
DEFAULT_MANIFESTS_DIRNAME = "manifests"
DEFAULT_EVENTS_DIRNAME = "events"

_READ_BLOCK = 1 << 20


@dataclass(frozen=True)
class EventFile:
    """Một file events được liệt kê trong manifest."""

    relative_path: str
    path: Path
    sha256: str
    physical_rows: int
    source_batch_id: str


@dataclass(frozen=True)
class EventSource:
    files: tuple[EventFile, ...]

    @property
    def expected_messages(self) -> int:
        """Tổng `physical_rows` của các file events trong manifest (kể cả replay đã tiêm)."""
        return sum(item.physical_rows for item in self.files)


def discover_event_files(
    source_dir: Path,
    *,
    manifests_dirname: str = DEFAULT_MANIFESTS_DIRNAME,
    events_dirname: str = DEFAULT_EVENTS_DIRNAME,
) -> EventSource:
    """Liệt kê file events từ các manifest, theo thứ tự tên file (shard tăng dần)."""
    manifests_dir = source_dir / manifests_dirname
    if not manifests_dir.is_dir():
        raise SourceDataError(f"Không tìm thấy thư mục manifest: {manifests_dir}")

    events_prefix = f"{events_dirname}/"
    found: dict[str, EventFile] = {}
    for manifest_path in sorted(manifests_dir.glob("*.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        result = parse_and_validate_json_schema(manifest, "source_manifest")
        if not result.is_valid:
            raise SourceDataError(f"Manifest {manifest_path} không đúng schema: {result.issues}")
        for item in manifest["files"]:
            rel = str(item["path"])
            if not rel.startswith(events_prefix):
                continue  # accounts, control_report, sao kê: không thuộc bước publish events
            if rel in found:
                raise SourceDataError(f"File events {rel} xuất hiện trong nhiều manifest.")
            found[rel] = EventFile(
                relative_path=rel,
                path=source_dir / rel,
                sha256=str(item["sha256"]),
                physical_rows=int(item["physical_rows"]),
                source_batch_id=str(manifest["source_batch_id"]),
            )

    if not found:
        raise SourceDataError(
            f"Không có file events nào (đường dẫn bắt đầu bằng {events_prefix!r}) trong manifest "
            f"tại {manifests_dir}."
        )
    on_disk = {
        p.relative_to(source_dir).as_posix()
        for p in (source_dir / events_dirname).rglob("*")
        if p.is_file()
    }
    unlisted = sorted(on_disk - set(found))
    if unlisted:
        raise SourceDataError(f"File events không có trong manifest nào: {unlisted}")
    return EventSource(files=tuple(found[rel] for rel in sorted(found)))


def sha256_and_rows(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    rows = 0
    with path.open("rb") as fh:
        while block := fh.read(_READ_BLOCK):
            digest.update(block)
            rows += block.count(b"\n")
    return digest.hexdigest(), rows


def verify_event_files(source: EventSource) -> None:
    """Kiểm tra sha256 và số dòng của từng file với manifest; sai thì dừng trước khi gửi."""
    for item in source.files:
        if not item.path.is_file():
            raise SourceDataError(
                f"File events {item.relative_path} có trong manifest nhưng thiếu."
            )
        sha, rows = sha256_and_rows(item.path)
        if sha != item.sha256 or rows != item.physical_rows:
            raise SourceDataError(
                f"File events {item.relative_path} không khớp manifest: sha256={sha} "
                f"(manifest {item.sha256}), số dòng={rows} (manifest {item.physical_rows})."
            )


def iter_lines(item: EventFile) -> Iterator[tuple[int, bytes]]:
    """Trả `(số dòng bắt đầu từ 1, byte của dòng không gồm LF cuối)`; đọc nhị phân."""
    with item.path.open("rb") as fh:
        for number, raw in enumerate(fh, start=1):
            yield number, raw[:-1] if raw.endswith(b"\n") else raw
