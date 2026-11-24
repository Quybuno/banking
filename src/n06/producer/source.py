"""Đọc nguồn events theo manifest và kiểm tra toàn vẹn trước khi gửi (Phần II mục 4, ADR-0010).

Chỉ đọc thư mục manifest và thư mục events dưới thư mục nguồn; không đọc gì khác.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from n06.domain import parse_and_validate_json_schema
from n06.producer.errors import SourceDataError
from n06.producer.kafka_client import DEFAULT_MAX_MESSAGE_BYTES, MESSAGE_OVERHEAD_MARGIN_BYTES
from n06.producer.keys import KeySelector

# Tên mặc định của bố cục do generator T13 sinh; đều ghi đè được bằng tham số CLI.
DEFAULT_SOURCE_DIRNAME = "source"
DEFAULT_MANIFESTS_DIRNAME = "manifests"
DEFAULT_EVENTS_DIRNAME = "events"


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


def _check_inside_source(source_dir: Path, relative: str, manifest_path: Path) -> None:
    """Đường dẫn trong manifest phải tương đối và nằm trong thư mục nguồn."""
    pure = PurePosixPath(relative)
    if pure.is_absolute() or ".." in pure.parts:
        raise SourceDataError(
            f"Đường dẫn {relative!r} trong {manifest_path.name} không hợp lệ: nằm ngoài thư mục "
            "nguồn (tuyệt đối hoặc chứa '..')."
        )
    root = source_dir.resolve()
    if not (root / relative).resolve().is_relative_to(root):
        raise SourceDataError(
            f"Đường dẫn {relative!r} trong {manifest_path.name} trỏ ra ngoài thư mục nguồn."
        )


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
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SourceDataError(
                f"Manifest {manifest_path} không đọc được hoặc hỏng: {exc}"
            ) from exc
        result = parse_and_validate_json_schema(manifest, "source_manifest")
        if not result.is_valid:
            raise SourceDataError(f"Manifest {manifest_path} không đúng schema: {result.issues}")
        for item in manifest["files"]:
            rel = str(item["path"])
            _check_inside_source(source_dir, rel, manifest_path)
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


def verify_event_files(
    source: EventSource,
    *,
    key_strategy: str = "transaction_id",
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
) -> None:
    """Kiểm toàn bộ nguồn trong một lượt đọc, trước khi gửi message đầu tiên.

    Mỗi file: sha256 và số dòng khớp manifest; mọi dòng có LF cuối, không trống, đúng JSON và đủ
    trường; key chọn được (orphan ledger bị phát hiện ở đây); kích thước không vượt giới hạn.
    """
    selector = KeySelector(key_strategy)
    limit = max_message_bytes - MESSAGE_OVERHEAD_MARGIN_BYTES
    for item in source.files:
        if not item.path.is_file():
            raise SourceDataError(
                f"File events {item.relative_path} có trong manifest nhưng thiếu."
            )
        selector.start_file()
        digest = hashlib.sha256()
        rows = 0
        try:
            with item.path.open("rb") as fh:
                for number, raw in enumerate(fh, start=1):
                    digest.update(raw)
                    where = f"{item.relative_path} dòng {number}"
                    if not raw.endswith(b"\n"):
                        raise SourceDataError(f"Dòng cuối thiếu ký tự LF tại {where}.")
                    rows += 1
                    value = raw[:-1]
                    key = selector.select(value, file_path=item.relative_path, line_number=number)
                    if len(key) + len(value) > limit:
                        raise SourceDataError(
                            f"Dòng tại {where} dài {len(value)} byte, vượt giới hạn message "
                            f"{limit} byte (message.max.bytes {max_message_bytes})."
                        )
        except OSError as exc:
            raise SourceDataError(
                f"Không đọc được file events {item.relative_path}: {exc}"
            ) from exc
        if digest.hexdigest() != item.sha256 or rows != item.physical_rows:
            actual_sha = digest.hexdigest()
            raise SourceDataError(
                f"File events {item.relative_path} không khớp manifest: sha256={actual_sha} "
                f"(manifest {item.sha256}), số dòng={rows} (manifest {item.physical_rows})."
            )


def iter_lines(item: EventFile) -> Iterator[tuple[int, bytes]]:
    """Trả `(số dòng bắt đầu từ 1, byte của dòng không gồm LF cuối)`; đọc nhị phân."""
    with item.path.open("rb") as fh:
        for number, raw in enumerate(fh, start=1):
            yield number, raw[:-1] if raw.endswith(b"\n") else raw
