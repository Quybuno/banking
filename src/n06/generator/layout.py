"""Bố cục thư mục và tên file đầu ra của `build_dataset` (TASK T13).

Mọi tên thư mục/file đầu ra được định nghĩa ở đây và truyền vào `build_dataset` qua `layout`.
Phần II mục 4: "Tên path/object key là cấu hình."
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path

from n06.generator.params import FaultConfigError


@dataclass(frozen=True)
class OutputLayout:
    # Thư mục gốc con (cùng cấp dưới `out_root`).
    source_dirname: str = "source"
    ground_truth_dirname: str = "ground_truth"
    # Dưới source/.
    events_dirname: str = "events"
    statements_dirname: str = "statements"
    manifests_dirname: str = "manifests"
    accounts_filename: str = "accounts.jsonl"
    control_report_filename: str = "control_report.json"
    deliveries_filename: str = "statement_deliveries.jsonl"
    events_file_template: str = "events_batch_{shard:05d}.jsonl"
    events_manifest_template: str = "events_batch_{shard:05d}.manifest.json"
    statements_file_template: str = "statements_w{window:09d}.jsonl"
    statements_manifest_template: str = "statements_{channel}_w{window:09d}.manifest.json"
    # Dưới ground_truth/.
    canonical_dirname: str = "canonical"
    fault_assignment_filename: str = "fault_assignment.jsonl"
    statement_rows_filename: str = "statement_rows.jsonl"
    fault_counts_filename: str = "fault_counts.json"
    canonical_index_filename: str = "canonical_index.json"

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if not isinstance(value, str) or not value.strip():
                raise FaultConfigError(f"Bố cục đầu ra: {field.name} phải là chuỗi không rỗng.")
            if "/" in value or "\\" in value or value in (".", ".."):
                raise FaultConfigError(
                    f"Bố cục đầu ra: {field.name}={value!r} không được chứa dấu phân cách "
                    "đường dẫn."
                )
        if self.source_dirname == self.ground_truth_dirname:
            raise FaultConfigError(
                "Bố cục đầu ra: thư mục nguồn và thư mục ground truth phải khác nhau."
            )

    def events_relpath(self, shard: int) -> str:
        return f"{self.events_dirname}/{self.events_file_template.format(shard=shard)}"

    def statements_relpath(self, channel: str, window: int) -> str:
        return (
            f"{self.statements_dirname}/{channel}/"
            f"{self.statements_file_template.format(window=window)}"
        )

    def events_manifest_relpath(self, shard: int) -> str:
        return f"{self.manifests_dirname}/{self.events_manifest_template.format(shard=shard)}"

    def statements_manifest_relpath(self, channel: str, window: int) -> str:
        name = self.statements_manifest_template.format(channel=channel, window=window)
        return f"{self.manifests_dirname}/{name}"

    def source_dir(self, out_root: Path) -> Path:
        return out_root / self.source_dirname

    def ground_truth_dir(self, out_root: Path) -> Path:
        return out_root / self.ground_truth_dirname


DEFAULT_LAYOUT = OutputLayout()
