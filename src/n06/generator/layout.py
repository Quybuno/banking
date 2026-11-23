"""Bố cục thư mục và tên file đầu ra của `build_dataset` (TASK T13).

Mọi tên thư mục/file đầu ra được định nghĩa ở đây và truyền vào `build_dataset` qua `layout`.
Phần II mục 4: "Tên path/object key là cấu hình."
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from string import Formatter

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
        self._check_templates()
        self._check_unique_names()

    def _check_templates(self) -> None:
        """Mỗi mẫu tên phải có đúng các trường bắt buộc, nếu thiếu thì nhiều file bị ghi đè."""
        required = {
            "events_file_template": {"shard"},
            "events_manifest_template": {"shard"},
            "statements_file_template": {"window"},
            "statements_manifest_template": {"channel", "window"},
        }
        for name, expected in required.items():
            template = getattr(self, name)
            found = [field for _, field, _, _ in Formatter().parse(template) if field is not None]
            names = {field.split("!")[0].split(":")[0] for field in found}
            if names != expected:
                raise FaultConfigError(
                    f"Bố cục đầu ra: {name}={template!r} phải chứa đúng các trường "
                    f"{sorted(expected)}, nhận {sorted(names)}."
                )

    def _check_unique_names(self) -> None:
        """Tên cùng cấp trong một thư mục không được trùng nhau (nếu trùng sẽ ghi đè im lặng)."""
        groups = {
            "source/": [
                self.events_dirname,
                self.statements_dirname,
                self.manifests_dirname,
                self.accounts_filename,
                self.control_report_filename,
                self.deliveries_filename,
            ],
            "ground_truth/": [
                self.canonical_dirname,
                self.fault_assignment_filename,
                self.statement_rows_filename,
                self.fault_counts_filename,
                self.canonical_index_filename,
            ],
        }
        for where, names in groups.items():
            duplicated = sorted({n for n in names if names.count(n) > 1})
            if duplicated:
                raise FaultConfigError(
                    f"Bố cục đầu ra: tên trùng nhau trong {where}: {duplicated}."
                )
        # Hai manifest cùng nằm trong một thư mục: mẫu events và statements phải cho tên khác nhau.
        events_manifest = self.events_manifest_template.format(shard=0)
        statements_manifest = self.statements_manifest_template.format(channel="C", window=0)
        if events_manifest == statements_manifest:
            raise FaultConfigError(
                "Bố cục đầu ra: mẫu tên manifest của events và statements cho cùng một tên."
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
