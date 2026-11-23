"""OutputLayout phải từ chối bố cục làm các file ghi đè lên nhau (review T13, phát hiện N2)."""

from __future__ import annotations

from typing import Any

import pytest

from n06.generator import DEFAULT_LAYOUT, FaultConfigError, OutputLayout, build_dataset


@pytest.mark.parametrize(
    "overrides",
    [
        {"statements_file_template": "st.jsonl"},  # thiếu {window}: mỗi kênh chỉ còn một file
        {"statements_file_template": "st_{window}_{channel}.jsonl"},  # trường lạ
        {"events_file_template": "events.jsonl"},  # thiếu {shard}
        {"events_file_template": "e_{shard}_{window}.jsonl"},
        {"events_manifest_template": "events.manifest.json"},
        {"statements_manifest_template": "st_{window}.manifest.json"},  # thiếu {channel}
        {"statements_manifest_template": "st_{channel}.manifest.json"},  # thiếu {window}
    ],
)
def test_template_missing_or_extra_placeholder_is_rejected(overrides: dict[str, str]) -> None:
    with pytest.raises(FaultConfigError, match="phải chứa đúng các trường"):
        OutputLayout(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"accounts_filename": "same.json", "control_report_filename": "same.json"},
        {"deliveries_filename": "acct.jsonl", "accounts_filename": "acct.jsonl"},
        {"events_dirname": "data", "statements_dirname": "data"},
        {"events_dirname": "accounts.jsonl"},  # thư mục trùng tên file cùng cấp
        {"fault_counts_filename": "x.json", "canonical_index_filename": "x.json"},
        {"canonical_dirname": "fault_assignment.jsonl"},  # trùng tên file mặc định
    ],
)
def test_duplicate_names_in_same_directory_are_rejected(overrides: dict[str, str]) -> None:
    with pytest.raises(FaultConfigError, match="trùng"):
        OutputLayout(**overrides)


def test_events_and_statements_manifest_templates_must_not_collide() -> None:
    # shard=0 cho "C0.json" và channel="C", window=0 cũng cho "C0.json".
    with pytest.raises(FaultConfigError, match="manifest"):
        OutputLayout(
            events_manifest_template="C{shard}.json",
            statements_manifest_template="{channel}{window}.json",
        )


def test_default_layout_still_valid_and_custom_valid_layout_accepted() -> None:
    assert OutputLayout() == DEFAULT_LAYOUT
    OutputLayout(
        events_file_template="ev_{shard:03d}.jsonl",
        statements_file_template="st_{window}.jsonl",
        statements_manifest_template="st_{channel}_{window}.manifest.json",
    )


def test_invalid_layout_never_reaches_build_dataset(tmp_path: Any, make_config: Any) -> None:
    with pytest.raises(FaultConfigError):
        build_dataset(
            make_config(transaction_count=20),
            tmp_path,
            layout=OutputLayout(statements_file_template="st.jsonl"),
        )
    assert list(tmp_path.iterdir()) == []
