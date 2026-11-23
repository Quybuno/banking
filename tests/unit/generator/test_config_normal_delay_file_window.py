"""Normal delay cộng cửa sổ file phải giữ statement trước cutoff (ADR-0011) — TASK T13 follow-up.

Expected tính bằng số học thời gian riêng trong test (giao dịch muộn nhất của ngày cách D đúng
1 giây, file đến muộn nhất `available_at + W`), không dùng công thức của mã.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from n06.domain import validate_generator_config_data
from n06.generator import FaultConfigError, build_dataset
from n06.generator.params import DatasetParams, check_statement_delays

from .conftest import read_jsonl, read_jsonl_dir

D = datetime(2026, 1, 1, 17, 0, 0, tzinfo=UTC)  # business_date 2026-01-01
LAST_EVENT = D - timedelta(seconds=1)


def _worst_file_before_cutoff(delay: int, window: int, lateness: int) -> bool:
    worst_file = LAST_EVENT + timedelta(seconds=delay + window)
    return worst_file < D + timedelta(seconds=lateness)


def test_default_configuration_is_accepted(make_config: Any) -> None:
    params = DatasetParams.from_config(make_config())
    assert (
        params.normal_statement_delay_seconds,
        params.statement_file_window_seconds,
        params.allowed_lateness_seconds,
    ) == (60, 3600, 86_400)
    assert _worst_file_before_cutoff(60, 3600, 86_400)
    check_statement_delays(params)


@pytest.mark.parametrize("window", [1, 60, 3600, 5000, 43_199])
def test_normal_plus_window_equal_allowed_is_accepted_and_one_more_is_rejected(
    make_config: Any, window: int
) -> None:
    base = replace(DatasetParams.from_config(make_config()), statement_file_window_seconds=window)
    allowed = base.allowed_lateness_seconds
    ok = replace(base, normal_statement_delay_seconds=allowed - window)  # normal + W == allowed
    bad = replace(base, normal_statement_delay_seconds=allowed - window + 1)  # == allowed + 1
    assert _worst_file_before_cutoff(allowed - window, window, allowed)
    assert not _worst_file_before_cutoff(allowed - window + 1, window, allowed)
    check_statement_delays(ok)
    with pytest.raises(FaultConfigError, match="statement_file_window_seconds"):
        check_statement_delays(bad)


def test_review_gap_configuration_passes_t10_but_is_rejected_by_t13(
    tmp_path: Any, make_config: Any
) -> None:
    """Ca review: normal 86000, cửa sổ 5000, lateness 86400, late_within 43200."""
    cfg = make_config(
        transaction_count=50,
        normal_statement_delay_seconds=86_000,
        statement_file_window_seconds=5000,
        allowed_lateness_seconds=86_400,
        late_within_delay_after_day_end_seconds=43_200,
    )
    assert validate_generator_config_data(cfg).is_valid  # T10 không bắt được ca này
    with pytest.raises(FaultConfigError, match="86000") as info:
        build_dataset(cfg, tmp_path)
    assert "5000" in str(info.value)
    assert list(tmp_path.iterdir()) == []


def test_accepted_boundary_configuration_has_no_normal_file_after_cutoff(
    tmp_path: Any, make_config: Any
) -> None:
    """Chạy thật với normal + W == allowed (cửa sổ 5000 không chia hết cho 86400)."""
    cfg = make_config(
        transaction_count=1500,
        normal_statement_delay_seconds=81_400,
        statement_file_window_seconds=5000,
    )
    assert cfg["normal_statement_delay_seconds"] + 5000 == cfg["allowed_lateness_seconds"]
    summary = build_dataset(cfg, tmp_path)
    txs = {
        t["transaction_id"]: t
        for t in read_jsonl_dir(summary.ground_truth_dir / "canonical", "transactions_part_*.jsonl")
    }
    rows = read_jsonl(summary.ground_truth_dir / "statement_rows.jsonl")
    checked = 0
    for row in rows:
        if row["fault_type"] in ("late_within_deadline", "late_after_deadline"):
            continue
        business_date = datetime.strptime(txs[row["transaction_id"]]["business_date"], "%Y-%m-%d")
        # D = 00:00 giờ Việt Nam của ngày sau = 17:00Z của business_date (UTC+7, không DST).
        d = business_date.replace(hour=17, tzinfo=UTC)
        cutoff = d + timedelta(seconds=cfg["allowed_lateness_seconds"])
        file_at = datetime.strptime(row["file_available_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=UTC
        )
        assert file_at < cutoff, row
        checked += 1
    assert checked > 1000
