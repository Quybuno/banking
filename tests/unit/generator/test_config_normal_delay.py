"""Normal delay phải giữ statement trước cutoff (spec Phần II mục 5.1, mục 1.10) — TASK T13."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from n06.generator import FaultConfigError, build_dataset
from n06.generator.params import DatasetParams, check_statement_delays

# business_date 2026-01-01 -> D = 2026-01-01T17:00:00Z (tính tay, không dùng hàm đang test).
D = datetime(2026, 1, 1, 17, 0, 0, tzinfo=UTC)
LAST_EVENT = D - timedelta(seconds=1)  # 23:59:59 giờ Việt Nam: giao dịch muộn nhất của ngày


@pytest.mark.parametrize("lateness", [3600, 86_400, 172_800])
@pytest.mark.parametrize("offset", [-60, -1, 0, 1, 2, 114_000])
def test_check_matches_independent_cutoff_arithmetic(
    make_config: Any, lateness: int, offset: int
) -> None:
    delay = lateness + offset
    assert delay >= 0
    params = replace(
        DatasetParams.from_config(make_config()),
        allowed_lateness_seconds=lateness,
        normal_statement_delay_seconds=delay,
    )
    cutoff = D + timedelta(seconds=lateness)
    available_at = LAST_EVENT + timedelta(seconds=delay)
    before_cutoff = available_at < cutoff  # đúng cutoff đã quá hạn
    if before_cutoff:
        check_statement_delays(params)
    else:
        with pytest.raises(FaultConfigError, match="normal_statement_delay_seconds"):
            check_statement_delays(params)


def test_boundary_cases_are_exactly_at_and_just_below_cutoff(make_config: Any) -> None:
    base = DatasetParams.from_config(make_config())
    lateness = base.allowed_lateness_seconds
    assert lateness == 86_400
    just_below = replace(base, normal_statement_delay_seconds=lateness)
    at_cutoff = replace(base, normal_statement_delay_seconds=lateness + 1)
    assert LAST_EVENT + timedelta(seconds=lateness) < D + timedelta(seconds=lateness)
    assert LAST_EVENT + timedelta(seconds=lateness + 1) == D + timedelta(seconds=lateness)
    check_statement_delays(just_below)
    with pytest.raises(FaultConfigError):
        check_statement_delays(at_cutoff)


def test_build_rejects_normal_delay_200000_with_lateness_86400(
    tmp_path: Any, make_config: Any
) -> None:
    cfg = make_config(transaction_count=20, normal_statement_delay_seconds=200_000)
    assert cfg["allowed_lateness_seconds"] == 86_400
    with pytest.raises(FaultConfigError, match="200000") as info:
        build_dataset(cfg, tmp_path)
    assert "cutoff" in str(info.value)
    # Từ chối ngay lúc startup: chưa tạo thư mục đầu ra nào.
    assert list(tmp_path.iterdir()) == []


def test_build_rejects_delay_landing_exactly_on_cutoff(tmp_path: Any, make_config: Any) -> None:
    cfg = make_config(transaction_count=20, normal_statement_delay_seconds=86_401)
    with pytest.raises(FaultConfigError, match="86401"):
        build_dataset(cfg, tmp_path)


def test_build_accepts_delay_just_below_cutoff(tmp_path: Any, make_config: Any) -> None:
    cfg = make_config(transaction_count=20, normal_statement_delay_seconds=86_400)
    summary = build_dataset(cfg, tmp_path)
    assert summary.statement_record_count > 0
