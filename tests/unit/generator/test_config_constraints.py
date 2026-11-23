"""Ràng buộc cấu hình mục 5.1 và 5.3 phải báo lỗi rõ ràng (TASK T13)."""

from __future__ import annotations

from typing import Any

import pytest

from n06.generator import FaultConfigError, build_dataset


def _try(tmp_path: Any, cfg: dict[str, Any]) -> None:
    build_dataset(cfg, tmp_path, validate_outputs=False)


def test_late_within_plus_window_must_be_below_lateness(tmp_path: Any, make_config: Any) -> None:
    # 82800 + 3600 == 86400: vi phạm vì phải nhỏ hơn hẳn.
    cfg = make_config(late_within_delay_after_day_end_seconds=82_800)
    with pytest.raises(FaultConfigError, match="late_within"):
        _try(tmp_path, cfg)


def test_late_after_must_be_at_least_lateness(tmp_path: Any, make_config: Any) -> None:
    cfg = make_config(late_after_delay_after_day_end_seconds=86_399)
    with pytest.raises(FaultConfigError, match="late_after|allowed_lateness"):
        _try(tmp_path, cfg)


def test_basis_points_sum_over_10000(tmp_path: Any, make_config: Any) -> None:
    cfg = make_config(faults={"event_replay_basis_points": 9_500})
    with pytest.raises(FaultConfigError, match="10000"):
        _try(tmp_path, cfg)


def test_boundaries_that_are_valid(tmp_path: Any, make_config: Any) -> None:
    # late_within + window == lateness - 1 và late_after == lateness đều hợp lệ.
    cfg = make_config(
        transaction_count=20,
        late_within_delay_after_day_end_seconds=82_799,
        late_after_delay_after_day_end_seconds=86_400,
    )
    build_dataset(cfg, tmp_path)


def test_output_directory_must_be_empty(tmp_path: Any, make_config: Any) -> None:
    (tmp_path / "source").mkdir()
    (tmp_path / "source" / "stale.jsonl").write_text("x\n")
    with pytest.raises(FaultConfigError, match="trống"):
        build_dataset(make_config(transaction_count=20), tmp_path)
