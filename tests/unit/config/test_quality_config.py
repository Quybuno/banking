"""Test mục `[quality]` của cấu hình (T32, ADR-0014): mặc định chặt, policy demo, giá trị sai."""

from __future__ import annotations

import pytest

from n06.config import ConfigValidationError, load_config

CREDENTIALS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-test",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-test",
    "N06_POSTGRES_MART_PASSWORD": "pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
    "N06_POSTGRES_METABASE_PASSWORD": "pw",
}


@pytest.mark.parametrize("profile", ["default", "benchmark", "test"])
def test_profiles_without_an_override_block_on_any_error(profile: str) -> None:
    quality = load_config(profile=profile, env=CREDENTIALS).quality
    assert (quality.max_quarantine_rate_bps, quality.max_conflict_count) == (0, 0)
    assert quality.fail_on_unexplained_control_mismatch is True
    assert quality.require_control_report is True


def test_demo_policy_tolerates_intentional_errors_but_keeps_the_control_check() -> None:
    quality = load_config(profile="demo", env=CREDENTIALS).quality
    assert (quality.max_quarantine_rate_bps, quality.max_conflict_count) == (100, 50)
    assert quality.fail_on_unexplained_control_mismatch is True


def test_environment_override() -> None:
    env = {**CREDENTIALS, "N06__QUALITY__MAX_QUARANTINE_RATE_BPS": "250"}
    assert load_config(profile="demo", env=env).quality.max_quarantine_rate_bps == 250


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("N06__QUALITY__MAX_QUARANTINE_RATE_BPS", "10001"),
        ("N06__QUALITY__MAX_QUARANTINE_RATE_BPS", "-1"),
        ("N06__QUALITY__MAX_CONFLICT_COUNT", "-1"),
    ],
)
def test_out_of_range_values_are_rejected(name: str, value: str) -> None:
    with pytest.raises(ConfigValidationError, match="quality"):
        load_config(profile="demo", env={**CREDENTIALS, name: value})
