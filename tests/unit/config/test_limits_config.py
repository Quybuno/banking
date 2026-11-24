"""Test mục `[limits]` của cấu hình (CFG-scale-guard): mặc định, ghi đè bằng env, giá trị sai."""

from __future__ import annotations

import pytest

from n06.config import ConfigValidationError, LimitsConfig, load_config

CREDENTIALS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-test",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-test",
    "N06_POSTGRES_MART_PASSWORD": "pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
    "N06_POSTGRES_METABASE_PASSWORD": "pw",
}


def _load(profile: str = "demo", **env: str) -> LimitsConfig:
    return load_config(profile=profile, env={**CREDENTIALS, **env}).limits


@pytest.mark.parametrize("profile", ["default", "demo", "benchmark", "test"])
def test_defaults_leave_optional_keys_unset_in_every_profile(profile: str) -> None:
    limits = _load(profile)
    assert limits.max_transactions is None  # mặc định không đặt trần
    assert limits.pipeline_bytes_per_tx is None  # CHƯA ĐO nên không có số nào
    assert limits.disk_probe_path is None
    assert limits.generator_bytes_per_tx == 4000  # số đo thật 3.993 B, làm tròn lên
    assert limits.min_free_disk_gb_reserve == 8


def test_environment_overrides_use_the_n06_double_underscore_syntax() -> None:
    limits = _load(
        N06__LIMITS__MAX_TRANSACTIONS="1000000",
        N06__LIMITS__MIN_FREE_DISK_GB_RESERVE="12",
        N06__LIMITS__GENERATOR_BYTES_PER_TX="4100",
        N06__LIMITS__PIPELINE_BYTES_PER_TX="9000",
        N06__LIMITS__DISK_PROBE_PATH="/mnt/d",
    )
    assert limits == LimitsConfig(1_000_000, 12, 4100, 9000, "/mnt/d")


def test_spec_scale_levels_are_unchanged_by_the_limits_section() -> None:
    assert load_config(profile="demo", env=CREDENTIALS).dataset.transaction_count == 10000
    assert load_config(profile="benchmark", env=CREDENTIALS).dataset.transaction_count == 1_000_000


@pytest.mark.parametrize(
    ("key", "value", "field"),
    [
        ("N06__LIMITS__MAX_TRANSACTIONS", "0", "limits.max_transactions"),
        ("N06__LIMITS__MAX_TRANSACTIONS", "-3", "limits.max_transactions"),
        ("N06__LIMITS__MAX_TRANSACTIONS", "abc", "limits.max_transactions"),
        ("N06__LIMITS__MIN_FREE_DISK_GB_RESERVE", "-1", "limits.min_free_disk_gb_reserve"),
        ("N06__LIMITS__GENERATOR_BYTES_PER_TX", "0", "limits.generator_bytes_per_tx"),
        ("N06__LIMITS__PIPELINE_BYTES_PER_TX", "0", "limits.pipeline_bytes_per_tx"),
        ("N06__LIMITS__NO_SUCH_KEY", "1", "limits.no_such_key"),
    ],
)
def test_invalid_limits_are_rejected_naming_the_field(key: str, value: str, field: str) -> None:
    with pytest.raises(ConfigValidationError) as caught:
        _load(**{key: value})
    assert caught.value.field_path == field


def test_empty_environment_value_for_an_optional_key_means_unset() -> None:
    limits = _load(
        N06__LIMITS__MAX_TRANSACTIONS="",
        N06__LIMITS__PIPELINE_BYTES_PER_TX="  ",
        N06__LIMITS__DISK_PROBE_PATH="",
    )
    assert (limits.max_transactions, limits.pipeline_bytes_per_tx, limits.disk_probe_path) == (
        None,
        None,
        None,
    )


def test_empty_environment_value_for_a_required_key_is_still_rejected() -> None:
    with pytest.raises(ConfigValidationError):
        _load(N06__LIMITS__MIN_FREE_DISK_GB_RESERVE="")
