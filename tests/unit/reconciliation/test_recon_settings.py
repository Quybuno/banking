"""Test `ReconciliationSettings` và mục `[reconciliation]` của cấu hình (T33; ADR-0006 Q10)."""

from __future__ import annotations

import pytest

from n06.config import AppConfig, ConfigValidationError, load_config
from n06.reconciliation import ReconciliationError, ReconciliationSettings

CREDENTIALS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-test",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-test",
    "N06_POSTGRES_MART_PASSWORD": "pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
    "N06_POSTGRES_METABASE_PASSWORD": "pw",
}


def _cfg(profile: str = "default", **env: str) -> AppConfig:
    return load_config(profile=profile, env={**CREDENTIALS, **env})


@pytest.mark.parametrize("profile", ["default", "demo", "benchmark", "test"])
def test_default_status_and_channel_sets_in_every_profile(profile: str) -> None:
    cfg = _cfg(profile)
    assert cfg.reconciliation.statuses == ("SUCCESS", "FAILED")
    assert cfg.reconciliation.channels == ("SIM_CHANNEL_A", "SIM_CHANNEL_B")


def test_env_override_splits_trims_and_keeps_order() -> None:
    cfg = _cfg(
        N06__RECONCILIATION__STATUSES=" FAILED , SUCCESS",
        N06__RECONCILIATION__CHANNELS="SIM_CHANNEL_B",
    )
    assert cfg.reconciliation.statuses == ("FAILED", "SUCCESS")
    assert cfg.reconciliation.channels == ("SIM_CHANNEL_B",)


@pytest.mark.parametrize(
    ("key", "value", "field"),
    [
        ("N06__RECONCILIATION__STATUSES", "SUCCESS,,FAILED", "reconciliation.statuses"),
        ("N06__RECONCILIATION__STATUSES", "SUCCESS,SUCCESS", "reconciliation.statuses"),
        ("N06__RECONCILIATION__CHANNELS", ",", "reconciliation.channels"),
    ],
)
def test_invalid_lists_are_rejected_with_field_path(key: str, value: str, field: str) -> None:
    with pytest.raises(ConfigValidationError) as excinfo:
        _cfg(**{key: value})
    assert excinfo.value.field_path == field


def test_settings_from_config_uses_dataset_lateness_and_business_timezone() -> None:
    cfg = _cfg(N06__DATASET__ALLOWED_LATENESS_SECONDS="43200")
    settings = ReconciliationSettings.from_config(cfg)
    assert settings.allowed_lateness_seconds == 43200
    assert settings.business_timezone == "Asia/Ho_Chi_Minh"
    assert settings.statuses == ("SUCCESS", "FAILED")


@pytest.mark.parametrize(
    "override",
    [
        {"allowed_lateness_seconds": -1},
        {"allowed_lateness_seconds": True},
        {"business_timezone": "Mars/Olympus"},
        {"statuses": ()},
        {"channels": ()},
        {"rule_version": " "},
    ],
)
def test_invalid_settings_raise(override: dict) -> None:
    base: dict = {
        "allowed_lateness_seconds": 86400,
        "business_timezone": "Asia/Ho_Chi_Minh",
        "statuses": ("SUCCESS",),
        "channels": ("SIM_CHANNEL_A",),
    }
    with pytest.raises(ReconciliationError):
        ReconciliationSettings(**{**base, **override})
