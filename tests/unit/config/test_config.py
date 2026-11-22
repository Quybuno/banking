"""Unit tests cho hệ thống cấu hình và profile của N06 (TASK T01)."""

from __future__ import annotations

import shutil
import tomllib
from pathlib import Path

import pytest

from n06.config import (
    DEFAULT_CONFIG_DIR,
    REPO_ROOT,
    AppConfig,
    ConfigValidationError,
    load_config,
)


def _valid_secret_env() -> dict[str, str]:
    """Bộ biến môi trường credential hợp lệ phục vụ unit test."""
    return {
        "N06_OBJECT_STORE_ACCESS_KEY": "test_minio_access",
        "N06_OBJECT_STORE_SECRET_KEY": "test_minio_secret",
        "N06_POSTGRES_MART_PASSWORD": "test_mart_password",
        "N06_POSTGRES_CONTROL_PASSWORD": "test_control_password",
        "N06_POSTGRES_AIRFLOW_PASSWORD": "test_airflow_password",
        "N06_POSTGRES_METABASE_PASSWORD": "test_metabase_password",
    }


def test_load_all_profiles_success() -> None:
    """Kiểm tra nạp thành công default, demo, benchmark, test và tách biệt 4 Postgres DB/user."""
    env = _valid_secret_env()

    default_cfg = load_config(profile="default", env=env)
    assert isinstance(default_cfg, AppConfig)
    assert default_cfg.profile == "default"
    assert default_cfg.business_timezone == "Asia/Ho_Chi_Minh"
    assert default_cfg.producer.key_strategy == "transaction_id"

    demo_cfg = load_config(profile="demo", env=env)
    assert demo_cfg.profile == "demo"
    assert demo_cfg.services.airflow_enabled is True
    assert demo_cfg.services.metabase_enabled is True
    assert demo_cfg.producer.rate == 5000

    bench_cfg = load_config(profile="benchmark", env=env)
    assert bench_cfg.profile == "benchmark"
    assert bench_cfg.services.airflow_enabled is False
    assert bench_cfg.services.metabase_enabled is False
    assert bench_cfg.producer.rate == 0
    assert bench_cfg.dataset.transaction_count == 1000000

    test_cfg = load_config(profile="test", env=env)
    assert test_cfg.profile == "test"
    assert test_cfg.services.airflow_enabled is False
    assert test_cfg.dataset.transaction_count == 100

    # Xác nhận 4 database và 4 user của PostgreSQL tách biệt hoàn toàn
    pg = demo_cfg.postgres
    dbs = {pg.mart.database, pg.control.database, pg.airflow.database, pg.metabase.database}
    users = {pg.mart.user, pg.control.user, pg.airflow.user, pg.metabase.user}
    assert len(dbs) == 4
    assert len(users) == 4


def test_missing_required_field_reports_exact_path(tmp_path: Path) -> None:
    """Kiểm tra thiếu trường bắt buộc (trong file TOML hoặc thiếu biến môi trường password)."""
    # Ca 1: Thiếu biến môi trường password của mart DB
    env_missing_pw = _valid_secret_env()
    del env_missing_pw["N06_POSTGRES_MART_PASSWORD"]

    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(profile="demo", env=env_missing_pw)
    assert exc_info.value.field_path == "postgres.mart.password"
    assert "postgres.mart.password" in str(exc_info.value)

    # Ca 2: Thiếu trường bắt buộc kafka.bootstrap_servers trong file cấu hình
    for toml_file in DEFAULT_CONFIG_DIR.glob("*.toml"):
        shutil.copy(toml_file, tmp_path / toml_file.name)
    default_toml = (tmp_path / "default.toml").read_text(encoding="utf-8")
    modified_toml = "\n".join(
        line
        for line in default_toml.splitlines()
        if not line.strip().startswith("bootstrap_servers")
    )
    (tmp_path / "default.toml").write_text(modified_toml + "\n", encoding="utf-8")

    with pytest.raises(ConfigValidationError) as exc_info_2:
        load_config(profile="default", config_dir=tmp_path, env=_valid_secret_env())
    assert exc_info_2.value.field_path == "kafka.bootstrap_servers"
    assert "kafka.bootstrap_servers" in str(exc_info_2.value)


@pytest.mark.parametrize(
    ("field_path", "bad_value"),
    [
        ("spark.shuffle_partitions", "not_an_int"),
        ("spark.shuffle_partitions", True),
        ("services.airflow_enabled", "not_a_bool"),
        ("dataset.seed", "forty-two"),
    ],
)
def test_wrong_type_reports_field_and_value(field_path: str, bad_value: object) -> None:
    """Kiểm tra sai kiểu dữ liệu (kể cả bool cho int) báo đúng tên trường và giá trị."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(
            profile="demo",
            env=_valid_secret_env(),
            cli_overrides={field_path: bad_value},
        )
    err = exc_info.value
    assert err.field_path == field_path
    assert err.invalid_value == bad_value
    assert field_path in str(err)
    assert repr(bad_value) in str(err)


def test_invalid_enum_key_strategy_reports_field_and_value() -> None:
    """Kiểm tra giá trị ngoài enum cho producer.key_strategy."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(
            profile="demo",
            env=_valid_secret_env(),
            cli_overrides={"producer.key_strategy": "round_robin"},
        )
    err = exc_info.value
    assert err.field_path == "producer.key_strategy"
    assert err.invalid_value == "round_robin"
    assert "producer.key_strategy" in str(err)
    assert "'round_robin'" in str(err)


def test_invalid_business_timezone_reports_field_and_value() -> None:
    """Kiểm tra timezone nghiệp vụ khác Asia/Ho_Chi_Minh bị từ chối ở v1."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(
            profile="demo",
            env=_valid_secret_env(),
            cli_overrides={"business_timezone": "UTC"},
        )
    err = exc_info.value
    assert err.field_path == "business_timezone"
    assert err.invalid_value == "UTC"
    assert "business_timezone" in str(err)
    assert "'UTC'" in str(err)


def test_from_date_greater_than_to_date_reports_field() -> None:
    """Kiểm tra dataset.from_date > dataset.to_date báo lỗi đúng trường dataset.from_date."""
    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(
            profile="demo",
            env=_valid_secret_env(),
            cli_overrides={
                "dataset.from_date": "2026-01-10",
                "dataset.to_date": "2026-01-03",
            },
        )
    err = exc_info.value
    assert err.field_path == "dataset.from_date"
    assert err.invalid_value == "2026-01-10"
    assert "dataset.from_date" in str(err)


def test_four_layer_precedence() -> None:
    """Kiểm tra thứ tự ưu tiên 4 tầng: default < profile < env < cli_overrides."""
    base_env = _valid_secret_env()

    # Tầng 1: default.toml (shuffle_partitions=50, producer.rate=0, key_strategy=transaction_id)
    cfg_default = load_config(profile="default", env=base_env)
    assert cfg_default.spark.shuffle_partitions == 50
    assert cfg_default.producer.rate == 0
    assert cfg_default.producer.key_strategy == "transaction_id"

    # Tầng 2: demo.toml ghi đè default.toml (shuffle_partitions = 16, producer.rate = 5000)
    cfg_profile = load_config(profile="demo", env=base_env)
    assert cfg_profile.spark.shuffle_partitions == 16
    assert cfg_profile.producer.rate == 5000
    assert cfg_profile.producer.key_strategy == "transaction_id"

    # Tầng 3: Biến môi trường ghi đè profile demo
    env_with_overrides = {
        **base_env,
        "N06_PROFILE": "demo",
        "N06__SPARK__SHUFFLE_PARTITIONS": "32",
        "N06__PRODUCER__RATE": "8000",
        "N06__PRODUCER__KEY_STRATEGY": "account_from",
    }
    cfg_env = load_config(env=env_with_overrides)
    assert cfg_env.profile == "demo"
    assert cfg_env.spark.shuffle_partitions == 32
    assert cfg_env.producer.rate == 8000
    assert cfg_env.producer.key_strategy == "account_from"

    # Tầng 4: cli_overrides ghi đè cả biến môi trường
    cfg_cli = load_config(
        env=env_with_overrides,
        cli_overrides={
            "spark.shuffle_partitions": 128,
            "producer": {"rate": 12000, "key_strategy": "transaction_id"},
        },
    )
    assert cfg_cli.spark.shuffle_partitions == 128
    assert cfg_cli.producer.rate == 12000
    assert cfg_cli.producer.key_strategy == "transaction_id"


def _assert_no_forbidden_keys(
    data: dict[str, object],
    forbidden_keys: set[str],
    file_name: str,
    prefix: str = "",
) -> None:
    for key, value in data.items():
        current = f"{prefix}.{key}" if prefix else key
        assert key.lower() not in forbidden_keys, (
            f"Phát hiện trường nhạy cảm '{current}' trong file {file_name}"
        )
        if isinstance(value, dict):
            _assert_no_forbidden_keys(value, forbidden_keys, file_name, current)


def test_no_config_file_contains_passwords_or_secrets(tmp_path: Path) -> None:
    """Xác nhận không file nào trong config/ chứa password/secret và chặn TOML chứa secret."""
    toml_files = sorted(DEFAULT_CONFIG_DIR.glob("*.toml"))
    assert len(toml_files) >= 4

    forbidden_keys = {"password", "secret_key", "access_key", "secret", "token"}
    for path in toml_files:
        raw_text = path.read_text(encoding="utf-8")
        data = tomllib.loads(raw_text)
        _assert_no_forbidden_keys(data, forbidden_keys, path.name)

    # Kiểm tra .env.example chỉ chứa giá trị mẫu CHANGE_ME_... cho các biến nhạy cảm
    env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    for line in env_example.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, _, val = stripped.partition("=")
        if any(word in key for word in ("PASSWORD", "SECRET", "ACCESS_KEY")):
            assert val.startswith("CHANGE_ME_"), (
                f"Biến {key} trong .env.example phải dùng placeholder CHANGE_ME_*"
            )

    # Kiểm tra load_config từ chối ngay nếu ai đó chèn password vào file TOML
    for toml_file in toml_files:
        shutil.copy(toml_file, tmp_path / toml_file.name)
    with (tmp_path / "demo.toml").open("a", encoding="utf-8") as f:
        f.write('\n[postgres.mart]\npassword = "leaked_password"\n')

    with pytest.raises(ConfigValidationError) as exc_info:
        load_config(profile="demo", config_dir=tmp_path, env=_valid_secret_env())
    assert exc_info.value.field_path == "postgres.mart.password"
