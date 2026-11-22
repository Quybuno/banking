"""Tests kiểm tra Docker Compose hạ tầng lõi và khởi tạo 4 database PostgreSQL (TASK T02).

Expected values được khai báo độc lập từ đặc tả (docs/spec.md Phần I mục 2, 3)
và ADR-0001, tuyệt đối không đọc ngược từ file đang test để sinh expected.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from n06.config import load_config

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
PG_INIT_SCRIPT = REPO_ROOT / "infra" / "postgres" / "init" / "01-init-databases.sh"
ENV_EXAMPLE_FILE = REPO_ROOT / ".env.example"

# Các hằng số kỳ vọng độc lập theo đặc tả Phần I mục 2 và ADR-0001
EXPECTED_CORE_SERVICES = ("kafka", "minio", "postgres", "spark")
EXPECTED_NAMED_VOLUMES = (
    "n06_kafka_data",
    "n06_minio_data",
    "n06_postgres_data",
    "n06_spark_data",
)
EXPECTED_POSTGRES_ISOLATION = {
    "mart": ("n06_mart", "n06_mart_user", "N06_POSTGRES_MART_DB", "N06_POSTGRES_MART_USER"),
    "control": (
        "n06_control",
        "n06_control_user",
        "N06_POSTGRES_CONTROL_DB",
        "N06_POSTGRES_CONTROL_USER",
    ),
    "airflow": (
        "n06_airflow",
        "n06_airflow_user",
        "N06_POSTGRES_AIRFLOW_DB",
        "N06_POSTGRES_AIRFLOW_USER",
    ),
    "metabase": (
        "n06_metabase",
        "n06_metabase_user",
        "N06_POSTGRES_METABASE_DB",
        "N06_POSTGRES_METABASE_USER",
    ),
}
EXPECTED_REQUIRED_SECRET_VARS = (
    "N06_OBJECT_STORE_ACCESS_KEY",
    "N06_OBJECT_STORE_SECRET_KEY",
    "N06_POSTGRES_SUPERUSER_PASSWORD",
    "N06_POSTGRES_MART_PASSWORD",
    "N06_POSTGRES_CONTROL_PASSWORD",
    "N06_POSTGRES_AIRFLOW_PASSWORD",
    "N06_POSTGRES_METABASE_PASSWORD",
)


def _sample_compose_env() -> dict[str, str]:
    """Bộ biến môi trường hợp lệ độc lập dùng khi kiểm tra `docker compose config`."""
    return {
        **os.environ,
        "N06_PROFILE": "demo",
        "N06_OBJECT_STORE_ACCESS_KEY": "minio_test_access_key",
        "N06_OBJECT_STORE_SECRET_KEY": "minio_test_secret_key",
        "N06_POSTGRES_SUPERUSER": "n06_admin",
        "N06_POSTGRES_SUPERUSER_PASSWORD": "pg_superuser_test_pw",
        "N06_POSTGRES_BOOTSTRAP_DB": "postgres",
        "N06_POSTGRES_MART_DB": "n06_mart",
        "N06_POSTGRES_MART_USER": "n06_mart_user",
        "N06_POSTGRES_MART_PASSWORD": "pg_mart_test_pw",
        "N06_POSTGRES_CONTROL_DB": "n06_control",
        "N06_POSTGRES_CONTROL_USER": "n06_control_user",
        "N06_POSTGRES_CONTROL_PASSWORD": "pg_control_test_pw",
        "N06_POSTGRES_AIRFLOW_DB": "n06_airflow",
        "N06_POSTGRES_AIRFLOW_USER": "n06_airflow_user",
        "N06_POSTGRES_AIRFLOW_PASSWORD": "pg_airflow_test_pw",
        "N06_POSTGRES_METABASE_DB": "n06_metabase",
        "N06_POSTGRES_METABASE_USER": "n06_metabase_user",
        "N06_POSTGRES_METABASE_PASSWORD": "pg_metabase_test_pw",
    }


def test_compose_file_structure_and_no_latest_or_secrets() -> None:
    """Kiểm tra docker-compose.yml có đủ 4 service core, healthcheck, volume, không tag latest."""
    assert COMPOSE_FILE.is_file(), "Thiếu file docker-compose.yml ở thư mục gốc"
    content = COMPOSE_FILE.read_text(encoding="utf-8")

    # 1. Đủ 4 service thuộc profile core
    for svc in EXPECTED_CORE_SERVICES:
        assert re.search(rf"^\s{{2}}{svc}:\s*$", content, re.MULTILINE), (
            f"Thiếu service '{svc}' trong docker-compose.yml"
        )
    assert content.count("healthcheck:") >= 4, (
        "Mỗi service (kafka, minio, postgres, spark) phải có healthcheck"
    )
    assert content.count("- core") >= 4, "Cả 4 service hạ tầng lõi phải thuộc profile 'core'"

    # 2. Đủ 4 named volumes được định nghĩa và gắn vào container
    for vol_name in EXPECTED_NAMED_VOLUMES:
        assert re.search(rf"^\s{{2}}{vol_name}:\s*$", content, re.MULTILINE), (
            f"Thiếu khai báo named volume '{vol_name}'"
        )
        assert f"- {vol_name}:" in content, f"Named volume '{vol_name}' chưa được mount vào service"

    # 3. Không dùng tag :latest cho bất kỳ image nào
    images = re.findall(r"^\s*image:\s*(\S+)\s*$", content, re.MULTILINE)
    assert len(images) == 4
    for img in images:
        assert ":" in img, f"Image '{img}' phải ghim tag phiên bản rõ ràng"
        assert not img.endswith(":latest"), f"Cấm dùng tag ':latest' ({img})"
    assert "n06-spark:3.5.8-py311" in images, (
        "Service spark phải dùng đúng image n06-spark:3.5.8-py311 từ T04"
    )

    # 4. Mọi credential đều bắt buộc đọc từ biến môi trường (${VAR:?...}), không có mật khẩu cứng
    for secret_var in EXPECTED_REQUIRED_SECRET_VARS:
        assert f"${{{secret_var}:?" in content, (
            f"Biến nhạy cảm {secret_var} phải bắt buộc truyền từ .env qua ${{{secret_var}:?...}}"
        )


def test_postgres_four_isolated_databases_and_idempotent_init_script() -> None:
    """Kiểm tra đủ 4 database/user riêng biệt và script khởi tạo PostgreSQL chạy lặp an toàn."""
    assert PG_INIT_SCRIPT.is_file(), "Thiếu script khởi tạo PostgreSQL"
    assert os.access(PG_INIT_SCRIPT, os.X_OK), (
        "Script khởi tạo PostgreSQL phải có quyền thực thi (+x)"
    )

    # Kiểm tra cú pháp bash hợp lệ bằng `bash -n`
    syntax_check = subprocess.run(
        ["bash", "-n", str(PG_INIT_SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert syntax_check.returncode == 0, (
        f"Lỗi cú pháp bash trong init script: {syntax_check.stderr}"
    )

    script_text = PG_INIT_SCRIPT.read_text(encoding="utf-8")
    compose_text = COMPOSE_FILE.read_text(encoding="utf-8")

    # Đối chiếu độc lập với cấu hình T01 (load_config)
    app_cfg = load_config(profile="default", env=_sample_compose_env())

    for role_key, (
        expected_db,
        expected_user,
        db_env_var,
        user_env_var,
    ) in EXPECTED_POSTGRES_ISOLATION.items():
        role_cfg = getattr(app_cfg.postgres, role_key)
        assert role_cfg.database == expected_db
        assert role_cfg.user == expected_user

        # Cả docker-compose.yml và init script đều phải tham chiếu đủ 4 cặp biến DB & USER
        assert db_env_var in compose_text
        assert user_env_var in compose_text
        assert f"${{{db_env_var}" in script_text
        assert f"${{{user_env_var}" in script_text
        assert f'provision_role_and_db "${{{db_env_var}}}" "${{{user_env_var}}}"' in script_text

    # Kiểm tra healthcheck của postgres trong docker-compose.yml kiểm tra đủ cả 4 database
    for _, _, db_env_var, _ in EXPECTED_POSTGRES_ISOLATION.values():
        assert f'psql -U "$${{POSTGRES_USER}}" -d "$${{{db_env_var}}}"' in compose_text

    # Kiểm tra tính idempotent của SQL trong init script: kiểm tra tồn tại trước khi CREATE
    assert (
        "WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'target_db')" in script_text
    )
    assert (
        "WHEN NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'role_name')"
        in script_text
    )
    assert "REVOKE CONNECT ON DATABASE" in script_text
    assert "REVOKE CREATE ON SCHEMA public FROM PUBLIC" in script_text


def test_env_example_synchronizes_all_compose_variables() -> None:
    """Kiểm tra .env.example khai báo đủ các biến mà docker-compose.yml cần, chỉ có placeholder."""
    env_lines = ENV_EXAMPLE_FILE.read_text(encoding="utf-8").splitlines()
    env_map: dict[str, str] = {}
    for line in env_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, _, val = stripped.partition("=")
        env_map[key.strip()] = val.strip()

    for secret_var in EXPECTED_REQUIRED_SECRET_VARS:
        assert secret_var in env_map, f"Thiếu biến {secret_var} trong .env.example"
        assert env_map[secret_var].startswith("CHANGE_ME_"), (
            f"Biến {secret_var} trong .env.example phải là placeholder CHANGE_ME_*"
        )

    for _, (
        expected_db,
        expected_user,
        db_env_var,
        user_env_var,
    ) in EXPECTED_POSTGRES_ISOLATION.items():
        assert env_map.get(db_env_var) == expected_db
        assert env_map.get(user_env_var) == expected_user


def test_docker_compose_config_cli_validation() -> None:
    """Chạy `docker compose --profile core config --format json` nếu máy có cài Docker CLI."""
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        pytest.skip("Máy hiện tại không có binary `docker` trong PATH (hoặc đang ở trong sandbox)")

    res = subprocess.run(
        [
            docker_bin,
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "--profile",
            "core",
            "config",
            "--format",
            "json",
        ],
        cwd=REPO_ROOT,
        env=_sample_compose_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert res.returncode == 0, f"`docker compose config` thất bại: {res.stderr}"

    parsed = json.loads(res.stdout)
    services = parsed.get("services", {})
    assert set(services.keys()) == set(EXPECTED_CORE_SERVICES)
    for svc_name in EXPECTED_CORE_SERVICES:
        hc = services[svc_name].get("healthcheck")
        assert hc is not None and hc.get("test"), f"Service {svc_name} thiếu healthcheck"

    volumes = parsed.get("volumes", {})
    assert set(volumes.keys()) == set(EXPECTED_NAMED_VOLUMES)
