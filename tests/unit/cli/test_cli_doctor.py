"""Unit tests cho khung CLI `n06`, `init-local` và `doctor` (TASK T03).

Kiểm tra:
1. `n06 --help` liệt kê đủ lệnh, exit code 0; sai tham số trả về exit code 2.
2. Đánh giá tài nguyên tách riêng (`evaluate_system_resources`) chạy bằng giá trị giả,
   không phụ thuộc vào máy chạy test, trả về đúng mã thoát riêng cho từng loại lỗi (11, 12).
3. `n06 init-local` sinh file `.env` hợp lệ, không ghi đè nếu không có `--force`,
   và tuyệt đối không in secret ra output.
4. `n06 doctor` báo đúng lỗi cấu hình (exit code 10), lỗi service cụ thể khi 1 service bị tắt
   (exit code 14), và không bao giờ lộ chuỗi secret trong output.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import patch

from n06.cli import (
    ExitCode,
    ResourceThresholds,
    SystemResourceSnapshot,
    evaluate_system_resources,
    main,
    parse_dotenv_file,
    run_doctor,
    run_init_local,
)
from n06.config import load_config


def _valid_env() -> dict[str, str]:
    return {
        "N06_PROFILE": "demo",
        "N06_OBJECT_STORE_ACCESS_KEY": "SUPER_SECRET_MINIO_AK_12345",
        "N06_OBJECT_STORE_SECRET_KEY": "SUPER_SECRET_MINIO_SK_67890",
        "N06_POSTGRES_SUPERUSER_PASSWORD": "SUPER_SECRET_PG_ADMIN_PW",
        "N06_POSTGRES_MART_PASSWORD": "SUPER_SECRET_MART_PW",
        "N06_POSTGRES_CONTROL_PASSWORD": "SUPER_SECRET_CONTROL_PW",
        "N06_POSTGRES_AIRFLOW_PASSWORD": "SUPER_SECRET_AIRFLOW_PW",
        "N06_POSTGRES_METABASE_PASSWORD": "SUPER_SECRET_METABASE_PW",
    }


def test_cli_help_lists_commands_and_exit_codes() -> None:
    """`n06 --help` liệt kê `init-local`, `doctor`, bảng mã thoát và trả về exit code 0."""
    out = io.StringIO()
    err = io.StringIO()
    code = main(["--help"], out=out, err=err)
    assert code == 0

    help_text = out.getvalue()
    assert "init-local" in help_text
    assert "doctor" in help_text
    assert "CONFIG_ERROR" in help_text
    assert "SERVICE_ERROR" in help_text

    # Sai lệnh con trả về mã thoát 2 (CLI_USAGE_ERROR)
    out_bad = io.StringIO()
    err_bad = io.StringIO()
    bad_code = main(["unknown-command"], out=out_bad, err=err_bad)
    assert bad_code == 2


def test_evaluate_system_resources_with_mock_values() -> None:
    """Kiểm tra hàm thuần túy `evaluate_system_resources` bằng giá trị giả độc lập với máy chạy."""
    thresholds = ResourceThresholds(
        required_python_major=3,
        required_python_minor=11,
        min_cpu_cores=4,
        min_ram_gib=8.0,
        min_free_disk_gib=20.0,
    )

    # Ca 1: Đủ toàn bộ tài nguyên và đúng Python 3.11 -> tất cả PASS (exit_code = 0)
    good_snap = SystemResourceSnapshot(
        python_major=3,
        python_minor=11,
        python_micro=11,
        cpu_cores=8,
        total_ram_gib=16.0,
        free_disk_gib=50.0,
    )
    good_items = {item.check_id: item for item in evaluate_system_resources(good_snap, thresholds)}
    assert set(good_items.keys()) == {
        "python.version",
        "resource.cpu",
        "resource.ram",
        "resource.disk",
    }
    assert all(item.passed and item.exit_code == 0 for item in good_items.values())

    # Ca 2: Sai phiên bản Python (3.10) -> chỉ python.version FAIL với mã 11 (PYTHON_ENV_ERROR)
    bad_py_snap = SystemResourceSnapshot(
        python_major=3,
        python_minor=10,
        python_micro=12,
        cpu_cores=8,
        total_ram_gib=16.0,
        free_disk_gib=50.0,
    )
    py_items = {item.check_id: item for item in evaluate_system_resources(bad_py_snap, thresholds)}
    assert py_items["python.version"].passed is False
    assert py_items["python.version"].exit_code == 11
    assert py_items["resource.cpu"].passed is True

    # Ca 3: Thiếu CPU (2 < 4) -> chỉ resource.cpu FAIL với mã 12 (RESOURCE_ERROR)
    low_cpu_snap = SystemResourceSnapshot(
        python_major=3,
        python_minor=11,
        python_micro=9,
        cpu_cores=2,
        total_ram_gib=16.0,
        free_disk_gib=50.0,
    )
    cpu_items = {
        item.check_id: item for item in evaluate_system_resources(low_cpu_snap, thresholds)
    }
    assert cpu_items["resource.cpu"].passed is False
    assert cpu_items["resource.cpu"].exit_code == 12
    assert "2 cores" in cpu_items["resource.cpu"].summary
    assert cpu_items["resource.ram"].passed is True
    assert cpu_items["resource.disk"].passed is True

    # Ca 4: Thiếu RAM (3.5 GiB < 8.0 GiB) và thiếu Disk (4.0 GiB < 20.0 GiB) -> FAIL mã 12
    low_ram_disk_snap = SystemResourceSnapshot(
        python_major=3,
        python_minor=11,
        python_micro=9,
        cpu_cores=4,
        total_ram_gib=3.5,
        free_disk_gib=4.0,
    )
    rd_items = {
        item.check_id: item for item in evaluate_system_resources(low_ram_disk_snap, thresholds)
    }
    assert rd_items["resource.ram"].passed is False
    assert rd_items["resource.ram"].exit_code == 12
    assert rd_items["resource.disk"].passed is False
    assert rd_items["resource.disk"].exit_code == 12


def test_init_local_generates_env_and_never_prints_secrets(tmp_path: Path) -> None:
    """`n06 init-local` sinh secret vào `.env`, nạp được bởi `load_config` và ẩn secret."""
    target_env = tmp_path / ".env"
    out = io.StringIO()

    # Test này chỉ kiểm việc sinh `.env`; bước tạo bucket (T03b) có test riêng nên tắt ở đây để
    # không phụ thuộc MinIO đang chạy trên máy.
    code = run_init_local(
        profile="benchmark",
        target_path=target_env,
        force=False,
        skip_buckets=True,
        as_json=False,
        out=out,
    )
    assert code == 0
    assert target_env.is_file()

    env_vars = parse_dotenv_file(target_env)
    assert env_vars["N06_PROFILE"] == "benchmark"

    # Không còn placeholder CHANGE_ME_* nào và không giá trị secret nào xuất hiện trên stdout
    printed = out.getvalue()
    for key, val in env_vars.items():
        assert not val.startswith("CHANGE_ME_"), f"Biến {key} vẫn còn CHANGE_ME_"
        if any(token in key for token in ("PASSWORD", "SECRET", "ACCESS_KEY")):
            assert val not in printed, f"Giá trị secret của {key} bị lộ ra stdout!"

    # Kiểm chứng file .env vừa sinh nạp thành công qua load_config của T01
    cfg = load_config(env=env_vars)
    assert cfg.profile == "benchmark"

    # Chạy lại không có --force không được ghi đè secret cũ
    old_mart_pw = env_vars["N06_POSTGRES_MART_PASSWORD"]
    out_second = io.StringIO()
    code_second = run_init_local(
        profile="demo",
        target_path=target_env,
        force=False,
        skip_buckets=True,
        as_json=True,
        out=out_second,
    )
    assert code_second == 0
    second_payload = json.loads(out_second.getvalue())
    assert second_payload["created"] is False
    assert second_payload["overwritten"] is False
    assert parse_dotenv_file(target_env)["N06_POSTGRES_MART_PASSWORD"] == old_mart_pw


def test_doctor_reports_specific_stopped_service_and_redacts_secrets(tmp_path: Path) -> None:
    """Khi 1 service trong Compose bị tắt, `doctor` chỉ đích danh service đó và trả về mã 14."""
    env = _valid_env()
    empty_env_file = tmp_path / "nonexistent.env"
    mock_snap = SystemResourceSnapshot(
        python_major=3,
        python_minor=11,
        python_micro=11,
        cpu_cores=4,
        total_ram_gib=8.0,
        free_disk_gib=25.0,
    )

    fake_compose_states = {
        "kafka": {"state": "running", "health": "healthy", "name": "n06-kafka-1"},
        "minio": {"state": "exited", "health": "", "name": "n06-minio-1"},
        "postgres": {"state": "running", "health": "healthy", "name": "n06-postgres-1"},
        "spark": {"state": "running", "health": "healthy", "name": "n06-spark-1"},
    }

    with (
        patch(
            "n06.cli.doctor.check_docker_engine",
            return_value=(
                evaluate_system_resources(mock_snap)[0].__class__(
                    check_id="docker.engine",
                    category="docker",
                    passed=True,
                    exit_code=0,
                    summary="Docker Engine OK",
                    detail="OK",
                ),
                "/usr/bin/docker",
            ),
        ),
        patch(
            "n06.cli.doctor._get_compose_service_states",
            return_value=fake_compose_states,
        ),
        patch("n06.cli.doctor._probe_tcp", return_value=(True, "TCP OK")),
        patch("n06.cli.doctor._probe_http", return_value=(True, "HTTP OK")),
    ):
        out = io.StringIO()
        code = run_doctor(
            profile="demo",
            env_file=empty_env_file,
            env=env,
            resource_snapshot=mock_snap,
            as_json=True,
            out=out,
        )

    assert code == int(ExitCode.SERVICE_ERROR) == 14
    report_json = json.loads(out.getvalue())
    checks_by_id = {c["check_id"]: c for c in report_json["checks"]}

    # Chỉ duy nhất service.minio bị FAIL, các service còn lại đều PASS
    assert checks_by_id["service.minio"]["passed"] is False
    assert checks_by_id["service.minio"]["exit_code"] == 14
    assert "minio" in checks_by_id["service.minio"]["summary"]
    assert "exited" in checks_by_id["service.minio"]["summary"]

    assert checks_by_id["service.kafka"]["passed"] is True
    assert checks_by_id["service.postgres"]["passed"] is True
    assert checks_by_id["service.spark"]["passed"] is True

    # Không một chuỗi secret nào trong env xuất hiện trong toàn bộ output JSON
    raw_output = out.getvalue()
    for key, val in env.items():
        if "SECRET" in key or "PASSWORD" in key or "ACCESS_KEY" in key:
            assert val not in raw_output


def test_doctor_returns_config_error_code_when_secret_missing(tmp_path: Path) -> None:
    """Khi thiếu biến môi trường bắt buộc, `doctor` trả về mã thoát 10 (CONFIG_ERROR)."""
    bad_env = _valid_env()
    del bad_env["N06_POSTGRES_MART_PASSWORD"]
    mock_snap = SystemResourceSnapshot(3, 11, 11, 4, 8.0, 20.0)

    out = io.StringIO()
    code = run_doctor(
        profile="demo",
        env_file=tmp_path / "empty.env",
        env=bad_env,
        resource_snapshot=mock_snap,
        skip_services=True,
        as_json=False,
        out=out,
    )
    assert code == int(ExitCode.CONFIG_ERROR) == 10
    text = out.getvalue()
    assert "[FAIL] config.schema" in text
    assert "postgres.mart.password" in text
