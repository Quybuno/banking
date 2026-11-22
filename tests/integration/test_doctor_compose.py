"""Integration test cho `n06 doctor` với Docker Compose thật (TASK T03).

Kiểm chứng điều kiện hoàn thành:
1. Khi cả 4 service (kafka, minio, postgres, spark) đang healthy -> `n06 doctor` trả về exit code 0.
2. Khi tắt một service cụ thể trong Compose (ví dụ `minio`) -> `n06 doctor` trả về exit code 14
   (`SERVICE_ERROR`), báo đúng `service.minio` bị lỗi trong khi `service.kafka`, `service.postgres`,
   `service.spark` vẫn đạt (`PASS`).
3. Không lộ bất kỳ secret nào từ `.env` ra stdout.
4. Khởi động lại service vừa tắt trong khối `finally` và xác nhận `n06 doctor` trở lại exit code 0.
"""

from __future__ import annotations

import io
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from n06.cli import ExitCode, main, parse_dotenv_file

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ENV_FILE = REPO_ROOT / ".env"


def test_doctor_detects_stopped_compose_service_and_recovers() -> None:
    """Tắt thử `minio` trong Compose, xác nhận `doctor` báo đúng `service.minio`, rồi bật lại."""
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        pytest.skip("Máy hiện tại không có binary `docker` trong PATH")

    if not ENV_FILE.is_file():
        init_out = io.StringIO()
        assert main(["init-local", "--profile", "demo"], out=init_out) == 0

    env_vars = parse_dotenv_file(ENV_FILE)
    runtime_env = {**os.environ, **env_vars}

    # 1. Đảm bảo toàn bộ 4 service core đang healthy trước khi kiểm tra
    up_res = subprocess.run(
        [docker_bin, "compose", "-f", str(COMPOSE_FILE), "--profile", "core", "up", "-d", "--wait"],
        cwd=REPO_ROOT,
        env=runtime_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert up_res.returncode == 0, f"Không khởi động được Compose core: {up_res.stderr}"

    # 2. Chạy `n06 doctor` khi cả 4 service đang healthy -> exit code = 0
    out_healthy = io.StringIO()
    code_healthy = main(["doctor", "--profile", "demo"], out=out_healthy)
    healthy_text = out_healthy.getvalue()
    assert code_healthy == int(ExitCode.OK) == 0, (
        f"Doctor báo lỗi khi hệ thống healthy:\n{healthy_text}"
    )
    assert "[PASS] service.kafka" in healthy_text
    assert "[PASS] service.minio" in healthy_text
    assert "[PASS] service.postgres" in healthy_text
    assert "[PASS] service.spark" in healthy_text

    # Xác nhận không có bất kỳ giá trị password/secret nào trong .env lọt ra output
    for key, val in env_vars.items():
        if any(token in key for token in ("PASSWORD", "SECRET", "ACCESS_KEY")):
            assert val not in healthy_text, f"Secret của biến {key} bị lộ ra output của doctor!"

    # 3. Tắt thử service `minio` trong Docker Compose
    try:
        stop_res = subprocess.run(
            [docker_bin, "compose", "-f", str(COMPOSE_FILE), "--profile", "core", "stop", "minio"],
            cwd=REPO_ROOT,
            env=runtime_env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert stop_res.returncode == 0, f"Không dừng được minio: {stop_res.stderr}"

        out_stopped = io.StringIO()
        code_stopped = main(["doctor", "--profile", "demo"], out=out_stopped)
        stopped_text = out_stopped.getvalue()

        assert code_stopped == int(ExitCode.SERVICE_ERROR) == 14
        assert "[FAIL] service.minio" in stopped_text
        assert "[PASS] service.kafka" in stopped_text
        assert "[PASS] service.postgres" in stopped_text
        assert "[PASS] service.spark" in stopped_text
    finally:
        # 4. Luôn bật lại `minio` và chờ trạng thái healthy
        recover_res = subprocess.run(
            [
                docker_bin,
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "--profile",
                "core",
                "up",
                "-d",
                "--wait",
                "minio",
            ],
            cwd=REPO_ROOT,
            env=runtime_env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert recover_res.returncode == 0, f"Không khôi phục được minio: {recover_res.stderr}"

    # 5. Xác nhận `n06 doctor` trở lại trạng thái PASS hoàn toàn (exit code = 0)
    out_recovered = io.StringIO()
    code_recovered = main(["doctor", "--profile", "demo"], out=out_recovered)
    assert code_recovered == int(ExitCode.OK) == 0
    assert "[PASS] service.minio" in out_recovered.getvalue()
