"""Hiện thực lệnh `n06 doctor` kiểm tra môi trường, tài nguyên và dịch vụ hạ tầng (TASK T03).

Các nguyên tắc bắt buộc:
- Không hardcode hostname, port hay credential trong mã nguồn; toàn bộ địa chỉ lấy từ cấu hình T01
  (`n06.config.load_config`) và biến môi trường `.env`.
- Tách riêng phần đánh giá tài nguyên máy chủ (`evaluate_system_resources`) để unit test truyền
  trực tiếp giá trị giả (`SystemResourceSnapshot`) mà không phụ thuộc máy chạy test.
- Khi tắt một service trong Compose, `doctor` báo chính xác tên service đó (`service.<name>`).
- Tuyệt đối không in giá trị secret/password ra output.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import urlparse

from n06.cli.init_local import parse_dotenv_file
from n06.config import (
    DEFAULT_CONFIG_DIR,
    REPO_ROOT,
    SECRET_FIELD_PATHS,
    AppConfig,
    ConfigValidationError,
    load_config,
)

CORE_SERVICES = ("kafka", "minio", "postgres", "spark")


class ExitCode(IntEnum):
    """Bảng mã thoát chuẩn của CLI `n06` (đặc tả Phần I mục 10, mục 12 M0)."""

    OK = 0
    CLI_USAGE_ERROR = 2
    CONFIG_ERROR = 10
    PYTHON_ENV_ERROR = 11
    RESOURCE_ERROR = 12
    DOCKER_ERROR = 13
    SERVICE_ERROR = 14


@dataclass(frozen=True)
class SystemResourceSnapshot:
    """Ảnh chụp tài nguyên máy chủ và phiên bản Python (có thể truyền giả trong unit test)."""

    python_major: int
    python_minor: int
    python_micro: int
    cpu_cores: int
    total_ram_gib: float
    free_disk_gib: float


@dataclass(frozen=True)
class ResourceThresholds:
    """Ngưỡng tối thiểu yêu cầu cho môi trường chạy cục bộ (cổng M0)."""

    required_python_major: int = 3
    required_python_minor: int = 11
    min_cpu_cores: int = 2
    min_ram_gib: float = 4.0
    min_free_disk_gib: float = 5.0


@dataclass(frozen=True)
class CheckItem:
    """Kết quả kiểm tra của một hạng mục đơn lẻ trong `n06 doctor`."""

    check_id: str
    category: str
    passed: bool
    exit_code: int
    summary: str
    detail: str


@dataclass(frozen=True)
class DoctorReport:
    """Báo cáo tổng hợp của lệnh `n06 doctor`."""

    profile: str
    checks: tuple[CheckItem, ...]
    exit_code: int

    @property
    def passed(self) -> bool:
        return self.exit_code == int(ExitCode.OK)


def _read_total_ram_gib() -> float:
    """Đọc tổng bộ nhớ RAM của hệ thống (GiB) theo cách đa nền tảng chỉ dùng thư viện chuẩn."""
    meminfo = Path("/proc/meminfo")
    if meminfo.is_file():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    kib = int(parts[1])
                    return round(kib / (1024.0 * 1024.0), 2)
    if hasattr(os, "sysconf"):
        try:
            page_size = os.sysconf("SC_PAGE_SIZE")
            phys_pages = os.sysconf("SC_PHYS_PAGES")
            if page_size > 0 and phys_pages > 0:
                return round((page_size * phys_pages) / (1024.0**3), 2)
        except (ValueError, OSError):
            pass
    return 0.0


def collect_system_resources(workspace_path: Path | None = None) -> SystemResourceSnapshot:
    """Thu thập thông số tài nguyên thực tế của máy hiện tại."""
    target_dir = workspace_path if workspace_path is not None else REPO_ROOT
    v = sys.version_info
    cpu_cores = os.cpu_count() or 1
    total_ram_gib = _read_total_ram_gib()
    disk_usage = shutil.disk_usage(target_dir)
    free_disk_gib = round(disk_usage.free / (1024.0**3), 2)
    return SystemResourceSnapshot(
        python_major=v.major,
        python_minor=v.minor,
        python_micro=v.micro,
        cpu_cores=cpu_cores,
        total_ram_gib=total_ram_gib,
        free_disk_gib=free_disk_gib,
    )


def evaluate_system_resources(
    snapshot: SystemResourceSnapshot,
    thresholds: ResourceThresholds | None = None,
) -> list[CheckItem]:
    """Đánh giá độc lập phiên bản Python, CPU, RAM và Disk từ `SystemResourceSnapshot`.

    Hàm thuần túy không chạm OS để unit test kiểm tra mọi biên lỗi bằng dữ liệu giả.
    """
    limits = thresholds if thresholds is not None else ResourceThresholds()
    items: list[CheckItem] = []

    py_ver_str = f"{snapshot.python_major}.{snapshot.python_minor}.{snapshot.python_micro}"
    expected_py = f"{limits.required_python_major}.{limits.required_python_minor}.x"
    py_ok = (
        snapshot.python_major == limits.required_python_major
        and snapshot.python_minor == limits.required_python_minor
    )
    items.append(
        CheckItem(
            check_id="python.version",
            category="python",
            passed=py_ok,
            exit_code=int(ExitCode.OK if py_ok else ExitCode.PYTHON_ENV_ERROR),
            summary=f"Python {py_ver_str} (yêu cầu {expected_py} theo ADR-0001)",
            detail=(
                "Khớp bộ phiên bản đã ghim tại ADR-0001."
                if py_ok
                else f"Phiên bản Python hiện tại là {py_ver_str}, cần dùng Python {expected_py}."
            ),
        )
    )

    cpu_ok = snapshot.cpu_cores >= limits.min_cpu_cores
    items.append(
        CheckItem(
            check_id="resource.cpu",
            category="resource",
            passed=cpu_ok,
            exit_code=int(ExitCode.OK if cpu_ok else ExitCode.RESOURCE_ERROR),
            summary=f"CPU: {snapshot.cpu_cores} cores (tối thiểu >= {limits.min_cpu_cores} cores)",
            detail=(
                f"Đạt yêu cầu CPU ({snapshot.cpu_cores} cores)."
                if cpu_ok
                else (
                    f"Thiếu CPU: máy có {snapshot.cpu_cores} core(s), "
                    f"cần ít nhất {limits.min_cpu_cores} cores."
                )
            ),
        )
    )

    ram_ok = snapshot.total_ram_gib >= limits.min_ram_gib
    items.append(
        CheckItem(
            check_id="resource.ram",
            category="resource",
            passed=ram_ok,
            exit_code=int(ExitCode.OK if ram_ok else ExitCode.RESOURCE_ERROR),
            summary=(
                f"RAM: {snapshot.total_ram_gib:.2f} GiB (tối thiểu >= {limits.min_ram_gib:.2f} GiB)"
            ),
            detail=(
                f"Đạt yêu cầu bộ nhớ RAM ({snapshot.total_ram_gib:.2f} GiB)."
                if ram_ok
                else (
                    f"Thiếu RAM: máy có {snapshot.total_ram_gib:.2f} GiB, "
                    f"cần ít nhất {limits.min_ram_gib:.2f} GiB."
                )
            ),
        )
    )

    disk_ok = snapshot.free_disk_gib >= limits.min_free_disk_gib
    items.append(
        CheckItem(
            check_id="resource.disk",
            category="resource",
            passed=disk_ok,
            exit_code=int(ExitCode.OK if disk_ok else ExitCode.RESOURCE_ERROR),
            summary=(
                f"Disk trống: {snapshot.free_disk_gib:.2f} GiB "
                f"(tối thiểu >= {limits.min_free_disk_gib:.2f} GiB)"
            ),
            detail=(
                f"Đạt yêu cầu dung lượng đĩa trống ({snapshot.free_disk_gib:.2f} GiB)."
                if disk_ok
                else (
                    f"Thiếu dung lượng đĩa trống: còn {snapshot.free_disk_gib:.2f} GiB, "
                    f"cần ít nhất {limits.min_free_disk_gib:.2f} GiB."
                )
            ),
        )
    )

    return items


def _collect_secret_values(env_map: Mapping[str, str]) -> set[str]:
    """Tập hợp các chuỗi giá trị nhạy cảm từ biến môi trường để lọc khỏi mọi thông báo."""
    secrets_set: set[str] = set()
    for key, val in env_map.items():
        clean_val = val.strip()
        if len(clean_val) >= 4 and any(
            token in key.upper() for token in ("PASSWORD", "SECRET", "ACCESS_KEY", "TOKEN")
        ):
            secrets_set.add(clean_val)
    return secrets_set


def redact_secrets(text: str, secret_values: set[str]) -> str:
    """Thay thế mọi chuỗi giá trị secret xuất hiện trong `text` bằng `***REDACTED***`."""
    redacted = text
    for secret in sorted(secret_values, key=len, reverse=True):
        if secret and secret in redacted:
            redacted = redacted.replace(secret, "***REDACTED***")
    return redacted


def check_configuration(
    *,
    profile: str | None,
    merged_env: Mapping[str, str],
    config_dir: Path | None = None,
) -> tuple[CheckItem, AppConfig | None]:
    """Kiểm tra hệ thống cấu hình T01 (`load_config`) và đảm bảo không lộ secret."""
    resolved_dir = config_dir if config_dir is not None else DEFAULT_CONFIG_DIR
    secret_values = _collect_secret_values(merged_env)
    try:
        cfg = load_config(profile=profile, config_dir=resolved_dir, env=merged_env)
    except (ConfigValidationError, FileNotFoundError) as exc:
        safe_msg = redact_secrets(str(exc), secret_values)
        return (
            CheckItem(
                check_id="config.schema",
                category="config",
                passed=False,
                exit_code=int(ExitCode.CONFIG_ERROR),
                summary=f"Cấu hình không hợp lệ: {safe_msg}",
                detail=(
                    f"{safe_msg} (Gợi ý: chạy `n06 init-local` để khởi tạo `.env` hợp lệ "
                    "hoặc kiểm tra file TOML trong `config/`)."
                ),
            ),
            None,
        )

    return (
        CheckItem(
            check_id="config.schema",
            category="config",
            passed=True,
            exit_code=int(ExitCode.OK),
            summary=(
                f"Cấu hình hợp lệ (profile='{cfg.profile}', "
                f"timezone='{cfg.business_timezone}', "
                f"{len(SECRET_FIELD_PATHS)} trường secret đã nạp & ẩn)"
            ),
            detail=(
                "Đã kiểm chứng đủ 4 database/user PostgreSQL riêng biệt "
                "(mart, control, airflow, metabase)."
            ),
        ),
        cfg,
    )


def check_docker_engine() -> tuple[CheckItem, str | None]:
    """Kiểm tra Docker CLI, Docker daemon và plugin `docker compose`."""
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        return (
            CheckItem(
                check_id="docker.engine",
                category="docker",
                passed=False,
                exit_code=int(ExitCode.DOCKER_ERROR),
                summary="Không tìm thấy lệnh `docker` trong PATH",
                detail="Cần cài đặt Docker Engine / Docker Desktop và bật WSL integration.",
            ),
            None,
        )

    compose_res = subprocess.run(
        [docker_bin, "compose", "version"],
        capture_output=True,
        text=True,
        check=False,
    )
    if compose_res.returncode != 0:
        return (
            CheckItem(
                check_id="docker.engine",
                category="docker",
                passed=False,
                exit_code=int(ExitCode.DOCKER_ERROR),
                summary="Plugin `docker compose` không khả dụng",
                detail=compose_res.stderr.strip() or "Không chạy được `docker compose version`.",
            ),
            None,
        )

    info_res = subprocess.run(
        [docker_bin, "info", "--format", "{{.ServerVersion}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if info_res.returncode != 0:
        return (
            CheckItem(
                check_id="docker.engine",
                category="docker",
                passed=False,
                exit_code=int(ExitCode.DOCKER_ERROR),
                summary="Docker daemon chưa chạy hoặc không có quyền truy cập",
                detail=info_res.stderr.strip() or "Lệnh `docker info` thất bại.",
            ),
            None,
        )

    server_ver = info_res.stdout.strip()
    compose_ver = compose_res.stdout.strip()
    return (
        CheckItem(
            check_id="docker.engine",
            category="docker",
            passed=True,
            exit_code=int(ExitCode.OK),
            summary=f"Docker Server {server_ver} ({compose_ver})",
            detail="Docker daemon và Docker Compose sẵn sàng.",
        ),
        docker_bin,
    )


def _get_compose_service_states(
    docker_bin: str,
    compose_file: Path,
    env: Mapping[str, str],
) -> dict[str, dict[str, str]]:
    """Đọc trạng thái (`State`, `Health`) của các container trong `docker compose ps`."""
    res = subprocess.run(
        [
            docker_bin,
            "compose",
            "-f",
            str(compose_file),
            "--profile",
            "core",
            "ps",
            "-a",
            "--format",
            "json",
        ],
        cwd=compose_file.parent,
        env=dict(env),
        capture_output=True,
        text=True,
        check=False,
    )
    states: dict[str, dict[str, str]] = {}
    if res.returncode != 0 or not res.stdout.strip():
        return states

    raw_out = res.stdout.strip()
    entries: list[dict[str, Any]] = []
    if raw_out.startswith("["):
        try:
            entries = json.loads(raw_out)
        except json.JSONDecodeError:
            entries = []
    else:
        for line in raw_out.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    for item in entries:
        svc = str(item.get("Service", "")).strip()
        if svc:
            states[svc] = {
                "state": str(item.get("State", "")).strip().lower(),
                "health": str(item.get("Health", "")).strip().lower(),
                "name": str(item.get("Name", "")).strip(),
            }
    return states


def _probe_tcp(host: str, port: int, timeout_sec: float = 2.0) -> tuple[bool, str]:
    """Kiểm tra kết nối TCP tới `(host, port)`."""
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True, f"Kết nối TCP {host}:{port} thành công"
    except OSError as exc:
        return False, f"Không kết nối được TCP {host}:{port} ({exc})"


def _probe_http(url: str, timeout_sec: float = 2.5) -> tuple[bool, str]:
    """Kiểm tra HTTP GET endpoint trả về mã 2xx."""
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            status = getattr(resp, "status", 200)
            if 200 <= status < 300:
                return True, f"HTTP GET {url} -> {status} OK"
            return False, f"HTTP GET {url} trả về mã {status}"
    except urllib.error.URLError as exc:
        return False, f"Không gọi được HTTP {url} ({exc})"


def _resolve_host_endpoints(
    merged_env: Mapping[str, str],
    config_dir: Path | None = None,
) -> dict[str, tuple[str, int, str]]:
    """Lấy địa chỉ kết nối từ phía host tới 4 service dựa trên cấu hình T01 (`test.toml`) và `.env`.

    Không hardcode host/port trong mã nguồn.
    """
    resolved_dir = config_dir if config_dir is not None else DEFAULT_CONFIG_DIR
    test_cfg = load_config(profile="test", config_dir=resolved_dir, env=merged_env)

    # 1. Kafka bootstrap host & port
    kafka_host_raw, _, kafka_port_raw = test_cfg.kafka.bootstrap_servers.partition(":")
    kafka_port = int(merged_env.get("N06_KAFKA_HOST_PORT", kafka_port_raw))

    # 2. MinIO HTTP endpoint
    parsed_minio = urlparse(test_cfg.object_store.endpoint)
    minio_host = parsed_minio.hostname or kafka_host_raw
    minio_port = int(merged_env.get("N06_MINIO_API_HOST_PORT", str(parsed_minio.port or 0)))
    minio_scheme = parsed_minio.scheme or "http"
    minio_health_url = f"{minio_scheme}://{minio_host}:{minio_port}/minio/health/live"

    # 3. PostgreSQL host & port
    pg_host = test_cfg.postgres.mart.host
    pg_port = int(merged_env.get("N06_POSTGRES_HOST_PORT", str(test_cfg.postgres.mart.port)))

    # 4. Spark Web UI host & port
    spark_host = pg_host
    spark_port = int(merged_env.get("N06_SPARK_UI_HOST_PORT", "18080"))
    spark_ui_url = f"http://{spark_host}:{spark_port}"

    return {
        "kafka": (kafka_host_raw, kafka_port, f"{kafka_host_raw}:{kafka_port}"),
        "minio": (minio_host, minio_port, minio_health_url),
        "postgres": (pg_host, pg_port, f"{pg_host}:{pg_port}"),
        "spark": (spark_host, spark_port, spark_ui_url),
    }


def check_services(
    *,
    docker_bin: str,
    compose_file: Path,
    merged_env: Mapping[str, str],
    config_dir: Path | None = None,
) -> list[CheckItem]:
    """Kiểm tra trạng thái container Compose, healthcheck và kết nối port của từng service."""
    states = _get_compose_service_states(docker_bin, compose_file, merged_env)
    endpoints = _resolve_host_endpoints(merged_env, config_dir=config_dir)
    results: list[CheckItem] = []

    for svc in CORE_SERVICES:
        check_id = f"service.{svc}"
        svc_state = states.get(svc)
        host, port, target_desc = endpoints[svc]

        if svc_state is None:
            results.append(
                CheckItem(
                    check_id=check_id,
                    category="service",
                    passed=False,
                    exit_code=int(ExitCode.SERVICE_ERROR),
                    summary=f"Service '{svc}' chưa được tạo trong Docker Compose",
                    detail=(
                        f"Không tìm thấy container cho service '{svc}'. "
                        "Hãy chạy `docker compose --profile core up -d`."
                    ),
                )
            )
            continue

        state_val = svc_state["state"]
        health_val = svc_state["health"]
        container_name = svc_state["name"] or svc

        if state_val != "running":
            results.append(
                CheckItem(
                    check_id=check_id,
                    category="service",
                    passed=False,
                    exit_code=int(ExitCode.SERVICE_ERROR),
                    summary=(
                        f"Service '{svc}' ({container_name}) đang không chạy "
                        f"(trạng thái='{state_val}')"
                    ),
                    detail=(
                        f"Container '{container_name}' của service '{svc}' đang ở trạng thái "
                        f"'{state_val}'. Hãy bật lại bằng: "
                        f"`docker compose --profile core up -d {svc}`."
                    ),
                )
            )
            continue

        if health_val and health_val != "healthy":
            results.append(
                CheckItem(
                    check_id=check_id,
                    category="service",
                    passed=False,
                    exit_code=int(ExitCode.SERVICE_ERROR),
                    summary=(
                        f"Service '{svc}' ({container_name}) chưa healthy (health='{health_val}')"
                    ),
                    detail=(
                        f"Container '{container_name}' đang chạy nhưng healthcheck ở trạng thái "
                        f"'{health_val}'."
                    ),
                )
            )
            continue

        # Kiểm tra kết nối port / HTTP thực tế từ host tới service đó
        if svc in ("minio", "spark"):
            probe_ok, probe_msg = _probe_http(target_desc)
        else:
            probe_ok, probe_msg = _probe_tcp(host, port)

        if not probe_ok:
            results.append(
                CheckItem(
                    check_id=check_id,
                    category="service",
                    passed=False,
                    exit_code=int(ExitCode.SERVICE_ERROR),
                    summary=f"Service '{svc}' ({container_name}) không phản hồi tại {target_desc}",
                    detail=probe_msg,
                )
            )
            continue

        results.append(
            CheckItem(
                check_id=check_id,
                category="service",
                passed=True,
                exit_code=int(ExitCode.OK),
                summary=(
                    f"Service '{svc}' ({container_name}): state={state_val}, "
                    f"health={health_val or 'ok'}, endpoint={target_desc}"
                ),
                detail=probe_msg,
            )
        )

    return results


def build_doctor_report(
    *,
    profile: str | None = None,
    env_file: Path | None = None,
    compose_file: Path | None = None,
    config_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
    resource_snapshot: SystemResourceSnapshot | None = None,
    thresholds: ResourceThresholds | None = None,
    skip_services: bool = False,
) -> DoctorReport:
    """Thực hiện toàn bộ chuỗi kiểm tra của `n06 doctor` và trả về `DoctorReport`."""
    resolved_env_file = env_file if env_file is not None else (REPO_ROOT / ".env")
    resolved_compose_file = (
        compose_file if compose_file is not None else (REPO_ROOT / "docker-compose.yml")
    )

    merged_env: dict[str, str] = {}
    if resolved_env_file.is_file():
        merged_env.update(parse_dotenv_file(resolved_env_file))
    if env is not None:
        merged_env.update(env)
    else:
        merged_env.update(os.environ)

    secret_values = _collect_secret_values(merged_env)
    checks: list[CheckItem] = []

    # 1. Kiểm tra cấu hình T01
    cfg_check, loaded_cfg = check_configuration(
        profile=profile,
        merged_env=merged_env,
        config_dir=config_dir,
    )
    checks.append(cfg_check)
    effective_profile = loaded_cfg.profile if loaded_cfg is not None else (profile or "default")

    # 2. Kiểm tra Python & Tài nguyên hệ thống (CPU, RAM, Disk)
    snap = (
        resource_snapshot if resource_snapshot is not None else collect_system_resources(REPO_ROOT)
    )
    checks.extend(evaluate_system_resources(snap, thresholds=thresholds))

    # 3. Kiểm tra Docker & các service hạ tầng (trừ khi yêu cầu --skip-services)
    if not skip_services:
        docker_check, docker_bin = check_docker_engine()
        checks.append(docker_check)
        if docker_check.passed and docker_bin is not None and loaded_cfg is not None:
            svc_checks = check_services(
                docker_bin=docker_bin,
                compose_file=resolved_compose_file,
                merged_env=merged_env,
                config_dir=config_dir,
            )
            checks.extend(svc_checks)

    # Đảm bảo không có bất kỳ chuỗi secret nào lọt vào summary/detail của các CheckItem
    sanitized_checks: list[CheckItem] = []
    for item in checks:
        sanitized_checks.append(
            CheckItem(
                check_id=item.check_id,
                category=item.category,
                passed=item.passed,
                exit_code=item.exit_code,
                summary=redact_secrets(item.summary, secret_values),
                detail=redact_secrets(item.detail, secret_values),
            )
        )

    # Xác định mã thoát ưu tiên của lần chạy
    failed_codes = [item.exit_code for item in sanitized_checks if not item.passed]
    final_exit_code = failed_codes[0] if failed_codes else int(ExitCode.OK)

    return DoctorReport(
        profile=effective_profile,
        checks=tuple(sanitized_checks),
        exit_code=final_exit_code,
    )


def format_doctor_report_text(report: DoctorReport) -> str:
    """Định dạng báo cáo `n06 doctor` dễ đọc cho con người, ghi rõ từng mục PASS/FAIL."""
    lines: list[str] = [
        f"=== N06 DOCTOR (profile={report.profile}) ===",
    ]
    passed_count = sum(1 for c in report.checks if c.passed)
    total_count = len(report.checks)

    for item in report.checks:
        badge = "[PASS]" if item.passed else "[FAIL]"
        lines.append(f"{badge} {item.check_id:<18} | {item.summary}")
        if not item.passed:
            lines.append(f"       -> Chi tiết (exit={item.exit_code}): {item.detail}")

    overall = "PASS" if report.passed else "FAIL"
    lines.append(
        f"=== Kết quả: {overall} ({passed_count}/{total_count} hạng mục đạt, "
        f"exit_code={report.exit_code}) ==="
    )
    return "\n".join(lines) + "\n"


def run_doctor(
    *,
    profile: str | None = None,
    env_file: Path | None = None,
    compose_file: Path | None = None,
    config_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
    resource_snapshot: SystemResourceSnapshot | None = None,
    thresholds: ResourceThresholds | None = None,
    skip_services: bool = False,
    as_json: bool = False,
    out: TextIO | None = None,
) -> int:
    """Thực thi lệnh `n06 doctor`, in kết quả ra `out` và trả về mã thoát tương ứng."""
    stream = out if out is not None else sys.stdout
    report = build_doctor_report(
        profile=profile,
        env_file=env_file,
        compose_file=compose_file,
        config_dir=config_dir,
        env=env,
        resource_snapshot=resource_snapshot,
        thresholds=thresholds,
        skip_services=skip_services,
    )

    if as_json:
        payload = {
            "command": "doctor",
            "profile": report.profile,
            "status": "PASS" if report.passed else "FAIL",
            "exit_code": report.exit_code,
            "checks": [
                {
                    "check_id": c.check_id,
                    "category": c.category,
                    "passed": c.passed,
                    "exit_code": c.exit_code,
                    "summary": c.summary,
                    "detail": c.detail,
                }
                for c in report.checks
            ],
        }
        stream.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    else:
        stream.write(format_doctor_report_text(report))

    return report.exit_code
