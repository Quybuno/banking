"""Phía host: chạy job ingestion trong một container mới của image `spark` (ADR-0012).

Mã, cấu hình, schema hợp đồng và migration được gắn **chỉ đọc** vào container; image không chứa
gói `n06`. Biến môi trường `N06*` được chuyển bằng tên (`-e TÊN`) nên giá trị không nằm trong argv.
"""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from urllib.parse import urlsplit

from n06.config import AppConfig

CONTAINER_ROOT = "/opt/n06"
CONTAINER_SOURCE_DIR = f"{CONTAINER_ROOT}/source-input"
_READ_ONLY_DIRS = ("src", "config", "contracts", "sql")


def _is_loopback(host: str, resolve: Callable[[str], str]) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        pass
    try:
        return ipaddress.ip_address(resolve(host)).is_loopback
    except (OSError, ValueError):  # tên chỉ phân giải được trong mạng Docker
        return False


def host_only_addresses(
    cfg: AppConfig, resolve: Callable[[str], str] = socket.gethostbyname
) -> list[str]:
    """Trường cấu hình đang trỏ về chính máy host (loopback), container không dùng được."""
    candidates = {
        "kafka.bootstrap_servers": cfg.kafka.bootstrap_servers.partition(":")[0],
        "object_store.endpoint": urlsplit(cfg.object_store.endpoint).hostname or "",
        "postgres.control.host": cfg.postgres.control.host,
    }
    return [name for name, host in candidates.items() if host and _is_loopback(host, resolve)]


def job_command(
    cfg: AppConfig,
    *,
    docker_bin: str,
    repo_root: Path,
    job_module: str,
    job_args: Sequence[str],
    env_names: Iterable[str],
    source_dir: Path | None = None,
    event_log_dir: Path | None = None,
) -> list[str]:
    command = [
        docker_bin, "compose", "-f", str(repo_root / "docker-compose.yml"),
        "--profile", "core", "run", "--rm", "--no-deps", "-T",
    ]  # fmt: skip
    for name in _READ_ONLY_DIRS:
        command += ["-v", f"{repo_root / name}:{CONTAINER_ROOT}/{name}:ro"]
    if source_dir is not None:
        # Thư mục `source/` của host, chỉ đọc. Không bao giờ gắn thư mục gốc chứa ground truth.
        command += ["-v", f"{source_dir}:{CONTAINER_SOURCE_DIR}:ro"]
    if event_log_dir is not None:
        command += ["-v", f"{event_log_dir}:{CONTAINER_ROOT}/{cfg.spark.event_log_dir}"]
    command += ["-e", f"PYTHONPATH={CONTAINER_ROOT}/src", "-e", "PYTHONDONTWRITEBYTECODE=1"]
    for name in sorted(env_names):
        command += ["-e", name]
    script = f"{CONTAINER_ROOT}/src/{job_module.replace('.', '/')}.py"
    command += [
        "spark", "/opt/spark/bin/spark-submit",
        "--master", cfg.spark.master,
        "--driver-memory", cfg.spark.driver_memory,
        "--conf", "spark.ui.enabled=false",
        script,
    ]  # fmt: skip
    return command + list(job_args)
