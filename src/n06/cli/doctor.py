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

import http.client
import json
import os
import re
import shutil
import socket
import struct
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from enum import IntEnum
from pathlib import Path
from typing import Any, TextIO, TypeVar
from urllib.parse import urlparse, urlsplit

from n06.cli.init_local import parse_dotenv_file
from n06.cli.object_store import (
    STATUS_ERROR,
    STATUS_EXISTS,
    STATUS_MISSING,
    BucketResult,
    ObjectStoreConfigError,
    ObjectStoreTarget,
    head_bucket,
    target_from_config,
)
from n06.config import (
    DEFAULT_CONFIG_DIR,
    REPO_ROOT,
    SECRET_FIELD_PATHS,
    AppConfig,
    ConfigValidationError,
    load_config,
)
from n06.limits import (
    BYTES_PER_GB,
    METHOD_SNAPSHOT,
    STATUS_REFUSE,
    STATUS_WARN,
    CapacityVerdict,
    DiskProbe,
    evaluate_capacity,
    resolve_disk_probe,
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
    KAFKA_DELIVERY_ERROR = 15
    CAPACITY_ERROR = 16


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
    warning: bool = False


@dataclass(frozen=True)
class DoctorReport:
    """Báo cáo tổng hợp của lệnh `n06 doctor`."""

    profile: str
    checks: tuple[CheckItem, ...]
    exit_code: int

    @property
    def passed(self) -> bool:
        return self.exit_code == int(ExitCode.OK)

    @property
    def warnings(self) -> int:
        return sum(1 for c in self.checks if c.passed and c.warning)


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
    """Thay thế mọi chuỗi giá trị secret xuất hiện trong `text` bằng `***REDACTED***`.

    Không phân biệt hoa thường: tên host lấy qua `urlparse` bị hạ chữ thường.
    """
    redacted = text
    for secret in sorted(secret_values, key=len, reverse=True):
        if secret:
            redacted = re.sub(re.escape(secret), "***REDACTED***", redacted, flags=re.IGNORECASE)
    return redacted


# Phần `user:mat-khau@` trong một URL bất kỳ, kể cả URL do công cụ ngoài (docker) chép lại.
_URL_USERINFO = re.compile(r"(://)[^/\s'\"]*@")


def _sanitize(text: str, secret_values: set[str]) -> str:
    """Che secret đã biết và mật khẩu nằm trong URL trước khi in."""
    return _URL_USERINFO.sub(r"\1***@", redact_secrets(text, secret_values))


def _describe_config_error(exc: Exception) -> str:
    """Thông báo lỗi cấu hình không kèm giá trị sai: giá trị đó có thể là mật khẩu.

    Bộ lọc `redact_secrets` chỉ biết giá trị của các biến secret, không biết mật khẩu bị đặt nhầm
    vào địa chỉ, vào trường khác hoặc vào tệp TOML.
    """
    if isinstance(exc, ConfigValidationError):
        return f"Lỗi cấu hình tại trường '{exc.field_path}': {exc.reason}"
    return str(exc)


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
        safe_msg = redact_secrets(_describe_config_error(exc), secret_values)
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
    except (OSError, ValueError) as exc:
        # Tên tệp quá dài, không có quyền đọc, số quá dài để phân tích: nội dung lỗi trích lại
        # giá trị cấu hình nên chỉ in tên loại lỗi.
        message = f"Không đọc hoặc phân tích được cấu hình ({type(exc).__name__})"
        return (
            CheckItem(
                check_id="config.schema",
                category="config",
                passed=False,
                exit_code=int(ExitCode.CONFIG_ERROR),
                summary=message,
                detail=f"{message}; kiểm biến N06_* và file TOML trong `config/`.",
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


class ProbeProtocolError(OSError):
    """Dịch vụ nhận kết nối nhưng không trả lời đúng giao thức."""


# Mọi lỗi một probe mạng có thể gây ra: socket và HTTP; tên host không mã hóa được (UnicodeError
# là ValueError); URL hỏng (ValueError); cổng ngoài miền (OverflowError).
_PROBE_ERRORS = (OSError, http.client.HTTPException, ValueError, OverflowError)
# Hạn tổng của một probe. Thời gian chờ socket chỉ áp cho từng thao tác, nên dịch vụ nhỏ giọt
# từng byte có thể giữ probe rất lâu nếu không có hạn tổng.
PROBE_DEADLINE_SECONDS = 8.0
_MAX_ERROR_TEXT = 200
_T = TypeVar("_T")


def _describe_error(exc: BaseException) -> str:
    """Tên loại lỗi và nội dung đã bỏ ký tự điều khiển (nội dung có thể do máy chủ gửi)."""
    text = "".join(ch if ch.isprintable() else "?" for ch in str(exc))[:_MAX_ERROR_TEXT]
    return f"{type(exc).__name__}: {text}"


def _call_with_deadline(function: Callable[[], _T], deadline_sec: float) -> tuple[bool, _T | None]:
    """Chạy `function` trong luồng nền; trả `(False, None)` nếu quá `deadline_sec`.

    Ngoại lệ của `function` được ném lại ở luồng gọi. Luồng quá hạn là daemon và bị bỏ lại.
    """
    values: list[_T] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            values.append(function())
        except BaseException as exc:  # chuyển nguyên về luồng gọi
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(deadline_sec)
    if thread.is_alive():
        return False, None
    if errors:
        raise errors[0]
    return True, values[0]


# PostgreSQL `SSLRequest` (độ dài 8, mã 80877103): máy chủ trả đúng một byte `S` hoặc `N`.
_POSTGRES_SSL_REQUEST = struct.pack("!ii", 8, 80877103)
# Kafka `ApiVersions` (api_key 18) phiên bản 0: phản hồi lặp lại correlation_id rồi mã lỗi.
_KAFKA_API_VERSIONS_KEY = 18
_KAFKA_CORRELATION_ID = 0x4E3036
_KAFKA_CLIENT_ID = b"n06-doctor"


def _recv_exactly(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ProbeProtocolError("dịch vụ đóng kết nối trước khi trả lời đủ")
        data += chunk
    return data


def _postgres_handshake(sock: socket.socket) -> None:
    sock.sendall(_POSTGRES_SSL_REQUEST)
    # Máy chủ PostgreSQL trả đúng MỘT byte. Đọc dư để dịch vụ khác trả chuỗi bắt đầu bằng `S`
    # (ví dụ SSH) không được coi là PostgreSQL.
    reply = sock.recv(64)
    if reply not in (b"S", b"N"):
        raise ProbeProtocolError("phản hồi không phải của PostgreSQL")


def _kafka_handshake(sock: socket.socket) -> None:
    body = (
        struct.pack(
            "!hhih", _KAFKA_API_VERSIONS_KEY, 0, _KAFKA_CORRELATION_ID, len(_KAFKA_CLIENT_ID)
        )
        + _KAFKA_CLIENT_ID
    )
    sock.sendall(struct.pack("!i", len(body)) + body)
    size, correlation_id, error_code = struct.unpack("!iih", _recv_exactly(sock, 10))
    if correlation_id != _KAFKA_CORRELATION_ID or size < 6:
        raise ProbeProtocolError("phản hồi không phải của Kafka broker")
    if error_code != 0:
        raise ProbeProtocolError(f"Kafka trả mã lỗi {error_code} cho ApiVersions")


def _probe_tcp(
    host: str,
    port: int,
    timeout_sec: float = 2.0,
    exchange: Callable[[socket.socket], None] | None = None,
    deadline_sec: float = PROBE_DEADLINE_SECONDS,
) -> tuple[bool, str]:
    """Kết nối TCP tới `(host, port)` rồi, nếu có `exchange`, bắt tay giao thức của dịch vụ.

    Không ném lỗi mạng: từ chối kết nối, quá thời gian (`TimeoutError` là `OSError`), ngắt kết
    nối, phản hồi sai, tên host hay cổng không hợp lệ đều trả `(False, lý do)`.
    """
    where = f"{host}:{port}"
    if not 1 <= port <= 65535:
        # `socket` quấn vòng cổng lớn hơn 65535 (84628 thành 19092) nên phải chặn trước.
        return False, f"Cổng ngoài miền 1-65535: {where}"

    def attempt() -> tuple[bool, str]:
        try:
            with socket.create_connection((host, port), timeout=timeout_sec) as sock:
                if exchange is not None:
                    exchange(sock)
        except _PROBE_ERRORS as exc:
            return False, f"Dịch vụ tại {where} không phản hồi đúng ({_describe_error(exc)})"
        suffix = ", dịch vụ trả lời đúng giao thức" if exchange is not None else ""
        return True, f"Kết nối TCP {where} thành công{suffix}"

    finished, outcome = _call_with_deadline(attempt, deadline_sec)
    if not finished or outcome is None:
        return False, f"Dịch vụ tại {where} không phản hồi đúng (quá hạn tổng {deadline_sec:g} s)"
    return outcome


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Probe sức khỏe không đi theo chuyển hướng: mã 3xx là lỗi của dịch vụ."""

    def http_error_302(self, *args: Any, **kwargs: Any) -> None:
        # Chặn trước khi `urllib` phân tích header `Location` (địa chỉ hỏng sẽ ném `ValueError`);
        # trả `None` để bộ xử lý mặc định ném `HTTPError` mang mã 3xx.
        return None

    http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302


# Probe gọi thẳng tới dịch vụ: bỏ qua `http_proxy` của môi trường (proxy làm kết quả FAIL hoặc
# PASS sai, và địa chỉ proxy hỏng có thể đưa mật khẩu proxy vào thông báo lỗi).
_PROBE_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())


def _probe_http(
    url: str, timeout_sec: float = 2.5, deadline_sec: float = PROBE_DEADLINE_SECONDS
) -> tuple[bool, str]:
    """Kiểm tra HTTP GET endpoint trả về mã 2xx; không ném lỗi mạng.

    `URLError` chỉ bọc lỗi lúc kết nối. Quá thời gian khi chờ phản hồi (`TimeoutError`), ngắt kết
    nối và phản hồi rác (`http.client.HTTPException`) thoát ra trực tiếp nên phải bắt riêng.
    """

    def attempt() -> tuple[bool, str]:
        try:
            # `http.client` không chặn cổng lớn hơn 65535 (bị quấn vòng); thuộc tính `port`
            # của `urlsplit` ném `ValueError` cho cổng ngoài miền.
            _ = urlsplit(url).port
            req = urllib.request.Request(url, method="GET")
            with _PROBE_OPENER.open(req, timeout=timeout_sec) as resp:
                status = getattr(resp, "status", 200)
        except urllib.error.HTTPError as exc:
            return False, f"HTTP GET {url} trả về mã {exc.code}"
        except _PROBE_ERRORS as exc:
            return False, f"Không gọi được HTTP {url} ({_describe_error(exc)})"
        if 200 <= status < 300:
            return True, f"HTTP GET {url} -> {status} OK"
        return False, f"HTTP GET {url} trả về mã {status}"

    finished, outcome = _call_with_deadline(attempt, deadline_sec)
    if not finished or outcome is None:
        return False, f"Không gọi được HTTP {url} (quá hạn tổng {deadline_sec:g} s)"
    return outcome


# Tên máy (gồm tên service Compose có `_`) hoặc địa chỉ IPv6 không ngoặc vuông.
_HOST_PATTERN = re.compile(r"[A-Za-z0-9._-]{1,253}|[0-9A-Fa-f]*:[0-9A-Fa-f:.]+")


def _resolve_host_endpoints(
    merged_env: Mapping[str, str],
    config_dir: Path | None = None,
) -> dict[str, tuple[str, int, str]]:
    """Lấy địa chỉ kết nối từ phía host tới 4 service dựa trên cấu hình T01 (`test.toml`) và `.env`.

    Không hardcode host/port trong mã nguồn.
    """
    resolved_dir = config_dir if config_dir is not None else DEFAULT_CONFIG_DIR
    test_cfg = load_config(profile="test", config_dir=resolved_dir, env=merged_env)

    # Mọi lỗi ở đây chỉ nêu tên trường hoặc biến, không in giá trị (có thể chứa mật khẩu).
    for field_path, address in (
        ("kafka.bootstrap_servers", test_cfg.kafka.bootstrap_servers),
        ("object_store.endpoint", test_cfg.object_store.endpoint),
        ("postgres.mart.host", test_cfg.postgres.mart.host),
    ):
        if "@" in address:
            raise ValueError(
                f"{field_path} (profile test) không được chứa '@': credential chỉ lấy từ biến "
                "môi trường, không đặt trong địa chỉ"
            )
    # Tên đích được in ra, nên host phải đúng là tên máy hoặc địa chỉ IP: chuỗi kết nối kiểu
    # `host=... password=...` bị từ chối trước khi in.
    for field_path, host in (
        ("kafka.bootstrap_servers", test_cfg.kafka.bootstrap_servers.partition(":")[0]),
        ("postgres.mart.host", test_cfg.postgres.mart.host),
    ):
        if not _HOST_PATTERN.fullmatch(host):
            raise ValueError(
                f"{field_path} (profile test) không phải tên máy hoặc địa chỉ IP hợp lệ"
            )

    def host_port(variable: str, default: str, default_field: str) -> int:
        # Biến rỗng nghĩa là dùng mặc định, như Compose `${VAR:-mặc định}`.
        from_variable = merged_env.get(variable, "").strip()
        try:
            port = int(from_variable or default)
        except ValueError:
            port = 0
        if not 1 <= port <= 65535:
            source = variable
            if not from_variable:
                source = (
                    f"{variable} để trống nên dùng cổng của {default_field} (profile test); cổng đó"
                )
            raise ValueError(f"{source} phải là số nguyên từ 1 đến 65535")
        return port

    # 1. Kafka bootstrap host & port
    kafka_host_raw, _, kafka_port_raw = test_cfg.kafka.bootstrap_servers.partition(":")
    kafka_port = host_port("N06_KAFKA_HOST_PORT", kafka_port_raw, "kafka.bootstrap_servers")

    # 2. MinIO HTTP endpoint
    try:
        parsed_minio = urlparse(test_cfg.object_store.endpoint)
        endpoint_port = parsed_minio.port or 0
    except ValueError:
        # Không chép nội dung lỗi của `urlparse`: nó trích lại một phần endpoint.
        raise ValueError(
            "object_store.endpoint (profile test) không hợp lệ: không đọc được host hoặc cổng"
        ) from None
    minio_host = parsed_minio.hostname or kafka_host_raw
    minio_port = host_port("N06_MINIO_API_HOST_PORT", str(endpoint_port), "object_store.endpoint")
    minio_scheme = parsed_minio.scheme or "http"
    minio_health_url = f"{minio_scheme}://{minio_host}:{minio_port}/minio/health/live"

    # 3. PostgreSQL host & port
    pg_host = test_cfg.postgres.mart.host
    pg_port = host_port(
        "N06_POSTGRES_HOST_PORT", str(test_cfg.postgres.mart.port), "postgres.mart.port"
    )

    # 4. Spark Web UI host & port
    spark_host = pg_host
    spark_port = host_port("N06_SPARK_UI_HOST_PORT", "18080", "cổng Spark UI mặc định")
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
    try:
        endpoints = _resolve_host_endpoints(merged_env, config_dir=config_dir)
    except (ValueError, OverflowError) as exc:  # ví dụ N06_*_HOST_PORT không phải số
        return [
            CheckItem(
                check_id=f"service.{svc}",
                category="service",
                passed=False,
                exit_code=int(ExitCode.CONFIG_ERROR),
                summary=f"Không suy được địa chỉ của service '{svc}' từ cấu hình",
                detail=(
                    _describe_config_error(exc)
                    if isinstance(exc, ConfigValidationError)
                    else _describe_error(exc)
                ),
            )
            for svc in CORE_SERVICES
        ]
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
            handshake = _kafka_handshake if svc == "kafka" else _postgres_handshake
            probe_ok, probe_msg = _probe_tcp(host, port, exchange=handshake)

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


def check_object_store_buckets(
    *,
    cfg: AppConfig,
    merged_env: Mapping[str, str],
    config_dir: Path | None = None,
    prober: Callable[[ObjectStoreTarget, str], BucketResult] | None = None,
    deadline_sec: float = PROBE_DEADLINE_SECONDS,
) -> list[CheckItem]:
    """Kiểm bucket object store của profile đang dùng (`object_store.bucket`) có tồn tại không.

    Báo đúng tên bucket thiếu; không in secret (lỗi chỉ gồm mã HTTP và mã lỗi S3).
    """
    bucket = cfg.object_store.bucket
    check_id = f"bucket.{bucket}"
    probe = prober if prober is not None else head_bucket
    try:
        target = target_from_config(cfg, merged_env, config_dir=config_dir)
        finished, probed = _call_with_deadline(lambda: probe(target, bucket), deadline_sec)
        result = (
            probed
            if finished and probed is not None
            else BucketResult(
                bucket,
                STATUS_ERROR,
                f"object store không trả lời trong hạn tổng {deadline_sec:g} s",
            )
        )
    except ObjectStoreConfigError as exc:
        return [
            CheckItem(
                check_id=check_id,
                category="bucket",
                passed=False,
                exit_code=int(ExitCode.CONFIG_ERROR),
                summary=f"Cấu hình bucket '{bucket}' không hợp lệ",
                detail=str(exc),
            )
        ]

    if result.status == STATUS_EXISTS:
        return [
            CheckItem(
                check_id=check_id,
                category="bucket",
                passed=True,
                exit_code=int(ExitCode.OK),
                summary=f"Bucket '{bucket}' đã tồn tại (profile={cfg.profile})",
                detail=result.detail,
            )
        ]
    if result.status == STATUS_MISSING:
        return [
            CheckItem(
                check_id=check_id,
                category="bucket",
                passed=False,
                exit_code=int(ExitCode.SERVICE_ERROR),
                summary=f"Bucket '{bucket}' chưa tồn tại (profile={cfg.profile})",
                detail=(
                    f"Bucket '{bucket}' do cấu hình của profile '{cfg.profile}' định nghĩa "
                    f"chưa được tạo. Chạy `n06 init-local --profile {cfg.profile}` "
                    "(an toàn khi chạy lại)."
                ),
            )
        ]
    return [
        CheckItem(
            check_id=check_id,
            category="bucket",
            passed=False,
            exit_code=int(ExitCode.SERVICE_ERROR),
            summary=(
                f"Không kiểm được bucket '{bucket}' trên object store MinIO (profile={cfg.profile})"
            ),
            detail=result.detail,
        )
    ]


def check_capacity(
    *,
    cfg: AppConfig,
    probe: DiskProbe,
    requested_transactions: int | None = None,
) -> CheckItem:
    """Hạng mục `capacity.limits`: chỗ trống thật, reserve, `max_transactions` đang áp dụng.

    REFUSE (vượt trần, thiếu chỗ, không xác định được ổ) là FAIL mã 16 kèm cách xử lý.
    WARN (phần pipeline chưa đo) vẫn đạt nhưng được đánh dấu cảnh báo, không phải PASS đầy đủ.
    """
    limits = cfg.limits
    if requested_transactions is not None and requested_transactions < 1:
        return CheckItem(
            check_id="capacity.limits",
            category="capacity",
            passed=False,
            exit_code=int(ExitCode.CONFIG_ERROR),
            summary=f"Số giao dịch yêu cầu không hợp lệ: {requested_transactions}",
            detail="Số giao dịch phải là số nguyên >= 1 (--transactions hoặc cấu hình).",
        )
    requested = (
        requested_transactions
        if requested_transactions is not None
        else cfg.dataset.transaction_count
    )
    verdict: CapacityVerdict = evaluate_capacity(requested, limits, probe)
    free_text = (
        f"{verdict.free_bytes / BYTES_PER_GB:.2f} GB" if verdict.free_bytes is not None else "?"
    )
    cap_text = (
        f"{limits.max_transactions:,}" if limits.max_transactions is not None else "không đặt"
    )
    where = f", {verdict.probe_path}" if verdict.probe_path else ""
    summary = (
        f"Chỗ trống {free_text} (đọc theo {verdict.probe_method}{where}); "
        f"reserve {limits.min_free_disk_gb_reserve} GB; max_transactions: {cap_text}; "
        f"yêu cầu {requested:,} giao dịch [{verdict.status}]"
    )
    refused = verdict.status == STATUS_REFUSE
    return CheckItem(
        check_id="capacity.limits",
        category="capacity",
        passed=not refused,
        exit_code=int(ExitCode.CAPACITY_ERROR if refused else ExitCode.OK),
        summary=summary,
        detail=verdict.message,
        warning=verdict.status == STATUS_WARN,
    )


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
    bucket_prober: Callable[[ObjectStoreTarget, str], BucketResult] | None = None,
    requested_transactions: int | None = None,
    disk_probe: DiskProbe | None = None,
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
    # Chỗ trống đọc ở đúng ổ chứa dữ liệu (trong WSL là ổ Windows chứa đĩa ảo, không phải `df`
    # của đĩa ảo). Snapshot được truyền vào (unit test) thì coi đó là chỗ trống và không đọc máy.
    probe: DiskProbe | None = None
    if loaded_cfg is not None:
        if disk_probe is not None:
            probe = disk_probe
        elif resource_snapshot is not None:
            probe = DiskProbe(
                free_bytes=int(snap.free_disk_gib * 1024**3), path=None, method=METHOD_SNAPSHOT
            )
        else:
            probe = resolve_disk_probe(
                loaded_cfg.limits,
                REPO_ROOT,
                target_path=Path(loaded_cfg.paths.generator_output_dir),
            )
        if resource_snapshot is None and probe.free_bytes is not None:
            snap = replace(snap, free_disk_gib=round(probe.free_bytes / (1024.0**3), 2))
    resource_items = evaluate_system_resources(snap, thresholds=thresholds)
    if resource_snapshot is None and probe is not None and probe.free_bytes is None:
        # Không đọc được ổ thật: không báo số `df` của đĩa ảo (sai); lý do nằm ở capacity.limits.
        resource_items = [
            CheckItem(
                check_id=item.check_id,
                category=item.category,
                passed=True,
                exit_code=int(ExitCode.OK),
                summary="Disk trống: không xác định được ổ thật (xem capacity.limits)",
                detail=(
                    "Không dùng `df` của thư mục dữ liệu: "
                    "trong WSL đó là đĩa ảo, không phải ổ thật."
                ),
                warning=True,
            )
            if item.check_id == "resource.disk"
            else item
            for item in resource_items
        ]
    checks.extend(resource_items)
    if loaded_cfg is not None and probe is not None:
        checks.append(
            check_capacity(
                cfg=loaded_cfg, probe=probe, requested_transactions=requested_transactions
            )
        )

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
            # 4. Bucket của profile: chỉ kiểm khi MinIO đã ổn (nếu không thì lỗi đã được báo ở trên)
            if any(c.check_id == "service.minio" and c.passed for c in svc_checks):
                checks.extend(
                    check_object_store_buckets(
                        cfg=loaded_cfg,
                        merged_env=merged_env,
                        config_dir=config_dir,
                        prober=bucket_prober,
                    )
                )

    # Đảm bảo không có bất kỳ chuỗi secret nào lọt vào summary/detail của các CheckItem
    sanitized_checks: list[CheckItem] = []
    for item in checks:
        sanitized_checks.append(
            CheckItem(
                check_id=_sanitize(item.check_id, secret_values),
                category=item.category,
                passed=item.passed,
                exit_code=item.exit_code,
                summary=_sanitize(item.summary, secret_values),
                detail=_sanitize(item.detail, secret_values),
                warning=item.warning,
            )
        )

    # Xác định mã thoát ưu tiên của lần chạy
    # `capacity.limits` hiện sớm nhưng xếp hạng cuối: lỗi của nó đến từ guard dung lượng, không được
    # che mã lỗi hạ tầng thật (13, 14) khi đọc ổ chập chờn.
    failed = [item for item in sanitized_checks if not item.passed]
    ranked = [i for i in failed if i.check_id != "capacity.limits"] + [
        i for i in failed if i.check_id == "capacity.limits"
    ]
    final_exit_code = ranked[0].exit_code if ranked else int(ExitCode.OK)

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
        if item.passed and item.warning:
            badge = "[WARN]"
        lines.append(f"{badge} {item.check_id:<18} | {item.summary}")
        if not item.passed:
            lines.append(f"       -> Chi tiết (exit={item.exit_code}): {item.detail}")
        elif item.warning:
            lines.append(f"       -> {item.detail}")

    overall = "PASS" if report.passed else "FAIL"
    if report.passed and report.warnings:
        overall = f"PASS (có {report.warnings} cảnh báo, không phải PASS đầy đủ)"
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
    requested_transactions: int | None = None,
    disk_probe: DiskProbe | None = None,
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
        requested_transactions=requested_transactions,
        disk_probe=disk_probe,
    )

    if as_json:
        payload = {
            "command": "doctor",
            "profile": report.profile,
            "status": (
                "FAIL"
                if not report.passed
                else ("PASS_WITH_WARNINGS" if report.warnings else "PASS")
            ),
            "exit_code": report.exit_code,
            "checks": [
                {
                    "check_id": c.check_id,
                    "category": c.category,
                    "passed": c.passed,
                    "warning": c.warning,
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
