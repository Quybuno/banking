"""Kiểm tra dung lượng trước khi sinh dữ liệu hoặc chạy benchmark (CFG-scale-guard).

Mục tiêu: từ chối chạy kèm lý do cụ thể khi ổ đĩa thật không đủ chỗ, thay vì làm đầy ổ.
Mọi con số (reserve, byte mỗi giao dịch, trần số giao dịch) lấy từ `config/` mục `[limits]`
(`n06.config.LimitsConfig`); không có quy mô hay dung lượng nào nằm cố định trong mã này.

Đơn vị: GB = 10^9 byte, KB = 10^3 byte.

Chỗ trống phải đọc ở ĐÚNG ổ chứa dữ liệu. Trong WSL2 thư mục dữ liệu nằm trên đĩa ảo `.vhdx`
(`df` báo kích thước tối đa của đĩa ảo, ví dụ 1 TB), trong khi chỗ thật là chỗ trống của ổ Windows
chứa file `.vhdx`. Vì vậy trong WSL, chỗ trống được đọc từ ổ Windows đó. Thứ tự xác định ổ:

1. `limits.disk_probe_path` nếu có;
2. trong WSL: đọc `BasePath` của distro hiện tại qua registry của Windows (`powershell.exe`),
   suy ra ổ, đổi sang đường dẫn Linux bằng `wslpath`;
3. ngoài WSL: thư mục dữ liệu.

Không xác định được thì từ chối chạy (không đoán).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from n06.config import LimitsConfig

BYTES_PER_GB = 10**9

STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_REFUSE = "REFUSE"

REASON_MAX_TRANSACTIONS = "max_transactions"
REASON_DISK_UNKNOWN = "disk_unknown"
REASON_INSUFFICIENT_DISK = "insufficient_disk"
REASON_PIPELINE_UNMEASURED = "pipeline_unmeasured"

METHOD_CONFIG = "limits.disk_probe_path"
METHOD_WSL = "wsl-registry"
METHOD_WORKSPACE = "workspace"
METHOD_TARGET_DRIVE = "wsl-target-drive"  # đích ghi nằm trực tiếp dưới /mnt/<ổ>
METHOD_SNAPSHOT = "snapshot"  # chỗ trống lấy từ ảnh chụp tài nguyên được truyền vào (kiểm thử)

COMMAND_TIMEOUT_SECONDS = 10.0
POWERSHELL_ATTEMPTS = 2  # powershell.exe qua interop đôi khi lỗi tạm; thử lại một lần

# Liệt kê `DistributionName|BasePath` của mọi distro WSL của người dùng hiện tại.
_LIST_DISTROS_SCRIPT = (
    r"Get-ChildItem HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss | "
    r'ForEach-Object { $p = Get-ItemProperty $_.PSPath; "$($p.DistributionName)|$($p.BasePath)" }'
)
_MNT_DRIVE_PATTERN = re.compile(r"^/mnt/([a-z])(?:/|$)")
_DRIVE_PATTERN = re.compile(r"^(?:\\\\\?\\)?([A-Za-z]):[\\/]")

DiskFree = Callable[[Path], int]
CommandRunner = Callable[[Sequence[str]], str]


class DiskProbeError(RuntimeError):
    """Không xác định được chỗ trống của ổ chứa dữ liệu."""


@dataclass(frozen=True)
class DiskProbe:
    """Kết quả đọc chỗ trống: `free_bytes` là `None` khi không xác định được (có `error`)."""

    free_bytes: int | None
    path: str | None
    method: str
    error: str | None = None


@dataclass(frozen=True)
class CapacityVerdict:
    """Kết luận một lần kiểm dung lượng; `message` nêu số cần, số có, reserve và nguyên nhân."""

    status: str
    reason: str | None
    requested_transactions: int
    max_transactions: int | None
    free_bytes: int | None
    reserve_bytes: int
    data_bytes: int
    required_bytes: int
    generator_bytes_per_tx: int
    pipeline_bytes_per_tx: int | None
    probe_path: str | None
    probe_method: str
    message: str

    @property
    def refused(self) -> bool:
        return self.status == STATUS_REFUSE

    @property
    def pipeline_measured(self) -> bool:
        return self.pipeline_bytes_per_tx is not None


def _gb(value: int) -> str:
    # Decimal: số giao dịch rất lớn không được làm tràn float khi định dạng.
    return f"{Decimal(value) / Decimal(BYTES_PER_GB):.2f} GB"


# --- Đọc chỗ trống ở đúng ổ --------------------------------------------------------------------


def _default_disk_free(path: Path) -> int:
    return shutil.disk_usage(path).free


def _default_run_command(argv: Sequence[str]) -> str:
    completed = subprocess.run(  # noqa: S603 - lệnh cố định, không có đầu vào của người dùng
        list(argv),
        capture_output=True,
        stdin=subprocess.DEVNULL,
        timeout=COMMAND_TIMEOUT_SECONDS,
        check=True,
    )
    return completed.stdout.decode("utf-8", errors="replace")


def _read_osrelease() -> str:
    path = Path("/proc/sys/kernel/osrelease")
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def is_wsl(env: Mapping[str, str], osrelease: str) -> bool:
    """Đang chạy trong WSL: có `WSL_DISTRO_NAME` hoặc nhân Linux ghi `microsoft`."""
    return bool(env.get("WSL_DISTRO_NAME")) or "microsoft" in osrelease.lower()


def select_distro_base_path(listing: str, distro: str) -> str | None:
    """Chọn `BasePath` của `distro` trong đầu ra dạng `Tên|Đường dẫn` (mỗi dòng một distro)."""
    for line in listing.splitlines():
        name, separator, base_path = line.strip().partition("|")
        if separator and name == distro and base_path.strip():
            return base_path.strip()
    return None


def drive_letter_of(windows_path: str) -> str | None:
    """Ký tự ổ của đường dẫn Windows, ví dụ `D:\\WSL\\Ubuntu` hoặc `\\\\?\\D:\\WSL` cho `d`."""
    match = _DRIVE_PATTERN.match(windows_path.strip())
    return match.group(1).lower() if match else None


def _wsl_windows_drive_path(env: Mapping[str, str], run_command: CommandRunner) -> tuple[str, str]:
    """Trả `(đường dẫn Linux của ổ Windows chứa đĩa ảo, mô tả)`; thất bại thì ném lỗi."""
    distro = env.get("WSL_DISTRO_NAME", "").strip()
    if not distro:
        raise DiskProbeError(
            "đang trong WSL nhưng thiếu biến môi trường WSL_DISTRO_NAME nên không biết distro nào"
        )
    listing = ""
    for attempt in range(POWERSHELL_ATTEMPTS):
        try:
            listing = run_command(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    _LIST_DISTROS_SCRIPT,
                ]
            )
            break
        except (OSError, subprocess.SubprocessError) as exc:
            if attempt == POWERSHELL_ATTEMPTS - 1:
                raise DiskProbeError(
                    "không chạy được powershell.exe để đọc vị trí đĩa ảo WSL "
                    f"({type(exc).__name__}, đã thử {POWERSHELL_ATTEMPTS} lần)"
                ) from exc
    base_path = select_distro_base_path(listing, distro)
    if base_path is None:
        raise DiskProbeError(f"registry của Windows không có BasePath cho distro '{distro}'")
    letter = drive_letter_of(base_path)
    if letter is None:
        raise DiskProbeError(f"BasePath của distro '{distro}' không bắt đầu bằng ký tự ổ đĩa")
    try:
        mount = run_command(["wslpath", "-u", f"{letter.upper()}:\\"]).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise DiskProbeError(
            f"wslpath không đổi được ổ {letter.upper()}: sang Linux ({type(exc).__name__})"
        ) from exc
    if not mount:
        raise DiskProbeError(f"wslpath trả về rỗng cho ổ {letter.upper()}:")
    return mount, f"ổ {letter.upper()}: chứa đĩa ảo của distro '{distro}'"


def resolve_disk_probe(
    limits: LimitsConfig,
    workspace: Path,
    *,
    env: Mapping[str, str] | None = None,
    disk_free: DiskFree | None = None,
    run_command: CommandRunner | None = None,
    osrelease: str | None = None,
    target_path: Path | None = None,
) -> DiskProbe:
    """Đọc chỗ trống ở đúng ổ chứa dữ liệu (xem thứ tự ở docstring module).

    `target_path` là nơi sắp ghi dữ liệu. Trong WSL, nếu nó nằm trực tiếp dưới `/mnt/<ổ>` thì dữ
    liệu ghi lên chính ổ Windows đó (không qua đĩa ảo), nên chỗ trống đọc ở `/mnt/<ổ>`.

    Không bao giờ ném ngoại lệ vì không xác định được ổ: trả `DiskProbe(free_bytes=None, ...)`
    kèm `error` để người gọi từ chối chạy.
    """
    resolved_env = env if env is not None else os.environ
    free_of = disk_free if disk_free is not None else _default_disk_free
    runner = run_command if run_command is not None else _default_run_command

    def read(path: Path, method: str) -> DiskProbe:
        try:
            return DiskProbe(free_bytes=int(free_of(path)), path=str(path), method=method)
        except OSError as exc:
            return DiskProbe(None, str(path), method, f"không đọc được chỗ trống ({exc})")

    if limits.disk_probe_path:
        return read(Path(limits.disk_probe_path), METHOD_CONFIG)

    release = osrelease if osrelease is not None else _read_osrelease()
    if is_wsl(resolved_env, release):
        if target_path is not None:
            on_drive = _MNT_DRIVE_PATTERN.match(target_path.resolve().as_posix())
            if on_drive:
                return read(Path(f"/mnt/{on_drive.group(1)}"), METHOD_TARGET_DRIVE)
        try:
            mount, _ = _wsl_windows_drive_path(resolved_env, runner)
        except DiskProbeError as exc:
            return DiskProbe(None, None, METHOD_WSL, str(exc))
        return read(Path(mount), METHOD_WSL)

    return read(workspace, METHOD_WORKSPACE)


# --- Đánh giá (hàm thuần) ----------------------------------------------------------------------


def evaluate_capacity(
    requested_transactions: int, limits: LimitsConfig, probe: DiskProbe
) -> CapacityVerdict:
    """Ước lượng cần = số giao dịch × byte mỗi giao dịch đã biết + reserve; so với chỗ trống.

    Thứ tự: vượt `max_transactions`, không xác định được chỗ trống, thiếu chỗ (đều là REFUSE);
    đủ chỗ nhưng `pipeline_bytes_per_tx` chưa đặt là WARN (không phải PASS đầy đủ).
    """
    if requested_transactions < 1:
        raise ValueError("requested_transactions phải >= 1")
    per_tx = limits.generator_bytes_per_tx + (limits.pipeline_bytes_per_tx or 0)
    data_bytes = requested_transactions * per_tx
    reserve_bytes = limits.min_free_disk_gb_reserve * BYTES_PER_GB
    required_bytes = data_bytes + reserve_bytes
    free = probe.free_bytes

    figures = (
        f"cần {_gb(required_bytes)} ({requested_transactions:,} giao dịch × {per_tx:,} B = "
        f"{_gb(data_bytes)} dữ liệu + reserve {_gb(reserve_bytes)}), "
        f"có {_gb(free) if free is not None else 'không xác định'}"
    )

    reason: str | None
    if limits.max_transactions is not None and requested_transactions > limits.max_transactions:
        status, reason = STATUS_REFUSE, REASON_MAX_TRANSACTIONS
        message = (
            f"Từ chối: yêu cầu {requested_transactions:,} giao dịch vượt trần "
            f"limits.max_transactions = {limits.max_transactions:,}; {figures}. "
            "Giảm số giao dịch, hoặc tăng N06__LIMITS__MAX_TRANSACTIONS nếu máy đủ dung lượng."
        )
    elif free is None:
        status, reason = STATUS_REFUSE, REASON_DISK_UNKNOWN
        message = (
            f"Từ chối: không xác định được chỗ trống ổ chứa dữ liệu ({probe.error}); {figures}. "
            "Đặt N06__LIMITS__DISK_PROBE_PATH trỏ tới thư mục trên ổ Windows chứa đĩa ảo WSL "
            "(ví dụ thư mục gốc của ổ đó dưới /mnt)."
        )
    elif required_bytes > free:
        status, reason = STATUS_REFUSE, REASON_INSUFFICIENT_DISK
        message = (
            f"Từ chối: không đủ chỗ trống; {figures}; thiếu {_gb(required_bytes - free)}. "
            "Giảm số giao dịch, giải phóng dung lượng, hoặc chạy trên ổ lớn hơn."
        )
    elif limits.pipeline_bytes_per_tx is None:
        status, reason = STATUS_WARN, REASON_PIPELINE_UNMEASURED
        message = (
            "Cảnh báo: phần pipeline chưa đo (limits.pipeline_bytes_per_tx chưa đặt), nên ước "
            "lượng chỉ gồm dữ liệu generator và không phải PASS đầy đủ; "
            f"{figures}. "
            "Kafka, MinIO và Postgres sẽ ghi thêm ngoài mức này."
        )
    else:
        status, reason = STATUS_PASS, None
        message = f"Đủ chỗ trống; {figures}."

    return CapacityVerdict(
        status=status,
        reason=reason,
        requested_transactions=requested_transactions,
        max_transactions=limits.max_transactions,
        free_bytes=free,
        reserve_bytes=reserve_bytes,
        data_bytes=data_bytes,
        required_bytes=required_bytes,
        generator_bytes_per_tx=limits.generator_bytes_per_tx,
        pipeline_bytes_per_tx=limits.pipeline_bytes_per_tx,
        probe_path=probe.path,
        probe_method=probe.method,
        message=message,
    )


def ensure_capacity(
    requested_transactions: int,
    limits: LimitsConfig,
    workspace: Path,
    *,
    env: Mapping[str, str] | None = None,
    disk_free: DiskFree | None = None,
    run_command: CommandRunner | None = None,
    osrelease: str | None = None,
    probe: DiskProbe | None = None,
    target_path: Path | None = None,
) -> CapacityVerdict:
    """Đọc chỗ trống (hoặc dùng `probe` đã có) rồi đánh giá. Người gọi quyết định từ chối khi
    `verdict.refused`; hàm này không tạo hay xóa file nào."""
    resolved = (
        probe
        if probe is not None
        else resolve_disk_probe(
            limits,
            workspace,
            env=env,
            disk_free=disk_free,
            run_command=run_command,
            osrelease=osrelease,
            target_path=target_path,
        )
    )
    return evaluate_capacity(requested_transactions, limits, resolved)
