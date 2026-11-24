"""Test `n06.limits` (CFG-scale-guard) với `disk_usage` và lệnh hệ thống được tiêm.

Expected viết tay (số học trực tiếp), không gọi lại hàm đang test để tạo expected.
GB = 10^9 byte. Cấu hình mẫu: dự trữ 8 GB, generator 4.000 B/giao dịch.
"""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest

from n06.config import LimitsConfig
from n06.limits import (
    METHOD_CONFIG,
    METHOD_WORKSPACE,
    METHOD_WSL,
    REASON_DISK_UNKNOWN,
    REASON_INSUFFICIENT_DISK,
    REASON_MAX_TRANSACTIONS,
    REASON_PIPELINE_UNMEASURED,
    STATUS_PASS,
    STATUS_REFUSE,
    STATUS_WARN,
    DiskProbe,
    drive_letter_of,
    ensure_capacity,
    evaluate_capacity,
    is_wsl,
    resolve_disk_probe,
    select_distro_base_path,
)

GB = 10**9


def _limits(**overrides: object) -> LimitsConfig:
    values: dict[str, object] = {
        "max_transactions": None,
        "min_free_disk_gb_reserve": 8,
        "generator_bytes_per_tx": 4000,
        "pipeline_bytes_per_tx": None,
        "disk_probe_path": None,
    }
    values.update(overrides)
    return LimitsConfig(**values)  # type: ignore[arg-type]


def _probe(free_gb: float | None, error: str | None = None) -> DiskProbe:
    free = None if free_gb is None else int(free_gb * GB)
    return DiskProbe(free_bytes=free, path="/mnt/x", method=METHOD_WSL, error=error)


# --- evaluate_capacity -------------------------------------------------------------------------


def test_enough_space_but_pipeline_unmeasured_is_warn_not_pass() -> None:
    # 1.000.000 × 4.000 B = 4 GB dữ liệu + 8 GB reserve = 12 GB cần; có 46,8 GB.
    verdict = evaluate_capacity(1_000_000, _limits(), _probe(46.8))
    assert verdict.status == STATUS_WARN and verdict.reason == REASON_PIPELINE_UNMEASURED
    assert not verdict.refused and not verdict.pipeline_measured
    assert (verdict.data_bytes, verdict.reserve_bytes, verdict.required_bytes) == (
        4 * GB,
        8 * GB,
        12 * GB,
    )
    assert "phần pipeline chưa đo" in verdict.message
    assert "không phải PASS đầy đủ" in verdict.message
    assert "12.00 GB" in verdict.message and "46.80 GB" in verdict.message


def test_enough_space_with_pipeline_measured_is_full_pass() -> None:
    # (4.000 + 1.000) × 1.000.000 = 5 GB + 8 GB reserve = 13 GB.
    verdict = evaluate_capacity(1_000_000, _limits(pipeline_bytes_per_tx=1000), _probe(46.8))
    assert verdict.status == STATUS_PASS and verdict.reason is None
    assert verdict.pipeline_measured
    assert (verdict.data_bytes, verdict.required_bytes) == (5 * GB, 13 * GB)
    assert "13.00 GB" in verdict.message


def test_insufficient_space_is_refused_with_all_figures() -> None:
    # Cần 12 GB, có 10 GB: thiếu 2 GB.
    verdict = evaluate_capacity(1_000_000, _limits(), _probe(10.0))
    assert verdict.refused and verdict.reason == REASON_INSUFFICIENT_DISK
    for text in ("cần 12.00 GB", "có 10.00 GB", "reserve 8.00 GB", "thiếu 2.00 GB"):
        assert text in verdict.message


def test_required_equal_to_free_is_allowed_and_one_byte_more_is_refused() -> None:
    exact = DiskProbe(12 * GB, "/mnt/x", METHOD_WSL)
    assert not evaluate_capacity(1_000_000, _limits(), exact).refused
    one_short = DiskProbe(12 * GB - 1, "/mnt/x", METHOD_WSL)
    assert evaluate_capacity(1_000_000, _limits(), one_short).refused


def test_pipeline_bytes_are_part_of_the_estimate_when_known() -> None:
    # Không có pipeline: cần 12 GB, có 12,5 GB thì đủ; có pipeline 1.000 B thì cần 13 GB thì thiếu.
    probe = DiskProbe(int(12.5 * GB), "/mnt/x", METHOD_WSL)
    assert not evaluate_capacity(1_000_000, _limits(), probe).refused
    assert evaluate_capacity(1_000_000, _limits(pipeline_bytes_per_tx=1000), probe).refused


def test_over_max_transactions_is_refused_even_when_disk_is_plentiful() -> None:
    limits = _limits(max_transactions=1000)
    refused = evaluate_capacity(1001, limits, _probe(900.0))
    assert refused.refused and refused.reason == REASON_MAX_TRANSACTIONS
    assert "1,001" in refused.message and "1,000" in refused.message
    assert "limits.max_transactions" in refused.message
    assert not evaluate_capacity(1000, limits, _probe(900.0)).refused  # bằng trần thì được


def test_max_transactions_check_comes_before_unknown_disk() -> None:
    verdict = evaluate_capacity(5, _limits(max_transactions=1), _probe(None, "không rõ"))
    assert verdict.reason == REASON_MAX_TRANSACTIONS


def test_unknown_free_space_is_refused_never_guessed() -> None:
    verdict = evaluate_capacity(10, _limits(), _probe(None, "registry không có distro"))
    assert verdict.refused and verdict.reason == REASON_DISK_UNKNOWN and verdict.free_bytes is None
    assert "registry không có distro" in verdict.message
    assert "N06__LIMITS__DISK_PROBE_PATH" in verdict.message
    assert "không xác định" in verdict.message


def test_zero_reserve_means_only_data_counts() -> None:
    verdict = evaluate_capacity(
        1_000_000, _limits(min_free_disk_gb_reserve=0, pipeline_bytes_per_tx=1000), _probe(5.0)
    )
    assert verdict.status == STATUS_PASS and verdict.required_bytes == 5 * GB


# --- Bộ phân tích thuần ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "letter"),
    [
        ("D:\\WSL\\Ubuntu2004", "d"),
        ("\\\\?\\D:\\WSL\\DockerDesktopWSL\\main", "d"),
        ("e:/wsl/x", "e"),
        ("\\\\server\\share\\x", None),
        ("relative\\path", None),
        ("", None),
    ],
)
def test_drive_letter_of(path: str, letter: str | None) -> None:
    assert drive_letter_of(path) == letter


def test_select_distro_base_path_matches_exact_name_only() -> None:
    listing = "Ubuntu-20.04|D:\\WSL\\Ubuntu2004\r\ndocker-desktop|\\\\?\\D:\\x\r\nUbuntu|C:\\u\r\n"
    assert select_distro_base_path(listing, "Ubuntu-20.04") == "D:\\WSL\\Ubuntu2004"
    assert select_distro_base_path(listing, "Ubuntu") == "C:\\u"
    assert select_distro_base_path(listing, "Debian") is None
    assert select_distro_base_path("Ubuntu-20.04|\r\n", "Ubuntu-20.04") is None


def test_is_wsl() -> None:
    assert is_wsl({"WSL_DISTRO_NAME": "Ubuntu"}, "")
    assert is_wsl({}, "6.6.0-microsoft-standard-WSL2")
    assert not is_wsl({}, "6.1.0-generic")


# --- resolve_disk_probe: đọc ĐÚNG ổ ------------------------------------------------------------


class _FakeSystem:
    """Giả lập `powershell.exe`, `wslpath` và `disk_usage`; ghi lại mọi lời gọi."""

    def __init__(self, listing: str, mount: str = "/mnt/d", free: int = 44 * GB) -> None:
        self.listing, self.mount, self.free = listing, mount, free
        self.commands: list[list[str]] = []
        self.disk_paths: list[Path] = []

    def run_command(self, argv: Sequence[str]) -> str:
        self.commands.append(list(argv))
        return self.listing if argv[0] == "powershell.exe" else self.mount + "\n"

    def disk_free(self, path: Path) -> int:
        self.disk_paths.append(path)
        return self.free


WSL_ENV = {"WSL_DISTRO_NAME": "Ubuntu-20.04"}
LISTING = "Ubuntu-20.04|D:\\WSL\\Ubuntu2004\r\ndocker-desktop|\\\\?\\D:\\WSL\\Docker\r\n"


def test_in_wsl_free_space_comes_from_the_windows_drive_not_the_data_directory(
    tmp_path: Path,
) -> None:
    fake = _FakeSystem(LISTING, mount="/mnt/d", free=44 * GB)
    probe = resolve_disk_probe(
        _limits(),
        tmp_path,
        env=WSL_ENV,
        disk_free=fake.disk_free,
        run_command=fake.run_command,
        osrelease="6.6.0-microsoft-standard-WSL2",
    )
    assert (probe.free_bytes, probe.method, probe.path) == (44 * GB, METHOD_WSL, "/mnt/d")
    assert fake.disk_paths == [Path("/mnt/d")]  # không bao giờ đọc thư mục dữ liệu (đĩa ảo)
    assert fake.commands[1] == ["wslpath", "-u", "D:\\"]


def test_in_wsl_uses_the_drive_letter_of_the_distro_base_path() -> None:
    fake = _FakeSystem("Ubuntu-20.04|E:\\wsl\\u\r\n", mount="/mnt/e")
    probe = resolve_disk_probe(
        _limits(), Path("."), env=WSL_ENV, disk_free=fake.disk_free,
        run_command=fake.run_command, osrelease="microsoft",
    )  # fmt: skip
    assert probe.path == "/mnt/e" and fake.commands[1] == ["wslpath", "-u", "E:\\"]


def test_explicit_probe_path_wins_and_runs_no_command(tmp_path: Path) -> None:
    fake = _FakeSystem(LISTING)

    def forbidden(argv: Sequence[str]) -> str:
        raise AssertionError(f"không được chạy lệnh nào: {argv}")

    probe = resolve_disk_probe(
        _limits(disk_probe_path="/mnt/z"),
        tmp_path,
        env=WSL_ENV,
        disk_free=fake.disk_free,
        run_command=forbidden,
        osrelease="microsoft",
    )
    assert (probe.method, probe.path, probe.free_bytes) == (METHOD_CONFIG, "/mnt/z", 44 * GB)
    assert fake.disk_paths == [Path("/mnt/z")]


def test_distro_missing_in_registry_gives_unknown_not_a_guess(tmp_path: Path) -> None:
    fake = _FakeSystem("Debian|D:\\x\r\n")
    probe = resolve_disk_probe(
        _limits(), tmp_path, env=WSL_ENV, disk_free=fake.disk_free,
        run_command=fake.run_command, osrelease="microsoft",
    )  # fmt: skip
    assert probe.free_bytes is None and "Ubuntu-20.04" in (probe.error or "")
    assert fake.disk_paths == []  # không đọc ổ nào khi chưa xác định được


def test_powershell_failure_gives_unknown(tmp_path: Path) -> None:
    def broken(argv: Sequence[str]) -> str:
        raise FileNotFoundError("powershell.exe")

    probe = resolve_disk_probe(
        _limits(), tmp_path, env=WSL_ENV, disk_free=lambda p: 1, run_command=broken,
        osrelease="microsoft",
    )  # fmt: skip
    assert probe.free_bytes is None and "powershell.exe" in (probe.error or "")


def test_called_process_error_gives_unknown(tmp_path: Path) -> None:
    def failing(argv: Sequence[str]) -> str:
        raise subprocess.CalledProcessError(1, list(argv))

    probe = resolve_disk_probe(
        _limits(), tmp_path, env=WSL_ENV, disk_free=lambda p: 1, run_command=failing,
        osrelease="microsoft",
    )  # fmt: skip
    assert probe.free_bytes is None


def test_wsl_without_distro_name_gives_unknown(tmp_path: Path) -> None:
    probe = resolve_disk_probe(
        _limits(), tmp_path, env={}, disk_free=lambda p: 1, run_command=lambda a: "",
        osrelease="6.6.0-microsoft-standard-WSL2",
    )  # fmt: skip
    assert probe.free_bytes is None and "WSL_DISTRO_NAME" in (probe.error or "")


def test_outside_wsl_the_data_directory_is_read(tmp_path: Path) -> None:
    fake = _FakeSystem("", free=77 * GB)
    probe = resolve_disk_probe(
        _limits(), tmp_path, env={}, disk_free=fake.disk_free, run_command=fake.run_command,
        osrelease="6.1.0-generic",
    )  # fmt: skip
    assert (probe.method, probe.free_bytes) == (METHOD_WORKSPACE, 77 * GB)
    assert fake.disk_paths == [tmp_path] and fake.commands == []


def test_disk_usage_oserror_gives_unknown(tmp_path: Path) -> None:
    def boom(path: Path) -> int:
        raise PermissionError("denied")

    probe = resolve_disk_probe(
        _limits(), tmp_path, env={}, disk_free=boom, run_command=lambda a: "", osrelease=""
    )
    assert probe.free_bytes is None and "denied" in (probe.error or "")


def test_ensure_capacity_uses_an_injected_probe_and_touches_no_disk(tmp_path: Path) -> None:
    verdict = ensure_capacity(1_000_000, _limits(), tmp_path / "does-not-exist", probe=_probe(46.8))
    assert verdict.status == STATUS_WARN
    assert not (tmp_path / "does-not-exist").exists()


def test_refuse_constant_is_exposed() -> None:
    assert STATUS_REFUSE == "REFUSE"


# --- Bổ sung sau review độc lập ----------------------------------------------------------------


def test_astronomically_large_request_is_refused_without_overflow() -> None:
    verdict = evaluate_capacity(10**400, _limits(), _probe(46.8))
    assert verdict.refused and verdict.reason == REASON_INSUFFICIENT_DISK
    assert "GB" in verdict.message and "thiếu" in verdict.message


@pytest.mark.parametrize("bad", [0, -1, -5])
def test_non_positive_request_is_a_programming_error(bad: int) -> None:
    with pytest.raises(ValueError, match=">= 1"):
        evaluate_capacity(bad, _limits(), _probe(46.8))


def test_powershell_is_retried_once_after_a_transient_failure(tmp_path: Path) -> None:
    calls: list[str] = []

    def flaky(argv: Sequence[str]) -> str:
        calls.append(argv[0])
        if argv[0] == "powershell.exe" and calls.count("powershell.exe") == 1:
            raise subprocess.TimeoutExpired(list(argv), 10)
        return LISTING if argv[0] == "powershell.exe" else "/mnt/d\n"

    probe = resolve_disk_probe(
        _limits(), tmp_path, env=WSL_ENV, disk_free=lambda p: 5 * GB, run_command=flaky,
        osrelease="microsoft",
    )  # fmt: skip
    assert probe.free_bytes == 5 * GB and calls.count("powershell.exe") == 2


def test_powershell_failing_every_attempt_is_unknown_and_says_how_many_tries(
    tmp_path: Path,
) -> None:
    def always_times_out(argv: Sequence[str]) -> str:
        raise subprocess.TimeoutExpired(list(argv), 10)

    probe = resolve_disk_probe(
        _limits(), tmp_path, env=WSL_ENV, disk_free=lambda p: 1, run_command=always_times_out,
        osrelease="microsoft",
    )  # fmt: skip
    assert probe.free_bytes is None
    assert "TimeoutExpired" in (probe.error or "") and "2 lần" in (probe.error or "")


def test_wslpath_timeout_or_empty_answer_is_unknown(tmp_path: Path) -> None:
    def wslpath_hangs(argv: Sequence[str]) -> str:
        if argv[0] == "wslpath":
            raise subprocess.TimeoutExpired(list(argv), 10)
        return LISTING

    hangs = resolve_disk_probe(
        _limits(), tmp_path, env=WSL_ENV, disk_free=lambda p: 1, run_command=wslpath_hangs,
        osrelease="microsoft",
    )  # fmt: skip
    assert hangs.free_bytes is None and "wslpath" in (hangs.error or "")
    empty = _FakeSystem(LISTING, mount="")
    blank = resolve_disk_probe(
        _limits(), tmp_path, env=WSL_ENV, disk_free=empty.disk_free, run_command=empty.run_command,
        osrelease="microsoft",
    )  # fmt: skip
    assert blank.free_bytes is None and "rỗng" in (blank.error or "")


@pytest.mark.parametrize(
    ("target", "expected_mount"),
    [
        ("/mnt/c/data/generated", "/mnt/c"),
        ("/mnt/e", "/mnt/e"),
        ("/mnt/d/x/y/z", "/mnt/d"),
    ],
)
def test_in_wsl_target_under_a_windows_drive_reads_that_drive(
    tmp_path: Path, target: str, expected_mount: str
) -> None:
    fake = _FakeSystem(LISTING, mount="/mnt/d")
    probe = resolve_disk_probe(
        _limits(), tmp_path, env=WSL_ENV, disk_free=fake.disk_free, run_command=fake.run_command,
        osrelease="microsoft", target_path=Path(target),
    )  # fmt: skip
    assert (probe.method, probe.path) == ("wsl-target-drive", expected_mount)
    assert fake.disk_paths == [Path(expected_mount)] and fake.commands == []


@pytest.mark.parametrize("target", ["/mnt/wsl/x", "/mnt/cc/x", "/home/user/data", "relative/dir"])
def test_in_wsl_target_elsewhere_reads_the_drive_holding_the_virtual_disk(
    tmp_path: Path, target: str
) -> None:
    fake = _FakeSystem(LISTING, mount="/mnt/d")
    probe = resolve_disk_probe(
        _limits(), tmp_path, env=WSL_ENV, disk_free=fake.disk_free, run_command=fake.run_command,
        osrelease="microsoft", target_path=Path(target),
    )  # fmt: skip
    assert probe.method == METHOD_WSL and probe.path == "/mnt/d"


def test_default_runner_really_runs_commands_without_a_stdin() -> None:
    from n06.limits import _default_run_command

    assert _default_run_command(["echo", "xin chào"]) == "xin chào\n"
    assert _default_run_command(["cat"]) == ""  # stdin là DEVNULL nên không bị treo chờ đầu vào
    with pytest.raises(subprocess.CalledProcessError):
        _default_run_command(["false"])
    with pytest.raises(FileNotFoundError):
        _default_run_command(["lenh-khong-ton-tai-n06"])
