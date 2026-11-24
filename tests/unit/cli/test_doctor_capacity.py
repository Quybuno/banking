"""Test hạng mục `capacity.limits` của `n06 doctor` (CFG-scale-guard).

`resource_snapshot` và `DiskProbe` được tiêm nên kết quả không phụ thuộc máy chạy test.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from n06.cli import ExitCode, SystemResourceSnapshot, main, run_doctor
from n06.cli.doctor import build_doctor_report, format_doctor_report_text
from n06.limits import METHOD_WSL, DiskProbe

GB = 10**9
GIB = 1024**3
CREDENTIALS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-test",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-test",
    "N06_POSTGRES_MART_PASSWORD": "pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
    "N06_POSTGRES_METABASE_PASSWORD": "pw",
}
SNAPSHOT = SystemResourceSnapshot(3, 11, 9, 8, 16.0, 500.0)


def _probe(free_gb: float | None) -> DiskProbe:
    if free_gb is None:
        return DiskProbe(None, None, METHOD_WSL, "registry không có distro")
    return DiskProbe(int(free_gb * GB), "/mnt/d", METHOD_WSL)


def _report(tmp_path: Path, probe: DiskProbe, env: dict[str, str] | None = None, **kwargs: object):
    return build_doctor_report(
        profile="demo",
        env_file=tmp_path / "none.env",
        env={**CREDENTIALS, **(env or {})},
        resource_snapshot=SNAPSHOT,
        skip_services=True,
        disk_probe=probe,
        **kwargs,  # type: ignore[arg-type]
    )


def _capacity(report):  # type: ignore[no-untyped-def]
    (item,) = [c for c in report.checks if c.check_id == "capacity.limits"]
    return item


def test_doctor_shows_free_space_reserve_and_max_transactions(tmp_path: Path) -> None:
    report = _report(tmp_path, _probe(46.8), {"N06__LIMITS__MAX_TRANSACTIONS": "1000000"})
    item = _capacity(report)
    for expected in ("46.80 GB", "wsl-registry", "/mnt/d", "reserve 8 GB", "1,000,000"):
        assert expected in item.summary


def test_doctor_without_max_transactions_says_not_set(tmp_path: Path) -> None:
    assert "max_transactions: không đặt" in _capacity(_report(tmp_path, _probe(46.8))).summary


def test_unmeasured_pipeline_is_a_warning_and_not_a_full_pass(tmp_path: Path) -> None:
    report = _report(tmp_path, _probe(46.8))
    item = _capacity(report)
    assert item.passed and item.warning and report.passed and report.warnings == 1
    text = format_doctor_report_text(report)
    assert "[WARN] capacity.limits" in text
    assert "phần pipeline chưa đo" in text
    assert "không phải PASS đầy đủ" in text


def test_measured_pipeline_gives_a_plain_pass(tmp_path: Path) -> None:
    report = _report(tmp_path, _probe(46.8), {"N06__LIMITS__PIPELINE_BYTES_PER_TX": "1000"})
    assert not _capacity(report).warning and report.warnings == 0
    assert "[PASS] capacity.limits" in format_doctor_report_text(report)


def test_insufficient_disk_fails_with_exit_16_and_remedy(tmp_path: Path) -> None:
    # Cấu hình demo: 10.000 giao dịch = 0,04 GB + 8 GB reserve = 8,04 GB cần; chỉ có 5 GB.
    report = _report(tmp_path, _probe(5.0))
    item = _capacity(report)
    assert not item.passed and item.exit_code == int(ExitCode.CAPACITY_ERROR) == 16
    assert report.exit_code == 16
    for expected in ("cần 8.04 GB", "có 5.00 GB", "reserve 8.00 GB", "thiếu 3.04 GB"):
        assert expected in item.detail
    assert "Giảm số giao dịch" in item.detail


def test_unknown_disk_fails_with_instruction(tmp_path: Path) -> None:
    item = _capacity(_report(tmp_path, _probe(None)))
    assert not item.passed and item.exit_code == 16
    assert "N06__LIMITS__DISK_PROBE_PATH" in item.detail


def test_requested_transactions_override_is_used(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        _probe(900.0),
        {"N06__LIMITS__MAX_TRANSACTIONS": "100"},
        requested_transactions=101,
    )
    item = _capacity(report)
    assert item.exit_code == 16 and "101" in item.summary


def test_resource_disk_uses_the_real_drive_not_the_data_directory(tmp_path: Path) -> None:
    """Không truyền snapshot: `resource.disk` phải theo chỗ trống của ổ thật (2 GiB), không phải
    `df` của thư mục dữ liệu."""
    report = build_doctor_report(
        profile="demo",
        env_file=tmp_path / "none.env",
        env=CREDENTIALS,
        skip_services=True,
        disk_probe=DiskProbe(2 * GIB, "/mnt/d", METHOD_WSL),
    )
    (disk,) = [c for c in report.checks if c.check_id == "resource.disk"]
    assert not disk.passed and "2.00 GiB" in disk.summary
    assert disk.exit_code == int(ExitCode.RESOURCE_ERROR)


def test_json_report_marks_the_warning_and_status(tmp_path: Path) -> None:
    out = io.StringIO()
    code = run_doctor(
        profile="demo",
        env_file=tmp_path / "none.env",
        env=CREDENTIALS,
        resource_snapshot=SNAPSHOT,
        skip_services=True,
        disk_probe=_probe(46.8),
        as_json=True,
        out=out,
    )
    payload = json.loads(out.getvalue())
    assert code == 0 and payload["status"] == "PASS_WITH_WARNINGS"
    (item,) = [c for c in payload["checks"] if c["check_id"] == "capacity.limits"]
    assert item["warning"] is True and item["passed"] is True


def test_main_doctor_transactions_flag_refuses_over_the_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key, value in CREDENTIALS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("N06__LIMITS__MAX_TRANSACTIONS", "5")
    monkeypatch.setenv("N06__LIMITS__DISK_PROBE_PATH", str(tmp_path))
    out = io.StringIO()
    code = main(
        [
            "doctor",
            "--profile",
            "demo",
            "--env-file",
            str(tmp_path / "none.env"),
            "--skip-services",
            "--transactions",
            "6",
        ],
        out=out,
        err=io.StringIO(),
    )
    assert code == 16
    assert (
        "[FAIL] capacity.limits" in out.getvalue() and "limits.max_transactions" in out.getvalue()
    )


# --- Bổ sung sau review độc lập ----------------------------------------------------------------


def _services_with_minio_stopped():  # type: ignore[no-untyped-def]
    from unittest.mock import patch

    from n06.cli.doctor import CheckItem

    states = {
        "kafka": {"state": "running", "health": "healthy", "name": "n06-kafka-1"},
        "minio": {"state": "exited", "health": "", "name": "n06-minio-1"},
        "postgres": {"state": "running", "health": "healthy", "name": "n06-postgres-1"},
        "spark": {"state": "running", "health": "healthy", "name": "n06-spark-1"},
    }
    docker_ok = CheckItem("docker.engine", "docker", True, 0, "Docker OK", "OK")
    return (
        patch("n06.cli.doctor.check_docker_engine", return_value=(docker_ok, "/usr/bin/docker")),
        patch("n06.cli.doctor._get_compose_service_states", return_value=states),
        patch("n06.cli.doctor._probe_tcp", return_value=(True, "TCP OK")),
        patch("n06.cli.doctor._probe_http", return_value=(True, "HTTP OK")),
    )


@pytest.mark.parametrize("probe", [_probe(None), _probe(1.0)])
def test_service_failure_keeps_exit_14_even_when_capacity_also_fails(
    tmp_path: Path, probe: DiskProbe
) -> None:
    """Lỗi đọc ổ chập chờn hoặc thiếu chỗ không được che mã lỗi hạ tầng thật."""
    p1, p2, p3, p4 = _services_with_minio_stopped()
    with p1, p2, p3, p4:
        report = build_doctor_report(
            profile="demo",
            env_file=tmp_path / "none.env",
            env=CREDENTIALS,
            resource_snapshot=SNAPSHOT,
            disk_probe=probe,
        )
    by_id = {c.check_id: c for c in report.checks}
    assert not by_id["capacity.limits"].passed and by_id["capacity.limits"].exit_code == 16
    assert not by_id["service.minio"].passed
    assert report.exit_code == int(ExitCode.SERVICE_ERROR) == 14


def test_capacity_failure_alone_still_exits_16(tmp_path: Path) -> None:
    assert _report(tmp_path, _probe(None)).exit_code == 16


def test_unknown_drive_does_not_show_the_virtual_disk_size_as_free_space(tmp_path: Path) -> None:
    report = build_doctor_report(
        profile="demo",
        env_file=tmp_path / "none.env",
        env=CREDENTIALS,
        skip_services=True,
        disk_probe=_probe(None),
    )
    (disk,) = [c for c in report.checks if c.check_id == "resource.disk"]
    assert disk.passed and disk.warning and "GiB" not in disk.summary
    assert "không xác định được ổ thật" in disk.summary


@pytest.mark.parametrize("bad", ["0", "-5", "abc", "1.5"])
def test_doctor_transactions_must_be_a_positive_integer(bad: str) -> None:
    code = main(
        ["doctor", "--skip-services", "--transactions", bad], out=io.StringIO(), err=io.StringIO()
    )
    assert code == 2


def test_huge_transactions_fail_cleanly_with_exit_16(tmp_path: Path) -> None:
    report = _report(tmp_path, _probe(46.8), requested_transactions=10**400)
    assert _capacity(report).exit_code == 16 and report.exit_code == 16


@pytest.mark.parametrize("bad", [0, -5])
def test_calling_doctor_as_a_function_with_non_positive_transactions_fails_cleanly(
    tmp_path: Path, bad: int
) -> None:
    report = _report(tmp_path, _probe(46.8), requested_transactions=bad)
    item = _capacity(report)
    assert not item.passed and item.exit_code == int(ExitCode.CONFIG_ERROR) == 10
    assert "không hợp lệ" in item.summary and ">= 1" in item.detail
