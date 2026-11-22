"""Khung CLI thống nhất `n06` (TASK T03).

Các lệnh hiện thực trong T03:
- `n06 --help`
- `n06 init-local`
- `n06 doctor`
"""

from __future__ import annotations

import argparse
import contextlib
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TextIO

from n06.cli.doctor import (
    CheckItem,
    DoctorReport,
    ExitCode,
    ResourceThresholds,
    SystemResourceSnapshot,
    build_doctor_report,
    collect_system_resources,
    evaluate_system_resources,
    format_doctor_report_text,
    redact_secrets,
    run_doctor,
)
from n06.cli.init_local import (
    InitLocalResult,
    generate_local_env,
    parse_dotenv_file,
    run_init_local,
)

__all__ = [
    "CheckItem",
    "DoctorReport",
    "ExitCode",
    "InitLocalResult",
    "ResourceThresholds",
    "SystemResourceSnapshot",
    "build_doctor_report",
    "build_parser",
    "collect_system_resources",
    "evaluate_system_resources",
    "format_doctor_report_text",
    "generate_local_env",
    "main",
    "parse_dotenv_file",
    "redact_secrets",
    "run_doctor",
    "run_init_local",
]


def build_parser() -> argparse.ArgumentParser:
    """Xây dựng parser dòng lệnh chính của `n06`."""
    epilog = (
        "Bảng mã thoát (Exit Codes):\n"
        "  0  (OK)                : Thành công\n"
        "  2  (CLI_USAGE_ERROR)   : Sai cú pháp hoặc tham số CLI\n"
        "  10 (CONFIG_ERROR)      : Lỗi cấu hình hoặc thiếu biến môi trường bắt buộc\n"
        "  11 (PYTHON_ENV_ERROR)  : Sai phiên bản Python (yêu cầu 3.11.x)\n"
        "  12 (RESOURCE_ERROR)    : Không đủ tài nguyên máy chủ (CPU / RAM / Disk)\n"
        "  13 (DOCKER_ERROR)      : Thiếu Docker CLI, Compose hoặc Docker daemon chưa chạy\n"
        "  14 (SERVICE_ERROR)     : Một hoặc nhiều service (kafka/minio/postgres/spark) lỗi"
    )
    parser = argparse.ArgumentParser(
        prog="n06",
        description="N06 — CLI quản trị và vận hành pipeline dữ liệu lớn ngân hàng mô phỏng.",
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(
        dest="command",
        title="commands",
        metavar="{init-local,doctor}",
    )

    # Lệnh 1: n06 init-local
    init_parser = subparsers.add_parser(
        "init-local",
        help="Sinh file .env cục bộ từ .env.example với secret ngẫu nhiên an toàn.",
        description="Khởi tạo file .env cục bộ từ .env.example mà không làm lộ secret ra màn hình.",
    )
    init_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Ghi đè N06_PROFILE khi sinh file .env (mặc định giữ theo .env.example).",
    )
    init_parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Đường dẫn file .env đích (mặc định: ./.env).",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Ghi đè nếu file .env đã tồn tại.",
    )
    init_parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="In kết quả dạng JSON có cấu trúc.",
    )

    # Lệnh 2: n06 doctor
    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Kiểm tra cấu hình, Python, CPU/RAM/disk, Docker và kết nối từng service.",
        description=(
            "Kiểm tra toàn diện môi trường chạy N06: cấu hình T01, phiên bản Python 3.11, "
            "CPU/RAM/disk, Docker Engine/Compose, và trạng thái kết nối tới Kafka, MinIO, "
            "PostgreSQL, Spark."
        ),
    )
    doctor_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Profile cấu hình cần kiểm tra (default | demo | benchmark | test).",
    )
    doctor_parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Đường dẫn file .env cần nạp trước khi kiểm tra (mặc định: ./.env).",
    )
    doctor_parser.add_argument(
        "--skip-services",
        action="store_true",
        help="Chỉ kiểm tra cấu hình, Python và tài nguyên máy (bỏ qua kiểm tra Docker/service).",
    )
    doctor_parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="In báo cáo kiểm tra dạng JSON có cấu trúc.",
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    """Entrypoint chính của CLI `n06`."""
    stdout = out if out is not None else sys.stdout
    stderr = err if err is not None else sys.stderr
    parser = build_parser()

    args_list = list(argv) if argv is not None else sys.argv[1:]
    if not args_list:
        parser.print_help(file=stdout)
        return int(ExitCode.OK)

    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            parsed = parser.parse_args(args_list)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else int(ExitCode.CLI_USAGE_ERROR)
        return code

    if parsed.command == "init-local":
        try:
            return run_init_local(
                profile=parsed.profile,
                target_path=parsed.env_file,
                force=parsed.force,
                as_json=parsed.as_json,
                out=stdout,
            )
        except FileNotFoundError as exc:
            stderr.write(f"[init-local] Lỗi: {exc}\n")
            return int(ExitCode.CONFIG_ERROR)

    if parsed.command == "doctor":
        return run_doctor(
            profile=parsed.profile,
            env_file=parsed.env_file,
            skip_services=parsed.skip_services,
            as_json=parsed.as_json,
            out=stdout,
        )

    parser.print_help(file=stdout)
    return int(ExitCode.CLI_USAGE_ERROR)
