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
from n06.cli.publish import run_publish
from n06.cli.validate import parse_as_of_time_utc, run_validate

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
    "parse_as_of_time_utc",
    "parse_dotenv_file",
    "redact_secrets",
    "run_doctor",
    "run_init_local",
    "run_publish",
    "run_validate",
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
        "\n  15 (KAFKA_DELIVERY_ERROR): Lỗi giao nhận Kafka khi publish (không retry được)"
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
        metavar="{init-local,doctor,validate,publish}",
    )

    # Lệnh 1: n06 init-local
    init_parser = subparsers.add_parser(
        "init-local",
        help="Sinh file .env cục bộ và tạo bucket object store theo cấu hình profile.",
        description=(
            "Khởi tạo file .env cục bộ từ .env.example mà không làm lộ secret ra màn hình, rồi tạo "
            "bucket object store của profile (an toàn khi chạy lại; MinIO chưa chạy thì cảnh báo)."
        ),
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
    bucket_group = init_parser.add_mutually_exclusive_group()
    bucket_group.add_argument(
        "--skip-buckets",
        action="store_true",
        help="Chỉ sinh .env, không tạo bucket object store.",
    )
    bucket_group.add_argument(
        "--require-buckets",
        action="store_true",
        help="Thoát mã 14 nếu bucket chưa sẵn sàng (kể cả khi MinIO chưa chạy).",
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

    # Lệnh 3: n06 validate
    validate_parser = subparsers.add_parser(
        "validate",
        help="Đối chiếu kết quả đối soát với oracle tại một as_of_time.",
        description="Khung kiểm tra và đối chiếu kết quả đối soát với oracle theo as_of_time.",
    )
    validate_parser.add_argument(
        "--dataset-id",
        required=True,
        help="Mã dataset cần đối chiếu.",
    )
    validate_parser.add_argument(
        "--against-ground-truth",
        action="store_true",
        dest="against_oracle",
        help="Kích hoạt đối chiếu qua test harness oracle độc lập.",
    )
    validate_parser.add_argument(
        "--as-of-time",
        required=True,
        help="Thời điểm đánh giá UTC dạng YYYY-MM-DDTHH:MM:SSZ.",
    )
    validate_parser.add_argument(
        "--allowed-lateness-seconds",
        type=int,
        default=86_400,
        help="Thời gian trễ cho phép tính bằng giây (mặc định: 86400).",
    )
    validate_parser.add_argument(
        "--fixture-dir",
        type=Path,
        default=None,
        help="Thư mục dữ liệu tham chiếu (mặc định dùng bộ nghiệm thu).",
    )
    validate_parser.add_argument(
        "--actual-file",
        type=Path,
        default=None,
        help="File JSON/JSONL kết quả đối soát thực tế để đối chiếu.",
    )
    validate_parser.add_argument(
        "--harness-module",
        default=None,
        help="Tên module Python của test harness (hoặc đặt qua N06_VALIDATE_HARNESS_MODULE).",
    )
    validate_parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="In kết quả dạng JSON có cấu trúc.",
    )

    # Lệnh 4: n06 publish
    publish_parser = subparsers.add_parser(
        "publish",
        help="Gửi events từ nguồn do generator sinh lên Kafka bằng producer idempotent.",
        description=(
            "Đọc file events theo manifest của source/, kiểm sha256 và số dòng, rồi gửi lên "
            "topic Kafka bằng producer idempotent. Topic, tốc độ và chiến lược key lấy từ cấu "
            "hình T01 và ghi đè được bằng tham số."
        ),
    )
    publish_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Profile cấu hình (default | demo | benchmark | test).",
    )
    publish_parser.add_argument(
        "--env-file", type=Path, default=None, help="File .env cần nạp (mặc định: ./.env)."
    )
    publish_parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help="Thư mục nguồn chứa manifests/ và events/ (mặc định: <generator_output_dir>/source).",
    )
    publish_parser.add_argument(
        "--topic", default=None, help="Ghi đè kafka.events_topic (tên topic)."
    )
    publish_parser.add_argument(
        "--rate",
        type=int,
        default=None,
        help="Ghi đè producer.rate: message/giây, 0 là không giới hạn.",
    )
    publish_parser.add_argument(
        "--key-strategy",
        choices=("transaction_id", "account_from"),
        default=None,
        help="Ghi đè producer.key_strategy (mặc định transaction_id; account_from cho E6).",
    )
    publish_parser.add_argument(
        "--allow-non-empty-topic",
        action="store_true",
        help="Cho phép publish vào topic đã có message (phát lại có chủ ý).",
    )
    publish_parser.add_argument(
        "--manifests-dir", default=None, help="Tên thư mục manifest dưới thư mục nguồn."
    )
    publish_parser.add_argument(
        "--events-dir", default=None, help="Tên thư mục events dưới thư mục nguồn."
    )
    publish_parser.add_argument(
        "--flush-timeout-seconds",
        type=float,
        default=None,
        help="Thời gian chờ tối đa khi flush producer.",
    )
    publish_parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="In kết quả dạng JSON có cấu trúc.",
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
                skip_buckets=parsed.skip_buckets,
                require_buckets=parsed.require_buckets,
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

    if parsed.command == "validate":
        return run_validate(
            dataset_id=parsed.dataset_id,
            against_oracle=parsed.against_oracle,
            as_of_time=parsed.as_of_time,
            allowed_lateness_seconds=parsed.allowed_lateness_seconds,
            fixture_dir=parsed.fixture_dir,
            actual_file=parsed.actual_file,
            harness_module=parsed.harness_module,
            as_json=parsed.as_json,
            out=stdout,
        )

    if parsed.command == "publish":
        publish_kwargs = {
            key: value
            for key, value in {
                "manifests_dir": parsed.manifests_dir,
                "events_dir": parsed.events_dir,
                "flush_timeout_seconds": parsed.flush_timeout_seconds,
            }.items()
            if value is not None
        }
        return run_publish(
            profile=parsed.profile,
            env_file=parsed.env_file,
            source_dir=parsed.source_dir,
            topic=parsed.topic,
            rate=parsed.rate,
            key_strategy=parsed.key_strategy,
            allow_non_empty_topic=parsed.allow_non_empty_topic,
            as_json=parsed.as_json,
            out=stdout,
            **publish_kwargs,
        )

    parser.print_help(file=stdout)
    return int(ExitCode.CLI_USAGE_ERROR)
