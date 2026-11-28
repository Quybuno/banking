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

from n06.cli.backfill import run_backfill
from n06.cli.benchmark import run_benchmark_cli
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
from n06.cli.generate import run_generate
from n06.cli.ingestion import (
    DEFAULT_WAIT_TIMEOUT_SECONDS,
    run_stream,
    run_wait_ingestion,
)
from n06.cli.init_local import (
    InitLocalResult,
    generate_local_env,
    parse_dotenv_file,
    run_init_local,
)
from n06.cli.load_mart import run_load_mart
from n06.cli.process_day import run_process_day
from n06.cli.publish import run_publish
from n06.cli.statements import run_ingest_statements
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
    "run_generate",
    "run_ingest_statements",
    "run_init_local",
    "run_load_mart",
    "run_publish",
    "run_validate",
]


def _positive_int(raw: str) -> int:
    """Kiểu tham số: số nguyên >= 1 (số giao dịch)."""
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"không phải số nguyên: {raw!r}") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"phải >= 1, nhận {value}")
    return value


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
        "\n  16 (CAPACITY_ERROR)    : Từ chối: không đủ dung lượng ổ hoặc vượt max_transactions"
        "\n  17 (WORKLOAD_UNAVAILABLE): Thí nghiệm/workload chưa có trên nhánh hiện tại"
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
        metavar=(
            "{init-local,doctor,validate,publish,generate,stream,wait-ingestion,"
            "ingest-statements,process-day,load-mart,backfill,benchmark}"
        ),
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
        "--transactions",
        type=_positive_int,
        default=None,
        help="Số giao dịch dự định sinh để kiểm dung lượng (mặc định: dataset.transaction_count).",
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
    stream_parser = subparsers.add_parser(
        "stream",
        help="Chạy query streaming ghi Raw và Bronze từ Kafka (trong image Spark).",
        description=(
            "Một query foreachBatch đọc topic events, ghi Raw rồi Bronze theo batch_id và upsert "
            "ingest_batches. Tiến trình dài hạn; dừng bằng Ctrl+C."
        ),
    )
    wait_parser = subparsers.add_parser(
        "wait-ingestion",
        help="Chờ tới khi mọi dòng events của một dataset đã nằm trong Raw đã commit.",
        description=(
            "So byte của file events nguồn với Raw của các batch có trong ingest_batches; thoát 0 "
            "khi đủ, 14 khi hết thời gian chờ."
        ),
    )
    wait_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Profile cấu hình; địa chỉ phải dùng được trong mạng Docker.",
    )
    wait_parser.add_argument(
        "--env-file", type=Path, default=None, help="File .env cần nạp (mặc định: ./.env)."
    )
    wait_parser.add_argument(
        "--query-name", default=None, help="Query streaming cần chờ (mặc định: kafka.query_name)."
    )
    wait_parser.add_argument("--dataset-id", required=True, help="dataset_id cần chờ.")
    wait_parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help="Thư mục source/ đã publish (mặc định: <generator_output_dir>/source).",
    )
    wait_parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_WAIT_TIMEOUT_SECONDS,
        help="Thời gian chờ tối đa (mặc định 300).",
    )
    stream_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Profile cấu hình; địa chỉ phải dùng được trong mạng Docker.",
    )
    stream_parser.add_argument(
        "--env-file", type=Path, default=None, help="File .env cần nạp (mặc định: ./.env)."
    )
    stream_parser.add_argument(
        "--query-name", default=None, help="Ghi đè kafka.query_name (tên query streaming)."
    )
    stream_parser.add_argument(
        "--available-now",
        action="store_true",
        help="Xử lý hết dữ liệu hiện có (vẫn chia batch theo cấu hình) rồi dừng.",
    )

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

    # Lệnh 5: n06 generate (tối thiểu, kèm kiểm dung lượng)
    generate_parser = subparsers.add_parser(
        "generate",
        help="Sinh dataset bằng generator sau khi kiểm dung lượng ổ đĩa (từ chối bằng mã 16).",
        description=(
            "Kiểm dung lượng ổ thật theo mục [limits] của cấu hình rồi gọi build_dataset. "
            "Bị từ chối (mã 16) thì không tạo file nào. Chỉ có các cờ tối thiểu; các cờ còn lại "
            "của lệnh generate trong đặc tả thuộc task khác."
        ),
    )
    generate_parser.add_argument(
        "--generator-config",
        type=Path,
        required=True,
        help="Tệp JSON cấu hình generator (ví dụ trong contracts/.../config/).",
    )
    generate_parser.add_argument(
        "--out",
        type=Path,
        default=None,
        dest="out_dir",
        help="Thư mục đầu ra (mặc định: paths.generator_output_dir).",
    )
    generate_parser.add_argument(
        "--transactions",
        type=_positive_int,
        default=None,
        help="Ghi đè transaction_count của cấu hình generator.",
    )
    generate_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Profile cấu hình (default | demo | benchmark | test).",
    )
    generate_parser.add_argument(
        "--env-file", type=Path, default=None, help="File .env cần nạp (mặc định: ./.env)."
    )
    generate_parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="In kết quả dạng JSON có cấu trúc.",
    )

    # Lệnh 6: n06 ingest-statements (T23)
    ingest_stmt_parser = subparsers.add_parser(
        "ingest-statements",
        help="Đưa file sao kê lên MinIO theo lịch giao và biên nhận _ingest/ (ADR-0002, ADR-0012).",
        description=(
            "Đọc statement_deliveries.jsonl và manifest từng lô sao kê trong source/, kiểm sha256 "
            "và số dòng, chỉ tải các lô có file_available_at <= as_of_time theo thứ tự: file dữ "
            "liệu -> manifest -> biên nhận _ingest/<delivery_order:09d>.json."
        ),
    )
    ingest_stmt_parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help="Thư mục source/ chứa statement_deliveries.jsonl, manifests/, statements/.",
    )
    ingest_stmt_parser.add_argument(
        "--as-of-time",
        default=None,
        help="Đồng hồ giả lập UTC dạng YYYY-MM-DDTHH:MM:SSZ (mặc định: thời điểm hiện tại UTC).",
    )
    ingest_stmt_parser.add_argument(
        "--dataset-id",
        default=None,
        help="Lọc đúng dataset_id nếu lịch giao chứa nhiều dataset.",
    )
    ingest_stmt_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Profile cấu hình (default | demo | benchmark | test).",
    )
    ingest_stmt_parser.add_argument(
        "--env-file", type=Path, default=None, help="File .env cần nạp (mặc định: ./.env)."
    )
    ingest_stmt_parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="In kết quả dạng JSON có cấu trúc.",
    )

    # Lệnh 8: n06 benchmark
    bench_parser = subparsers.add_parser(
        "benchmark",
        help="Chạy benchmark thí nghiệm E1–E8 theo ADR-0007 và xuất CSV/JSON/biểu đồ.",
        description=(
            "Chạy benchmark đo hiệu năng (CPU/RAM theo chuỗi thời gian, Spark event log, "
            "lưu trữ, tính đúng) và ghi kết quả vào reports/benchmark/<batch_id>/."
        ),
    )
    bench_parser.add_argument(
        "--experiment",
        required=True,
        help="Mã thí nghiệm (E1..E8).",
    )
    bench_parser.add_argument(
        "--level",
        "--levels",
        dest="levels",
        nargs="+",
        type=_positive_int,
        default=None,
        help="Mức biến thử nghiệm (mặc định lấy benchmark.smoke_level từ config).",
    )
    bench_parser.add_argument(
        "--repeat",
        "--repetitions",
        dest="repetitions",
        type=_positive_int,
        default=None,
        help="Số lần đo cho mỗi cấu hình (mặc định từ config: 3).",
    )
    bench_parser.add_argument(
        "--warmup",
        type=int,
        default=None,
        help="Số lần warm-up cho mỗi cấu hình (mặc định từ config: 1).",
    )
    bench_parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed sinh dữ liệu và hoán vị thứ tự chạy.",
    )
    bench_parser.add_argument(
        "--as-of-time",
        default=None,
        help="Mốc thời gian as_of_time UTC dạng ...Z.",
    )
    bench_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default="benchmark",
        help="Profile cấu hình (mặc định: benchmark).",
    )
    bench_parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="File .env cần nạp (mặc định: ./.env).",
    )
    bench_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Thư mục ghi kết quả (mặc định: reports/benchmark/<batch_id>).",
    )
    bench_parser.add_argument(
        "--generator-config",
        type=Path,
        default=None,
        help="File cấu hình generator JSON.",
    )
    bench_parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Cho phép chạy khi cây Git đang có thay đổi chưa commit.",
    )
    bench_parser.add_argument(
        "--share-baseline",
        action="store_true",
        help="Cho phép ghi dấu dùng chung cấu hình gốc (ADR-0007 B1).",
    )
    bench_parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="In kết quả dạng JSON có cấu trúc.",
    )

    # Lệnh 9: n06 process-day (T25)
    process_day_parser = subparsers.add_parser(
        "process-day",
        help="Bronze -> Silver (dedup cơ bản) -> đối soát MATCHED -> Gold, commit theo run_id.",
        description=(
            "Đọc Bronze theo ingest_batches và sao kê theo biên nhận _ingest/, ghi Silver và Gold "
            "Parquet vào đường dẫn run_id mới (đã kiểm chưa có object) rồi trỏ current_snapshot. "
            "Chạy trong image Spark. Dữ liệu cần quarantine hoặc xử lý xung đột (T30, T31) làm "
            "job dừng với mã lỗi có tên."
        ),
    )
    process_day_parser.add_argument("--dataset-id", required=True, help="dataset_id cần xử lý.")
    process_day_parser.add_argument(
        "--as-of-time",
        default=None,
        help="Đồng hồ giả lập UTC YYYY-MM-DDTHH:MM:SSZ; chỉ sao kê có biên nhận "
        "file_available_at <= mốc này được đối soát (mặc định: thời điểm hiện tại UTC).",
    )
    process_day_parser.add_argument(
        "--business-date",
        action="append",
        default=None,
        help="Chỉ xử lý ngày này (YYYY-MM-DD); lặp lại để chọn nhiều ngày "
        "(mặc định: mọi ngày có dữ liệu).",
    )
    process_day_parser.add_argument(
        "--query-name", default=None, help="Ghi đè kafka.query_name (query đã ghi Bronze)."
    )
    process_day_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Profile cấu hình; địa chỉ phải dùng được trong mạng Docker.",
    )
    process_day_parser.add_argument(
        "--env-file", type=Path, default=None, help="File .env cần nạp (mặc định: ./.env)."
    )
    process_day_parser.add_argument("--run-id", default=None, help=argparse.SUPPRESS)
    process_day_parser.add_argument(
        "--fail-at", choices=("after_write",), default=None, help=argparse.SUPPRESS
    )

    load_mart_parser = subparsers.add_parser(
        "load-mart",
        help="Publish dữ liệu Gold đã commit sang PostgreSQL mart qua staging + 1 transaction.",
        description=(
            "Đọc snapshot Gold đã commit từ current_snapshot, nạp bảng staging, kiểm tra và "
            "thay thế phạm vi ngày trên PostgreSQL mart trong một transaction, rồi đánh dấu "
            "MART_PUBLISHED trong control DB."
        ),
    )
    load_mart_parser.add_argument(
        "--dataset-id", required=True, help="dataset_id cần publish vào mart."
    )
    load_mart_parser.add_argument(
        "--from-date",
        default=None,
        help="Ngày bắt đầu phạm vi thay thế (YYYY-MM-DD; mặc định: ngày nhỏ nhất ở Gold).",
    )
    load_mart_parser.add_argument(
        "--to-date",
        default=None,
        help="Ngày kết thúc phạm vi thay thế (YYYY-MM-DD; mặc định: ngày lớn nhất ở Gold).",
    )
    load_mart_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Profile cấu hình; địa chỉ phải dùng được trong mạng Docker.",
    )
    load_mart_parser.add_argument(
        "--env-file", type=Path, default=None, help="File .env cần nạp (mặc định: ./.env)."
    )
    load_mart_parser.add_argument(
        "--fail-at", choices=("after_stage",), default=None, help=argparse.SUPPRESS
    )

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Điều phối process-day rồi load-mart theo khoảng ngày [from_date, to_date].",
        description=(
            "Chạy process-day cho toàn bộ khoảng ngày gồm hai đầu mút, sau đó publish sang "
            "PostgreSQL mart bằng load-mart. Nếu process-day lỗi hoặc bị cổng chất lượng chặn "
            "thì dừng ngay, không load mart."
        ),
    )
    backfill_parser.add_argument("--dataset-id", required=True, help="dataset_id cần backfill.")
    backfill_parser.add_argument(
        "--from-date", required=True, help="Ngày bắt đầu (YYYY-MM-DD, gồm đầu mút)."
    )
    backfill_parser.add_argument(
        "--to-date", required=True, help="Ngày kết thúc (YYYY-MM-DD, gồm đầu mút)."
    )
    backfill_parser.add_argument(
        "--as-of-time",
        required=True,
        help="Thời điểm đánh giá UTC dạng YYYY-MM-DDTHH:MM:SSZ.",
    )
    backfill_parser.add_argument(
        "--profile",
        choices=("default", "demo", "benchmark", "test"),
        default=None,
        help="Profile cấu hình; địa chỉ phải dùng được trong mạng Docker.",
    )
    backfill_parser.add_argument(
        "--env-file", type=Path, default=None, help="File .env cần nạp (mặc định: ./.env)."
    )
    backfill_parser.add_argument(
        "--query-name", default=None, help="Ghi đè kafka.query_name khi đọc Raw/Bronze."
    )
    backfill_parser.add_argument(
        "--mart-only",
        action="store_true",
        help="Bỏ qua process-day, chỉ chạy lại load-mart từ Gold đã commit.",
    )
    backfill_parser.add_argument(
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
            requested_transactions=parsed.transactions,
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

    if parsed.command == "stream":
        return run_stream(
            profile=parsed.profile,
            env_file=parsed.env_file,
            query_name=parsed.query_name,
            available_now=parsed.available_now,
            out=stdout,
        )

    if parsed.command == "wait-ingestion":
        return run_wait_ingestion(
            dataset_id=parsed.dataset_id,
            profile=parsed.profile,
            env_file=parsed.env_file,
            source_dir=parsed.source_dir,
            query_name=parsed.query_name,
            timeout_seconds=parsed.timeout_seconds,
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

    if parsed.command == "generate":
        return run_generate(
            generator_config=parsed.generator_config,
            out_dir=parsed.out_dir,
            transactions=parsed.transactions,
            profile=parsed.profile,
            env_file=parsed.env_file,
            as_json=parsed.as_json,
            out=stdout,
        )

    if parsed.command == "ingest-statements":
        return run_ingest_statements(
            source_dir=parsed.source_dir,
            as_of_time=parsed.as_of_time,
            dataset_id=parsed.dataset_id,
            profile=parsed.profile,
            env_file=parsed.env_file,
            as_json=parsed.as_json,
            out=stdout,
        )

    if parsed.command == "benchmark":
        return run_benchmark_cli(
            experiment=parsed.experiment,
            levels=parsed.levels,
            repetitions=parsed.repetitions,
            warmup=parsed.warmup,
            seed=parsed.seed,
            as_of_time=parsed.as_of_time,
            profile=parsed.profile,
            env_file=parsed.env_file,
            output_dir=parsed.output_dir,
            allow_dirty=parsed.allow_dirty,
            share_baseline=parsed.share_baseline,
            generator_config=parsed.generator_config,
            as_json=parsed.as_json,
            out=stdout,
        )

    if parsed.command == "process-day":
        return run_process_day(
            dataset_id=parsed.dataset_id,
            profile=parsed.profile,
            env_file=parsed.env_file,
            query_name=parsed.query_name,
            as_of_time=parsed.as_of_time,
            business_dates=parsed.business_date or (),
            run_id=parsed.run_id,
            fail_at=parsed.fail_at,
            out=stdout,
        )

    if parsed.command == "load-mart":
        return run_load_mart(
            dataset_id=parsed.dataset_id,
            from_date=parsed.from_date,
            to_date=parsed.to_date,
            profile=parsed.profile,
            env_file=parsed.env_file,
            fail_at=parsed.fail_at,
            out=stdout,
        )

    if parsed.command == "backfill":
        return run_backfill(
            dataset_id=parsed.dataset_id,
            from_date=parsed.from_date,
            to_date=parsed.to_date,
            as_of_time=parsed.as_of_time,
            profile=parsed.profile,
            env_file=parsed.env_file,
            query_name=parsed.query_name,
            mart_only=parsed.mart_only,
            as_json=parsed.as_json,
            out=stdout,
        )

    parser.print_help(file=stdout)
    return int(ExitCode.CLI_USAGE_ERROR)
