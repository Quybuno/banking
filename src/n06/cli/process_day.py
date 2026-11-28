"""`n06 process-day`: kiểm cấu hình ở host rồi chạy job Silver/Gold trong container Spark (T25).

Mã thoát: 0 xong; 10 cấu hình hoặc tham số sai; 13 không có Docker; 14 job hoặc hạ tầng thất bại,
kể cả dữ liệu input mà T25 chưa xử lý được và `run_id` đã có object (job in một dòng
`N06_PROCESS_DAY_ERROR={...}` nêu mã lỗi có tên).
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TextIO

from n06.cli.doctor import ExitCode
from n06.cli.ingestion import Runner, _run_job

PROCESS_DAY_MODULE = "n06.spark_jobs.process_day_job"
AS_OF_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def run_process_day(
    *,
    dataset_id: str,
    profile: str | None = None,
    env_file: Path | None = None,
    query_name: str | None = None,
    as_of_time: str | None = None,
    business_dates: Sequence[str] = (),
    run_id: str | None = None,
    fail_at: str | None = None,
    env: Mapping[str, str] | None = None,
    runner: Runner = subprocess.run,
    docker_bin: str | None = None,
    clock: Callable[[], datetime] | None = None,
    out: TextIO | None = None,
) -> int:
    stream = out or sys.stdout
    try:
        # Đồng hồ chỉ được đọc ở đây, một lần, rồi truyền tường minh cho job (đặc tả I.7.3).
        stamp = (
            as_of_time
            if as_of_time is not None
            else (clock() if clock is not None else datetime.now(UTC)).strftime(AS_OF_FORMAT)
        )
        datetime.strptime(stamp, AS_OF_FORMAT)
        for value in business_dates:
            date.fromisoformat(value)
    except ValueError:
        stream.write(
            "[process-day] Lỗi: --as-of-time phải có dạng YYYY-MM-DDTHH:MM:SSZ "
            "và --business-date dạng YYYY-MM-DD.\n"
        )
        return int(ExitCode.CONFIG_ERROR)
    job_args = ["--dataset-id", dataset_id, "--as-of-time", stamp]
    if query_name is not None:
        job_args += ["--query-name", query_name]
    for value in business_dates:
        job_args += ["--business-date", value]
    if run_id is not None:
        job_args += ["--run-id", run_id]
    if fail_at is not None:
        job_args += ["--fail-at", fail_at]
    return _run_job(
        "process-day",
        PROCESS_DAY_MODULE,
        job_args,
        profile=profile,
        env_file=env_file,
        env=env,
        runner=runner,
        docker_bin=docker_bin,
        out=stream,
    )
