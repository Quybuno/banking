"""`n06 load-mart`: kiểm cấu hình ở host rồi publish Gold đã commit sang PostgreSQL mart (T26).

Mã thoát: 0 xong; 10 cấu hình hoặc tham số ngày sai; 13 không có Docker; 14 job, thiếu snapshot
Gold hoặc hạ tầng mart/control thất bại (job in một dòng `N06_LOAD_MART_ERROR={...}`).
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import TextIO

from n06.cli.doctor import ExitCode
from n06.cli.ingestion import Runner, _run_job

LOAD_MART_MODULE = "n06.warehouse.load_mart_job"


def run_load_mart(
    *,
    dataset_id: str,
    from_date: str | None = None,
    to_date: str | None = None,
    profile: str | None = None,
    env_file: Path | None = None,
    fail_at: str | None = None,
    env: Mapping[str, str] | None = None,
    runner: Runner = subprocess.run,
    docker_bin: str | None = None,
    out: TextIO | None = None,
) -> int:
    stream = out or sys.stdout
    try:
        parsed_from = date.fromisoformat(from_date) if from_date is not None else None
        parsed_to = date.fromisoformat(to_date) if to_date is not None else None
    except ValueError:
        stream.write("[load-mart] Lỗi: --from-date và --to-date phải có dạng YYYY-MM-DD.\n")
        return int(ExitCode.CONFIG_ERROR)
    if parsed_from is not None and parsed_to is not None and parsed_from > parsed_to:
        stream.write("[load-mart] Lỗi: --from-date phải nhỏ hơn hoặc bằng --to-date.\n")
        return int(ExitCode.CONFIG_ERROR)

    job_args = ["--dataset-id", dataset_id]
    if from_date is not None:
        job_args += ["--from-date", from_date]
    if to_date is not None:
        job_args += ["--to-date", to_date]
    if fail_at is not None:
        job_args += ["--fail-at", fail_at]
    return _run_job(
        "load-mart",
        LOAD_MART_MODULE,
        job_args,
        profile=profile,
        env_file=env_file,
        env=env,
        runner=runner,
        docker_bin=docker_bin,
        out=stream,
    )
