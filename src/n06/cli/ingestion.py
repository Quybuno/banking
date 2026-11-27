"""`n06 stream`: kiểm cấu hình ở host rồi chạy job trong một container của image Spark.

Mã thoát: 0 xong; 10 cấu hình sai; 13 không có Docker; 14 job thất bại hoặc hạ tầng chưa sẵn sàng.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TextIO

from n06.cli.doctor import ExitCode
from n06.cli.init_local import parse_dotenv_file
from n06.config import REPO_ROOT, ConfigValidationError, load_config
from n06.ingestion.launcher import host_only_addresses, job_command

STREAM_MODULE = "n06.ingestion.stream_job"
_PASSED_THROUGH = {int(ExitCode.OK), int(ExitCode.CONFIG_ERROR), int(ExitCode.SERVICE_ERROR)}
Runner = Callable[..., "subprocess.CompletedProcess[str]"]


def _run_job(
    name: str,
    module: str,
    job_args: Sequence[str],
    *,
    profile: str | None,
    env_file: Path | None,
    env: Mapping[str, str] | None,
    runner: Runner,
    docker_bin: str | None,
    out: TextIO,
) -> int:
    merged: dict[str, str] = {}
    dotenv = env_file if env_file is not None else REPO_ROOT / ".env"
    if dotenv.is_file():
        merged.update(parse_dotenv_file(dotenv))
    merged.update(env if env is not None else os.environ)
    effective = profile or merged.get("N06_PROFILE") or "default"

    def fail(code: ExitCode, message: str) -> int:
        out.write(f"[{name}] Lỗi: {message}\n")
        return int(code)

    try:
        cfg = load_config(profile=effective, env=merged)
    except ConfigValidationError as exc:
        return fail(ExitCode.CONFIG_ERROR, exc.public_message)
    unusable = host_only_addresses(cfg)
    if unusable:
        return fail(
            ExitCode.CONFIG_ERROR,
            f"{', '.join(unusable)} trỏ về máy host; job chạy trong mạng Docker nên dùng profile "
            "default, demo hoặc benchmark, hoặc ghi đè bằng tên service.",
        )
    arguments = ["--profile", effective, *job_args]
    event_log_dir = None
    if cfg.spark.event_log_enabled and not Path(cfg.spark.event_log_dir).is_absolute():
        event_log_dir = REPO_ROOT / cfg.spark.event_log_dir
        event_log_dir.mkdir(parents=True, exist_ok=True)
    docker = docker_bin or shutil.which("docker")
    if docker is None:
        return fail(ExitCode.DOCKER_ERROR, "Không tìm thấy lệnh `docker` trong PATH.")
    command = job_command(
        cfg,
        docker_bin=docker,
        repo_root=REPO_ROOT,
        job_module=module,
        job_args=arguments,
        env_names=[key for key in merged if key.startswith("N06")],
        event_log_dir=event_log_dir,
    )
    sys.stdout.flush()
    result = runner(command, cwd=REPO_ROOT, env={**os.environ, **merged}, check=False)
    code = result.returncode
    return code if code in _PASSED_THROUGH else int(ExitCode.SERVICE_ERROR)


def run_stream(
    *,
    profile: str | None = None,
    env_file: Path | None = None,
    query_name: str | None = None,
    available_now: bool = False,
    env: Mapping[str, str] | None = None,
    runner: Runner = subprocess.run,
    docker_bin: str | None = None,
    out: TextIO | None = None,
) -> int:
    job_args = ["--query-name", query_name] if query_name is not None else []
    if available_now:
        job_args.append("--available-now")
    return _run_job(
        "stream", STREAM_MODULE, job_args, profile=profile, env_file=env_file, env=env,
        runner=runner, docker_bin=docker_bin, out=out or sys.stdout,
    )  # fmt: skip
