"""Khung lệnh `n06 validate --against-ground-truth` (TASK T14).

Tham chiếu: docs/spec.md Phần I mục 10, Phần II mục 6.
Quy tắc:
- Chỉ xử lý khung CLI và kiểm tra tham số; tuyệt đối không import `tests/` từ `src/n06/`.
- Khi chạy thực tế ở chế độ test harness, ủy quyền qua callback hoặc tiến trình con độc lập.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

from n06.cli.doctor import ExitCode

OracleRunner = Callable[..., dict[str, Any]]


def parse_as_of_time_utc(value: str) -> datetime:
    """Kiểm tra và parse `--as-of-time` chuẩn UTC (`...Z` hoặc `+00:00`)."""
    stripped = value.strip()
    if not stripped:
        raise ValueError("--as-of-time không được rỗng")
    normalized = stripped[:-1] + "+00:00" if stripped.endswith("Z") else stripped
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        raise ValueError(f"--as-of-time phải kèm múi giờ UTC (vd ...Z): {value!r}")
    return dt.astimezone(UTC)


def _resolve_harness_module(harness_module: str | None) -> str:
    """Lấy tên module harness từ tham số hoặc biến môi trường (không hardcode `tests/`)."""
    resolved = (harness_module or os.environ.get("N06_VALIDATE_HARNESS_MODULE") or "").strip()
    if not resolved:
        raise ValueError(
            "Chưa cấu hình harness đối chiếu (--harness-module hoặc "
            "N06_VALIDATE_HARNESS_MODULE); gói cài đặt production không bao gồm bộ test harness."
        )
    return resolved


def _run_harness_subprocess(
    *,
    dataset_id: str,
    as_of_time: str,
    allowed_lateness_seconds: int,
    fixture_dir: Path | None,
    actual_file: Path | None,
    harness_module: str | None,
) -> tuple[int, dict[str, Any]]:
    """Gọi test harness qua tiến trình con độc lập theo module được cấu hình."""
    module_name = _resolve_harness_module(harness_module)
    cmd = [
        sys.executable,
        "-m",
        module_name,
        "--dataset-id",
        dataset_id,
        "--as-of-time",
        as_of_time,
        "--allowed-lateness-seconds",
        str(allowed_lateness_seconds),
        "--json",
    ]
    if fixture_dir is not None:
        cmd.extend(["--fixture-dir", str(fixture_dir)])
    if actual_file is not None:
        cmd.extend(["--actual-file", str(actual_file)])

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode not in (0, 1):
        detail = proc.stderr.strip() or proc.stdout.strip() or f"exit={proc.returncode}"
        raise RuntimeError(f"Không thể chạy test harness: {detail}")
    payload = json.loads(proc.stdout)
    return proc.returncode, payload


def run_validate(
    *,
    dataset_id: str,
    against_oracle: bool,
    as_of_time: str,
    allowed_lateness_seconds: int = 86_400,
    fixture_dir: Path | None = None,
    actual_file: Path | None = None,
    harness_module: str | None = None,
    as_json: bool = False,
    out: TextIO | None = None,
    oracle_runner: OracleRunner | None = None,
) -> int:
    """Thực thi lệnh `n06 validate`."""
    stdout = out if out is not None else sys.stdout

    if not dataset_id.strip():
        stdout.write("[validate] Lỗi: --dataset-id không được rỗng\n")
        return int(ExitCode.CLI_USAGE_ERROR)

    if allowed_lateness_seconds < 0:
        stdout.write("[validate] Lỗi: --allowed-lateness-seconds phải >= 0\n")
        return int(ExitCode.CLI_USAGE_ERROR)

    try:
        parsed_as_of = parse_as_of_time_utc(as_of_time)
    except ValueError as exc:
        stdout.write(f"[validate] Lỗi tham số --as-of-time: {exc}\n")
        return int(ExitCode.CLI_USAGE_ERROR)

    if not against_oracle:
        stdout.write("[validate] Lỗi: hiện chỉ hỗ trợ chế độ --against-ground-truth\n")
        return int(ExitCode.CLI_USAGE_ERROR)

    normalized_as_of = parsed_as_of.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        if oracle_runner is not None:
            payload = oracle_runner(
                dataset_id=dataset_id,
                as_of_time=normalized_as_of,
                allowed_lateness_seconds=allowed_lateness_seconds,
                fixture_dir=fixture_dir,
                actual_file=actual_file,
            )
            exit_code = 0 if payload.get("passed", True) else 1
        else:
            exit_code, payload = _run_harness_subprocess(
                dataset_id=dataset_id,
                as_of_time=normalized_as_of,
                allowed_lateness_seconds=allowed_lateness_seconds,
                fixture_dir=fixture_dir,
                actual_file=actual_file,
                harness_module=harness_module,
            )
    except (RuntimeError, ValueError, OSError, json.JSONDecodeError) as exc:
        if as_json:
            err_payload = json.dumps({"passed": False, "error": str(exc)}, ensure_ascii=False)
            stdout.write(f"{err_payload}\n")
        else:
            stdout.write(f"[validate] Lỗi cấu hình/harness: {exc}\n")
        return int(ExitCode.CONFIG_ERROR)

    if as_json:
        stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    else:
        counts = payload.get("counts_by_status")
        if counts is not None:
            stdout.write(
                f"[validate] dataset_id={dataset_id} as_of_time={normalized_as_of} "
                f"match_keys={payload.get('match_key_count')} counts={counts}\n"
            )
        else:
            stdout.write(
                f"[validate] dataset_id={dataset_id} as_of_time={normalized_as_of} "
                f"passed={payload.get('passed')} mismatches={len(payload.get('mismatches', []))}\n"
            )
    return int(ExitCode.OK) if exit_code == 0 else 1
