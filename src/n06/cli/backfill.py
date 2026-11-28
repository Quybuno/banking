"""`n06 backfill`: điều phối `process-day` rồi `load-mart` theo khoảng ngày (TASK T42).

Đặc tả: docs/spec.md Phần I mục 8.2–8.3, mục 12.
- Khoảng ngày `--from-date`..`--to-date` gồm hai đầu mút, kiểm tra hợp lệ trước khi chạy.
- `process-day` lỗi hoặc bị chặn (`QUALITY_GATE_BLOCKED`) thì dừng ngay, không chạy `load-mart`.
- Nếu `load-mart` lỗi, có thể chạy lại `n06 load-mart` hoặc `n06 backfill --mart-only` trên đúng
  snapshot Gold đã commit mà không làm trùng dữ liệu.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TextIO

from n06.cli.doctor import ExitCode
from n06.cli.load_mart import run_load_mart
from n06.cli.process_day import AS_OF_FORMAT, run_process_day

ProcessDayFn = Callable[..., int]
LoadMartFn = Callable[..., int]


def expand_date_range(from_date: str, to_date: str) -> tuple[str, ...]:
    """Kiểm tra và mở rộng khoảng ngày `[from_date, to_date]` gồm hai đầu mút."""
    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)
    if start > end:
        raise ValueError("--from-date phải nhỏ hơn hoặc bằng --to-date.")
    days = (end - start).days + 1
    return tuple((start + timedelta(days=i)).isoformat() for i in range(days))


def run_backfill(
    *,
    dataset_id: str,
    from_date: str,
    to_date: str,
    as_of_time: str,
    profile: str | None = None,
    env_file: Path | None = None,
    query_name: str | None = None,
    mart_only: bool = False,
    as_json: bool = False,
    env: Mapping[str, str] | None = None,
    process_day_fn: ProcessDayFn = run_process_day,
    load_mart_fn: LoadMartFn = run_load_mart,
    out: TextIO | None = None,
) -> int:
    """Thực thi `n06 backfill`: validate tham số -> `process-day` -> `load-mart`."""
    stream = out if out is not None else sys.stdout

    def emit(payload: dict[str, object], text: str) -> None:
        if as_json:
            stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
        else:
            stream.write(text + "\n")

    if not dataset_id.strip():
        code = int(ExitCode.CONFIG_ERROR)
        emit(
            {"ok": False, "exit_code": code, "error": "--dataset-id không được rỗng."},
            "[backfill] Lỗi: --dataset-id không được rỗng.",
        )
        return code

    try:
        business_dates = expand_date_range(from_date, to_date)
    except ValueError as exc:
        code = int(ExitCode.CONFIG_ERROR)
        emit(
            {"ok": False, "exit_code": code, "error": str(exc)},
            f"[backfill] Lỗi khoảng ngày: {exc}",
        )
        return code

    try:
        datetime.strptime(as_of_time, AS_OF_FORMAT)
    except ValueError:
        code = int(ExitCode.CONFIG_ERROR)
        msg = "--as-of-time phải có dạng YYYY-MM-DDTHH:MM:SSZ."
        emit({"ok": False, "exit_code": code, "error": msg}, f"[backfill] Lỗi: {msg}")
        return code

    process_day_code: int | None = None
    if not mart_only:
        process_day_code = process_day_fn(
            dataset_id=dataset_id,
            profile=profile,
            env_file=env_file,
            query_name=query_name,
            as_of_time=as_of_time,
            business_dates=business_dates,
            env=env,
            out=stream,
        )
        if process_day_code != int(ExitCode.OK):
            emit(
                {
                    "ok": False,
                    "dataset_id": dataset_id,
                    "from_date": from_date,
                    "to_date": to_date,
                    "business_dates": list(business_dates),
                    "as_of_time": as_of_time,
                    "stage_failed": "process-day",
                    "process_day_exit_code": process_day_code,
                    "load_mart_exit_code": None,
                    "exit_code": process_day_code,
                },
                f"[backfill] Dừng: process-day thất bại (exit={process_day_code}), "
                "không chạy load-mart.",
            )
            return process_day_code

    load_mart_code = load_mart_fn(
        dataset_id=dataset_id,
        from_date=from_date,
        to_date=to_date,
        profile=profile,
        env_file=env_file,
        env=env,
        out=stream,
    )
    ok = load_mart_code == int(ExitCode.OK)
    emit(
        {
            "ok": ok,
            "dataset_id": dataset_id,
            "from_date": from_date,
            "to_date": to_date,
            "business_dates": list(business_dates),
            "as_of_time": as_of_time,
            "mart_only": mart_only,
            "process_day_exit_code": process_day_code,
            "load_mart_exit_code": load_mart_code,
            "exit_code": load_mart_code,
        },
        (
            f"[backfill] OK dataset={dataset_id} dates={from_date}..{to_date} as_of={as_of_time}"
            if ok
            else f"[backfill] Lỗi: load-mart thất bại (exit={load_mart_code}); "
            "có thể chạy lại với --mart-only trên Gold đã commit."
        ),
    )
    return load_mart_code


__all__: Sequence[str] = ("expand_date_range", "run_backfill")
