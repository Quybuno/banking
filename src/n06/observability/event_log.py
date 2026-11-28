"""Đọc và tổng hợp Spark Event Log cho benchmark (đặc tả I.14.2, ADR-0007 B3)."""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SparkMetricsSummary:
    """Các chỉ số Spark trích từ event log theo I.14.2."""

    spark_event_log_ref: str
    app_id: str | None
    app_name: str | None
    app_duration_ms: int
    input_rows: int
    output_rows: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    spill_memory_bytes: int
    spill_disk_bytes: int
    stage_count: int
    stage_duration_total_ms: int
    stage_duration_max_ms: int
    task_count: int
    task_duration_median_ms: float
    task_duration_max_ms: float
    task_max_median_ratio: float


def find_latest_event_log(event_log_dir: Path, *, after_mtime: float | None = None) -> Path | None:
    """Tìm file Spark event log mới nhất trong `event_log_dir`."""
    if not event_log_dir.is_dir():
        return None
    candidates = [
        p
        for p in event_log_dir.iterdir()
        if p.is_file()
        and not p.name.startswith(".")
        and (after_mtime is None or p.stat().st_mtime >= after_mtime)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def parse_spark_event_log(path: Path) -> SparkMetricsSummary:
    """Đọc một file Spark event log (JSON lines) và tính đầy đủ chỉ số I.14.2."""
    app_id: str | None = None
    app_name: str | None = None
    app_start_ms: int | None = None
    app_end_ms: int | None = None

    input_rows = 0
    output_rows = 0
    shuffle_read_bytes = 0
    shuffle_write_bytes = 0
    spill_memory_bytes = 0
    spill_disk_bytes = 0

    stage_durations_ms: list[int] = []
    task_durations_ms: list[int] = []

    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            try:
                event: dict[str, Any] = json.loads(line)
            except json.JSONDecodeError:
                continue

            etype = event.get("Event")
            if etype == "SparkListenerApplicationStart":
                app_id = event.get("App ID")
                app_name = event.get("App Name")
                app_start_ms = event.get("Timestamp")
            elif etype == "SparkListenerApplicationEnd":
                app_end_ms = event.get("Timestamp")
            elif etype == "SparkListenerStageCompleted":
                info = event.get("Stage Info") or {}
                sub = info.get("Submission Time")
                comp = info.get("Completion Time")
                if isinstance(sub, int) and isinstance(comp, int) and comp >= sub:
                    stage_durations_ms.append(comp - sub)
            elif etype == "SparkListenerTaskEnd":
                task_info = event.get("Task Info") or {}
                metrics = event.get("Task Metrics") or {}

                exec_run_ms = metrics.get("Executor Run Time")
                if isinstance(exec_run_ms, int) and exec_run_ms >= 0:
                    task_durations_ms.append(exec_run_ms)
                else:
                    launch = task_info.get("Launch Time")
                    finish = task_info.get("Finish Time")
                    if isinstance(launch, int) and isinstance(finish, int) and finish >= launch:
                        task_durations_ms.append(finish - launch)

                in_m = metrics.get("Input Metrics") or {}
                input_rows += int(in_m.get("Records Read") or 0)

                out_m = metrics.get("Output Metrics") or {}
                output_rows += int(out_m.get("Records Written") or 0)

                sr_m = metrics.get("Shuffle Read Metrics") or {}
                shuffle_read_bytes += int(sr_m.get("Remote Bytes Read") or 0) + int(
                    sr_m.get("Local Bytes Read") or 0
                )

                sw_m = metrics.get("Shuffle Write Metrics") or {}
                shuffle_write_bytes += int(sw_m.get("Shuffle Bytes Written") or 0)

                spill_memory_bytes += int(metrics.get("Memory Bytes Spilled") or 0)
                spill_disk_bytes += int(metrics.get("Disk Bytes Spilled") or 0)

    app_duration_ms = (
        app_end_ms - app_start_ms
        if (
            isinstance(app_start_ms, int)
            and isinstance(app_end_ms, int)
            and app_end_ms >= app_start_ms
        )
        else 0
    )
    task_count = len(task_durations_ms)
    if task_durations_ms:
        median_ms = float(statistics.median(task_durations_ms))
        max_ms = float(max(task_durations_ms))
        ratio = round(max_ms / median_ms, 4) if median_ms > 0 else 0.0
    else:
        median_ms = 0.0
        max_ms = 0.0
        ratio = 0.0

    return SparkMetricsSummary(
        spark_event_log_ref=str(path),
        app_id=app_id,
        app_name=app_name,
        app_duration_ms=app_duration_ms,
        input_rows=input_rows,
        output_rows=output_rows,
        shuffle_read_bytes=shuffle_read_bytes,
        shuffle_write_bytes=shuffle_write_bytes,
        spill_memory_bytes=spill_memory_bytes,
        spill_disk_bytes=spill_disk_bytes,
        stage_count=len(stage_durations_ms),
        stage_duration_total_ms=sum(stage_durations_ms),
        stage_duration_max_ms=max(stage_durations_ms) if stage_durations_ms else 0,
        task_count=task_count,
        task_duration_median_ms=round(median_ms, 3),
        task_duration_max_ms=round(max_ms, 3),
        task_max_median_ratio=ratio,
    )
