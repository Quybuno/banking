"""Ghi kết quả benchmark (CSV, JSON, summary, not_run, charts) theo ADR-0007 B6–B8."""

from __future__ import annotations

import csv
import dataclasses
import hashlib
import json
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from n06.config import AppConfig

_SECRET_FIELD_NAMES = frozenset({"access_key", "secret_key", "password"})

# Thứ tự cột chuẩn của `runs.csv` theo ADR-0007 B8.
RUNS_COLUMNS: tuple[str, ...] = (
    # Định danh
    "batch_id",
    "run_id",
    "experiment",
    "config_id",
    "variable_name",
    "variable_value",
    "variable2_name",
    "variable2_value",
    "repetition",
    "run_order",
    "phase",
    "cache_state",
    "shared_with",
    # Kết quả đếm
    "status",
    "counted",
    "exclusion_reason",
    "error_type",
    "error_message",
    "exit_code",
    # Thời gian
    "started_at",
    "ended_at",
    "wall_seconds",
    "phase_seconds_json",
    "startup_seconds",
    "end_to_end_seconds",
    # Máy
    "host_os",
    "kernel",
    "cpu_model",
    "cpu_logical",
    "ram_total_mb",
    "swap_total_mb",
    "disk_free_bytes_start",
    "container_limits_json",
    "services_stopped",
    "os_cache_dropped",
    "clock_source",
    "noise_note",
    # Phiên bản
    "n06_version",
    "python",
    "spark",
    "hadoop",
    "scala",
    "java",
    "kafka",
    "minio_image",
    "postgres_image",
    "duckdb",
    # Nguồn gốc
    "profile",
    "commit_sha",
    "git_dirty",
    "dataset_id",
    "transaction_count",
    "seed",
    "input_manifest_path",
    "input_manifest_sha256",
    "source_batch_id",
    "as_of_time",
    "config_hash",
    "config_json",
    # Tính đúng
    "correctness_ok",
    "unique_key_count",
    "totals_by_currency_json",
    "status_counts_json",
    "semantic_hash",
    "ground_" + "truth_match",
    # Kết quả chính
    "throughput_value",
    "throughput_unit",
    "throughput_entity",
    "throughput_scope",
    "cpu_peak_pct",
    "cpu_avg_pct",
    "ram_peak_mb",
    "ram_avg_mb",
    "ram_scope",
    "swap_peak_mb",
)

METRICS_COLUMNS: tuple[str, ...] = ("run_id", "metric", "value", "unit", "scope")
NOT_RUN_COLUMNS: tuple[str, ...] = (
    "experiment",
    "level",
    "reason_code",
    "measured_evidence_json",
    "highest_level_run",
)


def redact_config_dict(cfg: AppConfig) -> dict[str, Any]:
    """Chuyển `AppConfig` sang dict và loại bỏ hoàn toàn các trường secret."""
    raw = dataclasses.asdict(cfg)

    def _clean(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items() if k not in _SECRET_FIELD_NAMES}
        if isinstance(obj, list):
            return [_clean(v) for v in obj]
        return obj

    return _clean(raw)


def canonical_json(obj: Any) -> str:
    """JSON gọn với khóa sắp xếp ổn định theo ADR-0007 B8."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_config_hash(cfg: AppConfig) -> tuple[str, str]:
    """Trả về `(config_hash_sha256, config_json)` sau khi loại secret."""
    cleaned = redact_config_dict(cfg)
    cjson = canonical_json(cleaned)
    digest = hashlib.sha256(cjson.encode("utf-8")).hexdigest()
    return digest, cjson


@dataclass(frozen=True)
class MetricRow:
    """Một dòng trong `metrics.csv`."""

    run_id: str
    metric: str
    value: float | int | str
    unit: str
    scope: str


@dataclass(frozen=True)
class NotRunRecord:
    """Một dòng trong `not_run.csv` (ADR-0007 B7)."""

    experiment: str
    level: int
    reason_code: str
    measured_evidence_json: str
    highest_level_run: int | None


def compute_median(values: Sequence[float]) -> float | None:
    """Trung vị theo ADR-0007 B2.6 (chẵn lần lấy trung bình 2 giá trị giữa)."""
    if not values:
        return None
    return round(float(statistics.median(values)), 6)


def evaluate_run_counting(
    *,
    phase: str,
    status: str,
    correctness_ok: bool,
    swap_peak_mb: float,
    ram_available_start_mb: float,
    driver_memory_mb: float,
    noise_overhead_mb: int,
) -> tuple[bool, str | None]:
    """Quy tắc đếm lần chạy theo ADR-0007 B6 và Q9."""
    if phase == "warmup":
        return False, "warmup"
    if status == "error":
        return False, "error"
    if status == "invalid" or not correctness_ok:
        return False, "invalid"
    if swap_peak_mb > 0:
        return False, "noise"
    if ram_available_start_mb < (driver_memory_mb + noise_overhead_mb):
        return False, "noise"
    return True, None


def _csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _expand_json_fields_for_json_file(row: Mapping[str, Any]) -> dict[str, Any]:
    """ADR-0007 B8: JSON giữ cùng tên trường như CSV, `*_json` mở thành đối tượng lồng nhau."""
    out: dict[str, Any] = {}
    for key, val in row.items():
        if key.endswith("_json") and isinstance(val, str) and val:
            try:
                out[key] = json.loads(val)
                continue
            except json.JSONDecodeError:
                pass
        out[key] = val
    return out


def build_summary_rows(runs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Tổng hợp trung vị và min–max chỉ trên các hàng `counted = true` (ADR-0007 B6, B8)."""
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for r in runs:
        cid = str(r.get("config_id") or "")
        groups.setdefault(cid, []).append(r)

    summary_rows: list[dict[str, Any]] = []
    metric_keys = ("wall_seconds", "throughput_value", "cpu_peak_pct", "ram_peak_mb")

    for cid, items in groups.items():
        first = items[0]
        counted_items = [r for r in items if bool(r.get("counted"))]
        n_total = len(items)
        n_counted = len(counted_items)
        n_warmup = sum(1 for r in items if r.get("phase") == "warmup")
        n_error = sum(1 for r in items if r.get("phase") != "warmup" and r.get("status") == "error")
        n_invalid = sum(
            1 for r in items if r.get("phase") != "warmup" and r.get("status") == "invalid"
        )
        n_noise = sum(
            1 for r in items if r.get("phase") != "warmup" and r.get("exclusion_reason") == "noise"
        )

        row: dict[str, Any] = {
            "batch_id": first.get("batch_id"),
            "experiment": first.get("experiment"),
            "config_id": cid,
            "variable_name": first.get("variable_name"),
            "variable_value": first.get("variable_value"),
            "variable2_name": first.get("variable2_name"),
            "variable2_value": first.get("variable2_value"),
            "commit_sha": first.get("commit_sha"),
            "config_hash": first.get("config_hash"),
            "n_total": n_total,
            "n_counted": n_counted,
            "n_warmup": n_warmup,
            "n_error": n_error,
            "n_invalid": n_invalid,
            "n_noise": n_noise,
        }
        for mkey in metric_keys:
            vals = [
                float(r[mkey])
                for r in counted_items
                if r.get(mkey) is not None and r.get(mkey) != ""
            ]
            row[f"{mkey}_median"] = compute_median(vals)
            row[f"{mkey}_min"] = round(min(vals), 6) if vals else None
            row[f"{mkey}_max"] = round(max(vals), 6) if vals else None
        summary_rows.append(row)
    return summary_rows


def _chart_label(row: Mapping[str, Any]) -> str:
    base = f"{row.get('experiment')}:{row.get('variable_value')}"
    v2 = row.get("variable2_value")
    return f"{base}/{v2}" if v2 is not None and v2 != "" else base


def render_charts(
    charts_dir: Path,
    summary_rows: Sequence[Mapping[str, Any]],
    *,
    batch_id: str,
    commit_sha: str,
) -> list[Path]:
    """Vẽ biểu đồ từ `summary_rows` bằng matplotlib (backend Agg)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    charts_dir.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    short_sha = commit_sha[:10] if commit_sha else "unknown"
    footer = f"batch_id={batch_id} | commit={short_sha}"

    labels = [_chart_label(r) for r in summary_rows if r.get("wall_seconds_median") is not None]
    medians = [
        float(r["wall_seconds_median"])
        for r in summary_rows
        if r.get("wall_seconds_median") is not None
    ]
    mins = [
        float(r["wall_seconds_min"])
        for r in summary_rows
        if r.get("wall_seconds_median") is not None
    ]
    maxs = [
        float(r["wall_seconds_max"])
        for r in summary_rows
        if r.get("wall_seconds_median") is not None
    ]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    if labels:
        x = list(range(len(labels)))
        yerr_low = [max(0.0, m - lo) for m, lo in zip(medians, mins, strict=True)]
        yerr_high = [max(0.0, hi - m) for m, hi in zip(medians, maxs, strict=True)]
        ax.bar(x, medians, yerr=[yerr_low, yerr_high], capsize=4, color="#2563eb", alpha=0.85)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=20, ha="right")
        ax.set_ylabel("Wall time (seconds)")
    else:
        ax.text(
            0.5,
            0.5,
            "No counted runs (n_counted = 0)",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
    ax.set_title("Benchmark Wall Time (Median & Min-Max)")
    fig.text(0.01, 0.01, footer, fontsize=8, color="#475569")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    wall_chart = charts_dir / "wall_seconds.png"
    fig.savefig(wall_chart, dpi=120)
    plt.close(fig)
    generated.append(wall_chart)

    tp_labels = [
        _chart_label(r) for r in summary_rows if r.get("throughput_value_median") is not None
    ]
    tp_medians = [
        float(r["throughput_value_median"])
        for r in summary_rows
        if r.get("throughput_value_median") is not None
    ]
    fig2, ax2 = plt.subplots(figsize=(8, 4.5))
    if tp_labels:
        x2 = list(range(len(tp_labels)))
        ax2.bar(x2, tp_medians, color="#059669", alpha=0.85)
        ax2.set_xticks(x2)
        ax2.set_xticklabels(tp_labels, rotation=20, ha="right")
        ax2.set_ylabel("Throughput (records/s)")
    else:
        ax2.text(
            0.5,
            0.5,
            "No counted runs (n_counted = 0)",
            ha="center",
            va="center",
            transform=ax2.transAxes,
        )
    ax2.set_title("Benchmark Throughput (Median)")
    fig2.text(0.01, 0.01, footer, fontsize=8, color="#475569")
    fig2.tight_layout(rect=(0, 0.03, 1, 1))
    tp_chart = charts_dir / "throughput.png"
    fig2.savefig(tp_chart, dpi=120)
    plt.close(fig2)
    generated.append(tp_chart)

    return generated


def write_benchmark_bundle(
    output_dir: Path,
    *,
    runs: Sequence[Mapping[str, Any]],
    metrics: Sequence[MetricRow],
    not_run: Sequence[NotRunRecord],
    environment: Mapping[str, Any],
    batch_id: str,
    commit_sha: str,
) -> dict[str, Path]:
    """Ghi toàn bộ tệp kết quả vào `output_dir` theo ADR-0007 B8."""
    output_dir.mkdir(parents=True, exist_ok=True)

    runs_csv = output_dir / "runs.csv"
    with runs_csv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(RUNS_COLUMNS)
        for row in runs:
            writer.writerow([_csv_cell(row.get(col)) for col in RUNS_COLUMNS])

    runs_json = output_dir / "runs.json"
    runs_json_payload = [_expand_json_fields_for_json_file(r) for r in runs]
    runs_json.write_text(
        json.dumps(runs_json_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    metrics_csv = output_dir / "metrics.csv"
    with metrics_csv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(METRICS_COLUMNS)
        for m in metrics:
            writer.writerow([m.run_id, m.metric, m.value, m.unit, m.scope])

    not_run_csv = output_dir / "not_run.csv"
    with not_run_csv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(NOT_RUN_COLUMNS)
        for nr in not_run:
            writer.writerow(
                [
                    nr.experiment,
                    nr.level,
                    nr.reason_code,
                    nr.measured_evidence_json,
                    _csv_cell(nr.highest_level_run),
                ]
            )

    summary_rows = build_summary_rows(runs)
    summary_csv = output_dir / "summary.csv"
    summary_cols = (
        list(summary_rows[0].keys())
        if summary_rows
        else [
            "batch_id",
            "experiment",
            "config_id",
            "variable_name",
            "variable_value",
            "variable2_name",
            "variable2_value",
            "commit_sha",
            "config_hash",
            "n_total",
            "n_counted",
            "n_warmup",
            "n_error",
            "n_invalid",
            "n_noise",
            "wall_seconds_median",
            "wall_seconds_min",
            "wall_seconds_max",
            "throughput_value_median",
            "throughput_value_min",
            "throughput_value_max",
            "cpu_peak_pct_median",
            "cpu_peak_pct_min",
            "cpu_peak_pct_max",
            "ram_peak_mb_median",
            "ram_peak_mb_min",
            "ram_peak_mb_max",
        ]
    )
    with summary_csv.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(summary_cols)
        for srow in summary_rows:
            writer.writerow([_csv_cell(srow.get(c)) for c in summary_cols])

    summary_json = output_dir / "summary.json"
    summary_json_payload = {
        "batch_id": batch_id,
        "commit_sha": commit_sha,
        "summary": summary_rows,
        "not_run": [asdict(nr) for nr in not_run],
    }
    summary_json.write_text(
        json.dumps(summary_json_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    env_json = output_dir / "environment.json"
    env_json.write_text(
        json.dumps(environment, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    charts = render_charts(
        output_dir / "charts",
        summary_rows,
        batch_id=batch_id,
        commit_sha=commit_sha,
    )

    return {
        "runs_csv": runs_csv,
        "runs_json": runs_json,
        "metrics_csv": metrics_csv,
        "not_run_csv": not_run_csv,
        "summary_csv": summary_csv,
        "summary_json": summary_json,
        "environment_json": env_json,
        "charts_dir": output_dir / "charts",
        "wall_chart": charts[0],
        "throughput_chart": charts[1],
    }
