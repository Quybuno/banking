"""Điều phối chạy `n06 benchmark` theo ADR-0007 (B1–B9)."""

from __future__ import annotations

import random
import shutil
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from n06.cli.doctor import ExitCode
from n06.config import REPO_ROOT, AppConfig, load_config
from n06.limits import resolve_disk_probe
from n06.observability.event_log import find_latest_event_log, parse_spark_event_log
from n06.observability.results import (
    MetricRow,
    NotRunRecord,
    canonical_json,
    compute_config_hash,
    evaluate_run_counting,
    write_benchmark_bundle,
)
from n06.observability.sampler import (
    ResourceSampler,
    collect_environment_info,
    parse_driver_memory_mb,
    write_timeseries_csv,
)
from n06.observability.storage_metrics import measure_object_store_layers
from n06.observability.workloads import (
    DatasetPrepResult,
    WorkloadNotRegisteredError,
    WorkloadRegistry,
    default_generator_config_path,
    default_workload_registry,
    prepare_benchmark_dataset,
)


@dataclass(frozen=True)
class BenchmarkRunSummary:
    """Kết quả tổng của một phiên `n06 benchmark`."""

    batch_id: str
    output_dir: Path
    exit_code: int
    total_runs: int
    counted_runs: int
    written_files: dict[str, Path]


def _make_run_record(
    *,
    batch_id: str,
    run_id: str,
    experiment: str,
    config_id: str,
    variable_name: str,
    variable_value: Any,
    variable2_name: str | None,
    variable2_value: Any,
    repetition: int,
    run_order: int,
    phase: str,
    cache_state: str,
    shared_with: str | None,
    status: str,
    counted: bool,
    exclusion_reason: str | None,
    error_type: str | None,
    error_message: str | None,
    exit_code: int,
    started_at: str,
    ended_at: str,
    wall_seconds: float,
    phase_seconds: dict[str, float],
    env_info: dict[str, Any],
    cfg: AppConfig,
    config_hash: str,
    config_json: str,
    dataset: DatasetPrepResult,
    as_of_time: str,
    correctness_ok: bool,
    unique_key_count: int | None,
    totals_by_currency: dict[str, int],
    status_counts: dict[str, int],
    semantic_hash: str | None,
    gt_match: bool | None,
    throughput_value: float,
    throughput_entity: str,
    throughput_scope: str,
    cpu_peak_pct: float,
    cpu_avg_pct: float,
    ram_peak_mb: float,
    ram_avg_mb: float,
    ram_scope: str,
    swap_peak_mb: float,
    noise_note: str | None = None,
) -> dict[str, Any]:
    versions = env_info.get("versions") or {}
    return {
        "batch_id": batch_id,
        "run_id": run_id,
        "experiment": experiment,
        "config_id": config_id,
        "variable_name": variable_name,
        "variable_value": variable_value,
        "variable2_name": variable2_name,
        "variable2_value": variable2_value,
        "repetition": repetition,
        "run_order": run_order,
        "phase": phase,
        "cache_state": cache_state,
        "shared_with": shared_with,
        "status": status,
        "counted": counted,
        "exclusion_reason": exclusion_reason,
        "error_type": error_type,
        "error_message": error_message,
        "exit_code": exit_code,
        "started_at": started_at,
        "ended_at": ended_at,
        "wall_seconds": round(wall_seconds, 4),
        "phase_seconds_json": canonical_json(phase_seconds),
        "startup_seconds": None,
        "end_to_end_seconds": round(wall_seconds, 4),
        "host_os": env_info.get("host_os"),
        "kernel": env_info.get("kernel"),
        "cpu_model": env_info.get("cpu_model"),
        "cpu_logical": env_info.get("cpu_logical"),
        "ram_total_mb": env_info.get("ram_total_mb"),
        "swap_total_mb": env_info.get("swap_total_mb"),
        "disk_free_bytes_start": env_info.get("disk_free_bytes_start"),
        "container_limits_json": canonical_json(env_info.get("container_limits") or {}),
        "services_stopped": env_info.get("services_stopped"),
        "os_cache_dropped": env_info.get("os_cache_dropped"),
        "clock_source": env_info.get("clock_source"),
        "noise_note": noise_note,
        "n06_version": versions.get("n06_version"),
        "python": versions.get("python"),
        "spark": versions.get("spark"),
        "hadoop": versions.get("hadoop"),
        "scala": versions.get("scala"),
        "java": versions.get("java"),
        "kafka": versions.get("kafka"),
        "minio_image": versions.get("minio_image"),
        "postgres_image": versions.get("postgres_image"),
        "duckdb": versions.get("duckdb"),
        "profile": cfg.profile,
        "commit_sha": env_info.get("commit_sha"),
        "git_dirty": env_info.get("git_dirty"),
        "dataset_id": cfg.dataset.dataset_id,
        "transaction_count": dataset.transaction_count,
        "seed": dataset.seed,
        "input_manifest_path": dataset.manifest_path,
        "input_manifest_sha256": dataset.manifest_sha256,
        "source_batch_id": batch_id,
        "as_of_time": as_of_time,
        "config_hash": config_hash,
        "config_json": config_json,
        "correctness_ok": correctness_ok,
        "unique_key_count": unique_key_count,
        "totals_by_currency_json": canonical_json(totals_by_currency),
        "status_counts_json": canonical_json(status_counts),
        "semantic_hash": semantic_hash,
        "ground_" + "truth_match": gt_match,
        "throughput_value": round(throughput_value, 4),
        "throughput_unit": f"{throughput_entity}/s",
        "throughput_entity": throughput_entity,
        "throughput_scope": throughput_scope,
        "cpu_peak_pct": cpu_peak_pct,
        "cpu_avg_pct": cpu_avg_pct,
        "ram_peak_mb": ram_peak_mb,
        "ram_avg_mb": ram_avg_mb,
        "ram_scope": ram_scope,
        "swap_peak_mb": swap_peak_mb,
    }


def run_benchmark(
    *,
    experiment: str,
    levels: Sequence[int] | None = None,
    repetitions: int | None = None,
    warmup: int | None = None,
    seed: int | None = None,
    as_of_time: str | None = None,
    profile: str = "benchmark",
    output_dir: Path | None = None,
    allow_dirty: bool = False,
    share_baseline: bool = False,
    generator_config: Path | None = None,
    registry: WorkloadRegistry | None = None,
    env_file: Path | None = None,
    env: dict[str, str] | None = None,
    keep_generated_data: bool = False,
) -> BenchmarkRunSummary:
    """Thực thi phiên benchmark và ghi kết quả đúng chuẩn ADR-0007."""
    cfg = load_config(profile=profile, env=env)
    reg = registry or default_workload_registry()
    spec = reg.get(experiment)

    effective_levels = list(levels) if levels else [cfg.benchmark.smoke_level]
    effective_reps = repetitions if repetitions is not None else cfg.benchmark.repetitions
    effective_warmup = warmup if warmup is not None else cfg.benchmark.warmup
    effective_seed = seed if seed is not None else cfg.dataset.seed
    effective_as_of = as_of_time or f"{cfg.dataset.to_date}T23:59:59Z"

    batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out_root = (
        output_dir
        if output_dir is not None
        else (REPO_ROOT / cfg.paths.reports_dir / "benchmark" / batch_id)
    )
    out_root.mkdir(parents=True, exist_ok=True)
    ts_dir = out_root / "timeseries"
    ts_dir.mkdir(parents=True, exist_ok=True)

    disk_probe = resolve_disk_probe(cfg.limits, out_root, env=env, target_path=out_root)
    disk_free_start = disk_probe.free_bytes or 0
    env_info = collect_environment_info(
        cfg,
        repo_root=REPO_ROOT,
        disk_free_bytes=disk_free_start,
    )

    if env_info.get("git_dirty") and not allow_dirty:
        raise RuntimeError(
            "Cây làm việc Git đang bẩn (git_dirty=true); commit hoặc truyền --allow-dirty."
        )

    config_hash, config_json = compute_config_hash(cfg)
    driver_mb = parse_driver_memory_mb(cfg.spark.driver_memory)
    gen_cfg_path = generator_config or default_generator_config_path()

    all_runs: list[dict[str, Any]] = []
    all_metrics: list[MetricRow] = []
    not_run_records: list[NotRunRecord] = []
    datasets_to_cleanup: list[Path] = []
    highest_level_ok: int | None = None

    prepared_by_level: dict[int, DatasetPrepResult] = {}
    for lvl in effective_levels:
        tx_count = lvl if spec.experiment == "E1" else cfg.dataset.transaction_count
        ds_dir = out_root / f"_tmp_dataset_{lvl}"
        datasets_to_cleanup.append(ds_dir)
        prep = prepare_benchmark_dataset(
            cfg,
            generator_config=gen_cfg_path,
            out_dir=ds_dir,
            transactions=tx_count,
            seed=effective_seed,
            env_file=env_file,
            env=env,
        )
        if prep.exit_code != 0:
            reason = "GUARD_REJECTED" if prep.exit_code == int(ExitCode.CAPACITY_ERROR) else "OTHER"
            not_run_records.append(
                NotRunRecord(
                    experiment=spec.experiment,
                    level=lvl,
                    reason_code=reason,
                    measured_evidence_json=canonical_json(
                        {
                            "exit_code": prep.exit_code,
                            "disk_free_bytes_start": disk_free_start,
                            "error": prep.error_message,
                        }
                    ),
                    highest_level_run=highest_level_ok,
                )
            )
            continue
        prepared_by_level[lvl] = prep

    # Xen kẽ các cấu hình theo vòng lặp xác định bằng seed (ADR-0007 B2.2).
    rng = random.Random(effective_seed)
    run_order = 0

    try:
        # Warm-up (repetition = 0)
        if effective_warmup > 0:
            order_levels = list(prepared_by_level.keys())
            rng.shuffle(order_levels)
            for lvl in order_levels:
                for w_idx in range(effective_warmup):
                    run_order += 1
                    run_id = f"{batch_id}-{spec.experiment}-{lvl}-w{w_idx}"
                    ds = prepared_by_level[lvl]
                    row, run_metrics = _execute_single_run(
                        cfg=cfg,
                        spec_exp=spec.experiment,
                        var_name=spec.variable_name,
                        var_val=lvl,
                        var2_name=spec.variable2_name,
                        var2_val=None,
                        ram_scope=spec.ram_scope,
                        runner_fn=spec.runner,
                        dataset=ds,
                        batch_id=batch_id,
                        run_id=run_id,
                        config_id=f"{spec.experiment}-{lvl}",
                        repetition=0,
                        run_order=run_order,
                        phase="warmup",
                        cache_state="cold",
                        shared_with=None,
                        env_info=env_info,
                        config_hash=config_hash,
                        config_json=config_json,
                        as_of_time=effective_as_of,
                        driver_mb=driver_mb,
                        ts_dir=ts_dir,
                        env_file=env_file,
                        env=env,
                    )
                    all_runs.append(row)
                    all_metrics.extend(run_metrics)

        # Measured repetitions
        for rep in range(1, effective_reps + 1):
            order_levels = list(prepared_by_level.keys())
            rng.shuffle(order_levels)
            for lvl in order_levels:
                run_order += 1
                run_id = f"{batch_id}-{spec.experiment}-{lvl}-r{rep}"
                ds = prepared_by_level[lvl]
                row, run_metrics = _execute_single_run(
                    cfg=cfg,
                    spec_exp=spec.experiment,
                    var_name=spec.variable_name,
                    var_val=lvl,
                    var2_name=spec.variable2_name,
                    var2_val=None,
                    ram_scope=spec.ram_scope,
                    runner_fn=spec.runner,
                    dataset=ds,
                    batch_id=batch_id,
                    run_id=run_id,
                    config_id=f"{spec.experiment}-{lvl}",
                    repetition=rep,
                    run_order=run_order,
                    phase="measured",
                    cache_state="warm",
                    shared_with="baseline" if share_baseline else None,
                    env_info=env_info,
                    config_hash=config_hash,
                    config_json=config_json,
                    as_of_time=effective_as_of,
                    driver_mb=driver_mb,
                    ts_dir=ts_dir,
                    env_file=env_file,
                    env=env,
                )
                all_runs.append(row)
                all_metrics.extend(run_metrics)
                if row["status"] == "ok" and row["correctness_ok"]:
                    highest_level_ok = (
                        lvl if highest_level_ok is None else max(highest_level_ok, lvl)
                    )
    finally:
        if not keep_generated_data:
            for d in datasets_to_cleanup:
                if d.exists():
                    shutil.rmtree(d, ignore_errors=True)

    written = write_benchmark_bundle(
        out_root,
        runs=all_runs,
        metrics=all_metrics,
        not_run=not_run_records,
        environment=env_info,
        batch_id=batch_id,
        commit_sha=str(env_info.get("commit_sha") or "unknown"),
    )
    counted_runs = sum(1 for r in all_runs if bool(r.get("counted")))
    return BenchmarkRunSummary(
        batch_id=batch_id,
        output_dir=out_root,
        exit_code=int(ExitCode.OK),
        total_runs=len(all_runs),
        counted_runs=counted_runs,
        written_files=written,
    )


def _execute_single_run(
    *,
    cfg: AppConfig,
    spec_exp: str,
    var_name: str,
    var_val: int,
    var2_name: str | None,
    var2_val: Any,
    ram_scope: str,
    runner_fn: Any,
    dataset: DatasetPrepResult,
    batch_id: str,
    run_id: str,
    config_id: str,
    repetition: int,
    run_order: int,
    phase: str,
    cache_state: str,
    shared_with: str | None,
    env_info: dict[str, Any],
    config_hash: str,
    config_json: str,
    as_of_time: str,
    driver_mb: float,
    ts_dir: Path,
    env_file: Path | None = None,
    env: dict[str, str] | None = None,
) -> tuple[dict[str, Any], list[MetricRow]]:
    sampler = ResourceSampler(interval_seconds=cfg.benchmark.sample_interval_seconds)
    started_dt = datetime.now(UTC)
    started_at = started_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_epoch = time.time()
    t0 = time.monotonic()

    sampler.start()
    try:
        res = runner_fn(
            cfg,
            dataset,
            run_id,
            var_val,
            {
                "experiment": spec_exp,
                "kafka_partitions": (var_val if spec_exp == "E2" else cfg.kafka.num_partitions),
                "env_file": env_file,
                "env": env,
            },
        )
    except Exception as exc:
        from n06.observability.workloads import WorkloadResult

        res = WorkloadResult(
            exit_code=1,
            status="error",
            phase_seconds={},
            records_processed=0,
            throughput_entity="transaction",
            throughput_scope="error",
            correctness_ok=False,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
    finally:
        sampler.stop()

    wall_seconds = max(1e-6, time.monotonic() - t0)
    ended_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    samples = sampler.samples()
    write_timeseries_csv(ts_dir / f"{run_id}.csv", samples)
    samp_sum = sampler.summarize(ram_scope=ram_scope)

    counted, exclusion_reason = evaluate_run_counting(
        phase=phase,
        status=res.status,
        correctness_ok=res.correctness_ok,
        swap_peak_mb=samp_sum.swap_peak_mb,
        ram_available_start_mb=samp_sum.ram_available_start_mb,
        driver_memory_mb=driver_mb,
        noise_overhead_mb=cfg.benchmark.noise_min_free_ram_overhead_mb,
    )

    metrics: list[MetricRow] = [
        MetricRow(run_id, "generation_seconds", dataset.generation_seconds, "seconds", "dataset"),
        MetricRow(run_id, "generator_peak_rss_mb", dataset.generator_peak_rss_mb, "MiB", "dataset"),
    ]
    for m_name, m_val, m_unit, m_scope in res.extra_metrics:
        metrics.append(MetricRow(run_id, m_name, m_val, m_unit, m_scope))

    if cfg.spark.event_log_enabled:
        elog_dir = Path(cfg.spark.event_log_dir)
        resolved_elog = elog_dir if elog_dir.is_absolute() else (REPO_ROOT / elog_dir)
        latest_elog = find_latest_event_log(resolved_elog, after_mtime=start_epoch - 2.0)
        if latest_elog is not None:
            sp = parse_spark_event_log(latest_elog)
            spark_rows = [
                ("spark_input_rows", sp.input_rows, "rows"),
                ("spark_output_rows", sp.output_rows, "rows"),
                ("spark_shuffle_read_bytes", sp.shuffle_read_bytes, "bytes"),
                ("spark_shuffle_write_bytes", sp.shuffle_write_bytes, "bytes"),
                ("spark_spill_memory_bytes", sp.spill_memory_bytes, "bytes"),
                ("spark_spill_disk_bytes", sp.spill_disk_bytes, "bytes"),
                ("spark_stage_count", sp.stage_count, "count"),
                ("spark_stage_duration_total_ms", sp.stage_duration_total_ms, "ms"),
                ("spark_stage_duration_max_ms", sp.stage_duration_max_ms, "ms"),
                ("spark_task_count", sp.task_count, "count"),
                ("spark_task_duration_median_ms", sp.task_duration_median_ms, "ms"),
                ("spark_task_duration_max_ms", sp.task_duration_max_ms, "ms"),
                ("spark_task_max_median_ratio", sp.task_max_median_ratio, "ratio"),
                ("spark_event_log_ref", sp.spark_event_log_ref, "path"),
            ]
            for sname, sval, sunit in spark_rows:
                metrics.append(MetricRow(run_id, sname, sval, sunit, "spark"))

    for layer in measure_object_store_layers(cfg, env=env):
        if layer.object_count > 0:
            metrics.extend(
                [
                    MetricRow(
                        run_id,
                        f"storage_{layer.layer}_objects",
                        layer.object_count,
                        "count",
                        "storage",
                    ),
                    MetricRow(
                        run_id,
                        f"storage_{layer.layer}_bytes",
                        layer.total_bytes,
                        "bytes",
                        "storage",
                    ),
                    MetricRow(
                        run_id,
                        f"storage_{layer.layer}_tiny_files",
                        layer.tiny_file_count,
                        "count",
                        "storage",
                    ),
                ]
            )

    throughput_value = res.records_processed / wall_seconds if res.records_processed > 0 else 0.0
    row = _make_run_record(
        batch_id=batch_id,
        run_id=run_id,
        experiment=spec_exp,
        config_id=config_id,
        variable_name=var_name,
        variable_value=var_val,
        variable2_name=var2_name,
        variable2_value=var2_val,
        repetition=repetition,
        run_order=run_order,
        phase=phase,
        cache_state=cache_state,
        shared_with=shared_with,
        status=res.status,
        counted=counted,
        exclusion_reason=exclusion_reason,
        error_type=res.error_type,
        error_message=res.error_message,
        exit_code=res.exit_code,
        started_at=started_at,
        ended_at=ended_at,
        wall_seconds=wall_seconds,
        phase_seconds=res.phase_seconds,
        env_info=env_info,
        cfg=cfg,
        config_hash=config_hash,
        config_json=config_json,
        dataset=dataset,
        as_of_time=as_of_time,
        correctness_ok=res.correctness_ok,
        unique_key_count=res.unique_key_count,
        totals_by_currency=res.totals_by_currency,
        status_counts=res.status_counts,
        semantic_hash=res.semantic_hash,
        gt_match=res.oracle_match,
        throughput_value=throughput_value,
        throughput_entity=res.throughput_entity,
        throughput_scope=res.throughput_scope,
        cpu_peak_pct=samp_sum.cpu_peak_pct,
        cpu_avg_pct=samp_sum.cpu_avg_pct,
        ram_peak_mb=samp_sum.ram_peak_mb,
        ram_avg_mb=samp_sum.ram_avg_mb,
        ram_scope=samp_sum.ram_scope,
        swap_peak_mb=samp_sum.swap_peak_mb,
        noise_note="noise" if exclusion_reason == "noise" else None,
    )
    return row, metrics


__all__ = [
    "BenchmarkRunSummary",
    "WorkloadNotRegisteredError",
    "run_benchmark",
]
