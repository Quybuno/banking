"""Unit test cho writer kết quả benchmark (cột ADR-0007, config_hash, commit_sha, đếm)."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from n06.config import REPO_ROOT, load_config
from n06.observability.results import (
    RUNS_COLUMNS,
    MetricRow,
    NotRunRecord,
    build_summary_rows,
    compute_config_hash,
    evaluate_run_counting,
    write_benchmark_bundle,
)
from n06.observability.sampler import collect_git_state

CREDENTIALS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "secret-ak-value",
    "N06_OBJECT_STORE_SECRET_KEY": "secret-sk-value",
    "N06_POSTGRES_MART_PASSWORD": "secret-mart-pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "secret-ctrl-pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "secret-af-pw",
    "N06_POSTGRES_METABASE_PASSWORD": "secret-mb-pw",
}


def test_config_hash_is_stable_and_redacts_secrets() -> None:
    cfg1 = load_config(profile="benchmark", env=CREDENTIALS)
    cfg2 = load_config(
        profile="benchmark",
        env={**CREDENTIALS, "N06_POSTGRES_MART_PASSWORD": "different-password"},
    )
    hash1, json1 = compute_config_hash(cfg1)
    hash2, json2 = compute_config_hash(cfg2)

    assert len(hash1) == 64
    assert hash1 == hash2
    assert json1 == json2
    for secret in CREDENTIALS.values():
        assert secret not in json1


def test_real_commit_sha_from_repo() -> None:
    sha, _dirty = collect_git_state(REPO_ROOT)
    assert len(sha) == 40
    assert all(c in "0123456789abcdef" for c in sha)


def test_evaluate_run_counting_rules() -> None:
    assert evaluate_run_counting(
        phase="warmup",
        status="ok",
        correctness_ok=True,
        swap_peak_mb=0.0,
        ram_available_start_mb=8000.0,
        driver_memory_mb=2048.0,
        noise_overhead_mb=2048,
    ) == (False, "warmup")

    assert evaluate_run_counting(
        phase="measured",
        status="invalid",
        correctness_ok=False,
        swap_peak_mb=0.0,
        ram_available_start_mb=8000.0,
        driver_memory_mb=2048.0,
        noise_overhead_mb=2048,
    ) == (False, "invalid")

    assert evaluate_run_counting(
        phase="measured",
        status="ok",
        correctness_ok=True,
        swap_peak_mb=12.0,
        ram_available_start_mb=8000.0,
        driver_memory_mb=2048.0,
        noise_overhead_mb=2048,
    ) == (False, "noise")

    assert evaluate_run_counting(
        phase="measured",
        status="ok",
        correctness_ok=True,
        swap_peak_mb=0.0,
        ram_available_start_mb=3000.0,
        driver_memory_mb=2048.0,
        noise_overhead_mb=2048,
    ) == (False, "noise")

    assert evaluate_run_counting(
        phase="measured",
        status="ok",
        correctness_ok=True,
        swap_peak_mb=0.0,
        ram_available_start_mb=5000.0,
        driver_memory_mb=2048.0,
        noise_overhead_mb=2048,
    ) == (True, None)


def test_writer_outputs_all_required_columns_and_excludes_uncounted(tmp_path: Path) -> None:
    sha, _ = collect_git_state(REPO_ROOT)
    base = {col: None for col in RUNS_COLUMNS}
    base.update(
        {
            "batch_id": "20261128T100000Z",
            "experiment": "E1",
            "config_id": "E1-1000",
            "variable_name": "transaction_count",
            "variable_value": 1000,
            "commit_sha": sha,
            "config_hash": "abc123",
            "phase_seconds_json": '{"ingest": 10.0}',
            "totals_by_currency_json": '{"VND": 100000}',
        }
    )

    r_warmup = {
        **base,
        "run_id": "r0",
        "phase": "warmup",
        "status": "ok",
        "counted": False,
        "wall_seconds": 99.0,
        "throughput_value": 10.0,
        "cpu_peak_pct": 50.0,
        "ram_peak_mb": 500.0,
    }
    r_invalid = {
        **base,
        "run_id": "r_bad",
        "phase": "measured",
        "status": "invalid",
        "counted": False,
        "wall_seconds": 1.0,
        "throughput_value": 9999.0,
        "cpu_peak_pct": 10.0,
        "ram_peak_mb": 100.0,
    }
    r1 = {
        **base,
        "run_id": "r1",
        "phase": "measured",
        "status": "ok",
        "counted": True,
        "wall_seconds": 10.0,
        "throughput_value": 100.0,
        "cpu_peak_pct": 40.0,
        "ram_peak_mb": 400.0,
    }
    r2 = {
        **base,
        "run_id": "r2",
        "phase": "measured",
        "status": "ok",
        "counted": True,
        "wall_seconds": 20.0,
        "throughput_value": 50.0,
        "cpu_peak_pct": 60.0,
        "ram_peak_mb": 600.0,
    }

    summary_rows = build_summary_rows([r_warmup, r_invalid, r1, r2])
    assert len(summary_rows) == 1
    assert summary_rows[0]["n_total"] == 4
    assert summary_rows[0]["n_counted"] == 2
    assert summary_rows[0]["n_warmup"] == 1
    assert summary_rows[0]["n_invalid"] == 1
    assert summary_rows[0]["wall_seconds_median"] == 15.0
    assert summary_rows[0]["wall_seconds_min"] == 10.0
    assert summary_rows[0]["wall_seconds_max"] == 20.0

    written = write_benchmark_bundle(
        tmp_path,
        runs=[r_warmup, r_invalid, r1, r2],
        metrics=[MetricRow("r1", "generation_seconds", 1.2, "seconds", "dataset")],
        not_run=[NotRunRecord("E1", 5000000, "DISK_FULL", '{"free":0}', 1000000)],
        environment={"ram_total_mb": 8935},
        batch_id="20261128T100000Z",
        commit_sha=sha,
    )

    with written["runs_csv"].open(encoding="utf-8") as fh:
        header = next(csv.reader(fh))
    assert tuple(header) == RUNS_COLUMNS

    runs_json = json.loads(written["runs_json"].read_text(encoding="utf-8"))
    assert len(runs_json) == 4
    assert isinstance(runs_json[0]["phase_seconds_json"], dict)

    assert written["wall_chart"].is_file() and written["wall_chart"].stat().st_size > 0
    assert written["throughput_chart"].is_file() and written["throughput_chart"].stat().st_size > 0


def test_benchmark_float_env_override() -> None:
    import pytest

    from n06.config import ConfigValidationError

    cfg = load_config(
        profile="benchmark",
        env={**CREDENTIALS, "N06__BENCHMARK__SAMPLE_INTERVAL_SECONDS": "0.5"},
    )
    assert cfg.benchmark.sample_interval_seconds == 0.5

    with pytest.raises(ConfigValidationError):
        load_config(
            profile="benchmark",
            env={**CREDENTIALS, "N06__BENCHMARK__SAMPLE_INTERVAL_SECONDS": "abc"},
        )
