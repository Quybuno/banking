"""Integration test: chạy thật `n06 benchmark --experiment E1 --level <smoke> --repeat 1`."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from n06.cli import main
from n06.cli.init_local import parse_dotenv_file
from n06.config import REPO_ROOT, load_config
from n06.observability.results import RUNS_COLUMNS
from n06.storage.object_store import ensure_bucket, target_from_config


def test_benchmark_e1_smoke_end_to_end(tmp_path: Path) -> None:
    env_path = REPO_ROOT / ".env"
    merged_env = parse_dotenv_file(env_path) if env_path.is_file() else {}
    cfg = load_config(profile="benchmark", env=merged_env)
    smoke_level = cfg.benchmark.smoke_level

    # Bảo đảm bucket benchmark tồn tại trên MinIO trước khi đo.
    target = target_from_config(cfg, merged_env)
    bucket_res = ensure_bucket(target, cfg.object_store.bucket)
    assert bucket_res.ready, f"Bucket benchmark chưa sẵn sàng: {bucket_res}"

    out_dir = tmp_path / "bench_out"
    code = main(
        [
            "benchmark",
            "--experiment",
            "E1",
            "--level",
            str(smoke_level),
            "--repeat",
            "1",
            "--warmup",
            "0",
            "--profile",
            "benchmark",
            "--output-dir",
            str(out_dir),
            "--allow-dirty",
            "--json",
        ]
    )
    assert code == 0

    runs_csv = out_dir / "runs.csv"
    runs_json = out_dir / "runs.json"
    metrics_csv = out_dir / "metrics.csv"
    summary_csv = out_dir / "summary.csv"
    summary_json = out_dir / "summary.json"
    env_json = out_dir / "environment.json"
    wall_chart = out_dir / "charts" / "wall_seconds.png"

    artifacts = (runs_csv, runs_json, metrics_csv, summary_csv, summary_json, env_json, wall_chart)
    for artifact in artifacts:
        assert artifact.is_file(), f"Thiếu tệp kết quả: {artifact}"

    with runs_csv.open(encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    assert tuple(rows[0]) == RUNS_COLUMNS
    assert len(rows) >= 2

    parsed_runs = json.loads(runs_json.read_text(encoding="utf-8"))
    assert 1 <= len(parsed_runs) <= 1 + cfg.benchmark.max_makeup_runs
    assert parsed_runs[0]["experiment"] == "E1"
    assert parsed_runs[0]["status"] == "ok"
    assert len(parsed_runs[0]["commit_sha"]) == 40
    assert len(parsed_runs[0]["config_hash"]) == 64

    # Thư mục tạm sinh dữ liệu phải được dọn sau khi chạy xong.
    assert not (out_dir / f"_tmp_dataset_{smoke_level}").exists()
