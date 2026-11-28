"""Integration test: chạy job Spark thật trong container, sinh event log và parse bằng parser."""

from __future__ import annotations

import shutil
import subprocess
import uuid
from pathlib import Path

from n06.observability.event_log import find_latest_event_log, parse_spark_event_log


def test_parse_real_spark_event_log_from_container(tmp_path: Path) -> None:
    docker_bin = shutil.which("docker")
    assert docker_bin is not None, "Cần docker để chạy integration test Spark event log"

    tag = uuid.uuid4().hex[:8]
    c_script = f"/tmp/n06_elog_job_{tag}.py"
    c_elog_dir = f"/tmp/n06_elog_dir_{tag}"
    c_out_dir = f"/tmp/n06_elog_out_{tag}"

    pyspark_code = (
        "from pyspark.sql import SparkSession\n"
        f"spark = (SparkSession.builder.appName('n06-elog-{tag}').master('local[2]')\n"
        "    .config('spark.ui.enabled', 'false')\n"
        "    .config('spark.eventLog.enabled', 'true')\n"
        f"    .config('spark.eventLog.dir', 'file://{c_elog_dir}')\n"
        "    .config('spark.sql.shuffle.partitions', '4')\n"
        "    .getOrCreate())\n"
        "df = spark.range(0, 200).selectExpr('id', 'id % 5 as k')\n"
        "agg = df.groupBy('k').count()\n"
        f"agg.write.mode('overwrite').parquet('{c_out_dir}')\n"
        "spark.stop()\n"
    )

    local_script = tmp_path / "job.py"
    local_script.write_text(pyspark_code, encoding="utf-8")
    local_elog_dir = tmp_path / "events"
    local_elog_dir.mkdir()

    try:
        subprocess.run(
            [docker_bin, "exec", "n06-spark-1", "mkdir", "-p", c_elog_dir],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [docker_bin, "cp", str(local_script), f"n06-spark-1:{c_script}"],
            check=True,
            capture_output=True,
            text=True,
        )
        proc = subprocess.run(
            [
                docker_bin,
                "exec",
                "n06-spark-1",
                "/opt/spark/bin/spark-submit",
                "--master",
                "local[2]",
                c_script,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr or proc.stdout

        subprocess.run(
            [docker_bin, "cp", f"n06-spark-1:{c_elog_dir}/.", str(local_elog_dir)],
            check=True,
            capture_output=True,
            text=True,
        )
    finally:
        subprocess.run(
            [
                docker_bin,
                "exec",
                "n06-spark-1",
                "rm",
                "-rf",
                c_script,
                c_elog_dir,
                c_out_dir,
            ],
            check=False,
            capture_output=True,
        )

    latest = find_latest_event_log(local_elog_dir)
    assert latest is not None
    summary = parse_spark_event_log(latest)
    assert summary.app_name == f"n06-elog-{tag}"
    assert summary.stage_count >= 1
    assert summary.task_count >= 1
    assert summary.shuffle_write_bytes > 0
    assert summary.shuffle_read_bytes > 0
    assert summary.task_max_median_ratio >= 1.0
