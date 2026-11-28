"""Integration test cho TASK T32 trên PostgreSQL Control DB thật và PySpark thật.

1. Ghi & đọc lại `quality_metrics` trên PostgreSQL Control DB thật (idempotent upsert).
2. Chạy `spark_metrics.compute_control_totals_spark` trên Spark thật với bộ acceptance,
   đối chiếu khớp từng dòng (7/7) với `acceptance/control_report.json`.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from datetime import date
from pathlib import Path

from n06.cli.init_local import parse_dotenv_file
from n06.config import load_config
from n06.quality import LayerCounts, evaluate_quality_gate
from n06.storage import (
    BatchCommitCoordinator,
    PsqlCliControlStore,
    make_compose_psql_runner,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

_SPARK_SCRIPT = r"""
import json
import os
from datetime import date
from pathlib import Path

from n06.config import load_config
from n06.ingestion.spark_session import build_spark
from n06.quality import compare_control_totals, spark_metrics

CONTRACT = Path("/opt/n06/contracts/N06_INPUT_CONTRACT_V1")
ACCEPTANCE = CONTRACT / "fixtures" / "acceptance"

def read_jsonl(p: Path):
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]

spark = build_spark(load_config(profile="demo", env=os.environ), "it-t32-metrics")
try:
    txs = read_jsonl(ACCEPTANCE / "transactions.jsonl")
    ledgers = read_jsonl(ACCEPTANCE / "ledger_entries.jsonl")
    report = json.loads((ACCEPTANCE / "control_report.json").read_text(encoding="utf-8"))

    tx_df = spark.createDataFrame(
        [
            (
                date.fromisoformat(r["business_date"]),
                r["currency"],
                r["status"],
                int(r["amount_minor"]),
            )
            for r in txs
        ],
        "business_date date, currency string, status string, amount_minor long",
    )
    le_df = spark.createDataFrame(
        [
            (
                date.fromisoformat(r["business_date"]),
                r["currency"],
                r["side"],
                int(r["amount_minor"]),
            )
            for r in ledgers
        ],
        "business_date date, currency string, side string, amount_minor long",
    )
    actual = spark_metrics.compute_control_totals_spark(tx_df, le_df)
    comps = compare_control_totals(actual, report)
    out = {
        "groups": len(comps),
        "mismatches": [c.__dict__ for c in comps if not c.matched],
    }
    print("N06_T32_RESULT=" + json.dumps(out))
finally:
    spark.stop()
"""


def test_quality_metrics_postgres_upsert_and_spark_control_totals(tmp_path: Path) -> None:
    docker = shutil.which("docker")
    assert docker is not None, "Cần Docker CLI cho integration test T32."
    env_file = REPO_ROOT / ".env"
    assert env_file.is_file(), "Cần file .env."
    dotenv = parse_dotenv_file(env_file)
    env = {**os.environ, **dotenv, "N06__SPARK__EVENT_LOG_ENABLED": "false"}
    cfg = load_config(profile="demo", env=env)

    runner = make_compose_psql_runner(
        cfg.postgres.control, docker_bin=docker, compose_file=COMPOSE_FILE, repo_root=REPO_ROOT
    )
    store = PsqlCliControlStore(cfg.postgres.control, psql_runner=runner)
    coord = BatchCommitCoordinator(store)

    suffix = uuid.uuid4().hex[:8]
    dataset_id = f"ds-t32-{suffix}"
    run_id = f"run-t32-{suffix}"

    try:
        coord.start_run(
            dataset_id=dataset_id,
            job_name="process-day",
            profile="demo",
            business_date_from=date(2026, 1, 1),
            business_date_to=date(2026, 1, 2),
            input_versions=[],
            config_hash="cfg-hash",
            schema_version="1.0.0",
            rule_version="v1",
            code_version="dev",
            run_id=run_id,
        )
        eval_res = evaluate_quality_gate(
            run_id=run_id,
            dataset_id=dataset_id,
            default_date=date(2026, 1, 1),
            layer_counts=[LayerCounts("transaction", date(2026, 1, 1), 36, 0, 34)],
        )
        # Ghi 2 lần -> idempotent theo unique constraint của bảng quality_metrics
        store.upsert_quality_metrics(eval_res.metrics)
        store.upsert_quality_metrics(eval_res.metrics)
        loaded = store.list_quality_metrics(run_id=run_id)
        assert len(loaded) == len(eval_res.metrics)
        by_name = {(m.layer, m.metric_name): m for m in loaded}
        assert by_name[("bronze", "physical_records")].metric_value == 36
        assert by_name[("silver", "unique_business_records")].metric_value == 34
        assert by_name[("run", "publish_blocked")].status == "PASS"
    finally:
        runner(f"DELETE FROM runs WHERE run_id = '{run_id}';\n", False)

    # Chạy Spark thật đối chiếu control totals với acceptance/control_report.json
    script = tmp_path / "run_t32_metrics_spark.py"
    script.write_text(_SPARK_SCRIPT, encoding="utf-8")
    tmp_path.chmod(0o755)
    cmd = [
        docker,
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "--profile",
        "core",
        "run",
        "--rm",
        "--no-deps",
        "-T",
    ]
    for folder in ("src", "config", "contracts", "sql"):
        cmd += ["-v", f"{REPO_ROOT / folder}:/opt/n06/{folder}:ro"]
    cmd += ["-v", f"{tmp_path}:/opt/n06/it:ro", "-e", "PYTHONPATH=/opt/n06/src"]
    for key in sorted(env):
        if key.startswith("N06"):
            cmd += ["-e", key]
    cmd += [
        "spark",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--conf",
        "spark.ui.enabled=false",
        f"/opt/n06/it/{script.name}",
    ]
    proc = subprocess.run(
        cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=300, check=False
    )
    assert proc.returncode == 0, (
        f"Spark T32 thất bại:\n{proc.stdout[-3000:]}\n{proc.stderr[-3000:]}"
    )
    line = next(x for x in proc.stdout.splitlines() if x.startswith("N06_T32_RESULT="))
    res = json.loads(line.removeprefix("N06_T32_RESULT="))
    assert res["groups"] == 7
    assert res["mismatches"] == []
