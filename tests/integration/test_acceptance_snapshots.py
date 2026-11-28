"""Kiểm thử nghiệm thu M3 (TASK T34): 3 snapshot tính lại độc lập từ đầu, mỗi snapshot chạy 2 lần.

Chạy toàn bộ chuỗi T30/T31 (Silver dedup + conflicts) -> T33 (`n06.reconciliation.reconcile`) trên
Spark thật với bộ `contracts/N06_INPUT_CONTRACT_V1/fixtures/acceptance/` tại 3 mốc `as_of_time`:
  1. `2026-01-02T03:00:00Z` (early)
  2. `2026-01-02T18:00:00Z` (mid)
  3. `2026-01-03T06:00:00Z` (late)

Mỗi snapshot chạy độc lập 2 lần (khác `run_id`), kiểm tra bất biến nội dung giữa 2 lần chạy,
và gọi trực tiếp CLI `n06 validate --against-ground-truth` (oracle T14) xác nhận PASS cả 3 snapshot.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
from pathlib import Path

from n06.cli import main
from n06.cli.init_local import parse_dotenv_file

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ACCEPTANCE_DIR = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / "fixtures" / "acceptance"

SNAPSHOTS: tuple[tuple[str, str], ...] = (
    ("early", "2026-01-02T03:00:00Z"),
    ("mid", "2026-01-02T18:00:00Z"),
    ("late", "2026-01-03T06:00:00Z"),
)

_SPARK_SCRIPT = r"""
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from n06.config import load_config
from n06.ingestion.spark_session import build_spark
from n06.quality import spark_conflicts
from n06.reconciliation import ReconciliationSettings
from n06.reconciliation.reconcile import reconcile
from n06.spark_jobs import transforms

CONTRACT = Path("/opt/n06/contracts/N06_INPUT_CONTRACT_V1")
ACCEPTANCE = CONTRACT / "fixtures" / "acceptance"
OUT_DIR = Path("/opt/n06/it")

SNAPSHOTS = [
    ("early", "2026-01-02T03:00:00Z"),
    ("mid", "2026-01-02T18:00:00Z"),
    ("late", "2026-01-03T06:00:00Z"),
]

def read_jsonl(p: Path):
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]

cfg = load_config(profile="demo", env=os.environ)
spark = build_spark(cfg, "it-t34-acceptance")
try:
    tx_events = [
        ev for ev in read_jsonl(ACCEPTANCE / "events.jsonl")
        if ev["event_type"] == "TRANSACTION_CREATED"
    ]
    ts = datetime(2026, 1, 1, 1, 0, 10, tzinfo=UTC)
    tx_schema = StructType([
        StructField("topic", StringType(), False),
        StructField("partition", LongType(), False),
        StructField("offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("event_id", StringType(), False),
        StructField("dataset_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("external_reference", StringType(), False),
        StructField("account_from", StringType(), False),
        StructField("account_to", StringType(), False),
        StructField("amount_minor", LongType(), False),
        StructField("currency", StringType(), False),
        StructField("status", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("business_date", StringType(), False),
        StructField("_raw_path", StringType(), False),
    ])
    tx_rows = [
        (
            "n06.acceptance", 0, idx, ts, ts, ev["event_id"],
            ev["payload"]["dataset_id"], ev["payload"]["transaction_id"],
            ev["payload"]["external_reference"], ev["payload"]["account_from"],
            ev["payload"]["account_to"], int(ev["payload"]["amount_minor"]),
            ev["payload"]["currency"], ev["payload"]["status"],
            ev["payload"]["transaction_type"], ev["payload"]["channel"],
            ev["payload"]["event_time"], ev["payload"]["business_date"],
            "s3a://n06-lakehouse/raw/acc/b0/",
        )
        for idx, ev in enumerate(tx_events)
    ]
    bronze_tx = spark.createDataFrame(tx_rows, schema=tx_schema)

    # Đọc statements + lịch giao file từ statement_deliveries.jsonl (ADR-0006 D3)
    deliveries = read_jsonl(ACCEPTANCE / "statement_deliveries.jsonl")
    file_avail_by_stmt_id = {}
    delivery_order_by_stmt_id = {}
    for d in deliveries:
        batch_file = CONTRACT / d["file_path"]
        for line in batch_file.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            sid = rec["statement_record_id"]
            file_avail_by_stmt_id[sid] = d["file_available_at"]
            delivery_order_by_stmt_id[sid] = int(d["delivery_order"])

    stmts = read_jsonl(ACCEPTANCE / "statements.jsonl")
    stmt_rows = [
        (
            s["dataset_id"], s["statement_record_id"], s["external_reference"], s["channel"],
            int(s["amount_minor"]), s["currency"], s["status"],
            datetime.fromisoformat(s["event_time"].replace("Z", "+00:00")),
            datetime.fromisoformat(s["business_date"]).date(),
            datetime.fromisoformat(s["available_at"].replace("Z", "+00:00")),
            datetime.fromisoformat(
                file_avail_by_stmt_id[s["statement_record_id"]].replace("Z", "+00:00")
            ),
            delivery_order_by_stmt_id[s["statement_record_id"]],
            "statements.jsonl", "sha256-acc", idx + 1, "run-stmt",
        )
        for idx, s in enumerate(stmts)
    ]
    silver_stmts = spark.createDataFrame(
        stmt_rows,
        "dataset_id string, statement_record_id string, external_reference string, channel string, "
        "amount_minor long, currency string, status string, event_time timestamp, "
        "business_date date, available_at timestamp, file_available_at timestamp, "
        "delivery_order int, source_file string, source_sha256 string, "
        "source_line long, run_id string",
    )

    settings = ReconciliationSettings.from_config(cfg)

    def run_once(as_of_str: str, run_id: str):
        grouped, _, _, kc = spark_conflicts.split_event_conflicts_spark(
            bronze_tx,
            dataset_id="n06-acceptance-v1",
            entity="transaction",
            key_col="transaction_id",
            payload_cols=transforms.TRANSACTION_PAYLOAD,
            run_id=run_id,
        )
        silver_tx = transforms.silver_transactions(grouped, run_id=run_id)
        as_of_dt = datetime.fromisoformat(as_of_str.replace("Z", "+00:00"))
        gold_recon = reconcile(
            silver_tx,
            silver_stmts,
            kc,
            as_of_time=as_of_dt,
            settings=settings,
            run_id=run_id,
            input_snapshot_id=f"snap-{as_of_str}",
        )
        rows = []
        for r in gold_recon.collect():
            anchor = r["anchor_event_time"]
            rows.append({
                "dataset_id": r["dataset_id"],
                "channel": r["channel"],
                "external_reference": r["external_reference"],
                "business_date": r["business_date"].isoformat(),
                "recon_status": r["recon_status"],
                "issue_codes": list(r["issue_codes"]),
                "anchor_event_time": (
                    anchor.replace(tzinfo=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    if anchor is not None
                    else None
                ),
                "discrepancy_age_seconds": (
                    int(r["discrepancy_age_seconds"])
                    if r["discrepancy_age_seconds"] is not None
                    else None
                ),
            })
        return sorted(rows, key=lambda x: (x["dataset_id"], x["channel"], x["external_reference"]))

    summary = {}
    for label, as_of_str in SNAPSHOTS:
        run1 = run_once(as_of_str, f"run-{label}-1")
        run2 = run_once(as_of_str, f"run-{label}-2")
        assert run1 == run2, f"Snapshot {label} không idempotent giữa 2 lần chạy!"
        (OUT_DIR / f"actual_{label}.json").write_text(
            json.dumps(run1, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        summary[label] = len(run1)

    print("N06_T34_RESULT=" + json.dumps(summary))
finally:
    spark.stop()
"""


def test_t34_three_acceptance_snapshots_idempotent_and_pass_n06_validate(tmp_path: Path) -> None:
    docker = shutil.which("docker")
    assert docker is not None, "Cần Docker CLI để chạy Spark nghiệm thu T34."
    env_file = REPO_ROOT / ".env"
    assert env_file.is_file(), "Cần file .env."

    script = tmp_path / "run_t34_acceptance.py"
    script.write_text(_SPARK_SCRIPT, encoding="utf-8")
    tmp_path.chmod(0o777)

    dotenv = parse_dotenv_file(env_file)
    env = {**os.environ, **dotenv, "N06__SPARK__EVENT_LOG_ENABLED": "false"}
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
    cmd += ["-v", f"{tmp_path}:/opt/n06/it", "-e", "PYTHONPATH=/opt/n06/src"]
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
        cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=360, check=False
    )
    assert proc.returncode == 0, (
        f"Spark T34 thất bại:\n{proc.stdout[-3000:]}\n{proc.stderr[-3000:]}"
    )

    # Gọi trực tiếp CLI `n06 validate --against-ground-truth` cho cả 3 snapshot!
    for label, as_of_str in SNAPSHOTS:
        actual_file = tmp_path / f"actual_{label}.json"
        assert actual_file.is_file(), f"Thiếu file {actual_file}"
        out = io.StringIO()
        code = main(
            [
                "validate",
                "--dataset-id",
                "n06-acceptance-v1",
                "--against-ground-truth",
                "--as-of-time",
                as_of_str,
                "--fixture-dir",
                str(ACCEPTANCE_DIR),
                "--actual-file",
                str(actual_file),
                "--harness-module",
                "tests.harness.oracle",
                "--json",
            ],
            out=out,
        )
        payload = json.loads(out.getvalue())
        assert code == 0, f"n06 validate FAIL cho snapshot {label}: {payload}"
        assert payload["passed"] is True and payload["mismatches"] == []
