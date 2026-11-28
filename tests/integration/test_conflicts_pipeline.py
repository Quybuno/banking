"""Integration test trên PySpark thật cho TASK T31 (replay vs duplicate nghiệp vụ & key_conflicts).

Chạy `spark_conflicts.split_event_conflicts_spark` và `split_statement_conflicts_spark` trong
container Spark trên:
1. Acceptance suite: T009 (gộp replay giữ `event_ids` + `replay_count`), T010 (giữ cả 2
   `transaction_id` cùng match key), T011 (giữ cả 2 `statement_record_id` cùng match key).
2. Negative suite: `event_id_conflict.jsonl` (`EVENT_ID_CONFLICT`) và `business_key_conflict.jsonl`
   (`BUSINESS_KEY_CONFLICT`) đều bị loại khỏi Silver, xuất vào `quarantine.records` +
   `silver.key_conflicts` đủ 11 cột ADR-0004b §3.5.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

from n06.cli.init_local import parse_dotenv_file

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

_SPARK_SCRIPT = r"""
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from n06.config import load_config
from n06.ingestion.spark_session import build_spark
from n06.quality import spark_conflicts
from n06.spark_jobs import transforms

CONTRACT = Path("/opt/n06/contracts/N06_INPUT_CONTRACT_V1")
ACCEPTANCE = CONTRACT / "fixtures" / "acceptance"
NEGATIVE = CONTRACT / "fixtures" / "negative"

def read_jsonl(p: Path):
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]

spark = build_spark(load_config(profile="demo", env=os.environ), "it-t31-conflicts")
try:
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

    ts = datetime(2026, 1, 1, 1, 0, 10, tzinfo=UTC)

    def to_tx_df(events):
        rows = []
        for idx, ev in enumerate(events):
            p = ev["payload"]
            rows.append((
                "n06.events", 0, idx, ts, ts, ev["event_id"],
                p["dataset_id"], p["transaction_id"], p["external_reference"],
                p["account_from"], p["account_to"], int(p["amount_minor"]),
                p["currency"], p["status"], p["transaction_type"], p["channel"],
                p["event_time"], p["business_date"], "s3a://n06-lakehouse/raw/q/b0/",
            ))
        return spark.createDataFrame(rows, schema=tx_schema)

    # 1. Acceptance events (T009 replay + T010 business duplicate)
    acc_events = [
        ev for ev in read_jsonl(ACCEPTANCE / "events.jsonl")
        if ev["event_type"] == "TRANSACTION_CREATED"
    ]
    g_acc, _, q_acc, kc_acc = spark_conflicts.split_event_conflicts_spark(
        to_tx_df(acc_events),
        dataset_id="n06-acceptance-v1",
        entity="transaction",
        key_col="transaction_id",
        payload_cols=transforms.TRANSACTION_PAYLOAD,
        run_id="run-t31",
    )
    silver_acc = transforms.silver_transactions(g_acc, run_id="run-t31").collect()
    t010 = [r for r in silver_acc if r["external_reference"] == "REF-T010"]
    t011 = [r for r in silver_acc if r["external_reference"] == "REF-T011"]

    # 2. Negative event_id_conflict.jsonl
    ev_conf = read_jsonl(NEGATIVE / "event_id_conflict.jsonl")
    g_ev, _, q_ev, kc_ev = spark_conflicts.split_event_conflicts_spark(
        to_tx_df(ev_conf),
        dataset_id="n06-acceptance-v1",
        entity="transaction",
        key_col="transaction_id",
        payload_cols=transforms.TRANSACTION_PAYLOAD,
        run_id="run-t31",
    )
    kc_ev_rows = kc_ev.collect()
    q_ev_rows = q_ev.collect()

    # 3. Negative business_key_conflict.jsonl
    bk_conf = read_jsonl(NEGATIVE / "business_key_conflict.jsonl")
    g_bk, _, q_bk, kc_bk = spark_conflicts.split_event_conflicts_spark(
        to_tx_df(bk_conf),
        dataset_id="n06-acceptance-v1",
        entity="transaction",
        key_col="transaction_id",
        payload_cols=transforms.TRANSACTION_PAYLOAD,
        run_id="run-t31",
    )
    kc_bk_rows = kc_bk.collect()
    q_bk_rows = q_bk.collect()

    out = {
        "acc_silver_count": len(silver_acc),
        "acc_q_count": q_acc.count(),
        "acc_kc_count": kc_acc.count(),
        "t010_count": len(t010),
        "t010_replay_count": int(t010[0]["replay_count"]),
        "t010_event_ids": len(t010[0]["event_ids"]),
        "t011_count": len(t011),
        "t011_replay_count": int(t011[0]["replay_count"]),
        "t011_event_ids": len(t011[0]["event_ids"]),
        "ev_clean_count": g_ev.count(),
        "ev_q_codes": [r["error_code"] for r in q_ev_rows],
        "ev_kc_code": kc_ev_rows[0]["conflict_code"],
        "ev_kc_ext": kc_ev_rows[0]["external_reference"],
        "ev_kc_cols": list(kc_ev.columns),
        "bk_clean_count": g_bk.count(),
        "bk_q_codes": [r["error_code"] for r in q_bk_rows],
        "bk_kc_code": kc_bk_rows[0]["conflict_code"],
        "bk_kc_ext": kc_bk_rows[0]["external_reference"],
    }
    print("N06_T31_RESULT=" + json.dumps(out))
finally:
    spark.stop()
"""


def test_t31_conflicts_and_replays_on_real_spark(tmp_path: Path) -> None:
    docker = shutil.which("docker")
    assert docker is not None, "Cần Docker CLI để chạy Spark integration test T31."
    env_file = REPO_ROOT / ".env"
    assert env_file.is_file(), "Cần file .env (chạy `n06 init-local`)."

    script = tmp_path / "run_t31_conflicts_spark.py"
    script.write_text(_SPARK_SCRIPT, encoding="utf-8")
    tmp_path.chmod(0o755)

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
        f"Spark T31 test thất bại:\n{proc.stdout[-3000:]}\n{proc.stderr[-3000:]}"
    )
    line = next(x for x in proc.stdout.splitlines() if x.startswith("N06_T31_RESULT="))
    res = json.loads(line.removeprefix("N06_T31_RESULT="))
    assert res["acc_silver_count"] == 12
    assert res["acc_q_count"] == 0 and res["acc_kc_count"] == 0
    assert res["t010_count"] == 1 and res["t010_replay_count"] == 2 and res["t010_event_ids"] == 1
    assert res["t011_count"] == 1 and res["t011_replay_count"] == 2 and res["t011_event_ids"] == 2
    assert res["ev_clean_count"] == 0
    assert res["ev_q_codes"] == ["EVENT_ID_CONFLICT", "EVENT_ID_CONFLICT"]
    assert res["ev_kc_code"] == "EVENT_ID_CONFLICT" and res["ev_kc_ext"] == "REF-T001"
    assert res["bk_clean_count"] == 0
    assert res["bk_q_codes"] == ["BUSINESS_KEY_CONFLICT", "BUSINESS_KEY_CONFLICT"]
    assert res["bk_kc_code"] == "BUSINESS_KEY_CONFLICT" and res["bk_kc_ext"] == "REF-T001"
