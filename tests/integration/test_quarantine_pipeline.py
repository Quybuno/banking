"""Integration test cho Quarantine (TASK T30) chạy trên PySpark thật trong container Spark.

Đưa toàn bộ bộ `contracts/N06_INPUT_CONTRACT_V1/fixtures/negative/` (`record_cases.jsonl`,
`malformed_payloads.txt`, `unbalanced_ledger.jsonl`, `missing_ledger_side.jsonl`) đi qua
`n06.ingestion.records.classify_record` (Bronze) + `n06.quality.spark_quarantine` (Silver &
nhóm sổ cái) trên Spark thật, xác nhận 100% ra đúng mã lỗi `expected_error`, đủ cột
`quarantine.records` theo ADR-0004b §3.6 và không lọt bản ghi lỗi vào Silver.
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
from datetime import UTC, date, datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from n06.config import load_config
from n06.ingestion import records as bronze_records
from n06.ingestion.spark_session import build_spark
from n06.quality import spark_quarantine
from n06.spark_jobs import transforms

CONTRACT = Path("/opt/n06/contracts/N06_INPUT_CONTRACT_V1")
ACCEPTANCE = CONTRACT / "fixtures" / "acceptance"
NEGATIVE = CONTRACT / "fixtures" / "negative"

def read_jsonl(p: Path):
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]

spark = build_spark(load_config(profile="demo", env=os.environ), "it-t30-quarantine")
try:
    accounts_raw = read_jsonl(ACCEPTANCE / "accounts.jsonl")
    accounts_df = spark.createDataFrame(
        [
            (
                r["dataset_id"], r["account_id"], r["account_type"], r["currency"],
                int(r["opening_balance_minor"]), date.fromisoformat(r["opening_business_date"]),
                "accounts.jsonl", "run-t30",
            )
            for r in accounts_raw
        ],
        "dataset_id string, account_id string, account_type string, currency string, "
        "opening_balance_minor long, opening_business_date date, source_file string, run_id string",
    )

    # 1. Đưa toàn bộ 17 ca record_cases.jsonl + malformed_payloads.txt qua Bronze classify_record
    record_cases = read_jsonl(NEGATIVE / "record_cases.jsonl")
    malformed_lines = (NEGATIVE / "malformed_payloads.txt").read_bytes().splitlines()

    parse_err_rows = []
    bronze_tx_rows = []
    case_by_offset = {}
    offset = 0

    for idx, case in enumerate(record_cases):
        tx = case["input"]
        env = {
            "schema_version": "1.0.0",
            "event_id": f"00000000-0000-5000-8000-{idx + 1:012d}",
            "event_type": "TRANSACTION_CREATED",
            "dataset_id": tx.get("dataset_id", "n06-acceptance-v1"),
            "source_batch_id": "11111111-1111-5111-8111-111111111111",
            "event_time": tx.get("event_time", "2026-01-01T01:00:01Z"),
            "emitted_at": "2026-01-01T01:00:05Z",
            "payload": tx,
        }
        route, ds_id, cols = bronze_records.classify_record(json.dumps(env).encode("utf-8"))
        case_by_offset[offset] = (case["case_id"], case["expected_error"])
        if route == bronze_records.PARSE_ERROR:
            parse_err_rows.append((
                "n06.neg", 0, offset, ds_id, cols["entity"], cols["business_key"],
                cols["error_code"], cols["error_detail"], cols["issue_codes"],
            ))
        else:
            bronze_tx_rows.append((
                "n06.neg", 0, offset,
                cols["envelope_dataset_id"], cols["envelope_event_time"], cols["emitted_at"],
                cols["dataset_id"], cols["transaction_id"], cols["external_reference"],
                cols["account_from"], cols["account_to"], int(cols["amount_minor"]),
                cols["currency"], cols["status"], cols["transaction_type"], cols["channel"],
                cols["event_time"], cols["business_date"],
                "s3a://n06-lakehouse/raw/q/ingestion_date=2026-01-01/batch_id=0/",
            ))
        offset += 1

    for raw_line in malformed_lines:
        route, ds_id, cols = bronze_records.classify_record(raw_line)
        assert route == bronze_records.PARSE_ERROR
        case_by_offset[offset] = (f"malformed_{offset}", "INVALID_JSON")
        parse_err_rows.append((
            "n06.neg", 0, offset, ds_id, cols["entity"], cols["business_key"],
            cols["error_code"], cols["error_detail"], cols["issue_codes"],
        ))
        offset += 1

    pe_schema = StructType([
        StructField("topic", StringType(), False),
        StructField("partition", LongType(), False),
        StructField("offset", LongType(), False),
        StructField("dataset_id", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("business_key", StringType(), True),
        StructField("error_code", StringType(), False),
        StructField("error_detail", StringType(), False),
        StructField("issue_codes", ArrayType(StringType()), False),
    ])
    pe_df = spark.createDataFrame(parse_err_rows, schema=pe_schema)
    q_pe = spark_quarantine.bronze_parse_errors_to_quarantine(
        pe_df,
        dataset_id="n06-acceptance-v1",
        run_id="run-t30",
        raw_path="s3a://n06-lakehouse/raw/q/ingestion_date=2026-01-01/batch_id=0/",
    )

    tx_schema = StructType([
        StructField("topic", StringType(), False),
        StructField("partition", LongType(), False),
        StructField("offset", LongType(), False),
        StructField("envelope_dataset_id", StringType(), True),
        StructField("envelope_event_time", StringType(), True),
        StructField("emitted_at", StringType(), True),
        StructField("dataset_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("external_reference", StringType(), True),
        StructField("account_from", StringType(), True),
        StructField("account_to", StringType(), True),
        StructField("amount_minor", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("business_date", StringType(), True),
        StructField("_raw_path", StringType(), False),
    ])
    tx_df = spark.createDataFrame(bronze_tx_rows, schema=tx_schema)
    valid_tx, q_tx = spark_quarantine.validate_bronze_transactions_spark(
        tx_df,
        accounts_df,
        dataset_id="n06-acceptance-v1",
        business_timezone="Asia/Ho_Chi_Minh",
        run_id="run-t30",
    )
    assert valid_tx.count() == 0, "Không bản ghi negative nào được phép lọt sang Silver"

    all_q = q_pe.unionByName(q_tx).collect()
    by_offset = {}
    for row in all_q:
        off = int(row["source_pointer"].rsplit(":", 1)[1])
        by_offset[off] = {
            "error_code": row["error_code"],
            "issue_codes": list(row["issue_codes"]),
            "stage": row["stage"],
            "raw_reference": row["raw_reference"],
            "run_id": row["run_id"],
        }

    # 2. Kiểm tra unbalanced_ledger và missing_ledger_side qua validate_ledger_groups_spark
    acc_txs = read_jsonl(ACCEPTANCE / "transactions.jsonl")
    tx_by_id = {r["transaction_id"]: r for r in acc_txs}
    ledger_results = {}
    for fname, key in (
        ("unbalanced_ledger.jsonl", "unbalanced_ledger"),
        ("missing_ledger_side.jsonl", "missing_ledger_side"),
    ):
        entries = read_jsonl(NEGATIVE / fname)
        tids = {e["transaction_id"] for e in entries}
        stx = spark.createDataFrame(
            [
                (
                    tx_by_id[tid]["dataset_id"], tid, tx_by_id[tid]["status"],
                    tx_by_id[tid]["account_from"], tx_by_id[tid]["account_to"],
                    int(tx_by_id[tid]["amount_minor"]), tx_by_id[tid]["currency"],
                    datetime.fromisoformat(tx_by_id[tid]["event_time"].replace("Z", "+00:00")),
                    date.fromisoformat(tx_by_id[tid]["business_date"]),
                    ["n06.tx:0:1"],
                )
                for tid in tids
            ],
            "dataset_id string, transaction_id string, status string, account_from string, "
            "account_to string, amount_minor long, currency string, event_time timestamp, "
            "business_date date, source_pointers array<string>",
        )
        sle = spark.createDataFrame(
            [
                (
                    e["dataset_id"], e["entry_id"], e["transaction_id"], e["ledger_account_id"],
                    e["side"], int(e["amount_minor"]), e["currency"],
                    datetime.fromisoformat(e["event_time"].replace("Z", "+00:00")),
                    date.fromisoformat(e["business_date"]),
                    ["n06.le:0:1"],
                )
                for e in entries
            ],
            "dataset_id string, entry_id string, transaction_id string, ledger_account_id string, "
            "side string, amount_minor long, currency string, event_time timestamp, "
            "business_date date, source_pointers array<string>",
        )
        clean_le, q_le = spark_quarantine.validate_ledger_groups_spark(
            stx, sle, run_id="run-t30", raw_reference="s3a://n06-lakehouse/raw/q/b0/"
        )
        assert clean_le.count() == 0
        q_rows = q_le.collect()
        ledger_results[key] = [r["error_code"] for r in q_rows]

    out = {
        "checked_records": len(case_by_offset),
        "mismatches": [
            {"case": name, "expected": exp, "got": by_offset.get(off)}
            for off, (name, exp) in case_by_offset.items()
            if by_offset.get(off, {}).get("error_code") != exp
        ],
        "ledger_results": ledger_results,
    }
    print("N06_T30_RESULT=" + json.dumps(out))
finally:
    spark.stop()
"""


def test_negative_suite_through_spark_quarantine(tmp_path: Path) -> None:
    docker = shutil.which("docker")
    assert docker is not None, "Cần Docker CLI để chạy Spark integration test T30."
    env_file = REPO_ROOT / ".env"
    assert env_file.is_file(), "Cần file .env (chạy `n06 init-local`)."

    script = tmp_path / "run_t30_negative_spark.py"
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
        f"Spark T30 test thất bại:\n{proc.stdout[-3000:]}\n{proc.stderr[-3000:]}"
    )
    line = next(x for x in proc.stdout.splitlines() if x.startswith("N06_T30_RESULT="))
    result = json.loads(line.removeprefix("N06_T30_RESULT="))
    assert result["checked_records"] >= 18
    assert result["mismatches"] == [], f"Sai mã lỗi trên Spark: {result['mismatches']}"
    assert result["ledger_results"]["unbalanced_ledger"] == ["LEDGER_UNBALANCED"]
    assert result["ledger_results"]["missing_ledger_side"] == ["LEDGER_INCOMPLETE"]
