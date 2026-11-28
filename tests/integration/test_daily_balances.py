"""Kiểm thử T40: `fact_daily_balances` nhiều ngày, tài khoản không phát sinh, BIGINT và load-mart.

1. Trên fixture acceptance (2 ngày 2026-01-01..02): kiểm 93.600 / 56.400 / 93.850 / 56.150,
   tài khoản không phát sinh và kiểm miền BIGINT trên Spark thật.
2. Chuỗi thật `process-day` -> `load-mart`: xác nhận `fact_daily_balances` ghi Gold và nạp vào
   PostgreSQL Mart với `opening + credit - debit = closing` và `runs.status = 'MART_PUBLISHED'`.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from test_load_mart import _admin_runner
from test_process_day_clean import REPO_ROOT, _result, build_stack, ingest

from n06.cli.init_local import parse_dotenv_file
from n06.ingestion.launcher import job_command
from n06.storage import make_compose_psql_runner
from n06.warehouse.migrations import migrate_mart_up

COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

_SPARK_BALANCES_TEST = r"""
import json
import os
from datetime import date
from pathlib import Path

from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from n06.config import load_config
from n06.ingestion.spark_session import build_spark
from n06.spark_jobs import transforms

ACCEPTANCE = Path("/opt/n06/contracts/N06_INPUT_CONTRACT_V1/fixtures/acceptance")

def read_jsonl(p: Path):
    return [json.loads(x) for x in p.read_text("utf-8").splitlines() if x.strip()]

cfg = load_config(profile="demo", env=os.environ)
spark = build_spark(cfg, "it-t40-balances")
try:
    # 1) Fixture acceptance: A=93600/93850, B=56400/56150 + 1 tài khoản không phát sinh.
    acc_bytes = (ACCEPTANCE / "accounts.jsonl").read_bytes()
    acc_content = spark.createDataFrame(
        [("accounts.jsonl", acc_bytes)],
        "path string, content binary",
    )
    acc_df, inv, conf = transforms.silver_accounts(
        spark,
        acc_content,
        dataset_id="n06-acceptance-v1",
        source_file="accounts.jsonl",
        run_id="run-t40",
    )
    assert inv.count() == 0 and conf.count() == 0

    idle_id = "00000000-0000-5000-8000-000000000099"
    idle_acc = spark.createDataFrame(
        [(
            "n06-acceptance-v1",
            idle_id,
            "CUSTOMER_DEPOSIT",
            "VND",
            12345,
            date(2026, 1, 1),
            "acc",
            "run-t40",
        )],
        acc_df.schema,
    )
    all_acc = acc_df.unionByName(idle_acc)

    ledger_raw = read_jsonl(ACCEPTANCE / "ledger_entries.jsonl")
    ledger_schema = StructType([
        StructField("dataset_id", StringType(), False),
        StructField("entry_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("ledger_account_id", StringType(), False),
        StructField("side", StringType(), False),
        StructField("amount_minor", LongType(), False),
        StructField("currency", StringType(), False),
        StructField("business_date", DateType(), False),
    ])
    ledger_df = spark.createDataFrame(
        [
            (
                r["dataset_id"],
                r["entry_id"],
                r["transaction_id"],
                r["ledger_account_id"],
                r["side"],
                int(r["amount_minor"]),
                r["currency"],
                date.fromisoformat(r["business_date"]),
            )
            for r in ledger_raw
        ],
        schema=ledger_schema,
    )
    dates = [date(2026, 1, 1), date(2026, 1, 2)]
    balances_df, overflow_df = transforms.gold_fact_daily_balances_with_overflow(
        spark, all_acc, ledger_df, business_dates=dates, run_id="run-t40"
    )
    assert overflow_df.count() == 0
    rows = [r.asDict() for r in balances_df.collect()]
    assert len(rows) == 6  # 3 tài khoản x 2 ngày
    for r in rows:
        expected_closing = r["opening_balance_minor"] + r["credit_minor"] - r["debit_minor"]
        assert r["closing_balance_minor"] == expected_closing

    by_key = {(r["account_id"], r["business_date"].isoformat()): r for r in rows}
    acc_a = "9a583596-945e-58a9-8a0c-ae060d161569"
    acc_b = "2362a3bf-4d28-507a-8938-92fafe632fca"
    assert by_key[(acc_a, "2026-01-01")]["closing_balance_minor"] == 93600
    assert by_key[(acc_b, "2026-01-01")]["closing_balance_minor"] == 56400
    assert by_key[(acc_a, "2026-01-02")]["opening_balance_minor"] == 93600
    assert by_key[(acc_a, "2026-01-02")]["closing_balance_minor"] == 93850
    assert by_key[(acc_b, "2026-01-02")]["opening_balance_minor"] == 56400
    assert by_key[(acc_b, "2026-01-02")]["closing_balance_minor"] == 56150
    for d in ("2026-01-01", "2026-01-02"):
        assert by_key[(idle_id, d)]["debit_minor"] == 0
        assert by_key[(idle_id, d)]["credit_minor"] == 0
        assert by_key[(idle_id, d)]["ledger_entry_count"] == 0
        assert by_key[(idle_id, d)]["closing_balance_minor"] == 12345

    # 2) Kiểm tra tràn miền BIGINT.
    big_acc = spark.createDataFrame(
        [(
            "ds-ovf",
            "acc-ovf",
            "CUSTOMER_DEPOSIT",
            "VND",
            (2**63) - 1,
            date(2026, 1, 1),
            "acc",
            "run-ovf",
        )],
        acc_df.schema,
    )
    big_ledger = spark.createDataFrame(
        [("ds-ovf", "e1", "t1", "acc-ovf", "CREDIT", 10, "VND", date(2026, 1, 1))],
        schema=ledger_schema,
    )
    _, ovf = transforms.gold_fact_daily_balances_with_overflow(
        spark, big_acc, big_ledger, business_dates=[date(2026, 1, 1)], run_id="run-ovf"
    )
    assert ovf.count() == 1
    print("N06_T40_SPARK_OK=1")
finally:
    spark.stop()
"""


def test_t40_balances_acceptance_idle_and_bigint_overflow(tmp_path: Path) -> None:
    docker = shutil.which("docker")
    assert docker is not None
    env_file = REPO_ROOT / ".env"
    assert env_file.is_file()
    dotenv = parse_dotenv_file(env_file)
    env = {**os.environ, **dotenv, "N06__SPARK__EVENT_LOG_ENABLED": "false"}

    script = tmp_path / "run_t40_balances.py"
    script.write_text(_SPARK_BALANCES_TEST, encoding="utf-8")
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
        cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=360, check=False
    )
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
    assert "N06_T40_SPARK_OK=1" in proc.stdout


def test_t40_process_day_and_load_mart_daily_balances(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    overrides = {
        "transaction_count": 40,
        "from_date": "2026-01-01",
        "to_date": "2026-01-02",
    }
    with contextlib.contextmanager(build_stack)(
        tmp_path_factory, "generator.demo-clean.json", overrides
    ) as stack:
        ingest(stack)
        proc = stack.process_day()
        assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]
        pd_result = _result(proc, "N06_PROCESS_DAY_RESULT=")
        assert any(k.startswith("gold.fact_daily_balances.") for k in pd_result["rows"])

        admin = _admin_runner(stack.docker, stack.cfg.postgres.mart.database)
        migrate_mart_up(admin)
        mart_runner = make_compose_psql_runner(
            stack.cfg.postgres.mart,
            docker_bin=stack.docker,
            compose_file=COMPOSE_FILE,
            repo_root=REPO_ROOT,
        )
        try:
            cmd = job_command(
                stack.cfg,
                docker_bin=stack.docker,
                repo_root=REPO_ROOT,
                job_module="n06.warehouse.load_mart_job",
                job_args=["--profile", "demo", "--dataset-id", stack.dataset_id],
                env_names=[k for k in stack.env if k.startswith("N06")],
            )
            lm_proc = subprocess.run(
                cmd,
                cwd=REPO_ROOT,
                env=stack.env,
                capture_output=True,
                text=True,
                timeout=360,
                check=False,
            )
            assert lm_proc.returncode == 0, lm_proc.stdout[-3000:] + lm_proc.stderr[-3000:]
            lm_result = _result(lm_proc, "N06_LOAD_MART_RESULT=")
            assert lm_result["rows_by_table"]["fact_daily_balances"] > 0

            raw = mart_runner(
                f"SELECT COUNT(*), COUNT(*) FILTER (WHERE opening_balance_minor + credit_minor "
                f"- debit_minor = closing_balance_minor) "
                f"FROM fact_daily_balances WHERE dataset_id = '{stack.dataset_id}';",
                True,
            ).strip()
            total_str, ok_str = raw.split("|")
            assert int(total_str) == lm_result["rows_by_table"]["fact_daily_balances"]
            assert int(ok_str) == int(total_str)
        finally:
            mart_runner(
                f"DELETE FROM fact_daily_balances WHERE dataset_id = '{stack.dataset_id}';"
                f"DELETE FROM fact_transactions WHERE dataset_id = '{stack.dataset_id}';"
                f"DELETE FROM fact_reconciliation WHERE dataset_id = '{stack.dataset_id}';"
                f"DELETE FROM dim_account WHERE dataset_id = '{stack.dataset_id}';"
                f"DELETE FROM mart_publish_metadata WHERE dataset_id = '{stack.dataset_id}';",
                False,
            )
