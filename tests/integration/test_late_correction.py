"""Kiểm thử T61 (P1): sao kê đến muộn sửa kết quả đã publish, giữ `first_seen_at`/`resolved_at`,
và bút toán ngày cũ thay đổi lan sang số dư các ngày sau (đặc tả I.7.3 & I.8.3).
"""

from __future__ import annotations

import io
import os
import shutil
import subprocess
from pathlib import Path

from test_acceptance_snapshots import DATASET_ID, acceptance_stack  # noqa: F401
from test_load_mart import _admin_runner
from test_process_day_clean import REPO_ROOT, Stack, _result

from n06.cli.init_local import parse_dotenv_file
from n06.cli.statements import run_ingest_statements
from n06.ingestion.launcher import job_command
from n06.storage import make_compose_psql_runner
from n06.warehouse.migrations import migrate_mart_up

COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

_SPARK_LATE_LEDGER_TEST = r"""
import os
from datetime import date

from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from n06.config import load_config
from n06.ingestion.spark_session import build_spark
from n06.spark_jobs import transforms

cfg = load_config(profile="demo", env=os.environ)
spark = build_spark(cfg, "it-t61-late-ledger")
try:
    acc_schema = StructType([
        StructField("dataset_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("account_type", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("opening_balance_minor", LongType(), False),
        StructField("opening_business_date", DateType(), False),
        StructField("source_file", StringType(), False),
        StructField("run_id", StringType(), False),
    ])
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
    accounts = spark.createDataFrame(
        [
            ("ds-t61", "acc-a", "CUSTOMER_DEPOSIT", "VND", 100000, date(2026, 1, 1), "a", "r1"),
            ("ds-t61", "acc-b", "CUSTOMER_DEPOSIT", "VND", 50000, date(2026, 1, 1), "a", "r1"),
        ],
        schema=acc_schema,
    )
    base_ledger = [
        ("ds-t61", "e1", "t1", "acc-a", "DEBIT", 5000, "VND", date(2026, 1, 1)),
        ("ds-t61", "e2", "t1", "acc-b", "CREDIT", 5000, "VND", date(2026, 1, 1)),
        ("ds-t61", "e3", "t2", "acc-b", "DEBIT", 1000, "VND", date(2026, 1, 2)),
        ("ds-t61", "e4", "t2", "acc-a", "CREDIT", 1000, "VND", date(2026, 1, 2)),
    ]
    dates = [date(2026, 1, 1), date(2026, 1, 2)]
    b1, o1 = transforms.gold_fact_daily_balances_with_overflow(
        spark, accounts, spark.createDataFrame(base_ledger, ledger_schema),
        business_dates=dates, run_id="r1",
    )
    assert o1.count() == 0
    m1 = {(r["account_id"], r["business_date"].isoformat()): r.asDict() for r in b1.collect()}
    assert m1[("acc-a", "2026-01-01")]["closing_balance_minor"] == 95000
    assert m1[("acc-a", "2026-01-02")]["opening_balance_minor"] == 95000
    assert m1[("acc-a", "2026-01-02")]["closing_balance_minor"] == 96000

    # Bút toán đến muộn ngày 01/01 (A -> B 2.000 VND) phải làm thay đổi số dư ngày 02/01.
    late_ledger = base_ledger + [
        ("ds-t61", "e5", "t3", "acc-a", "DEBIT", 2000, "VND", date(2026, 1, 1)),
        ("ds-t61", "e6", "t3", "acc-b", "CREDIT", 2000, "VND", date(2026, 1, 1)),
    ]
    b2, o2 = transforms.gold_fact_daily_balances_with_overflow(
        spark, accounts, spark.createDataFrame(late_ledger, ledger_schema),
        business_dates=dates, run_id="r2",
    )
    assert o2.count() == 0
    m2 = {(r["account_id"], r["business_date"].isoformat()): r.asDict() for r in b2.collect()}
    assert m2[("acc-a", "2026-01-01")]["closing_balance_minor"] == 93000
    assert m2[("acc-a", "2026-01-02")]["opening_balance_minor"] == 93000
    assert m2[("acc-a", "2026-01-02")]["closing_balance_minor"] == 94000
    assert m2[("acc-b", "2026-01-01")]["closing_balance_minor"] == 57000
    assert m2[("acc-b", "2026-01-02")]["opening_balance_minor"] == 57000
    assert m2[("acc-b", "2026-01-02")]["closing_balance_minor"] == 56000
    print("N06_T61_LEDGER_OK=1")
finally:
    spark.stop()
"""


def _load_mart(stack: Stack) -> None:
    cmd = job_command(
        stack.cfg,
        docker_bin=stack.docker,
        repo_root=REPO_ROOT,
        job_module="n06.warehouse.load_mart_job",
        job_args=["--profile", "demo", "--dataset-id", DATASET_ID],
        env_names=[k for k in stack.env if k.startswith("N06")],
    )
    proc = subprocess.run(
        cmd, cwd=REPO_ROOT, env=stack.env, capture_output=True, text=True, timeout=360, check=False
    )
    assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]


def _query_recon(mart_runner: object, ref: str) -> tuple[str, str, str]:
    fmt = 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
    sql = (
        "SELECT recon_status, "
        f"COALESCE(to_char(first_seen_at AT TIME ZONE 'UTC', '{fmt}'), ''), "
        f"COALESCE(to_char(resolved_at AT TIME ZONE 'UTC', '{fmt}'), '') "
        f"FROM fact_reconciliation WHERE dataset_id = '{DATASET_ID}' "
        f"AND external_reference = '{ref}';"
    )
    raw = mart_runner(sql, True).strip()  # type: ignore[operator]
    status, first_seen, resolved = raw.split("|")
    return status, first_seen, resolved


def test_t61_late_statement_updates_published_mart_and_preserves_timestamps(
    acceptance_stack: Stack,  # noqa: F811
) -> None:
    admin = _admin_runner(acceptance_stack.docker, acceptance_stack.cfg.postgres.mart.database)
    migrate_mart_up(admin)
    mart_runner = make_compose_psql_runner(
        acceptance_stack.cfg.postgres.mart,
        docker_bin=acceptance_stack.docker,
        compose_file=COMPOSE_FILE,
        repo_root=REPO_ROOT,
    )
    try:
        # 1) early: 2026-01-02T04:00:00Z
        early = "2026-01-02T04:00:00Z"
        out = io.StringIO()
        assert (
            run_ingest_statements(
                source_dir=acceptance_stack.source,
                as_of_time=early,
                dataset_id=DATASET_ID,
                profile="test",
                env_file=REPO_ROOT / ".env",
                env=acceptance_stack.env,
                as_json=True,
                out=out,
            )
            == 0
        ), out.getvalue()
        p1 = acceptance_stack.process_day(as_of=early)
        assert p1.returncode == 0, p1.stdout[-3000:] + p1.stderr[-3000:]
        _load_mart(acceptance_stack)

        cnt1 = int(
            mart_runner(
                f"SELECT COUNT(*) FROM fact_reconciliation WHERE dataset_id = '{DATASET_ID}';",
                True,
            ).strip()
        )
        assert cnt1 == 13
        assert _query_recon(mart_runner, "REF-T007") == ("PENDING", early, "")
        assert _query_recon(mart_runner, "REF-T008") == ("PENDING", early, "")

        # 2) after_cutoff: 2026-01-02T18:00:00Z (nạp sao kê muộn T007)
        mid = "2026-01-02T18:00:00Z"
        out = io.StringIO()
        assert (
            run_ingest_statements(
                source_dir=acceptance_stack.source,
                as_of_time=mid,
                dataset_id=DATASET_ID,
                profile="test",
                env_file=REPO_ROOT / ".env",
                env=acceptance_stack.env,
                as_json=True,
                out=out,
            )
            == 0
        ), out.getvalue()
        p2 = acceptance_stack.process_day(as_of=mid)
        assert p2.returncode == 0, p2.stdout[-3000:] + p2.stderr[-3000:]
        _load_mart(acceptance_stack)

        cnt2 = int(
            mart_runner(
                f"SELECT COUNT(*) FROM fact_reconciliation WHERE dataset_id = '{DATASET_ID}';",
                True,
            ).strip()
        )
        assert cnt2 == 13
        assert _query_recon(mart_runner, "REF-T007") == ("MATCHED", early, mid)
        assert _query_recon(mart_runner, "REF-T008") == ("MISSING_EXTERNAL", early, "")

        # 3) late_corrected: 2026-01-03T01:00:00Z (nạp sao kê muộn T008, chạy lại 2 lần)
        late = "2026-01-03T01:00:00Z"
        out = io.StringIO()
        assert (
            run_ingest_statements(
                source_dir=acceptance_stack.source,
                as_of_time=late,
                dataset_id=DATASET_ID,
                profile="test",
                env_file=REPO_ROOT / ".env",
                env=acceptance_stack.env,
                as_json=True,
                out=out,
            )
            == 0
        ), out.getvalue()
        p3a = acceptance_stack.process_day(as_of=late)
        assert p3a.returncode == 0, p3a.stdout[-3000:] + p3a.stderr[-3000:]
        r3a = _result(p3a, "N06_PROCESS_DAY_RESULT=")
        p3b = acceptance_stack.process_day(as_of=late)
        assert p3b.returncode == 0, p3b.stdout[-3000:] + p3b.stderr[-3000:]
        r3b = _result(p3b, "N06_PROCESS_DAY_RESULT=")
        assert r3a["semantic_hash"] == r3b["semantic_hash"]
        _load_mart(acceptance_stack)

        cnt3 = int(
            mart_runner(
                f"SELECT COUNT(*) FROM fact_reconciliation WHERE dataset_id = '{DATASET_ID}';",
                True,
            ).strip()
        )
        assert cnt3 == 13
        # first_seen_at và resolved_at không bị reset khi chạy lại!
        assert _query_recon(mart_runner, "REF-T007") == ("MATCHED", early, mid)
        assert _query_recon(mart_runner, "REF-T008") == ("MATCHED", early, late)
    finally:
        mart_runner(
            f"DELETE FROM fact_daily_balances WHERE dataset_id = '{DATASET_ID}';"
            f"DELETE FROM fact_transactions WHERE dataset_id = '{DATASET_ID}';"
            f"DELETE FROM fact_reconciliation WHERE dataset_id = '{DATASET_ID}';"
            f"DELETE FROM dim_account WHERE dataset_id = '{DATASET_ID}';"
            f"DELETE FROM mart_publish_metadata WHERE dataset_id = '{DATASET_ID}';",
            False,
        )


def test_t61_late_ledger_propagates_to_subsequent_daily_balances(tmp_path: Path) -> None:
    docker = shutil.which("docker")
    assert docker is not None
    dotenv = parse_dotenv_file(REPO_ROOT / ".env")
    env = {**os.environ, **dotenv, "N06__SPARK__EVENT_LOG_ENABLED": "false"}
    script = tmp_path / "run_t61_late_ledger.py"
    script.write_text(_SPARK_LATE_LEDGER_TEST, encoding="utf-8")
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
    assert "N06_T61_LEDGER_OK=1" in proc.stdout
