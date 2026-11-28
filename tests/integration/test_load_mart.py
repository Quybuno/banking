"""Integration test cho `n06 load-mart` (TASK T26) trên Kafka, MinIO, Spark và PostgreSQL thật.

Chuỗi end-to-end thật:
  generate (bản sạch nhỏ) -> publish (Kafka) -> stream --available-now (Raw+Bronze)
  -> wait-ingestion -> ingest-statements (MinIO) -> process-day (Silver+Gold+current_snapshot)
  -> migrate_mart_up -> load-mart lần 1 -> load-mart lần 2 (idempotent, không tăng số dòng)
  -> load-mart --fail-at after_stage (thoát 14, mart giữ nguyên, không sót bảng staging).

Expected tính độc lập từ `control_report.json` và `accounts.jsonl` của dataset vừa sinh.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from n06.cli import main
from n06.cli.init_local import parse_dotenv_file
from n06.cli.object_store import ensure_bucket, target_from_config
from n06.cli.statements import run_ingest_statements
from n06.config import AppConfig, load_config
from n06.generator import build_dataset
from n06.ingestion.launcher import job_command
from n06.storage import PsqlCliControlStore, make_compose_psql_runner
from n06.warehouse import SqlRunner, migrate_mart_up, staging_table_name

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIR = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
AS_OF_LATE = "2099-01-01T00:00:00Z"
PROCESS_DAY_MODULE = "n06.spark_jobs.process_day_job"
LOAD_MART_MODULE = "n06.warehouse.load_mart_job"


@dataclass
class _MartE2EStack:
    docker: str
    env: dict[str, str]
    cfg: AppConfig
    topic: str
    query: str
    dataset_id: str
    store: PsqlCliControlStore
    control_sql: Any
    mart_sql: Any
    source_dir: Path
    control_report: dict[str, Any]
    account_count: int

    def spark_job(
        self, module: str, args: list[str], *, timeout: int = 600
    ) -> subprocess.CompletedProcess[str]:
        command = job_command(
            self.cfg,
            docker_bin=self.docker,
            repo_root=REPO_ROOT,
            job_module=module,
            job_args=["--profile", "demo", *args],
            env_names=[k for k in self.env if k.startswith("N06")],
        )
        return subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )


def _admin_runner(docker: str, database: str) -> SqlRunner:
    """Chạy psql bằng superuser của container `postgres` để áp dụng migration mart idempotent."""

    def run(sql: str, tuples_only: bool) -> str:
        shell = f'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d {database}'
        if tuples_only:
            shell += " -t -A"
        proc = subprocess.run(
            [
                docker,
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "exec",
                "-T",
                "postgres",
                "sh",
                "-c",
                shell,
            ],
            cwd=REPO_ROOT,
            input=sql,
            text=True,
            encoding="utf-8",
            capture_output=True,
            check=True,
        )
        return proc.stdout

    return run


def _extract_json_line(stdout: str, prefix: str) -> dict[str, Any]:
    for line in stdout.splitlines():
        if line.startswith(prefix):
            return json.loads(line[len(prefix) :])
    raise AssertionError(f"Không thấy dòng {prefix!r} trong stdout:\n{stdout}")


@pytest.fixture()
def mart_e2e_stack(tmp_path: Path) -> Iterator[_MartE2EStack]:
    docker = shutil.which("docker")
    assert docker is not None, "Cần Docker CLI cho integration test T26."
    env_file = REPO_ROOT / ".env"
    assert env_file.is_file(), "Cần file .env (chạy `n06 init-local`)."

    dotenv = parse_dotenv_file(env_file)
    suffix = uuid.uuid4().hex[:8]
    dataset_id = f"ds-t26-{suffix}"
    topic = f"n06.it.t26.{suffix}"
    query = f"q-t26-{suffix}"

    # Sinh dataset sạch nhỏ theo cấu hình profile test (không hardcode quy mô/lỗi).
    host_env = {**dotenv, **os.environ}
    test_cfg = load_config(profile="test", env=host_env)
    bucket = test_cfg.object_store.bucket
    target = target_from_config(test_cfg, host_env)
    assert ensure_bucket(target, bucket).ready

    gen_root = tmp_path / "generated"
    source_dir = gen_root / "source"
    generator = json.loads((CONTRACT_DIR / "config/generator.demo-clean.json").read_text("utf-8"))
    generator.update(
        dataset_id=dataset_id,
        transaction_count=test_cfg.dataset.transaction_count,
        account_count=test_cfg.dataset.account_count,
        from_date=test_cfg.dataset.from_date,
        to_date=test_cfg.dataset.to_date,
    )
    build_dataset(generator, gen_root, validate_outputs=False)

    control_report = json.loads((source_dir / "control_report.json").read_text(encoding="utf-8"))
    account_lines = [
        line
        for line in (source_dir / "accounts.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    demo_prefixes = load_config(profile="demo", env=host_env).paths
    merged_env = {
        **host_env,
        "N06_PROFILE": "demo",
        "N06__KAFKA__EVENTS_TOPIC": topic,
        "N06__KAFKA__QUERY_NAME": query,
        "N06__OBJECT_STORE__BUCKET": bucket,
        "N06__PATHS__STATEMENTS_PREFIX": demo_prefixes.statements_prefix,
        "N06__SPARK__SHUFFLE_PARTITIONS": "4",
        "N06__SPARK__EVENT_LOG_ENABLED": "false",
    }
    demo_cfg = load_config(profile="demo", env=merged_env)
    host_cfg = test_cfg

    control_sql = make_compose_psql_runner(
        demo_cfg.postgres.control, docker_bin=docker, compose_file=COMPOSE_FILE, repo_root=REPO_ROOT
    )
    mart_sql = make_compose_psql_runner(
        demo_cfg.postgres.mart, docker_bin=docker, compose_file=COMPOSE_FILE, repo_root=REPO_ROOT
    )
    migrate_mart_up(_admin_runner(docker, demo_cfg.postgres.mart.database))

    store = PsqlCliControlStore(demo_cfg.postgres.control, psql_runner=control_sql)
    stack = _MartE2EStack(
        docker=docker,
        env=merged_env,
        cfg=demo_cfg,
        topic=topic,
        query=query,
        dataset_id=dataset_id,
        store=store,
        control_sql=control_sql,
        mart_sql=mart_sql,
        source_dir=source_dir,
        control_report=control_report,
        account_count=len(account_lines),
    )

    # Đưa dữ liệu lên Kafka -> Raw/Bronze -> Statements -> Silver/Gold.
    pub_code = main(
        [
            "publish",
            "--profile",
            "test",
            "--source-dir",
            str(source_dir),
            "--topic",
            topic,
            "--rate",
            "0",
        ]
    )
    assert pub_code == 0, "n06 publish thất bại"

    stream_proc = stack.spark_job(
        "n06.ingestion.stream_job", ["--query-name", query, "--available-now"]
    )
    assert stream_proc.returncode == 0, (
        f"stream_job thất bại:\n{stream_proc.stdout}\n{stream_proc.stderr}"
    )

    stmt_code = run_ingest_statements(
        source_dir=source_dir,
        as_of_time=AS_OF_LATE,
        dataset_id=dataset_id,
        profile="test",
        env={**host_env, "N06__PATHS__STATEMENTS_PREFIX": demo_prefixes.statements_prefix},
    )
    assert stmt_code == 0, "ingest-statements thất bại"

    pday_proc = stack.spark_job(
        PROCESS_DAY_MODULE,
        ["--dataset-id", dataset_id, "--as-of-time", AS_OF_LATE, "--query-name", query],
    )
    assert pday_proc.returncode == 0, (
        f"process-day thất bại:\n{pday_proc.stdout}\n{pday_proc.stderr}"
    )

    try:
        yield stack
    finally:
        # Dọn dữ liệu mart và control DB thuộc dataset test (không chạm dataset khác).
        for tbl in (
            "fact_transactions",
            "fact_daily_balances",
            "fact_reconciliation",
            "dim_account",
            "mart_publish_metadata",
        ):
            mart_sql(f"DELETE FROM {tbl} WHERE dataset_id = '{dataset_id}';\n", False)
        control_sql(
            f"DELETE FROM current_snapshot WHERE dataset_id = '{dataset_id}';\n"
            f"DELETE FROM runs WHERE dataset_id = '{dataset_id}';\n"
            f"DELETE FROM ingest_batches WHERE query_name = '{query}';\n",
            False,
        )
        try:
            admin = AdminClient({"bootstrap.servers": host_cfg.kafka.bootstrap_servers})
            admin.delete_topics([topic], operation_timeout=10)
        except Exception:
            pass


def _mart_counts(mart_sql: Any, dataset_id: str) -> dict[str, int]:
    sql = (
        f"SELECT 'fact_transactions', count(*) FROM fact_transactions "
        f"WHERE dataset_id = '{dataset_id}'\n"
        f"UNION ALL SELECT 'fact_reconciliation', count(*) FROM fact_reconciliation "
        f"WHERE dataset_id = '{dataset_id}'\n"
        f"UNION ALL SELECT 'dim_account', count(*) FROM dim_account "
        f"WHERE dataset_id = '{dataset_id}'\n"
        f"UNION ALL SELECT 'mart_publish_metadata', count(*) FROM mart_publish_metadata "
        f"WHERE dataset_id = '{dataset_id}';\n"
    )
    out = mart_sql(sql, True)
    return {
        line.split("|")[0]: int(line.split("|")[1]) for line in out.splitlines() if line.strip()
    }


def test_load_mart_end_to_end_idempotent_and_safe_on_failure(
    mart_e2e_stack: _MartE2EStack,
) -> None:
    s = mart_e2e_stack

    # 1. Chạy `load-mart` lần 1 trên Gold đã commit.
    first = s.spark_job(LOAD_MART_MODULE, ["--dataset-id", s.dataset_id])
    assert first.returncode == 0, f"load-mart lần 1 thất bại:\n{first.stdout}\n{first.stderr}"
    res1 = _extract_json_line(first.stdout, "N06_LOAD_MART_RESULT=")
    gold_run_id = res1["gold_run_id"]

    # Expected độc lập từ control_report.json và accounts.jsonl.
    tx_totals = [row for row in s.control_report["totals"] if row["entity"] == "transaction"]
    expected_tx_count = sum(int(row["record_count"]) for row in tx_totals)
    expected_tx_amount = sum(int(row["amount_minor_total"]) for row in tx_totals)

    counts1 = _mart_counts(s.mart_sql, s.dataset_id)
    assert counts1["fact_transactions"] == expected_tx_count
    assert counts1["fact_reconciliation"] == expected_tx_count
    assert counts1["dim_account"] == s.account_count
    assert counts1["mart_publish_metadata"] == 1

    # Kiểm tổng tiền trên fact_transactions khớp control_report.
    sum_raw = s.mart_sql(
        f"SELECT COALESCE(SUM(amount_minor), 0) FROM fact_transactions "
        f"WHERE dataset_id = '{s.dataset_id}';\n",
        True,
    ).strip()
    assert int(sum_raw) == expected_tx_amount

    # Mọi khóa trên fact_reconciliation đều MATCHED (dataset bản sạch).
    non_matched = s.mart_sql(
        f"SELECT count(*) FROM fact_reconciliation "
        f"WHERE dataset_id = '{s.dataset_id}' AND recon_status <> 'MATCHED';\n",
        True,
    ).strip()
    assert int(non_matched) == 0

    # Control DB đã chuyển trạng thái run sang MART_PUBLISHED (ADR-0005 §4.4).
    run_rec = s.store.get_run(gold_run_id)
    assert run_rec is not None and run_rec.status == "MART_PUBLISHED"

    # 2. Chạy `load-mart` lần 2 -> không tăng số dòng ở bất kỳ bảng nào (điều kiện T26).
    second = s.spark_job(LOAD_MART_MODULE, ["--dataset-id", s.dataset_id])
    assert second.returncode == 0, f"load-mart lần 2 thất bại:\n{second.stdout}\n{second.stderr}"
    counts2 = _mart_counts(s.mart_sql, s.dataset_id)
    assert counts2 == counts1

    # 3. Chạy `load-mart --fail-at after_stage` -> thoát mã 14, mart không đổi, không sót staging.
    failed = s.spark_job(
        LOAD_MART_MODULE, ["--dataset-id", s.dataset_id, "--fail-at", "after_stage"]
    )
    assert failed.returncode == 14
    err_payload = _extract_json_line(failed.stdout, "N06_LOAD_MART_ERROR=")
    assert err_payload["code"] == "INJECTED_FAILURE"
    assert _mart_counts(s.mart_sql, s.dataset_id) == counts1

    stg_name = staging_table_name("fact_transactions", gold_run_id)
    leftover = s.mart_sql(
        "SELECT count(*) FROM pg_tables "
        f"WHERE schemaname = 'public' AND tablename = '{stg_name}';\n",
        True,
    ).strip()
    assert int(leftover) == 0
