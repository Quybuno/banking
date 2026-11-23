"""Integration tests cho migration Control DB (TASK T21).

Yêu cầu PostgreSQL control DB đang chạy (profile `test` / `docker compose --profile core up -d`)
và có `psycopg`/`psycopg2`, `psql` trên host, hoặc `docker compose exec` vào container `postgres`.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import uuid
from pathlib import Path

import pytest

from n06.cli import parse_dotenv_file
from n06.config import PostgresDbConfig, load_config
from n06.control import (
    DEFAULT_CONTROL_MIGRATIONS_DIR,
    MigrationError,
    SqlExecutor,
    connect_control_db,
    get_applied_versions,
    migrate_down,
    migrate_up,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ENV_FILE = REPO_ROOT / ".env"


class _DockerComposePsqlExecutor:
    """Chạy `psql` bên trong container `postgres` qua `docker compose exec`."""

    def __init__(self, docker_bin: str, db_config: PostgresDbConfig) -> None:
        self._docker_bin = docker_bin
        self._db_config = db_config

    def _run_psql(self, sql: str, *, tuples_only: bool = False) -> str:
        cmd = [
            self._docker_bin,
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "-e",
            f"PGPASSWORD={self._db_config.password}",
            "postgres",
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            self._db_config.user,
            "-d",
            self._db_config.database,
        ]
        if tuples_only:
            cmd.extend(["-t", "-A"])
        try:
            proc = subprocess.run(
                cmd,
                cwd=REPO_ROOT,
                input=sql,
                text=True,
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise MigrationError(
                f"psql trong container thất bại (exit {exc.returncode}): {stderr}"
            ) from exc
        return proc.stdout

    def execute_in_transaction(self, statements: list[str]) -> None:
        body = "\n".join(stmt.strip() for stmt in statements if stmt.strip())
        if not body:
            return
        self._run_psql(f"BEGIN;\n{body}\nCOMMIT;\n")

    def query_versions(self, sql: str) -> list[str]:
        output = self._run_psql(sql, tuples_only=True)
        return [line.strip() for line in output.splitlines() if line.strip()]


def _control_executor_or_skip() -> SqlExecutor:
    dotenv_vars = parse_dotenv_file(ENV_FILE) if ENV_FILE.is_file() else {}
    env = {
        **dotenv_vars,
        **os.environ,
        "N06_PROFILE": dotenv_vars.get("N06_PROFILE") or os.environ.get("N06_PROFILE", "demo"),
        "N06_OBJECT_STORE_ACCESS_KEY": (
            os.environ.get("N06_OBJECT_STORE_ACCESS_KEY")
            or dotenv_vars.get("N06_OBJECT_STORE_ACCESS_KEY", "test_ak")
        ),
        "N06_OBJECT_STORE_SECRET_KEY": (
            os.environ.get("N06_OBJECT_STORE_SECRET_KEY")
            or dotenv_vars.get("N06_OBJECT_STORE_SECRET_KEY", "test_sk")
        ),
        "N06_POSTGRES_MART_PASSWORD": (
            os.environ.get("N06_POSTGRES_MART_PASSWORD")
            or dotenv_vars.get("N06_POSTGRES_MART_PASSWORD", "test_pw")
        ),
        "N06_POSTGRES_CONTROL_PASSWORD": (
            os.environ.get("N06_POSTGRES_CONTROL_PASSWORD")
            or dotenv_vars.get("N06_POSTGRES_CONTROL_PASSWORD", "test_pw")
        ),
        "N06_POSTGRES_AIRFLOW_PASSWORD": (
            os.environ.get("N06_POSTGRES_AIRFLOW_PASSWORD")
            or dotenv_vars.get("N06_POSTGRES_AIRFLOW_PASSWORD", "test_pw")
        ),
        "N06_POSTGRES_METABASE_PASSWORD": (
            os.environ.get("N06_POSTGRES_METABASE_PASSWORD")
            or dotenv_vars.get("N06_POSTGRES_METABASE_PASSWORD", "test_pw")
        ),
    }
    # Đảm bảo khớp host port và database/user đã khởi tạo trong Compose từ .env
    if "N06_POSTGRES_CONTROL_DB" in env:
        env.setdefault("N06__POSTGRES__CONTROL__DATABASE", env["N06_POSTGRES_CONTROL_DB"])
    if "N06_POSTGRES_CONTROL_USER" in env:
        env.setdefault("N06__POSTGRES__CONTROL__USER", env["N06_POSTGRES_CONTROL_USER"])
    env.setdefault("N06__POSTGRES__CONTROL__HOST", "localhost")
    env.setdefault(
        "N06__POSTGRES__CONTROL__PORT",
        env.get("N06_POSTGRES_HOST_PORT", "15432"),
    )
    cfg = load_config(profile=env["N06_PROFILE"], env=env).postgres.control

    try:
        with socket.create_connection((cfg.host, cfg.port), timeout=1.5):
            pass
    except OSError as exc:
        pytest.skip(f"PostgreSQL control DB chưa sẵn sàng tại {cfg.host}:{cfg.port}: {exc}")

    try:
        return connect_control_db(cfg)
    except MigrationError as exc:
        docker_bin = shutil.which("docker")
        if docker_bin is not None:
            return _DockerComposePsqlExecutor(docker_bin, cfg)
        pytest.skip(f"Không có driver/CLI để kết nối control DB: {exc}")


def test_control_migrations_up_down_up_and_unique_constraints() -> None:
    executor = _control_executor_or_skip()
    dataset_id = f"t21-it-{uuid.uuid4().hex[:8]}"
    run_id = f"run-{uuid.uuid4().hex[:12]}"

    # 1. Đưa về trạng thái sạch rồi chạy up -> down -> up
    migrate_down(executor, DEFAULT_CONTROL_MIGRATIONS_DIR)
    assert migrate_up(executor, DEFAULT_CONTROL_MIGRATIONS_DIR) == ["0001"]
    assert get_applied_versions(executor) == ["0001"]

    # Chạy lại up lần nữa không lỗi và không áp dụng lại
    assert migrate_up(executor, DEFAULT_CONTROL_MIGRATIONS_DIR) == []

    # Kiểm tra down rồi up lại
    assert migrate_down(executor, DEFAULT_CONTROL_MIGRATIONS_DIR) == ["0001"]
    assert get_applied_versions(executor) == []
    assert migrate_up(executor, DEFAULT_CONTROL_MIGRATIONS_DIR) == ["0001"]

    try:
        # 2. Kiểm tra unique constraint (query_name, batch_id) trên ingest_batches
        upsert_batch_sql = f"""
        INSERT INTO ingest_batches (
            query_name, batch_id, dataset_id, ingestion_date,
            topic_offsets, record_count, raw_path, bronze_path
        ) VALUES (
            'q_{dataset_id}', 42, '{dataset_id}', DATE '2026-01-01',
            '{{"events": {{"0": [0, 10]}}}}'::jsonb, 10,
            'raw/p1', 'bronze/p1'
        )
        ON CONFLICT (query_name, batch_id) DO UPDATE
            SET record_count = EXCLUDED.record_count,
                updated_at = NOW();
        """
        executor.execute_in_transaction([upsert_batch_sql])
        executor.execute_in_transaction([upsert_batch_sql])

        batch_count = executor.query_versions(
            f"SELECT COUNT(*)::text FROM ingest_batches WHERE dataset_id = '{dataset_id}';"
        )
        assert batch_count == ["1"]

        # Chèn trùng không có ON CONFLICT phải bị chặn bởi unique constraint
        dup_batch_sql = f"""
        INSERT INTO ingest_batches (
            query_name, batch_id, dataset_id, ingestion_date,
            topic_offsets, record_count, raw_path, bronze_path
        ) VALUES (
            'q_{dataset_id}', 42, '{dataset_id}', DATE '2026-01-01',
            '{{"events": {{"0": [0, 10]}}}}'::jsonb, 10,
            'raw/p1', 'bronze/p1'
        );
        """
        with pytest.raises(MigrationError):
            executor.execute_in_transaction([dup_batch_sql])

        # 3. Kiểm tra unique constraint (dataset_id, layer, entity, business_date)
        insert_run_sql = f"""
        INSERT INTO runs (
            run_id, dataset_id, job_name, profile, status,
            row_count, config_hash, schema_version, rule_version, code_version
        ) VALUES (
            '{run_id}', '{dataset_id}', 'process_day', 'test', 'SUCCEEDED',
            10, 'cfg_hash', '1.0.0', '1.0.0', 'dev'
        );
        """
        upsert_snapshot_sql = f"""
        INSERT INTO current_snapshot (
            dataset_id, layer, entity, business_date, run_id, output_path
        ) VALUES (
            '{dataset_id}', 'silver', 'transactions', DATE '2026-01-01',
            '{run_id}', 'silver/transactions/run={run_id}'
        )
        ON CONFLICT (dataset_id, layer, entity, business_date) DO UPDATE
            SET run_id = EXCLUDED.run_id,
                output_path = EXCLUDED.output_path,
                updated_at = NOW();
        """
        executor.execute_in_transaction([insert_run_sql, upsert_snapshot_sql])
        executor.execute_in_transaction([upsert_snapshot_sql])

        snap_count = executor.query_versions(
            f"SELECT COUNT(*)::text FROM current_snapshot WHERE dataset_id = '{dataset_id}';"
        )
        assert snap_count == ["1"]
    finally:
        executor.execute_in_transaction(
            [
                f"DELETE FROM current_snapshot WHERE dataset_id = '{dataset_id}';",
                f"DELETE FROM ingest_batches WHERE dataset_id = '{dataset_id}';",
                f"DELETE FROM runs WHERE dataset_id = '{dataset_id}';",
            ]
        )
