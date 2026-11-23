"""Unit tests cho migration Control DB (TASK T21)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from n06.config import PostgresDbConfig
from n06.control import (
    DEFAULT_CONTROL_MIGRATIONS_DIR,
    MigrationError,
    build_postgres_dsn,
    discover_migrations,
    migrate_down,
    migrate_up,
)


class _FakeExecutor:
    """Executor giả lập để kiểm tra luồng up/down idempotent không cần DB thật."""

    def __init__(self) -> None:
        self.has_table = False
        self.applied: list[str] = []
        self.executed_batches: list[list[str]] = []

    def execute_in_transaction(self, statements: list[str]) -> None:
        self.executed_batches.append(statements)
        joined = "\n".join(statements)
        if "CREATE TABLE IF NOT EXISTS schema_migrations" in joined:
            self.has_table = True
        if "INSERT INTO schema_migrations" in joined:
            match = re.search(r"VALUES\s*\('([^']+)'", joined)
            if match and match.group(1) not in self.applied:
                self.applied.append(match.group(1))
        if "DROP TABLE IF EXISTS schema_migrations" in joined:
            self.has_table = False
            self.applied.clear()
        elif "DELETE FROM schema_migrations" in joined:
            match = re.search(r"version\s*=\s*'([^']+)'", joined)
            if match and match.group(1) in self.applied:
                self.applied.remove(match.group(1))

    def query_versions(self, sql: str) -> list[str]:
        if "to_regclass" in sql:
            return ["1"] if self.has_table else [""]
        return sorted(self.applied)


def test_discover_migrations_finds_init_pair() -> None:
    pairs = discover_migrations(DEFAULT_CONTROL_MIGRATIONS_DIR)
    assert len(pairs) >= 1
    first = pairs[0]
    assert first.version == "0001"
    assert first.name == "init_control_schema"
    assert first.up_path.is_file()
    assert first.down_path.is_file()


def test_discover_migrations_rejects_missing_down(tmp_path: Path) -> None:
    (tmp_path / "0001_init.up.sql").write_text("SELECT 1;", encoding="utf-8")
    with pytest.raises(MigrationError, match="thiếu file"):
        discover_migrations(tmp_path)


def test_discover_migrations_rejects_invalid_name(tmp_path: Path) -> None:
    (tmp_path / "bad_name.sql").write_text("SELECT 1;", encoding="utf-8")
    with pytest.raises(MigrationError, match="không hợp lệ"):
        discover_migrations(tmp_path)


def test_init_sql_has_required_tables_and_unique_constraints() -> None:
    pairs = discover_migrations(DEFAULT_CONTROL_MIGRATIONS_DIR)
    up_sql = pairs[0].up_path.read_text(encoding="utf-8")
    down_sql = pairs[0].down_path.read_text(encoding="utf-8")

    for table in (
        "schema_migrations",
        "ingest_batches",
        "runs",
        "current_snapshot",
        "quality_metrics",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in up_sql
        assert f"DROP TABLE IF EXISTS {table}" in down_sql

    assert "UNIQUE (query_name, batch_id)" in up_sql
    assert "UNIQUE (dataset_id, layer, entity, business_date)" in up_sql
    assert "UNIQUE (run_id, layer, entity, business_date, metric_name)" in up_sql

    # Không lưu secret hoặc mật khẩu trong file migration
    lowered = up_sql.lower() + down_sql.lower()
    assert "password" not in lowered
    assert "secret" not in lowered


def test_build_postgres_dsn_redacts_password() -> None:
    cfg = PostgresDbConfig(
        host="pg-control.internal",
        port=5432,
        database="n06_control",
        user="n06_control_user",
        password="s3cr3t!value",
    )
    masked = build_postgres_dsn(cfg, redact_password=True)
    assert "s3cr3t" not in masked
    assert "postgresql://n06_control_user:***@pg-control.internal:5432/n06_control" == masked

    raw = build_postgres_dsn(cfg, redact_password=False)
    assert "s3cr3t%21value" in raw


def test_migrate_up_down_up_cycle_with_fake_executor() -> None:
    executor = _FakeExecutor()

    applied_first = migrate_up(executor, DEFAULT_CONTROL_MIGRATIONS_DIR)
    assert applied_first == ["0001"]

    # Chạy lại lần hai phải idempotent
    applied_second = migrate_up(executor, DEFAULT_CONTROL_MIGRATIONS_DIR)
    assert applied_second == []

    rolled_back = migrate_down(executor, DEFAULT_CONTROL_MIGRATIONS_DIR)
    assert rolled_back == ["0001"]
    assert executor.has_table is False

    # Chạy lại up sau down phải thành công
    reapplied = migrate_up(executor, DEFAULT_CONTROL_MIGRATIONS_DIR)
    assert reapplied == ["0001"]
    assert executor.has_table is True
