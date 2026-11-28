"""Unit test migration mart (TASK T41): cặp up/down, nội dung DDL và bộ chuyển đổi runner."""

from __future__ import annotations

import re

import pytest

from n06.control import MigrationError
from n06.warehouse import (
    ALL_TABLES,
    DEFAULT_MART_MIGRATIONS_DIR,
    RunnerSqlExecutor,
    discover_mart_migrations,
    migrate_mart_down,
    migrate_mart_up,
)

UP_0001 = (DEFAULT_MART_MIGRATIONS_DIR / "0001_init_mart_schema.up.sql").read_text(encoding="utf-8")
DOWN_0001 = (DEFAULT_MART_MIGRATIONS_DIR / "0001_init_mart_schema.down.sql").read_text(
    encoding="utf-8"
)
UP_0002 = (DEFAULT_MART_MIGRATIONS_DIR / "0002_read_only_role.up.sql").read_text(encoding="utf-8")


def test_discovers_paired_migrations_in_order() -> None:
    pairs = discover_mart_migrations()
    assert [(p.version, p.name) for p in pairs] == [
        ("0001", "init_mart_schema"),
        ("0002", "read_only_role"),
    ]


def _code(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def test_up_defines_every_table_column_and_primary_key_of_the_schema_module() -> None:
    code = _code(UP_0001)
    for spec in ALL_TABLES:
        match = re.search(
            rf"CREATE TABLE IF NOT EXISTS {spec.name} \((.*?)\n\);", code, flags=re.DOTALL
        )
        assert match, spec.name
        body = match.group(1)
        for column in spec.columns:
            line = re.search(
                rf"^\s+{column.name} {re.escape(column.sql_type)}(?=[ ,\n]|$)([^\n]*)$",
                body,
                re.M,
            )
            assert line, (spec.name, column.name)
            has_not_null = "NOT NULL" in line.group(1) or "PRIMARY KEY" in line.group(1)
            assert has_not_null != column.nullable, (spec.name, column.name)
        if len(spec.primary_key) > 1:
            assert f"PRIMARY KEY ({', '.join(spec.primary_key)})" in body, spec.name


def test_up_is_idempotent_and_has_no_float_money() -> None:
    code = _code(UP_0001)
    assert "CREATE TABLE " not in code.replace("CREATE TABLE IF NOT EXISTS ", "")
    assert "CREATE INDEX " not in code.replace("CREATE INDEX IF NOT EXISTS ", "")
    assert not re.search(r"\b(FLOAT|REAL|DOUBLE)\b|NUMERIC\s*\(", code.upper())
    columns = re.findall(r"^\s+(\w+_minor) (\w+)", code, re.M)
    assert columns
    assert all(sql_type == "BIGINT" for _, sql_type in columns), columns


def test_every_up_has_matching_down_that_drops_all_tables() -> None:
    for spec in ALL_TABLES:
        assert f"DROP TABLE IF EXISTS {spec.name};" in DOWN_0001
    assert "DROP TABLE IF EXISTS mart_publish_metadata;" in DOWN_0001
    # fact bị xóa trước dim (khóa ngoại).
    assert DOWN_0001.index("fact_transactions") < DOWN_0001.index("dim_channel")


def test_read_only_role_migration_grants_select_only_and_no_password() -> None:
    assert "n06_mart_ro" in UP_0002
    grants = re.findall(r"GRANT (\w+(?:, \w+)*) ON", UP_0002)
    assert "SELECT" in grants
    assert not any(p in g for g in grants for p in ("INSERT", "UPDATE", "DELETE", "ALL"))
    assert "PASSWORD" not in _code(UP_0002).upper()
    assert "NOSUPERUSER" in UP_0002 and "NOCREATEROLE" in UP_0002


class _FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []
        self.applied: list[str] = []
        self.fail = False

    def __call__(self, sql: str, tuples_only: bool) -> str:
        self.calls.append((sql, tuples_only))
        if self.fail:
            raise RuntimeError("boom")
        if tuples_only and "to_regclass" in sql:
            return "1\n" if self.applied else "\n"
        if tuples_only and "SELECT version" in sql:
            return "".join(f"{v}\n" for v in sorted(self.applied))
        for version in re.findall(r"VALUES \('(\d{4})'", sql):
            self.applied.append(version)
        if "DELETE FROM schema_migrations WHERE version =" in sql:
            version = re.search(r"version = '(\d{4})'", sql).group(1)  # type: ignore[union-attr]
            self.applied.remove(version)
        return ""


def test_migrate_up_and_down_run_each_file_in_one_transaction() -> None:
    runner = _FakeRunner()
    assert migrate_mart_up(runner) == ["0001", "0002"]
    assert migrate_mart_up(runner) == []
    scripts = [sql for sql, tuples in runner.calls if not tuples]
    assert len(scripts) == 2
    for script in scripts:
        assert script.startswith("BEGIN;\n") and script.rstrip().endswith("COMMIT;")
    assert migrate_mart_down(runner, steps=1) == ["0002"]
    assert migrate_mart_down(runner) == ["0001"]


def test_runner_errors_become_migration_errors() -> None:
    runner = _FakeRunner()
    runner.fail = True
    with pytest.raises(MigrationError, match="boom"):
        RunnerSqlExecutor(runner).execute_in_transaction(["SELECT 1;"])
    with pytest.raises(MigrationError, match="boom"):
        RunnerSqlExecutor(runner).query_versions("SELECT 1;")
    RunnerSqlExecutor(runner).execute_in_transaction(["   "])  # rỗng: không gọi runner
