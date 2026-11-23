"""Trình chạy migration lên/xuống cho Control DB (ADR-0003, mục 8.1)."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

from n06.config import PostgresDbConfig

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONTROL_MIGRATIONS_DIR = REPO_ROOT / "sql" / "migrations" / "control"
MIGRATION_FILENAME_RE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.(up|down)\.sql$")


class MigrationError(RuntimeError):
    """Lỗi khi khám phá hoặc thực thi migration Control DB."""


@dataclass(frozen=True)
class MigrationPair:
    """Một cặp migration lên/xuống cho Control DB."""

    version: str
    name: str
    up_path: Path
    down_path: Path


class SqlExecutor(Protocol):
    """Giao diện thực thi SQL trong một transaction."""

    def execute_in_transaction(self, statements: list[str]) -> None:
        """Thực thi tuần tự các câu SQL trong một transaction duy nhất."""

    def query_versions(self, sql: str) -> list[str]:
        """Truy vấn danh sách version (cột đầu tiên) từ DB."""


class PsycopgConnectionExecutor:
    """Adapter cho kết nối DB-API 2.0 (psycopg / psycopg2)."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute_in_transaction(self, statements: list[str]) -> None:
        try:
            with self._conn.cursor() as cur:
                for stmt in statements:
                    if stmt.strip():
                        cur.execute(stmt)
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            raise MigrationError(f"Thực thi migration thất bại: {exc}") from exc

    def query_versions(self, sql: str) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return [str(row[0]) for row in rows]


class PsqlSubprocessExecutor:
    """Adapter chạy SQL qua công cụ dòng lệnh `psql` với PGPASSWORD qua env."""

    def __init__(self, db_config: PostgresDbConfig, psql_bin: str = "psql") -> None:
        self._db_config = db_config
        self._psql_bin = psql_bin

    def _run_psql(self, sql: str, *, tuples_only: bool = False) -> str:
        env = {**os.environ, "PGPASSWORD": self._db_config.password}
        cmd = [
            self._psql_bin,
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            self._db_config.host,
            "-p",
            str(self._db_config.port),
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
                input=sql,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise MigrationError(f"Lệnh psql thất bại (exit {exc.returncode}): {stderr}") from exc
        return proc.stdout

    def execute_in_transaction(self, statements: list[str]) -> None:
        body = "\n".join(stmt.strip() for stmt in statements if stmt.strip())
        if not body:
            return
        wrapped = f"BEGIN;\n{body}\nCOMMIT;\n"
        self._run_psql(wrapped)

    def query_versions(self, sql: str) -> list[str]:
        output = self._run_psql(sql, tuples_only=True)
        return [line.strip() for line in output.splitlines() if line.strip()]


def build_postgres_dsn(db_config: PostgresDbConfig, *, redact_password: bool = False) -> str:
    """Tạo chuỗi kết nối PostgreSQL từ `PostgresDbConfig`, hỗ trợ ẩn mật khẩu."""
    user = quote(db_config.user, safe="")
    password = "***" if redact_password else quote(db_config.password, safe="")
    database = quote(db_config.database, safe="")
    return f"postgresql://{user}:{password}@{db_config.host}:{db_config.port}/{database}"


def connect_control_db(
    db_config: PostgresDbConfig,
    *,
    connector: Callable[..., Any] | None = None,
) -> SqlExecutor:
    """Mở executor kết nối tới Control DB từ `load_config().postgres.control`."""
    if connector is not None:
        conn = connector(build_postgres_dsn(db_config))
        return PsycopgConnectionExecutor(conn)

    for module_name in ("psycopg", "psycopg2"):
        try:
            driver = __import__(module_name)
            conn = driver.connect(
                host=db_config.host,
                port=db_config.port,
                dbname=db_config.database,
                user=db_config.user,
                password=db_config.password,
            )
            return PsycopgConnectionExecutor(conn)
        except ImportError:
            continue

    psql_bin = shutil.which("psql")
    if psql_bin is not None:
        return PsqlSubprocessExecutor(db_config, psql_bin=psql_bin)

    raise MigrationError(
        "Không tìm thấy driver PostgreSQL (psycopg/psycopg2) hoặc công cụ psql trong PATH."
    )


def discover_migrations(migrations_dir: Path | None = None) -> list[MigrationPair]:
    """Quét và kiểm tra tính đầy đủ cặp `.up.sql` / `.down.sql` theo thứ tự version."""
    target_dir = migrations_dir or DEFAULT_CONTROL_MIGRATIONS_DIR
    if not target_dir.is_dir():
        raise MigrationError(f"Thư mục migration không tồn tại: {target_dir}")

    grouped: dict[str, dict[str, tuple[str, Path]]] = {}
    for path in sorted(target_dir.iterdir()):
        if not path.is_file() or not path.name.endswith(".sql"):
            continue
        match = MIGRATION_FILENAME_RE.match(path.name)
        if match is None:
            raise MigrationError(
                f"Tên file migration không hợp lệ '{path.name}' "
                "(yêu cầu: NNNN_ten_ngan.up.sql hoặc .down.sql)."
            )
        version, name, direction = match.groups()
        bucket = grouped.setdefault(version, {})
        if direction in bucket:
            raise MigrationError(f"Trùng file {direction} cho version {version}.")
        bucket[direction] = (name, path)

    pairs: list[MigrationPair] = []
    for version in sorted(grouped):
        bucket = grouped[version]
        if "up" not in bucket or "down" not in bucket:
            raise MigrationError(f"Migration version {version} thiếu file .up.sql hoặc .down.sql.")
        up_name, up_path = bucket["up"]
        down_name, down_path = bucket["down"]
        if up_name != down_name:
            raise MigrationError(
                f"Tên up/down không khớp ở version {version}: {up_name} vs {down_name}."
            )
        pairs.append(
            MigrationPair(
                version=version,
                name=up_name,
                up_path=up_path,
                down_path=down_path,
            )
        )
    return pairs


def _escape_sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def ensure_migrations_table(executor: SqlExecutor) -> None:
    """Đảm bảo bảng `schema_migrations` tồn tại trước khi truy vấn."""
    ddl = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    executor.execute_in_transaction([ddl])


def get_applied_versions(executor: SqlExecutor) -> list[str]:
    """Trả về danh sách version đã áp dụng theo thứ tự tăng dần; rỗng nếu chưa có bảng."""
    exists = executor.query_versions(
        "SELECT CASE WHEN to_regclass('public.schema_migrations') IS NOT NULL THEN '1' ELSE '' END;"
    )
    if not exists or exists[0] != "1":
        return []
    return executor.query_versions("SELECT version FROM schema_migrations ORDER BY version ASC;")


def migrate_up(executor: SqlExecutor, migrations_dir: Path | None = None) -> list[str]:
    """Áp dụng các migration chưa chạy theo thứ tự version tăng dần."""
    pairs = discover_migrations(migrations_dir)
    applied = set(get_applied_versions(executor))
    newly_applied: list[str] = []

    for pair in pairs:
        if pair.version in applied:
            continue
        up_sql = pair.up_path.read_text(encoding="utf-8")
        record_sql = (
            "INSERT INTO schema_migrations (version, name) VALUES "
            f"({_escape_sql_literal(pair.version)}, {_escape_sql_literal(pair.name)}) "
            "ON CONFLICT (version) DO NOTHING;"
        )
        executor.execute_in_transaction([up_sql, record_sql])
        newly_applied.append(pair.version)

    return newly_applied


def migrate_down(
    executor: SqlExecutor,
    migrations_dir: Path | None = None,
    *,
    steps: int | None = None,
) -> list[str]:
    """Gỡ bỏ các migration đã chạy theo thứ tự ngược lại."""
    if steps is not None and steps <= 0:
        return []

    pairs_by_version = {pair.version: pair for pair in discover_migrations(migrations_dir)}
    applied_desc = list(reversed(get_applied_versions(executor)))
    if steps is not None:
        applied_desc = applied_desc[:steps]

    rolled_back: list[str] = []
    for version in applied_desc:
        pair = pairs_by_version.get(version)
        if pair is None:
            raise MigrationError(f"Không tìm thấy file down cho version đã áp dụng: {version}")
        down_sql = pair.down_path.read_text(encoding="utf-8")
        delete_sql = (
            "DO $$ BEGIN "
            "IF to_regclass('public.schema_migrations') IS NOT NULL THEN "
            f"DELETE FROM schema_migrations WHERE version = {_escape_sql_literal(version)}; "
            "END IF; END $$;"
        )
        executor.execute_in_transaction([down_sql, delete_sql])
        rolled_back.append(version)

    return rolled_back
