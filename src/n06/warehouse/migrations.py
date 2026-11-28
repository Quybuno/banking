"""Migration lên/xuống cho database mart, dùng lại trình chạy của `n06.control` (ADR-0005, 0013 Q6).

Chỉ khác thư mục migration (`sql/migrations/mart`) và cách thực thi SQL (qua `SqlRunner`). Bảng
`schema_migrations` nằm trong chính database mart.

Lần chạy đầu cần quyền tạo role (superuser hoặc CREATEROLE) vì migration 0002 tạo role chỉ đọc
`n06_mart_ro` của cả cluster; các lần sau idempotent.
"""

from __future__ import annotations

from pathlib import Path

from n06.control import MigrationError, MigrationPair, discover_migrations, migrate_down, migrate_up
from n06.warehouse.runner import SqlRunner

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MART_MIGRATIONS_DIR = REPO_ROOT / "sql" / "migrations" / "mart"


class RunnerSqlExecutor:
    """Bộ thực thi theo giao diện `SqlExecutor` của control, chạy qua một `SqlRunner`."""

    def __init__(self, runner: SqlRunner) -> None:
        self._runner = runner

    def execute_in_transaction(self, statements: list[str]) -> None:
        body = "\n".join(stmt.strip() for stmt in statements if stmt.strip())
        if not body:
            return
        try:
            self._runner(f"BEGIN;\n{body}\nCOMMIT;\n", False)
        except Exception as exc:
            raise MigrationError(f"Thực thi migration mart thất bại: {exc}") from exc

    def query_versions(self, sql: str) -> list[str]:
        try:
            output = self._runner(sql, True)
        except Exception as exc:
            raise MigrationError(f"Truy vấn migration mart thất bại: {exc}") from exc
        return [line.strip() for line in output.splitlines() if line.strip()]


def discover_mart_migrations(migrations_dir: Path | None = None) -> list[MigrationPair]:
    """Quét cặp up/down trong `sql/migrations/mart` (hoặc thư mục chỉ định)."""
    return discover_migrations(migrations_dir or DEFAULT_MART_MIGRATIONS_DIR)


def migrate_mart_up(runner: SqlRunner, migrations_dir: Path | None = None) -> list[str]:
    """Áp dụng migration mart chưa chạy; trả về các version vừa áp dụng."""
    return migrate_up(RunnerSqlExecutor(runner), migrations_dir or DEFAULT_MART_MIGRATIONS_DIR)


def migrate_mart_down(
    runner: SqlRunner,
    migrations_dir: Path | None = None,
    *,
    steps: int | None = None,
) -> list[str]:
    """Gỡ migration mart theo thứ tự ngược; trả về các version đã gỡ."""
    return migrate_down(
        RunnerSqlExecutor(runner),
        migrations_dir or DEFAULT_MART_MIGRATIONS_DIR,
        steps=steps,
    )
