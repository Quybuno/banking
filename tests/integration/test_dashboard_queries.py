"""Integration test: kiểm tra 6 truy vấn dashboard chạy bằng role chỉ đọc `n06_mart_ro` (T43)."""

from __future__ import annotations

import subprocess

from n06.cli.init_local import parse_dotenv_file
from n06.config import REPO_ROOT

QUERIES_DIR = REPO_ROOT / "sql" / "dashboard_queries"


def test_compose_bi_profile_renders_metabase_service() -> None:
    proc = subprocess.run(
        ["docker", "compose", "--profile", "core", "--profile", "bi", "config", "--services"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    services = set(proc.stdout.strip().splitlines())
    assert "metabase" in services
    assert "postgres" in services


def test_all_dashboard_sql_queries_execute_as_readonly_role() -> None:
    env_path = REPO_ROOT / ".env"
    merged_env = parse_dotenv_file(env_path) if env_path.is_file() else {}
    superuser = merged_env.get("N06_POSTGRES_SUPERUSER", "n06_admin")
    mart_db = merged_env.get("N06_POSTGRES_MART_DB", "n06_mart")

    # Đảm bảo migration mart 0001 + 0002 đã chạy và đặt mật khẩu cho n06_mart_ro.
    for mig in (
        REPO_ROOT / "sql" / "migrations" / "mart" / "0001_init_mart_schema.up.sql",
        REPO_ROOT / "sql" / "migrations" / "mart" / "0002_read_only_role.up.sql",
    ):
        subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "n06-postgres-1",
                "psql",
                "-U",
                superuser,
                "-d",
                mart_db,
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=mig.read_text(encoding="utf-8"),
            text=True,
            capture_output=True,
            check=True,
        )

    sql_files = sorted(QUERIES_DIR.glob("[0-9][0-9]_*.sql"))
    assert len(sql_files) == 6

    for sql_file in sql_files:
        proc = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "n06-postgres-1",
                "psql",
                "-U",
                "n06_mart_ro",
                "-d",
                mart_db,
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=sql_file.read_text(encoding="utf-8"),
            text=True,
            capture_output=True,
            check=False,
        )
        assert proc.returncode == 0, f"Lỗi khi chạy {sql_file.name}: {proc.stderr}"

    # Xác nhận n06_mart_ro không có quyền ghi vào bảng mart.
    write_attempt = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "n06-postgres-1",
            "psql",
            "-U",
            "n06_mart_ro",
            "-d",
            mart_db,
            "-c",
            "DELETE FROM public.fact_transactions WHERE 1 = 0;",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert write_attempt.returncode != 0
