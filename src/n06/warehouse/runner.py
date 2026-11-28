"""Chạy SQL trên PostgreSQL mart qua `psql` và bộ chuyển đổi cho trình chạy migration.

Cơ chế giống `n06.control` và `n06.storage.control_store`: một lệnh `psql` đọc script từ stdin với
`ON_ERROR_STOP=1`, nên một script `BEGIN; ...; COMMIT;` là một transaction thật; lỗi giữa chừng làm
`psql` thoát không COMMIT và PostgreSQL rollback khi phiên đóng. Giá trị dữ liệu đi qua biến psql
(`\\set params '<json>'` rồi `:'params'`), không ghép vào câu SQL.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Callable, Mapping
from typing import Any

from n06.config import PostgresDbConfig

# (sql, tuples_only) -> stdout. Cùng chữ ký với `n06.storage.make_compose_psql_runner`.
SqlRunner = Callable[[str, bool], str]


class MartError(RuntimeError):
    """Lỗi chạy SQL trên database mart."""


def with_params(sql: str, params: Mapping[str, Any]) -> str:
    """Đặt biến psql `params` (JSON một dòng) trước `sql`.

    Trong tham số nháy đơn của meta-command, psql hiểu `\\` kiểu C và `''` là một dấu nháy, nên mỗi
    `\\` và `'` được nhân đôi. `json.dumps` không sinh ký tự xuống dòng thô.
    """
    text = json.dumps(params, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    escaped = text.replace("\\", "\\\\").replace("'", "''")
    return f"\\set params '{escaped}'\n{sql}"


def make_psql_runner(db_config: PostgresDbConfig, *, psql_bin: str | None = None) -> SqlRunner:
    """Runner gọi `psql` có sẵn trong PATH, mật khẩu qua `PGPASSWORD` (không nằm trên argv)."""
    binary = psql_bin or shutil.which("psql")
    if binary is None:
        raise MartError("Không tìm thấy công cụ psql trong PATH.")

    def _runner(sql: str, tuples_only: bool) -> str:
        env = {**os.environ, "PGPASSWORD": db_config.password, "PGCLIENTENCODING": "UTF8"}
        cmd = [
            binary,
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-h",
            db_config.host,
            "-p",
            str(db_config.port),
            "-U",
            db_config.user,
            "-d",
            db_config.database,
        ]
        if tuples_only:
            cmd.extend(["-t", "-A"])
        try:
            proc = subprocess.run(
                cmd,
                input=sql,
                text=True,
                encoding="utf-8",
                capture_output=True,
                env=env,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise MartError(f"psql thất bại (exit {exc.returncode}): {stderr}") from exc
        return proc.stdout

    return _runner
