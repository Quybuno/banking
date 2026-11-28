"""Integration test mart trên PostgreSQL thật của Compose (TASK T41, ADR-0005).

Mỗi lần chạy tạo một database tạm `n06_it_mart_<hex>` (chủ sở hữu là user mart) trong cùng
Postgres Compose rồi xóa khi xong, nên không đụng `n06_mart` dùng chung, DB control hay dữ liệu
của test khác.
Migration chạy bằng superuser của container (cần để tạo role `n06_mart_ro`); publish chạy bằng
chính user mart ứng dụng để kiểm tra quyền sở hữu bảng sau migration.

Không skip: thiếu docker hoặc Postgres chưa chạy thì test FAIL (điều kiện nghiệm thu T41).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from n06.cli import parse_dotenv_file
from n06.config import PostgresDbConfig, load_config
from n06.warehouse import (
    DEFAULT_MART_MIGRATIONS_DIR,
    MartPublisher,
    MartPublishError,
    MartValidationError,
    PublishRequest,
    SqlRunner,
    migrate_mart_down,
    migrate_mart_up,
    staging_table_name,
)
from n06.warehouse.schema import ALL_TABLES

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ENV_FILE = REPO_ROOT / ".env"
RO_ROLE = "n06_mart_ro"


class _PsqlFailed(RuntimeError):
    """psql trong container thoát khác 0 (kèm stderr)."""


# ----------------------------------------------------------------------- hạ tầng test


def _docker_bin() -> str:
    docker = shutil.which("docker")
    if docker is None:
        pytest.fail("Cần docker và Postgres của Compose (không skip): không tìm thấy `docker`")
    return docker


def _mart_config() -> PostgresDbConfig:
    dotenv_vars = parse_dotenv_file(ENV_FILE) if ENV_FILE.is_file() else {}
    env = {**dotenv_vars, **os.environ}
    env.setdefault("N06_PROFILE", "demo")
    for secret in (
        "N06_OBJECT_STORE_ACCESS_KEY",
        "N06_OBJECT_STORE_SECRET_KEY",
        "N06_POSTGRES_MART_PASSWORD",
        "N06_POSTGRES_CONTROL_PASSWORD",
        "N06_POSTGRES_AIRFLOW_PASSWORD",
        "N06_POSTGRES_METABASE_PASSWORD",
    ):
        env.setdefault(secret, "test_secret")
    if "N06_POSTGRES_MART_DB" in env:
        env.setdefault("N06__POSTGRES__MART__DATABASE", env["N06_POSTGRES_MART_DB"])
    if "N06_POSTGRES_MART_USER" in env:
        env.setdefault("N06__POSTGRES__MART__USER", env["N06_POSTGRES_MART_USER"])
    env.setdefault("N06__POSTGRES__MART__HOST", "localhost")
    env.setdefault("N06__POSTGRES__MART__PORT", env.get("N06_POSTGRES_HOST_PORT", "15432"))
    return load_config(profile=env["N06_PROFILE"], env=env).postgres.mart


def _run_in_container(
    docker: str, shell_cmd: str, sql: str, *, tuples_only: bool, env: dict[str, str] | None = None
) -> str:
    cmd = [docker, "compose", "-f", str(COMPOSE_FILE), "exec", "-T"]
    for key, value in (env or {}).items():
        cmd += ["-e", f"{key}={value}"]
    cmd += ["postgres", "sh", "-c", shell_cmd + (" -t -A" if tuples_only else "")]
    try:
        proc = subprocess.run(
            cmd, cwd=REPO_ROOT, input=sql, text=True, encoding="utf-8", capture_output=True,
            check=True,
        )  # fmt: skip
    except subprocess.CalledProcessError as exc:
        raise _PsqlFailed((exc.stderr or "").strip()) from exc
    return proc.stdout


def _admin_runner(docker: str, database: str) -> SqlRunner:
    """Chạy psql bằng superuser của container (tên và mật khẩu lấy từ môi trường container)."""
    shell = f'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d {database}'

    def run(sql: str, tuples_only: bool) -> str:
        return _run_in_container(docker, shell, sql, tuples_only=tuples_only)

    return run


def _app_runner(docker: str, cfg: PostgresDbConfig) -> SqlRunner:
    """Chạy psql bằng user mart ứng dụng; mật khẩu qua môi trường, không qua argv của host."""
    shell = 'psql -X -v ON_ERROR_STOP=1 -U "$N06_IT_USER" -d "$N06_IT_DB"'
    env = {
        "N06_IT_USER": cfg.user,
        "N06_IT_DB": cfg.database,
        "PGPASSWORD": cfg.password,
        "PGCLIENTENCODING": "UTF8",
    }

    def run(sql: str, tuples_only: bool) -> str:
        return _run_in_container(docker, shell, sql, tuples_only=tuples_only, env=env)

    return run


@dataclass(frozen=True)
class MartDb:
    name: str
    admin: SqlRunner
    app: SqlRunner


def _create_temp_db(docker: str, cfg: PostgresDbConfig) -> str:
    name = f"n06_it_mart_{uuid.uuid4().hex[:12]}"
    postgres_admin = _admin_runner(docker, "postgres")
    postgres_admin(f'CREATE DATABASE {name} OWNER "{cfg.user}";', False)
    return name


def _drop_temp_db(docker: str, name: str) -> None:
    _admin_runner(docker, "postgres")(f"DROP DATABASE IF EXISTS {name} WITH (FORCE);", False)


@pytest.fixture(scope="module")
def mart() -> Iterator[MartDb]:
    docker = _docker_bin()
    cfg = _mart_config()
    name = _create_temp_db(docker, cfg)
    try:
        admin = _admin_runner(docker, name)
        assert migrate_mart_up(admin) == ["0001", "0002"]
        yield MartDb(name=name, admin=admin, app=_app_runner(docker, replace(cfg, database=name)))
    finally:
        _drop_temp_db(docker, name)


def _scalar(runner: SqlRunner, sql: str) -> str:
    return runner(sql, True).strip()


# ----------------------------------------------------------------------- dữ liệu dựng

BASE_DAY = date(2026, 3, 2)
CHANNELS = {"CH_A": "Kênh A", "CH_B": "Kênh B"}
TX_TYPES = {"TRANSFER": "Chuyển khoản"}
RUN_TS = datetime(2026, 3, 10, 1, 2, 3, tzinfo=UTC)


def _date_key(day: date) -> int:
    return day.year * 10000 + day.month * 100 + day.day


def _dim_date_row(day: date) -> dict[str, Any]:
    return {
        "date_key": _date_key(day),
        "calendar_date": day,
        "year": day.year,
        "quarter": (day.month - 1) // 3 + 1,
        "month": day.month,
        "day": day.day,
        "day_of_week": day.isoweekday(),
    }


def _gold(
    dataset_id: str,
    run_id: str,
    day_offsets: tuple[int, ...] = (0, 1, 2),
    *,
    skip_tx: frozenset[str] = frozenset(),
    skip_balance: frozenset[tuple[str, int]] = frozenset(),
    skip_recon: frozenset[str] = frozenset(),
    amount_bonus: int = 0,
) -> dict[str, list[dict[str, Any]]]:
    """Gold giả theo schema ADR-0005: 2 tài khoản, mỗi ngày 2 giao dịch, đối soát từng giao dịch."""
    accounts = {"acc-1": 1_000_000, "acc-2": 500_000}
    tx, balances, recon = [], [], []
    closing = dict(accounts)
    for offset in day_offsets:
        day = BASE_DAY + timedelta(days=offset)
        moves = [
            (f"tx-{offset}-1", "acc-1", "acc-2", 1000 * (offset + 1) + amount_bonus),
            (f"tx-{offset}-2", "acc-2", "acc-1", 300),
        ]
        debit = {a: 0 for a in accounts}
        credit = {a: 0 for a in accounts}
        entries = {a: 0 for a in accounts}
        for tx_id, src, dst, amount in moves:
            debit[src] += amount
            credit[dst] += amount
            entries[src] += 1
            entries[dst] += 1
            if tx_id not in skip_tx:
                tx.append(
                    {
                        "dataset_id": dataset_id, "transaction_id": tx_id,
                        "business_date": day, "date_key": _date_key(day),
                        "external_reference": f"ext-{tx_id}", "account_from": src,
                        "account_to": dst, "channel": "CH_A", "transaction_type": "TRANSFER",
                        "currency": "VND", "status": "SUCCESS", "amount_minor": amount,
                        "event_time": RUN_TS, "replay_count": 1,
                        "source_pointer": f"kafka://t/0/{offset}", "run_id": run_id,
                    }
                )  # fmt: skip
            if tx_id not in skip_recon:
                mismatch = tx_id.endswith("-2")
                recon.append(
                    {
                        "dataset_id": dataset_id, "channel": "CH_A",
                        "external_reference": f"ext-{tx_id}", "business_date": day,
                        "date_key": _date_key(day),
                        "recon_status": "AMOUNT_MISMATCH" if mismatch else "MATCHED",
                        "issue_codes": ["AMOUNT_MISMATCH"] if mismatch else [],
                        "transaction_id": tx_id, "internal_amount_minor": amount,
                        "internal_currency": "VND", "internal_status": "SUCCESS",
                        "statement_amount_minor": amount + 1 if mismatch else amount,
                        "statement_currency": "VND", "statement_status": "SUCCESS",
                        "internal_record_count": 1, "statement_record_count": 1,
                        "anchor_event_time": RUN_TS if mismatch else None,
                        "discrepancy_age_seconds": 3600 if mismatch else None,
                        "as_of_time": RUN_TS, "last_evaluated_at": RUN_TS,
                        "recon_rule_version": "v1", "source_pointers": ["p1", "p2"],
                        "run_id": run_id, "input_snapshot_id": "snap-1",
                    }
                )  # fmt: skip
        for account, opening in closing.items():
            closing_value = opening + credit[account] - debit[account]
            if (account, offset) not in skip_balance:
                balances.append(
                    {
                        "dataset_id": dataset_id, "account_id": account,
                        "business_date": day, "currency": "VND", "date_key": _date_key(day),
                        "opening_balance_minor": opening, "debit_minor": debit[account],
                        "credit_minor": credit[account], "closing_balance_minor": closing_value,
                        "ledger_entry_count": entries[account], "run_id": run_id,
                    }
                )  # fmt: skip
            closing[account] = closing_value
    dims_date = [
        _dim_date_row(BASE_DAY + timedelta(days=o)) for o in sorted(set(day_offsets) | {0})
    ]
    return {
        "dim_date": dims_date,
        "dim_account": [
            {
                "dataset_id": dataset_id,
                "account_id": a,
                "account_type": "CHECKING",
                "currency": "VND",
                "opening_balance_minor": o,
                "opening_business_date": BASE_DAY,
                "run_id": run_id,
            }
            for a, o in accounts.items()
        ],  # fmt: skip
        "dim_channel": [{"channel": c, "channel_label": label} for c, label in CHANNELS.items()],
        "dim_transaction_type": [
            {"transaction_type": t, "description": d} for t, d in TX_TYPES.items()
        ],
        "fact_transactions": tx,
        "fact_daily_balances": balances,
        "fact_reconciliation": recon,
    }


def _request(
    dataset_id: str, run_id: str, first: int = 0, last: int = 2, **kwargs: Any
) -> PublishRequest:
    return PublishRequest(
        dataset_id=dataset_id,
        run_id=run_id,
        business_date_from=BASE_DAY + timedelta(days=first),
        business_date_to=BASE_DAY + timedelta(days=last),
        **kwargs,
    )


def _new_dataset() -> str:
    return f"t41-it-{uuid.uuid4().hex[:10]}"


def _keys(mart: MartDb, table: str, key_sql: str, dataset_id: str) -> set[str]:
    out = _scalar(
        mart.admin,
        f"SELECT coalesce(string_agg({key_sql}, ',' ORDER BY {key_sql}), '') FROM {table} "
        f"WHERE dataset_id = '{dataset_id}';",
    )
    return set(out.split(",")) if out else set()


def _fingerprint(mart: MartDb, dataset_id: str) -> dict[str, tuple[str, str]]:
    """(số dòng, md5 nội dung) của từng bảng: bảng theo dataset lọc theo dataset_id."""
    result: dict[str, tuple[str, str]] = {}
    for spec in ALL_TABLES:
        where = f"WHERE t.dataset_id = '{dataset_id}'" if spec.dataset_scoped else ""
        out = _scalar(
            mart.admin,
            "SELECT count(*) || '|' || "
            "coalesce(md5(string_agg(t::text, '~' ORDER BY t::text)), '') "
            f"FROM {spec.name} t {where};",
        )
        count, digest = out.split("|")
        result[spec.name] = (count, digest)
    meta = _scalar(
        mart.admin,
        f"SELECT count(*) FROM mart_publish_metadata WHERE dataset_id = '{dataset_id}';",
    )
    result["mart_publish_metadata"] = (meta, "")
    return result


def _staging_tables_left(mart: MartDb, run_id: str) -> str:
    names = ", ".join(f"'{staging_table_name(spec.name, run_id)}'" for spec in ALL_TABLES)
    return _scalar(mart.admin, f"SELECT count(*) FROM pg_tables WHERE tablename IN ({names});")


# ------------------------------------------------------------- 1. migration và schema

# Kỳ vọng chép tay từ ADR-0005 (độc lập với n06.warehouse.schema): tên cột -> (udt_name, nullable).
ADR_0005: dict[str, tuple[tuple[str, ...], dict[str, tuple[str, bool]]]] = {
    "dim_date": (
        ("date_key",),
        {
            "date_key": ("int4", False),
            "calendar_date": ("date", False),
            "year": ("int2", False),
            "quarter": ("int2", False),
            "month": ("int2", False),
            "day": ("int2", False),
            "day_of_week": ("int2", False),
        },
    ),  # fmt: skip
    "dim_account": (
        ("dataset_id", "account_id"),
        {
            "dataset_id": ("text", False),
            "account_id": ("text", False),
            "account_type": ("text", False),
            "currency": ("text", False),
            "opening_balance_minor": ("int8", False),
            "opening_business_date": ("date", False),
            "run_id": ("text", False),
        },
    ),  # fmt: skip
    "dim_channel": (
        ("channel",),
        {"channel": ("text", False), "channel_label": ("text", False)},
    ),
    "dim_transaction_type": (
        ("transaction_type",),
        {"transaction_type": ("text", False), "description": ("text", False)},
    ),
    "fact_transactions": (
        ("dataset_id", "transaction_id"),
        {
            "dataset_id": ("text", False),
            "transaction_id": ("text", False),
            "business_date": ("date", False),
            "date_key": ("int4", False),
            "external_reference": ("text", False),
            "account_from": ("text", False),
            "account_to": ("text", False),
            "channel": ("text", False),
            "transaction_type": ("text", False),
            "currency": ("text", False),
            "status": ("text", False),
            "amount_minor": ("int8", False),
            "event_time": ("timestamptz", False),
            "replay_count": ("int4", False),
            "source_pointer": ("text", False),
            "run_id": ("text", False),
        },
    ),  # fmt: skip
    "fact_daily_balances": (
        ("dataset_id", "account_id", "business_date", "currency"),
        {
            "dataset_id": ("text", False),
            "account_id": ("text", False),
            "business_date": ("date", False),
            "currency": ("text", False),
            "date_key": ("int4", False),
            "opening_balance_minor": ("int8", False),
            "debit_minor": ("int8", False),
            "credit_minor": ("int8", False),
            "closing_balance_minor": ("int8", False),
            "ledger_entry_count": ("int4", False),
            "run_id": ("text", False),
        },
    ),  # fmt: skip
    "fact_reconciliation": (
        ("dataset_id", "channel", "external_reference"),
        {
            "dataset_id": ("text", False),
            "channel": ("text", False),
            "external_reference": ("text", False),
            "business_date": ("date", False),
            "date_key": ("int4", False),
            "recon_status": ("text", False),
            "issue_codes": ("_text", False),
            "transaction_id": ("text", True),
            "internal_amount_minor": ("int8", True),
            "internal_currency": ("text", True),
            "internal_status": ("text", True),
            "statement_amount_minor": ("int8", True),
            "statement_currency": ("text", True),
            "statement_status": ("text", True),
            "internal_record_count": ("int4", False),
            "statement_record_count": ("int4", False),
            "anchor_event_time": ("timestamptz", True),
            "discrepancy_age_seconds": ("int8", True),
            "as_of_time": ("timestamptz", False),
            "last_evaluated_at": ("timestamptz", False),
            "first_seen_at": ("timestamptz", True),
            "resolved_at": ("timestamptz", True),
            "recon_rule_version": ("text", False),
            "source_pointers": ("_text", False),
            "run_id": ("text", False),
            # Cột bổ sung theo spec S361 (quyết định giám sát), không có trong ADR-0005.
            "input_snapshot_id": ("text", True),
        },
    ),  # fmt: skip
}


def test_schema_matches_adr_0005_columns_types_and_keys(mart: MartDb) -> None:
    for table, (pk, columns) in ADR_0005.items():
        rows = _scalar(
            mart.admin,
            "SELECT string_agg(column_name || ':' || udt_name || ':' || is_nullable, ',' "
            "ORDER BY ordinal_position) FROM information_schema.columns "
            f"WHERE table_schema = 'public' AND table_name = '{table}';",
        )
        actual = [item.split(":") for item in rows.split(",")]
        expected = [
            [name, udt, "YES" if nullable else "NO"] for name, (udt, nullable) in columns.items()
        ]
        assert actual == expected, table

        pk_actual = _scalar(
            mart.admin,
            "SELECT string_agg(a.attname, ',' ORDER BY k.ord) "
            "FROM pg_index i "
            "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) "
            "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
            f"WHERE i.indrelid = 'public.{table}'::regclass AND i.indisprimary;",
        )
        assert tuple(pk_actual.split(",")) == pk, table

    unique_date = _scalar(
        mart.admin,
        "SELECT count(*) FROM pg_constraint WHERE conrelid = 'public.dim_date'::regclass "
        "AND contype = 'u' AND pg_get_constraintdef(oid) = 'UNIQUE (calendar_date)';",
    )
    assert unique_date == "1"


def test_warehouse_schema_module_matches_database(mart: MartDb) -> None:
    for spec in ALL_TABLES:
        rows = _scalar(
            mart.admin,
            "SELECT string_agg(column_name || ':' || is_nullable, ',' ORDER BY ordinal_position) "
            f"FROM information_schema.columns WHERE table_name = '{spec.name}';",
        )
        expected = ",".join(f"{c.name}:{'YES' if c.nullable else 'NO'}" for c in spec.columns)
        assert rows == expected, spec.name
        assert spec.primary_key == ADR_0005[spec.name][0]


def test_migrations_up_down_up_idempotent_on_fresh_database() -> None:
    docker = _docker_bin()
    cfg = _mart_config()
    name = _create_temp_db(docker, cfg)
    try:
        admin = _admin_runner(docker, name)
        assert migrate_mart_up(admin) == ["0001", "0002"]
        assert migrate_mart_up(admin) == []

        # Chạy lại nguyên văn từng file up khi đã áp dụng: không lỗi, không đổi dữ liệu.
        for path in sorted(DEFAULT_MART_MIGRATIONS_DIR.glob("*.up.sql")):
            admin(f"BEGIN;\n{path.read_text(encoding='utf-8')}\nCOMMIT;\n", False)
        assert _scalar(admin, "SELECT count(*) FROM schema_migrations;") == "2"

        assert migrate_mart_down(admin) == ["0002", "0001"]
        assert _scalar(admin, "SELECT to_regclass('public.fact_transactions') IS NULL;") == "t"
        assert _scalar(admin, "SELECT to_regclass('public.schema_migrations') IS NULL;") == "t"
        # Role của cả cluster được giữ lại, nhưng không còn quyền nào trên database này.
        assert _scalar(admin, f"SELECT count(*) FROM pg_roles WHERE rolname = '{RO_ROLE}';") == "1"
        assert migrate_mart_down(admin) == []

        assert migrate_mart_up(admin) == ["0001", "0002"]
        assert migrate_mart_up(admin) == []
    finally:
        _drop_temp_db(docker, name)


def test_down_of_read_only_role_revokes_privileges() -> None:
    docker = _docker_bin()
    name = _create_temp_db(docker, _mart_config())
    try:
        admin = _admin_runner(docker, name)
        migrate_mart_up(admin)
        assert migrate_mart_down(admin, steps=1) == ["0002"]
        granted = _scalar(
            admin,
            "SELECT count(*) FROM information_schema.role_table_grants "
            f"WHERE grantee = '{RO_ROLE}' AND table_schema = 'public';",
        )
        assert granted == "0"
    finally:
        _drop_temp_db(docker, name)


def test_read_only_role_can_select_but_not_write(mart: MartDb) -> None:
    role = _scalar(
        mart.admin,
        "SELECT rolcanlogin::text || '|' || rolsuper::text || '|' || rolcreaterole::text "
        "|| '|' || rolcreatedb::text "
        f"FROM pg_roles WHERE rolname = '{RO_ROLE}';",
    )
    assert role == "true|false|false|false"

    privileges = _scalar(
        mart.admin,
        "SELECT string_agg(DISTINCT privilege_type, ',') FROM information_schema.role_table_grants "
        f"WHERE grantee = '{RO_ROLE}' AND table_schema = 'public';",
    )
    assert privileges == "SELECT"
    tables_granted = _scalar(
        mart.admin,
        "SELECT count(DISTINCT table_name) FROM information_schema.role_table_grants "
        f"WHERE grantee = '{RO_ROLE}' AND table_schema = 'public';",
    )
    assert tables_granted == str(len(ADR_0005) + 2)  # 7 bảng + metadata + schema_migrations

    # SELECT được phép.
    mart.admin(
        f"BEGIN;\nSET LOCAL ROLE {RO_ROLE};\nSELECT count(*) FROM dim_channel;\nCOMMIT;\n", True
    )

    denied = [
        "INSERT INTO dim_channel (channel, channel_label) VALUES ('X', 'x')",
        "UPDATE dim_channel SET channel_label = 'y'",
        "DELETE FROM dim_channel",
        "TRUNCATE dim_channel",
        "CREATE TABLE ro_should_not_exist (id int)",
        "INSERT INTO mart_publish_metadata (dataset_id) VALUES ('x')",
    ]
    for statement in denied:
        with pytest.raises(_PsqlFailed, match="permission denied"):
            mart.admin(f"BEGIN;\nSET LOCAL ROLE {RO_ROLE};\n{statement};\nCOMMIT;\n", False)


# --------------------------------------------------------- ràng buộc ở mức database


def _direct_fact_tx(dataset_id: str, tx_id: str, **override: Any) -> str:
    values = {
        "dataset_id": f"'{dataset_id}'", "transaction_id": f"'{tx_id}'",
        "business_date": "DATE '2026-03-02'", "date_key": "20260302",
        "external_reference": "'e'", "account_from": "'a'", "account_to": "'b'",
        "channel": "'CH_A'", "transaction_type": "'TRANSFER'", "currency": "'VND'",
        "status": "'SUCCESS'", "amount_minor": "10", "event_time": "now()",
        "replay_count": "1", "source_pointer": "'p'", "run_id": "'r'",
    }  # fmt: skip
    values.update({k: str(v) for k, v in override.items()})
    return (
        f"INSERT INTO fact_transactions ({', '.join(values)}) VALUES ({', '.join(values.values())})"
    )


def test_database_rejects_invalid_money_keys_and_balances(mart: MartDb) -> None:
    dataset_id = _new_dataset()
    seed = _gold(dataset_id, "run-seed", (0,))
    MartPublisher(mart.app).publish(
        _request(dataset_id, f"run-seed-{uuid.uuid4().hex[:8]}", 0, 0),
        {**seed, "fact_transactions": [], "fact_daily_balances": [], "fact_reconciliation": []},
    )

    def reject(sql: str, message: str) -> None:
        with pytest.raises(_PsqlFailed, match=message):
            mart.app(f"BEGIN;\n{sql};\nCOMMIT;\n", False)

    ok = _direct_fact_tx(dataset_id, "tx-ok")
    mart.app(f"BEGIN;\n{ok};\nCOMMIT;\n", False)

    reject(_direct_fact_tx(dataset_id, "tx-ok"), "duplicate key")  # trùng PK
    reject(_direct_fact_tx(dataset_id, "tx-null", transaction_id="NULL"), "null value")  # PK null
    reject(_direct_fact_tx(dataset_id, "tx-big", amount_minor=2**63), "out of range")
    reject(_direct_fact_tx(dataset_id, "tx-zero", amount_minor=0), "check constraint")
    reject(_direct_fact_tx(dataset_id, "tx-neg", amount_minor=-5), "check constraint")
    reject(_direct_fact_tx(dataset_id, "tx-date", date_key=20991231), "foreign key")
    reject(_direct_fact_tx(dataset_id, "tx-ch", channel="'NOPE'"), "foreign key")
    reject(_direct_fact_tx(dataset_id, "tx-st", status="'WEIRD'"), "check constraint")

    balance = (
        "INSERT INTO fact_daily_balances (dataset_id, account_id, business_date, currency, "
        "date_key, opening_balance_minor, debit_minor, credit_minor, closing_balance_minor, "
        "ledger_entry_count, run_id) VALUES "
        f"('{dataset_id}', 'acc-x', DATE '2026-03-02', 'VND', 20260302, {{op}}, {{debit}}, "
        "{credit}, {closing}, 1, 'r')"
    )
    # opening + credit - debit != closing
    reject(balance.format(op=100, debit=10, credit=5, closing=100), "fact_daily_balances_invariant")
    # tràn BIGINT khi cộng: tính bằng numeric nên vẫn bị bắt bởi invariant, không bị bỏ lọt
    reject(
        balance.format(op=2**63 - 1, debit=0, credit=1, closing=2**63 - 1),
        "fact_daily_balances_invariant",
    )
    reject(balance.format(op=100, debit=-1, credit=0, closing=101), "check constraint")
    mart.app(
        "BEGIN;\n"
        + balance.format(op=2**63 - 1, debit=1, credit=0, closing=2**63 - 2)
        + ";\nCOMMIT;\n",
        False,
    )  # biên miền BIGINT hợp lệ


# -------------------------------------------------------------------------- 2. publish


def test_publish_inserts_all_tables_and_is_idempotent(mart: MartDb) -> None:
    dataset_id, run_id = _new_dataset(), f"run-{uuid.uuid4().hex[:12]}"
    publisher = MartPublisher(mart.app)
    gold = _gold(dataset_id, run_id)
    request = _request(dataset_id, run_id, source_watermark_at=RUN_TS)

    result = publisher.publish(request, gold)
    assert result.rows_by_table["fact_transactions"] == 6
    assert result.rows_by_table["fact_daily_balances"] == 6
    assert result.rows_by_table["fact_reconciliation"] == 6
    first = _fingerprint(mart, dataset_id)
    assert first["fact_transactions"][0] == "6"
    assert first["fact_daily_balances"][0] == "6"
    assert first["fact_reconciliation"][0] == "6"
    assert first["dim_account"][0] == "2" or int(first["dim_account"][0]) >= 2
    assert first["mart_publish_metadata"][0] == "1"
    assert _staging_tables_left(mart, run_id) == "0"

    # Chạy lại cùng output: không tăng số dòng, nội dung không đổi (kể cả dimension).
    publisher.publish(request, gold)
    assert _fingerprint(mart, dataset_id) == first

    meta = _scalar(
        mart.admin,
        "SELECT gold_run_id || '|' || business_date_from || '|' || business_date_to || '|' || "
        "(source_watermark_at = timestamptz '2026-03-10 01:02:03+00')::text "
        f"FROM mart_publish_metadata WHERE dataset_id = '{dataset_id}';",
    )
    assert meta == f"{run_id}|2026-03-02|2026-03-04|true"
    # Tiền là số nguyên (BIGINT) ở mọi cột *_minor.
    non_bigint = _scalar(
        mart.admin,
        "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' "
        "AND column_name LIKE '%\\_minor' AND udt_name <> 'int8';",
    )
    assert non_bigint == "0"


def test_new_output_dropping_old_record_removes_it_and_leaves_other_dates_untouched(
    mart: MartDb,
) -> None:
    """Điều kiện T41: output mới bỏ một record cũ thì record đó biến mất khỏi mart."""
    dataset_id = _new_dataset()
    other_dataset = _new_dataset()
    publisher = MartPublisher(mart.app)
    run_1, run_2, run_3 = (f"run-{uuid.uuid4().hex[:12]}" for _ in range(3))

    publisher.publish(
        _request(other_dataset, f"run-{uuid.uuid4().hex[:12]}"), _gold(other_dataset, "r0")
    )
    other_before = _fingerprint(mart, other_dataset)

    publisher.publish(_request(dataset_id, run_1), _gold(dataset_id, run_1))
    assert len(_keys(mart, "fact_transactions", "transaction_id", dataset_id)) == 6
    day3_before = _scalar(
        mart.admin,
        "SELECT md5(string_agg(t::text, '~' ORDER BY t::text)) FROM fact_transactions t "
        f"WHERE dataset_id = '{dataset_id}' AND business_date = DATE '2026-03-04';",
    )

    # Lần 2 chỉ thay hai ngày đầu: bỏ giao dịch tx-0-2, số dư (acc-2, ngày 1), đối soát của tx-1-1;
    # đổi số tiền các giao dịch còn lại; ngày thứ ba không nằm trong phạm vi.
    gold_2 = _gold(
        dataset_id,
        run_2,
        (0, 1),
        skip_tx=frozenset({"tx-0-2"}),
        skip_balance=frozenset({("acc-2", 0)}),
        skip_recon=frozenset({"tx-1-1"}),
        amount_bonus=7,
    )
    # Số dư ngày 1 của acc-2 bị bỏ nên phải kiểm tra riêng; chuỗi opening phụ thuộc giao dịch cũ.
    publisher.publish(_request(dataset_id, run_2, 0, 1), gold_2)

    assert "tx-0-2" not in _keys(mart, "fact_transactions", "transaction_id", dataset_id)
    assert "ext-tx-1-1" not in _keys(mart, "fact_reconciliation", "external_reference", dataset_id)
    balances = _keys(mart, "fact_daily_balances", "account_id || '@' || business_date", dataset_id)
    assert "acc-2@2026-03-02" not in balances
    assert "acc-1@2026-03-02" in balances
    assert (
        len(_keys(mart, "fact_transactions", "transaction_id", dataset_id)) == 5
    )  # 3 + 2 cũ ngày 3
    amount = _scalar(
        mart.admin,
        f"SELECT amount_minor FROM fact_transactions WHERE dataset_id = '{dataset_id}' "
        "AND transaction_id = 'tx-0-1';",
    )
    assert amount == "1007"
    day3_after = _scalar(
        mart.admin,
        "SELECT md5(string_agg(t::text, '~' ORDER BY t::text)) FROM fact_transactions t "
        f"WHERE dataset_id = '{dataset_id}' AND business_date = DATE '2026-03-04';",
    )
    assert day3_after == day3_before  # ngoài phạm vi ngày không bị đụng
    assert _fingerprint(mart, other_dataset) == other_before  # dataset khác không bị đụng

    # Chạy lại đúng output lần 2: không tăng số dòng.
    after_2 = _fingerprint(mart, dataset_id)
    publisher.publish(_request(dataset_id, run_2, 0, 1), gold_2)
    assert _fingerprint(mart, dataset_id) == after_2

    # Output rỗng cho một phạm vi xóa sạch phạm vi đó (ngày 1 và 2), giữ ngày 3.
    empty = {name: [] for name in (s.name for s in ALL_TABLES)}
    publisher.publish(_request(dataset_id, run_3, 0, 1), empty)
    assert _keys(mart, "fact_transactions", "business_date::text", dataset_id) == {"2026-03-04"}
    assert len(_keys(mart, "fact_reconciliation", "external_reference", dataset_id)) == 2


@pytest.mark.parametrize(
    "failure",
    ["fk_missing_dim_date", "negative_debit", "zero_amount"],
)
def test_failure_inside_transaction_rolls_back_to_previous_version(
    mart: MartDb, failure: str
) -> None:
    dataset_id = _new_dataset()
    run_1, run_2 = (f"run-{uuid.uuid4().hex[:12]}" for _ in range(2))
    publisher = MartPublisher(mart.app)
    publisher.publish(_request(dataset_id, run_1), _gold(dataset_id, run_1))
    before = _fingerprint(mart, dataset_id)

    gold_2 = _gold(dataset_id, run_2, skip_tx=frozenset({"tx-0-1"}), amount_bonus=3)
    # Dimension thay đổi và có dòng mới: phải bị rollback cùng các fact.
    gold_2["dim_channel"] = [{"channel": "CH_A", "channel_label": "đổi nhãn"}]
    gold_2["dim_channel"].append({"channel": "CH_NEW", "channel_label": "mới"})
    request = _request(dataset_id, run_2, 0, 3 if failure == "fk_missing_dim_date" else 2)
    if failure == "fk_missing_dim_date":
        # Ngày 4 hợp lệ về phạm vi, qua kiểm tra staging, nhưng dim_date chưa có -> lỗi FK ở lệnh
        # INSERT fact đầu tiên, sau khi DELETE và upsert dimension đã chạy trong transaction.
        extra_day = BASE_DAY + timedelta(days=3)
        gold_2["fact_transactions"].append(
            {
                **gold_2["fact_transactions"][0],
                "transaction_id": "tx-late",
                "business_date": extra_day,
                "date_key": _date_key(extra_day),
            }  # noqa: E501
        )
    elif failure == "negative_debit":
        # Lỗi ở fact thứ hai: fact_transactions đã thay xong trong transaction rồi mới thất bại.
        gold_2["fact_daily_balances"][0].update(
            debit_minor=-1,
            closing_balance_minor=gold_2["fact_daily_balances"][0]["opening_balance_minor"]
            + gold_2["fact_daily_balances"][0]["credit_minor"]
            + 1,
        )  # noqa: E501
    else:
        gold_2["fact_transactions"][0]["amount_minor"] = 0

    with pytest.raises(MartPublishError, match="check constraint|foreign key"):
        publisher.publish(request, gold_2)

    assert _fingerprint(mart, dataset_id) == before  # mart giữ nguyên bản cũ, kể cả dimension
    assert _scalar(mart.admin, "SELECT count(*) FROM dim_channel WHERE channel = 'CH_NEW';") == "0"
    assert (
        _scalar(mart.admin, "SELECT channel_label FROM dim_channel WHERE channel = 'CH_A';")
        == CHANNELS["CH_A"]
    )
    assert _staging_tables_left(mart, run_2) == "0"

    # Sau lỗi, publish đúng vẫn chạy được trên cùng run_id (staging được làm mới).
    publisher.publish(_request(dataset_id, run_2), _gold(dataset_id, run_2))
    assert len(_keys(mart, "fact_transactions", "transaction_id", dataset_id)) == 6


def _gold_with(
    dataset_id: str, run_id: str, table: str, mutate: Any
) -> dict[str, list[dict[str, Any]]]:
    gold = _gold(dataset_id, run_id)
    mutate(gold[table])
    return gold


@pytest.mark.parametrize(
    ("table", "mutate", "expected"),
    [
        ("fact_transactions", lambda rows: rows[0].update(transaction_id=None), "null_primary_key"),
        ("fact_transactions", lambda rows: rows.append(dict(rows[0])), "duplicate_primary_key"),
        ("fact_transactions", lambda rows: rows[0].update(currency=None), "null_required_column"),
        (
            "fact_transactions",
            lambda rows: rows[0].update(business_date=date(2030, 1, 1), date_key=20300101),
            "outside_business_date_range",
        ),
        (
            "fact_transactions",
            lambda rows: rows[0].update(dataset_id="other-ds"),
            "wrong_dataset_id",
        ),
        ("fact_transactions", lambda rows: rows[0].update(date_key=20990101), "date_key_mismatch"),
        (
            "fact_daily_balances",
            lambda rows: rows[0].update(closing_balance_minor=rows[0]["closing_balance_minor"] + 1),
            "balance_invariant_violation",
        ),
        ("fact_daily_balances", lambda rows: rows.append(dict(rows[1])), "duplicate_primary_key"),
        ("fact_reconciliation", lambda rows: rows[0].update(channel=None), "null_primary_key"),
        ("dim_account", lambda rows: rows.append(dict(rows[0])), "duplicate_primary_key"),
        ("dim_account", lambda rows: rows[0].update(dataset_id="other-ds"), "wrong_dataset_id"),
        ("dim_channel", lambda rows: rows.append(dict(rows[0])), "duplicate_primary_key"),
        ("dim_date", lambda rows: rows[0].update(date_key=None), "null_primary_key"),
    ],
)
def test_staging_validation_blocks_publish_and_keeps_mart(
    mart: MartDb, table: str, mutate: Any, expected: str
) -> None:
    dataset_id = _new_dataset()
    run_1, run_2 = (f"run-{uuid.uuid4().hex[:12]}" for _ in range(2))
    publisher = MartPublisher(mart.app)
    publisher.publish(_request(dataset_id, run_1), _gold(dataset_id, run_1))
    before = _fingerprint(mart, dataset_id)

    with pytest.raises(MartValidationError, match=f"{table}: {expected}"):
        publisher.publish(_request(dataset_id, run_2), _gold_with(dataset_id, run_2, table, mutate))

    assert _fingerprint(mart, dataset_id) == before
    assert _staging_tables_left(mart, run_2) == "0"


def test_money_must_be_integer_in_bigint_domain_before_any_write(mart: MartDb) -> None:
    dataset_id, run_id = _new_dataset(), f"run-{uuid.uuid4().hex[:12]}"
    publisher = MartPublisher(mart.app)
    for bad in (1.5, True, 2**63, -(2**63) - 1, "100", None):
        gold = _gold(dataset_id, run_id)
        gold["fact_transactions"][0]["amount_minor"] = bad
        if bad is None:
            with pytest.raises(MartValidationError, match="null_required_column"):
                publisher.publish(_request(dataset_id, run_id), gold)
        else:
            with pytest.raises(MartPublishError, match="amount_minor"):
                publisher.publish(_request(dataset_id, run_id), gold)
    assert _fingerprint(mart, dataset_id)["fact_transactions"][0] == "0"

    # Biên hợp lệ của BIGINT đi qua nguyên vẹn.
    gold = _gold(dataset_id, run_id, (0,))
    gold["dim_account"][0]["opening_balance_minor"] = 2**63 - 1
    gold["fact_transactions"][0]["amount_minor"] = 2**63 - 1
    publisher.publish(_request(dataset_id, run_id, 0, 0), gold)
    stored = _scalar(
        mart.admin,
        f"SELECT amount_minor::text FROM fact_transactions WHERE dataset_id = '{dataset_id}' "
        "AND transaction_id = 'tx-0-1';",
    )
    assert stored == str(2**63 - 1)
    stored_opening = _scalar(
        mart.admin,
        f"SELECT opening_balance_minor::text FROM dim_account WHERE dataset_id = '{dataset_id}' "
        "AND account_id = 'acc-1';",
    )
    assert stored_opening == str(2**63 - 1)


def test_values_travel_as_parameters_not_sql_text(mart: MartDb) -> None:
    dataset_id, run_id = _new_dataset(), f"run-{uuid.uuid4().hex[:12]}"
    nasty = "x'); DROP TABLE dim_channel; -- \\n \\\\ \" ' é 漢字\nxuống dòng $$ :'params' :foo"
    gold = _gold(dataset_id, run_id, (0,))
    gold["fact_transactions"][0]["external_reference"] = nasty
    gold["fact_transactions"][0]["source_pointer"] = nasty
    gold["fact_reconciliation"][0]["external_reference"] = nasty
    gold["fact_reconciliation"][0]["source_pointers"] = [nasty, "b"]
    gold["dim_channel"][0]["channel_label"] = nasty
    MartPublisher(mart.app).publish(_request(dataset_id, run_id, 0, 0), gold)

    assert _scalar(mart.admin, "SELECT to_regclass('public.dim_channel') IS NOT NULL;") == "t"
    for table, column in (
        ("fact_transactions", "external_reference"),
        ("fact_transactions", "source_pointer"),
        ("dim_channel", "channel_label"),
    ):
        stored = _scalar(
            mart.admin,
            f"SELECT encode(convert_to({column}, 'UTF8'), 'hex') FROM {table} "
            f"WHERE {column} = convert_from(decode('{nasty.encode().hex()}', 'hex'), 'UTF8') "
            "LIMIT 1;",
        )
        assert stored == nasty.encode("utf-8").hex(), (table, column)
    pointers = _scalar(
        mart.admin,
        "SELECT encode(convert_to(source_pointers[1], 'UTF8'), 'hex') FROM fact_reconciliation "
        f"WHERE dataset_id = '{dataset_id}' AND channel = 'CH_A' "
        "AND transaction_id = 'tx-0-1';",
    )
    assert pointers == nasty.encode("utf-8").hex()


def test_mart_user_owns_tables_and_stage_rows_in_chunks(mart: MartDb) -> None:
    owner = _scalar(
        mart.admin,
        "SELECT string_agg(DISTINCT tableowner, ',') FROM pg_tables "
        "WHERE schemaname = 'public' AND tablename NOT LIKE 'stg\\_%';",
    )
    assert owner == _scalar(
        mart.admin,
        "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database();",
    )
    dataset_id, run_id = _new_dataset(), f"run-{uuid.uuid4().hex[:12]}"
    gold = _gold(dataset_id, run_id)
    publisher = MartPublisher(mart.app, chunk_rows=2)  # 6 dòng/bảng -> 3 lô mỗi bảng
    publisher.publish(_request(dataset_id, run_id), gold)
    assert _fingerprint(mart, dataset_id)["fact_transactions"][0] == "6"


def test_publish_request_rejects_unsafe_identifiers_and_inverted_ranges() -> None:
    with pytest.raises(MartPublishError):
        PublishRequest("Bad Dataset!", "run-1", BASE_DAY, BASE_DAY)
    with pytest.raises(MartPublishError):
        PublishRequest("t41-ok-ds", "run-1", BASE_DAY + timedelta(days=1), BASE_DAY)
