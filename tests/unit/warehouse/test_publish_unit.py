"""Unit test publish mart (TASK T41) với runner giả: không cần PostgreSQL."""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

import pytest

from n06.warehouse import (
    ALL_TABLES,
    DIMENSION_TABLES,
    FACT_TABLES,
    MartPublisher,
    MartPublishError,
    MartValidationError,
    PublishRequest,
    check_bigint,
    normalize_rows,
    staging_table_name,
    with_params,
)
from n06.warehouse.schema import TABLES_BY_NAME

DAY = date(2026, 5, 4)


def _request(**kwargs: Any) -> PublishRequest:
    values: dict[str, Any] = {
        "dataset_id": "t41-unit",
        "run_id": "run-abc123",
        "business_date_from": DAY,
        "business_date_to": DAY + timedelta(days=2),
    }
    values.update(kwargs)
    return PublishRequest(**values)


class FakeRunner:
    """Ghi lại SQL; trả về kết quả kiểm tra staging đã dựng sẵn."""

    def __init__(self, check_overrides: dict[tuple[str, str], int] | None = None) -> None:
        self.calls: list[tuple[str, bool]] = []
        self.overrides = check_overrides or {}
        self.fail_on: str | None = None

    def __call__(self, sql: str, tuples_only: bool) -> str:
        self.calls.append((sql, tuples_only))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("psql: ERROR: giả lập lỗi")
        if tuples_only and "'null_primary_key'" in sql:
            lines = []
            for spec in ALL_TABLES:
                lines.append(f"{spec.name}|rows|3")
                for check in ("null_primary_key", "duplicate_primary_key"):
                    lines.append(f"{spec.name}|{check}|{self.overrides.get((spec.name, check), 0)}")
            return "\n".join(lines) + "\n"
        return ""

    def transaction_scripts(self) -> list[str]:
        return [sql for sql, _ in self.calls if "BEGIN;" in sql and "DELETE FROM" in sql]


def _empty_tables() -> dict[str, list[dict[str, Any]]]:
    return {spec.name: [] for spec in ALL_TABLES}


def test_staging_name_is_controlled_identifier() -> None:
    name = staging_table_name("fact_transactions", "run-abc123")
    assert re.fullmatch(r"stg_fact_transactions_[0-9a-f]{16}", name)
    assert name == staging_table_name("fact_transactions", "run-abc123")
    assert name != staging_table_name("fact_transactions", "run-abc124")
    with pytest.raises(MartPublishError):
        staging_table_name("fact_transactions; DROP TABLE x", "run-abc123")
    with pytest.raises(MartPublishError):
        staging_table_name("fact_transactions", "run'; DROP TABLE x; --")


def test_request_validation() -> None:
    with pytest.raises(MartPublishError):
        _request(dataset_id="Bad ID")
    with pytest.raises(MartPublishError):
        _request(run_id="x; y")
    with pytest.raises(MartPublishError):
        _request(business_date_from=DAY + timedelta(days=3))
    with pytest.raises(MartPublishError):
        _request(business_date_from=datetime(2026, 5, 4, tzinfo=UTC))  # datetime không phải date
    with pytest.raises(MartPublishError):
        _request(source_watermark_at=datetime(2026, 5, 4))  # không múi giờ
    params = _request(
        source_watermark_at=datetime(2026, 5, 4, 7, 0, tzinfo=timezone(timedelta(hours=7)))
    ).params()
    assert params["source_watermark_at"] == "2026-05-04T00:00:00+00:00"


@pytest.mark.parametrize("bad", [1.5, True, "1", 2**63, -(2**63) - 1, None])
def test_check_bigint_rejects_non_integers_and_out_of_domain(bad: object) -> None:
    with pytest.raises(MartPublishError):
        check_bigint(bad, "amount_minor")


def test_check_bigint_accepts_domain_edges() -> None:
    assert check_bigint(2**63 - 1, "x") == 2**63 - 1
    assert check_bigint(-(2**63), "x") == -(2**63)


def test_normalize_rows_types_and_unknown_columns() -> None:
    spec = TABLES_BY_NAME["fact_reconciliation"]
    row = {
        "dataset_id": "t41-unit",
        "issue_codes": ["A", "B"],
        "anchor_event_time": datetime(2026, 5, 4, 7, 0, tzinfo=timezone(timedelta(hours=7))),
        "discrepancy_age_seconds": 5,
    }
    out = normalize_rows(spec, [row])[0]
    assert out["issue_codes"] == ["A", "B"]
    assert out["anchor_event_time"] == "2026-05-04T00:00:00+00:00"
    assert out["transaction_id"] is None  # cột thiếu thành NULL
    assert set(out) == set(spec.column_names)

    with pytest.raises(MartPublishError, match="không có trong schema"):
        normalize_rows(spec, [{"dataset_id": "x", "ghost": 1}])
    with pytest.raises(MartPublishError, match="múi giờ"):
        normalize_rows(spec, [{"anchor_event_time": datetime(2026, 5, 4)}])
    with pytest.raises(MartPublishError, match="danh sách chuỗi"):
        normalize_rows(spec, [{"issue_codes": "A"}])
    with pytest.raises(MartPublishError, match="cần date"):
        normalize_rows(TABLES_BY_NAME["dim_date"], [{"calendar_date": "2026-05-04"}])
    with pytest.raises(MartPublishError, match="NUL"):
        normalize_rows(TABLES_BY_NAME["dim_channel"], [{"channel": "a\x00b"}])
    with pytest.raises(MartPublishError, match="INT"):
        normalize_rows(TABLES_BY_NAME["dim_date"], [{"date_key": 2**31}])


def test_with_params_escapes_quotes_and_backslashes_on_one_line() -> None:
    sql = with_params("SELECT :'params';", {"v": "a'b\\c\nd"})
    first, rest = sql.split("\n", 1)
    assert rest == "SELECT :'params';"
    assert first == "\\set params '{\"v\":\"a''b\\\\\\\\c\\\\nd\"}'"


def test_values_never_appear_in_sql_text_outside_the_params_line() -> None:
    runner = FakeRunner()
    publisher = MartPublisher(runner)
    nasty = "x'); DROP TABLE dim_channel; --"
    tables = _empty_tables()
    tables["dim_channel"] = [{"channel": nasty, "channel_label": nasty}]
    publisher.publish(_request(dataset_id="t41-inject"), tables)
    for sql, _ in runner.calls:
        body = "\n".join(line for line in sql.splitlines() if not line.startswith("\\set params"))
        assert "DROP TABLE dim_channel" not in body
        assert "t41-inject" not in body
    assert any("DROP TABLE dim_channel" in sql for sql, _ in runner.calls)  # chỉ trong dòng \set


def test_publish_runs_one_transaction_with_delete_before_insert_in_scope() -> None:
    runner = FakeRunner()
    result = MartPublisher(runner).publish(_request(), _empty_tables())
    assert result.rows_by_table == {spec.name: 3 for spec in ALL_TABLES}

    scripts = runner.transaction_scripts()
    assert len(scripts) == 1  # đúng một transaction publish
    script = scripts[0]
    assert script.count("BEGIN;") == 1 and script.count("COMMIT;") == 1
    assert script.index("BEGIN;") < script.index("COMMIT;")

    positions = []
    for spec in DIMENSION_TABLES:
        assert f"INSERT INTO {spec.name} (" in script
        assert "ON CONFLICT" in script
        positions.append(script.index(f"INSERT INTO {spec.name} ("))
    for spec in FACT_TABLES:
        delete = script.index(f"DELETE FROM {spec.name} WHERE dataset_id =")
        insert = script.index(f"INSERT INTO {spec.name} (")
        assert delete < insert
        scope = script[delete:insert]
        assert "business_date BETWEEN" in scope
        assert "params" in scope
        positions.append(delete)
    assert max(positions[: len(DIMENSION_TABLES)]) < min(positions[len(DIMENSION_TABLES) :])
    # Metadata nằm trong cùng transaction, sau các fact.
    assert script.index("INSERT INTO mart_publish_metadata") > script.index(
        f"INSERT INTO {FACT_TABLES[-1].name} ("
    )
    # Không có DDL hay TRUNCATE trong đường publish.
    assert "TRUNCATE" not in script and "DROP" not in script.split("\n", 1)[1]


def test_validation_failure_stops_before_any_transaction_and_still_drops_staging() -> None:
    runner = FakeRunner({("fact_transactions", "duplicate_primary_key"): 2})
    with pytest.raises(MartValidationError, match="fact_transactions: duplicate_primary_key = 2"):
        MartPublisher(runner).publish(_request(), _empty_tables())
    assert runner.transaction_scripts() == []
    last_sql = runner.calls[-1][0]
    assert last_sql.count("DROP TABLE IF EXISTS stg_") == len(ALL_TABLES)


def test_runner_failure_is_wrapped_and_staging_is_dropped() -> None:
    runner = FakeRunner()
    runner.fail_on = "DELETE FROM"
    with pytest.raises(MartPublishError, match="giả lập lỗi"):
        MartPublisher(runner).publish(_request(), _empty_tables())
    assert "DROP TABLE IF EXISTS stg_" in runner.calls[-1][0]


def test_publish_requires_all_seven_tables_and_checks_types_before_touching_db() -> None:
    runner = FakeRunner()
    partial = _empty_tables()
    del partial["fact_reconciliation"]
    with pytest.raises(MartPublishError, match="Cần đúng các bảng"):
        MartPublisher(runner).publish(_request(), partial)
    bad = _empty_tables()
    bad["fact_transactions"] = [{"amount_minor": 1.5}]
    with pytest.raises(MartPublishError, match="amount_minor"):
        MartPublisher(runner).publish(_request(), bad)
    assert runner.calls == []


def test_stage_rows_chunks_and_sends_json_params() -> None:
    runner = FakeRunner()
    rows = [{"channel": f"c{i}", "channel_label": f"l{i}"} for i in range(5)]
    sent = MartPublisher(runner, chunk_rows=2).stage_rows(_request(), "dim_channel", rows)
    assert sent == 5
    assert len(runner.calls) == 3
    first_line = runner.calls[0][0].split("\n", 1)[0]
    payload = json.loads(
        first_line.removeprefix("\\set params '").removesuffix("'").replace("''", "'")
    )
    assert [r["channel"] for r in payload["rows"]] == ["c0", "c1"]
    with pytest.raises(MartPublishError):
        MartPublisher(runner).stage_rows(_request(), "not_a_table", [])
    with pytest.raises(MartPublishError):
        MartPublisher(runner, chunk_rows=0)


def test_validation_sql_covers_adr_checks() -> None:
    runner = FakeRunner()
    publisher = MartPublisher(runner)
    sql = publisher._validation_sql(_request())
    for check in (
        "null_primary_key",
        "null_required_column",
        "duplicate_primary_key",
        "wrong_dataset_id",
        "outside_business_date_range",
        "date_key_mismatch",
        "balance_invariant_violation",
    ):
        assert f"'{check}'" in sql
    assert "opening_balance_minor::numeric + credit_minor::numeric" in sql
