"""Unit tests cho `n06 load-mart` (T26): lập kế hoạch snapshot, dựng dim, chuẩn hóa dòng và CLI."""

from __future__ import annotations

import io
import subprocess
from datetime import UTC, date, datetime
from typing import Any

import pytest

from n06.cli import ExitCode, main
from n06.cli.load_mart import LOAD_MART_MODULE, run_load_mart
from n06.storage import SnapshotRecord
from n06.warehouse import (
    TABLES_BY_NAME,
    LoadMartError,
    build_dim_channel_rows,
    build_dim_date_rows,
    build_dim_transaction_type_rows,
    select_publish_plan,
    spark_row_to_mart_row,
)


def _snap(
    *,
    dataset_id: str = "ds-clean",
    layer: str = "gold",
    entity: str = "fact_transactions",
    business_date: date = date(2026, 1, 1),
    run_id: str = "run-001",
    output_path: str | None = None,
) -> SnapshotRecord:
    return SnapshotRecord(
        dataset_id=dataset_id,
        layer=layer,
        entity=entity,
        business_date=business_date,
        run_id=run_id,
        output_path=output_path
        or f"s3a://bucket/{layer}/{dataset_id}/{entity}/run_id={run_id}/business_date={business_date.isoformat()}",
        manifest_path=None,
        semantic_hash="abc123",
    )


def test_build_dim_date_rows_computes_calendar_fields() -> None:
    # 2026-01-01 là Thứ 5 (isoweekday=4), quý 1; 2026-04-05 là Chủ nhật (isoweekday=7), quý 2.
    rows = build_dim_date_rows([date(2026, 4, 5), date(2026, 1, 1), date(2026, 1, 1)])
    assert rows == [
        {
            "date_key": 20260101,
            "calendar_date": date(2026, 1, 1),
            "year": 2026,
            "quarter": 1,
            "month": 1,
            "day": 1,
            "day_of_week": 4,
        },
        {
            "date_key": 20260405,
            "calendar_date": date(2026, 4, 5),
            "year": 2026,
            "quarter": 2,
            "month": 4,
            "day": 5,
            "day_of_week": 7,
        },
    ]


def test_build_dim_channel_and_tx_type_rows_deduplicate_and_sort() -> None:
    channels = build_dim_channel_rows(["SIM_CHANNEL_B", "SIM_CHANNEL_A", "SIM_CHANNEL_B", "OTHER"])
    assert [r["channel"] for r in channels] == ["OTHER", "SIM_CHANNEL_A", "SIM_CHANNEL_B"]
    assert all(isinstance(r["channel_label"], str) and r["channel_label"] for r in channels)

    tx_types = build_dim_transaction_type_rows(["TRANSFER", "PAYMENT", "TRANSFER"])
    assert [r["transaction_type"] for r in tx_types] == ["PAYMENT", "TRANSFER"]
    assert all(isinstance(r["description"], str) and r["description"] for r in tx_types)


def test_spark_row_to_mart_row_normalizes_utc_and_nullable_columns() -> None:
    naive_ts = datetime(2026, 1, 1, 10, 0, 0)
    raw_recon = {
        "dataset_id": "ds-clean",
        "channel": "SIM_CHANNEL_A",
        "external_reference": "EXT-001",
        "business_date": date(2026, 1, 1),
        "date_key": 20260101,
        "recon_status": "MATCHED",
        "issue_codes": [],
        "transaction_id": "tx-1",
        "internal_amount_minor": 1000,
        "internal_currency": "VND",
        "internal_status": "SUCCESS",
        "statement_amount_minor": 1000,
        "statement_currency": "VND",
        "statement_status": "SUCCESS",
        "internal_record_count": 1,
        "statement_record_count": 1,
        "anchor_event_time": None,
        "discrepancy_age_seconds": None,
        "as_of_time": naive_ts,
        "last_evaluated_at": naive_ts,
        "first_seen_at": None,
        "resolved_at": None,
        "recon_rule_version": "v1",
        "source_pointers": ["p1"],
        "run_id": "run-1",
        # Không có input_snapshot_id (giống output T25) + có cột phụ thừa.
        "extra_unused_col": "ignored",
    }
    converted = spark_row_to_mart_row(TABLES_BY_NAME["fact_reconciliation"], raw_recon)
    assert converted["input_snapshot_id"] is None
    assert "extra_unused_col" not in converted
    assert converted["as_of_time"] == datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC)
    assert converted["last_evaluated_at"] == datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC)


def test_spark_row_to_mart_row_drops_silver_account_extra_columns() -> None:
    raw_acc = {
        "dataset_id": "ds-clean",
        "account_id": "acc-1",
        "account_type": "CUSTOMER_DEPOSIT",
        "currency": "VND",
        "opening_balance_minor": 50000,
        "opening_business_date": date(2026, 1, 1),
        "source_file": "accounts.jsonl",
        "business_date": date(2026, 1, 1),
        "run_id": "run-1",
    }
    converted = spark_row_to_mart_row(TABLES_BY_NAME["dim_account"], raw_acc)
    assert set(converted) == set(TABLES_BY_NAME["dim_account"].column_names)
    assert converted["opening_balance_minor"] == 50000


def test_spark_row_to_mart_row_raises_on_missing_required_column() -> None:
    with pytest.raises(LoadMartError) as exc:
        spark_row_to_mart_row(TABLES_BY_NAME["dim_channel"], {"channel": "SIM_CHANNEL_A"})
    assert exc.value.code == "MISSING_COLUMN"


def test_select_publish_plan_raises_when_no_gold_snapshots() -> None:
    with pytest.raises(LoadMartError) as exc:
        select_publish_plan([], [], dataset_id="ds-empty")
    assert exc.value.code == "NO_GOLD_SNAPSHOTS"


def test_select_publish_plan_filters_dates_and_falls_back_to_silver_accounts() -> None:
    gold = [
        _snap(entity="fact_transactions", business_date=date(2026, 1, 1), run_id="run-1"),
        _snap(entity="fact_transactions", business_date=date(2026, 1, 2), run_id="run-2"),
        _snap(entity="fact_reconciliation", business_date=date(2026, 1, 1), run_id="run-1"),
        _snap(entity="fact_reconciliation", business_date=date(2026, 1, 2), run_id="run-2"),
    ]
    silver_acc = [
        _snap(layer="silver", entity="accounts", business_date=date(2026, 1, 1), run_id="run-1"),
        _snap(layer="silver", entity="accounts", business_date=date(2026, 1, 2), run_id="run-2"),
    ]
    plan = select_publish_plan(
        gold,
        silver_acc,
        dataset_id="ds-clean",
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 1),
    )
    assert plan.business_date_from == date(2026, 1, 1)
    assert plan.business_date_to == date(2026, 1, 1)
    assert plan.gold_run_id == "run-1"
    assert plan.gold_run_ids == ("run-1",)
    assert len(plan.fact_paths["fact_transactions"]) == 1
    assert len(plan.fact_paths["fact_reconciliation"]) == 1
    assert plan.fact_paths["fact_daily_balances"] == ()
    assert plan.account_paths == (silver_acc[0].output_path,)


def test_select_publish_plan_prefers_gold_dim_account_over_silver() -> None:
    gold = [
        _snap(entity="fact_transactions", business_date=date(2026, 1, 1), run_id="run-1"),
        _snap(entity="dim_account", business_date=date(2026, 1, 1), run_id="run-1"),
    ]
    silver_acc = [
        _snap(layer="silver", entity="accounts", business_date=date(2026, 1, 1), run_id="run-1"),
    ]
    plan = select_publish_plan(gold, silver_acc, dataset_id="ds-clean")
    assert plan.account_paths == (gold[1].output_path,)


def test_cli_load_mart_validates_dates_before_launching_docker() -> None:
    out = io.StringIO()
    code = main(["load-mart", "--dataset-id", "ds-clean", "--from-date", "bad-date"], out=out)
    assert code == int(ExitCode.CONFIG_ERROR)
    assert "YYYY-MM-DD" in out.getvalue()

    out2 = io.StringIO()
    code2 = main(
        [
            "load-mart",
            "--dataset-id",
            "ds-clean",
            "--from-date",
            "2026-01-05",
            "--to-date",
            "2026-01-01",
        ],
        out=out2,
    )
    assert code2 == int(ExitCode.CONFIG_ERROR)
    assert "--from-date" in out2.getvalue()


def test_run_load_mart_builds_spark_submit_command() -> None:
    captured: dict[str, Any] = {}

    def fake_runner(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, "", "")

    env = {
        "N06_OBJECT_STORE_ACCESS_KEY": "minioadmin",
        "N06_OBJECT_STORE_SECRET_KEY": "minioadmin123",
        "N06_POSTGRES_MART_PASSWORD": "p1",
        "N06_POSTGRES_CONTROL_PASSWORD": "p2",
        "N06_POSTGRES_AIRFLOW_PASSWORD": "p3",
        "N06_POSTGRES_METABASE_PASSWORD": "p4",
    }
    code = run_load_mart(
        dataset_id="ds-clean",
        from_date="2026-01-01",
        to_date="2026-01-02",
        profile="demo",
        env=env,
        runner=fake_runner,
        docker_bin="/usr/bin/docker",
        out=io.StringIO(),
    )
    assert code == 0
    cmd = captured["cmd"]
    assert f"/opt/n06/src/{LOAD_MART_MODULE.replace('.', '/')}.py" in cmd
    assert "--dataset-id" in cmd and "ds-clean" in cmd
    assert "--from-date" in cmd and "2026-01-01" in cmd
    assert "--to-date" in cmd and "2026-01-02" in cmd
