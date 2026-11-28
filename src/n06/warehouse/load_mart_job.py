"""Job `n06 load-mart` (T26): đọc Gold Parquet từ `current_snapshot`, publish vào PostgreSQL mart.

Giao thức (đặc tả I.8.2, ADR-0005 §4):
1. Đọc danh sách đường dẫn đã commit từ `current_snapshot` (`layer='gold'`, và `silver.accounts`
   nếu chưa có `gold.dim_account`), KHÔNG đọc run đang chạy/thất bại và KHÔNG glob toàn bucket.
2. Nạp vào 7 bảng staging `stg_<bảng>_<hex>` qua `MartPublisher` theo từng lô (dùng
   `toLocalIterator()` để không gom toàn bộ fact vào RAM driver).
3. Kiểm tra staging (`validate_staging`) rồi publish toàn bộ 4 dim + 3 fact + metadata trong MỘT
   transaction PostgreSQL duy nhất (`publish_staged`). Luôn dọn staging dù thành công hay lỗi.
4. CHỈ SAU KHI transaction mart commit thành công mới cập nhật `runs.status = 'MART_PUBLISHED'`
   trong Control DB.

Chạy trong image Spark bằng `spark-submit` (xem `n06.cli.load_mart`).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from n06.config import AppConfig, ConfigValidationError, load_config
from n06.control import connect_control_db, migrate_up
from n06.ingestion.spark_session import build_spark
from n06.storage import CommitError, ControlStore, connect_control_store
from n06.warehouse.load_mart import (
    LoadMartError,
    LoadPlan,
    build_dim_channel_rows,
    build_dim_date_rows,
    build_dim_transaction_type_rows,
    select_publish_plan,
    spark_row_to_mart_row,
)
from n06.warehouse.publish import (
    DEFAULT_STAGE_CHUNK_ROWS,
    MartPublisher,
    MartPublishError,
    PublishRequest,
)
from n06.warehouse.runner import MartError, make_psql_runner
from n06.warehouse.schema import TABLES_BY_NAME

CONFIG_ERROR_EXIT = 10
SERVICE_ERROR_EXIT = 14
FAIL_POINTS = ("after_stage",)
RESULT_PREFIX = "N06_LOAD_MART_RESULT="
ERROR_PREFIX = "N06_LOAD_MART_ERROR="


def _stage_fact_table(
    spark: SparkSession,
    publisher: MartPublisher,
    request: PublishRequest,
    table_name: str,
    paths: Sequence[str],
    *,
    seen_dates: set[date],
    seen_channels: set[str],
    seen_tx_types: set[str],
    max_as_of: list[datetime | None],
    chunk_rows: int = DEFAULT_STAGE_CHUNK_ROWS,
) -> int:
    """Đọc Parquet của một bảng fact và nạp theo lô vào staging, đồng thời gom tập dim."""
    if not paths:
        return 0
    spec = TABLES_BY_NAME[table_name]
    df = spark.read.parquet(*paths).where(
        (F.col("business_date") >= F.lit(request.business_date_from))
        & (F.col("business_date") <= F.lit(request.business_date_to))
    )
    total = 0
    batch: list[dict[str, object]] = []
    for row in df.toLocalIterator():
        mart_row = spark_row_to_mart_row(spec, row.asDict(recursive=False))
        biz_date = mart_row.get("business_date")
        if type(biz_date) is date:
            seen_dates.add(biz_date)
        channel = mart_row.get("channel")
        if isinstance(channel, str):
            seen_channels.add(channel)
        tx_type = mart_row.get("transaction_type")
        if isinstance(tx_type, str):
            seen_tx_types.add(tx_type)
        as_of = mart_row.get("as_of_time")
        if isinstance(as_of, datetime):
            aware = as_of.replace(tzinfo=UTC) if as_of.tzinfo is None else as_of.astimezone(UTC)
            if max_as_of[0] is None or aware > max_as_of[0]:
                max_as_of[0] = aware
        batch.append(mart_row)
        if len(batch) >= chunk_rows:
            total += publisher.stage_rows(request, table_name, batch)
            batch.clear()
    if batch:
        total += publisher.stage_rows(request, table_name, batch)
    return total


def _stage_dim_accounts(
    spark: SparkSession,
    publisher: MartPublisher,
    request: PublishRequest,
    paths: Sequence[str],
    *,
    chunk_rows: int = DEFAULT_STAGE_CHUNK_ROWS,
) -> int:
    if not paths:
        return 0
    spec = TABLES_BY_NAME["dim_account"]
    df = spark.read.parquet(*paths).dropDuplicates(["dataset_id", "account_id"])
    total = 0
    batch: list[dict[str, object]] = []
    for row in df.toLocalIterator():
        batch.append(spark_row_to_mart_row(spec, row.asDict(recursive=False)))
        if len(batch) >= chunk_rows:
            total += publisher.stage_rows(request, "dim_account", batch)
            batch.clear()
    if batch:
        total += publisher.stage_rows(request, "dim_account", batch)
    return total


def run_job(
    store: ControlStore,
    spark: SparkSession,
    publisher: MartPublisher,
    *,
    dataset_id: str,
    date_from: date | None = None,
    date_to: date | None = None,
    fail_at: str | None = None,
) -> dict[str, object]:
    """Thực thi trọn vẹn `load-mart` trên `store`, `spark`, `publisher` đã khởi tạo."""
    gold_snapshots = store.list_current_snapshots(
        dataset_id=dataset_id,
        layer="gold",
        business_date_from=date_from,
        business_date_to=date_to,
    )
    account_snapshots = store.list_current_snapshots(
        dataset_id=dataset_id,
        layer="silver",
        entity="accounts",
    )
    plan: LoadPlan = select_publish_plan(
        gold_snapshots,
        account_snapshots,
        dataset_id=dataset_id,
        date_from=date_from,
        date_to=date_to,
    )

    base_request = PublishRequest(
        dataset_id=plan.dataset_id,
        run_id=plan.gold_run_id,
        business_date_from=plan.business_date_from,
        business_date_to=plan.business_date_to,
    )

    seen_dates: set[date] = set()
    seen_channels: set[str] = set()
    seen_tx_types: set[str] = set()
    max_as_of: list[datetime | None] = [None]

    publisher.create_staging(base_request)
    try:
        for table_name, paths in plan.fact_paths.items():
            _stage_fact_table(
                spark,
                publisher,
                base_request,
                table_name,
                paths,
                seen_dates=seen_dates,
                seen_channels=seen_channels,
                seen_tx_types=seen_tx_types,
                max_as_of=max_as_of,
            )
        _stage_dim_accounts(spark, publisher, base_request, plan.account_paths)
        publisher.stage_rows(base_request, "dim_date", build_dim_date_rows(seen_dates))
        publisher.stage_rows(base_request, "dim_channel", build_dim_channel_rows(seen_channels))
        publisher.stage_rows(
            base_request,
            "dim_transaction_type",
            build_dim_transaction_type_rows(seen_tx_types),
        )

        if fail_at == "after_stage":
            raise LoadMartError(
                "INJECTED_FAILURE",
                "Dừng có chủ đích sau khi nạp staging, trước khi publish transaction mart.",
            )

        publish_request = PublishRequest(
            dataset_id=plan.dataset_id,
            run_id=plan.gold_run_id,
            business_date_from=plan.business_date_from,
            business_date_to=plan.business_date_to,
            source_watermark_at=max_as_of[0],
        )
        result = publisher.publish_staged(publish_request)
    finally:
        publisher.drop_staging(base_request)

    # ADR-0005 §4.4: chỉ cập nhật MART_PUBLISHED sau khi transaction mart đã commit thành công.
    for run_id in plan.gold_run_ids:
        store.mark_run_mart_published(run_id=run_id)

    return {
        "dataset_id": plan.dataset_id,
        "gold_run_id": plan.gold_run_id,
        "gold_run_ids": list(plan.gold_run_ids),
        "business_date_from": plan.business_date_from.isoformat(),
        "business_date_to": plan.business_date_to.isoformat(),
        "source_watermark_at": None if max_as_of[0] is None else max_as_of[0].isoformat(),
        "snapshots_read": plan.snapshots_count,
        "rows_by_table": dict(result.rows_by_table),
    }


def run(cfg: AppConfig, args: argparse.Namespace) -> dict[str, object]:
    migrate_up(connect_control_db(cfg.postgres.control))
    store = connect_control_store(cfg.postgres.control)
    publisher = MartPublisher(make_psql_runner(cfg.postgres.mart))
    date_from = date.fromisoformat(args.from_date) if args.from_date else None
    date_to = date.fromisoformat(args.to_date) if args.to_date else None
    spark = build_spark(cfg, "load-mart")
    try:
        return run_job(
            store,
            spark,
            publisher,
            dataset_id=args.dataset_id,
            date_from=date_from,
            date_to=date_to,
            fail_at=args.fail_at,
        )
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="n06-load-mart-job")
    parser.add_argument("--profile")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--fail-at", choices=FAIL_POINTS, help="chỉ dùng cho kiểm thử")
    args = parser.parse_args(argv)
    try:
        cfg = load_config(profile=args.profile, env=os.environ)
        d_from = date.fromisoformat(args.from_date) if args.from_date else None
        d_to = date.fromisoformat(args.to_date) if args.to_date else None
        if d_from is not None and d_to is not None and d_from > d_to:
            raise ValueError("from-date phải <= to-date")
    except (ConfigValidationError, ValueError) as exc:
        message = getattr(exc, "public_message", str(exc))
        print(f"[load-mart] Lỗi: {message}", file=sys.stderr)
        return CONFIG_ERROR_EXIT
    try:
        result = run(cfg, args)
    except LoadMartError as exc:
        print(
            ERROR_PREFIX + json.dumps({"code": exc.code, "message": str(exc)}, ensure_ascii=False),
            flush=True,
        )
        return SERVICE_ERROR_EXIT
    except (MartPublishError, MartError, CommitError) as exc:
        print(
            ERROR_PREFIX
            + json.dumps({"code": type(exc).__name__, "message": str(exc)}, ensure_ascii=False),
            flush=True,
        )
        return SERVICE_ERROR_EXIT
    except Exception as exc:
        print(f"[load-mart] Job thất bại ({type(exc).__name__}): {exc}", file=sys.stderr)
        return SERVICE_ERROR_EXIT
    print(RESULT_PREFIX + json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
