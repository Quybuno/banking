"""Job `n06 process-day` (T25): Bronze -> Silver -> đối soát MATCHED -> Gold, commit theo `run_id`.

Giao thức (đặc tả I.8.1 batch, ADR-0002 mục 3 và 6):
1. đọc input theo danh sách `ingest_batches` và biên nhận `_ingest/`, không glob toàn bucket;
2. `start_run`, rồi kiểm MỌI gốc `run_id=<id>/` còn trống bằng bản liệt kê S3A thật
   (`require_unused_run_prefix`) trước khi ghi object đầu tiên;
3. ghi từng (tầng, entity, business_date) vào đường dẫn dựng bằng `build_run_output_path`, đọc lại
   và đối chiếu số dòng;
4. chỉ sau khi mọi output đã ghi và kiểm tra xong mới commit `current_snapshot`;
5. lỗi ở bước nào cũng `fail_run`; `current_snapshot` không bị chạm tới.

Chạy trong image Spark bằng `spark-submit` (xem `n06.cli.process_day`).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from n06.config import QUERY_NAME_PATTERN, AppConfig, ConfigValidationError, load_config
from n06.control import connect_control_db, migrate_up
from n06.ingestion import layout
from n06.ingestion.spark_session import build_spark
from n06.spark_jobs import plan, receipts, s3_listing, transforms
from n06.storage import (
    BatchCommitCoordinator,
    CommitError,
    ControlStore,
    IngestBatchRecord,
    connect_control_store,
    generate_run_id,
)

CONFIG_ERROR_EXIT = 10
SERVICE_ERROR_EXIT = 14
FAIL_POINTS = ("after_write",)
RESULT_PREFIX = "N06_PROCESS_DAY_RESULT="
ERROR_PREFIX = "N06_PROCESS_DAY_ERROR="


class DataError(RuntimeError):
    """Input mà T25 chưa xử lý được (T30 quarantine, T31 conflict): job dừng, không bỏ qua."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class Written:
    layer: str
    entity: str
    business_date: date
    path: str
    rows: int


@dataclass
class RunState:
    cfg: AppConfig
    dataset_id: str
    run_id: str
    as_of_time: datetime
    spark: SparkSession
    files_per_partition: int
    written: list[Written] = field(default_factory=list)
    digests: dict[str, str] = field(default_factory=dict)


def parse_as_of_time(value: str) -> datetime:
    parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    return parsed.replace(tzinfo=UTC)


def write_entity(
    state: RunState,
    layer: str,
    entity: str,
    frames: Sequence[tuple[date, DataFrame, int]],
) -> int:
    """Ghi một entity, mỗi `business_date` một thư mục run bất biến, rồi đọc lại để đối chiếu."""
    total = 0
    for business_date, frame, rows in frames:
        path = plan.output_path(
            state.cfg,
            layer,
            entity,
            dataset_id=state.dataset_id,
            run_id=state.run_id,
            business_date=business_date,
        )
        frame.coalesce(state.files_per_partition).write.mode("errorifexists").parquet(path)
        back = state.spark.read.parquet(path).count()
        if back != rows:
            raise DataError(
                "OUTPUT_COUNT_MISMATCH",
                f"{layer}.{entity} {business_date}: ghi {rows} dòng, đọc lại {back} dòng.",
            )
        state.written.append(Written(layer, entity, business_date, path, rows))
        total += rows
    return total


def _require_zero(df: DataFrame, code: str, message: str) -> None:
    count = df.count()
    if count:
        raise DataError(code, f"{message} ({count} bản ghi).")


def _bronze_frame(
    spark: SparkSession, batches: Sequence[IngestBatchRecord], entity: str
) -> DataFrame | None:
    uris = [
        layout.bronze_entity_uri(b.bronze_path, entity)
        for b in batches
        if s3_listing.exists(spark, layout.bronze_entity_uri(b.bronze_path, entity))
    ]
    return spark.read.parquet(*uris) if uris else None


def _load_receipts(
    spark: SparkSession, cfg: AppConfig, dataset_id: str
) -> tuple[list[receipts.Receipt], dict[str, str]]:
    directory = receipts.receipts_dir_uri(cfg, dataset_id)
    names = s3_listing.list_directory(spark, directory)
    if not names:
        return [], {}
    rows = (
        spark.read.format("binaryFile")
        .load([directory + name for name in names])
        .select("path", "content")
        .collect()
    )
    parsed = [
        receipts.parse_receipt(
            str(r["path"]).rsplit("/", 1)[-1], bytes(r["content"]), dataset_id=dataset_id
        )
        for r in rows
    ]
    ordered = receipts.ordered(parsed)
    uris = {
        r.file_path: receipts.statement_object_uri(cfg, dataset_id, r.file_path) for r in ordered
    }
    for r in ordered:  # biên nhận mà thiếu file hoặc manifest là lỗi toàn vẹn (ADR-0002 mục 4)
        if not s3_listing.exists(spark, uris[r.file_path]):
            raise DataError(
                "RECEIPT_WITHOUT_FILE", f"Biên nhận {r.delivery_order} thiếu file dữ liệu."
            )
        manifest = receipts.statement_object_uri(cfg, dataset_id, r.manifest_path)
        if not s3_listing.exists(spark, manifest):
            raise DataError(
                "RECEIPT_WITHOUT_MANIFEST", f"Biên nhận {r.delivery_order} thiếu manifest."
            )
    return ordered, uris


def _statement_integrity(
    lines: DataFrame, ordered: Sequence[receipts.Receipt], uris: dict[str, str]
) -> None:
    """So sha256 và số dòng vật lý của từng file với biên nhận trước khi đọc nội dung."""
    seen = {
        row["path"]: (row["sha256"], int(row["rows"]))
        for row in lines.groupBy("path", "sha256").agg(F.count(F.lit(1)).alias("rows")).collect()
    }
    for r in ordered:
        sha, rows = seen.get(uris[r.file_path], (None, 0))
        if sha != r.sha256 or rows != r.physical_rows:
            raise DataError(
                "STATEMENT_FILE_MISMATCH",
                f"File sao kê {r.file_path} không khớp biên nhận {r.delivery_order}: "
                f"sha256={sha} (biên nhận {r.sha256}), "
                f"số dòng={rows} (biên nhận {r.physical_rows}).",
            )


def run_job(
    cfg: AppConfig,
    store: ControlStore,
    spark: SparkSession,
    args: argparse.Namespace,
) -> dict[str, object]:
    dataset_id: str = args.dataset_id
    as_of_time = parse_as_of_time(args.as_of_time)
    query_name = args.query_name or cfg.kafka.query_name
    run_id = args.run_id or generate_run_id()
    filter_dates = {date.fromisoformat(d) for d in args.business_date or []}
    coordinator = BatchCommitCoordinator(store)

    batches = coordinator.list_committed_ingest_batches(query_name=query_name)
    if not batches:
        raise DataError(
            "NO_INGEST_BATCHES", f"Không có batch COMMITTED nào của query {query_name!r}."
        )
    ordered_receipts, uris = _load_receipts(spark, cfg, dataset_id)
    visible = receipts.visible_at(ordered_receipts, as_of_time)
    accounts_uri = receipts.accounts_uri(cfg, dataset_id)
    if not s3_listing.exists(spark, accounts_uri):
        raise DataError(
            "ACCOUNTS_MISSING", f"Chưa có {accounts_uri}; chạy `n06 ingest-statements`."
        )

    input_versions = _input_versions(batches, ordered_receipts, args, with_paths=True)

    started = coordinator.start_run(
        dataset_id=dataset_id,
        job_name="process-day",
        profile=cfg.profile,
        business_date_from=min(filter_dates) if filter_dates else None,
        business_date_to=max(filter_dates) if filter_dates else None,
        input_versions=input_versions,
        config_hash=plan.config_hash(cfg),
        schema_version=plan.SCHEMA_VERSION,
        rule_version=plan.RECON_RULE_VERSION,
        code_version=plan.CODE_VERSION,
        run_id=run_id,
    )
    state = RunState(
        cfg=cfg,
        dataset_id=dataset_id,
        run_id=started.run_id,
        as_of_time=as_of_time,
        spark=spark,
        files_per_partition=max(1, cfg.spark.cores),
    )
    try:
        # Bước 2 của giao thức: bản liệt kê S3A thật, trước object đầu tiên của run.
        plan.claim_run_prefixes(
            cfg, s3_listing.make_list_objects(spark), dataset_id=dataset_id, run_id=state.run_id
        )
        summary = _build_and_write(
            state,
            store,
            coordinator,
            batches,
            ordered_receipts,
            uris,
            accounts_uri,
            filter_dates,
            visible,
            args,
        )
    except Exception as exc:
        try:
            coordinator.fail_run(
                run_id=state.run_id, error_summary=f"{type(exc).__name__}: {exc}"[:1000]
            )
        except CommitError:
            pass  # run đã commit hoặc không còn RUNNING; lỗi gốc quan trọng hơn
        raise
    return summary


def _build_and_write(
    state: RunState,
    store: ControlStore,
    coordinator: BatchCommitCoordinator,
    batches: Sequence[IngestBatchRecord],
    ordered_receipts: Sequence[receipts.Receipt],
    uris: dict[str, str],
    accounts_uri: str,
    filter_dates: set[date],
    visible: Sequence[receipts.Receipt],
    args: argparse.Namespace,
) -> dict[str, object]:
    cfg, spark, dataset_id, run_id = state.cfg, state.spark, state.dataset_id, state.run_id
    tz = cfg.business_timezone

    def date_filter(df: DataFrame) -> DataFrame:
        return df.where(F.col("business_date").isin(*filter_dates)) if filter_dates else df

    # --- Silver giao dịch và bút toán -------------------------------------------------------
    counts: dict[str, int] = {}
    silver: dict[str, DataFrame] = {}
    for entity, bronze_entity, key, payload, build in (
        (
            "transactions",
            "transaction",
            "transaction_id",
            transforms.TRANSACTION_PAYLOAD,
            transforms.silver_transactions,
        ),
        (
            "ledger_entries",
            "ledger_entry",
            "entry_id",
            transforms.LEDGER_PAYLOAD,
            transforms.silver_ledger_entries,
        ),
    ):
        bronze = _bronze_frame(spark, batches, bronze_entity)
        if bronze is None:
            raise DataError("NO_BRONZE_DATA", f"Không có Bronze entity={bronze_entity}.")
        grouped, source = transforms.dedup_event_bronze(
            bronze, dataset_id=dataset_id, key=key, payload=payload
        )
        grouped = grouped.persist()
        _require_zero(
            grouped.where(F.col("variants") > 1),
            "BUSINESS_KEY_CONFLICT",
            f"{entity}: cùng khóa nghiệp vụ khác nội dung (T31 chưa có)",
        )
        _require_zero(
            transforms.event_id_conflicts(source),
            "EVENT_ID_CONFLICT",
            f"{entity}: cùng event_id khác nội dung (T31 chưa có)",
        )
        frame = build(grouped, run_id=run_id)
        _require_zero(
            transforms.invalid_event_rows(frame, tz),
            "BUSINESS_DATE_INVALID",
            f"{entity}: event_time/business_date sai hoặc lệch ngày Asia/Ho_Chi_Minh (T30 chưa có)",
        )
        silver[entity] = date_filter(frame).persist()
        counts[f"bronze_physical_{entity}"] = int(source.count())

    # --- Silver sao kê ----------------------------------------------------------------------
    if not ordered_receipts:
        raise DataError(
            "NO_STATEMENT_RECEIPTS", "Chưa có biên nhận sao kê; chạy `n06 ingest-statements`."
        )
    lines = transforms.statement_lines(spark, ordered_receipts, uris).persist()
    _statement_integrity(lines, ordered_receipts, uris)
    flat, invalid = transforms.silver_statements(
        spark, lines, ordered_receipts, uris, dataset_id=dataset_id
    )
    _require_zero(invalid, "STATEMENT_INVALID_ROWS", "sao kê không hợp lệ (T30 chưa có)")
    flat = flat.persist()
    _require_zero(
        flat.where(F.col("variants") > 1),
        "STATEMENT_ID_CONFLICT",
        "cùng statement_record_id khác nội dung (T31 chưa có)",
    )
    statements = transforms.finish_silver_statements(flat, run_id=run_id)
    _require_zero(
        transforms.invalid_event_rows(statements, tz),
        "BUSINESS_DATE_INVALID",
        "sao kê: event_time/business_date lệch ngày Asia/Ho_Chi_Minh (T30 chưa có)",
    )
    silver["statements"] = date_filter(statements).persist()

    # --- Silver tài khoản --------------------------------------------------------------------
    account_files = transforms.read_binary_files(spark, [accounts_uri])
    accounts, bad_accounts, conflicting = transforms.silver_accounts(
        spark, account_files, dataset_id=dataset_id, source_file="accounts.jsonl", run_id=run_id
    )
    _require_zero(bad_accounts, "ACCOUNT_INVALID_ROWS", "tài khoản không hợp lệ (T30 chưa có)")
    _require_zero(conflicting, "ACCOUNT_ID_CONFLICT", "cùng account_id khác nội dung (T31 chưa có)")
    accounts = accounts.persist()

    dates = sorted(
        {
            d
            for name in ("transactions", "ledger_entries", "statements")
            for d, _, _ in transforms.split_by_date(silver[name])
        }
    )
    if not dates:
        raise DataError("NO_BUSINESS_DATES", "Không có business_date nào để xử lý.")

    # --- Gold -------------------------------------------------------------------------------
    gold_tx = transforms.gold_fact_transactions(silver["transactions"], run_id=run_id).persist()
    # Đối soát cần toàn bộ giao dịch và sao kê của dataset, không chỉ ngày đang lọc.
    recon, recon_summary = transforms.reconcile_matched(
        silver["transactions"], silver["statements"], as_of_time=state.as_of_time, run_id=run_id
    )
    recon = recon.persist()

    outputs: dict[tuple[str, str], DataFrame] = {
        (plan.LAYER_SILVER, "transactions"): silver["transactions"],
        (plan.LAYER_SILVER, "ledger_entries"): silver["ledger_entries"],
        (plan.LAYER_SILVER, "statements"): silver["statements"],
        (plan.LAYER_GOLD, "fact_transactions"): gold_tx,
        (plan.LAYER_GOLD, "fact_reconciliation"): recon,
    }
    for (layer, entity), frame in outputs.items():
        state.digests[f"{layer}.{entity}"] = transforms.entity_digest(frame)
    state.digests["silver.accounts"] = transforms.entity_digest(accounts)

    total_rows = 0
    for (layer, entity), frame in outputs.items():
        total_rows += write_entity(state, layer, entity, list(transforms.split_by_date(frame)))
    account_rows = accounts.count()
    account_frames = [
        (d, accounts.withColumn("business_date", F.lit(d)), account_rows) for d in dates
    ]
    total_rows += write_entity(state, plan.LAYER_SILVER, "accounts", account_frames)

    if args.fail_at == "after_write":  # chỉ cho kiểm thử: run thất bại không bao giờ được đọc
        raise DataError("INJECTED_FAILURE", "Tắt có chủ đích sau khi ghi output, trước commit.")

    semantic_hash = plan.run_semantic_hash(
        dataset_id=dataset_id,
        business_date=dates[0],
        input_versions=_input_versions(batches, ordered_receipts, args, with_paths=False),
        config_hash_value=plan.config_hash(cfg),
        entity_digests=state.digests,
    )
    snapshots = []
    for item in state.written:
        snap = coordinator.commit_run(
            run_id=run_id,
            dataset_id=dataset_id,
            layer=item.layer,
            entity=item.entity,
            business_date=item.business_date,
            output_path=item.path,
            row_count=total_rows,
            semantic_hash=semantic_hash,
        )
        snapshots.append(snap)

    summary_row = recon_summary.collect()[0]
    total_keys, matched_keys = int(summary_row["total_keys"]), int(summary_row["matched_keys"] or 0)
    return {
        "run_id": run_id,
        "dataset_id": dataset_id,
        "as_of_time": args.as_of_time,
        "semantic_hash": semantic_hash,
        "business_dates": [d.isoformat() for d in dates],
        "visible_statement_files": len(visible),
        "statement_files": len(ordered_receipts),
        "rows": {
            f"{w.layer}.{w.entity}.{w.business_date.isoformat()}": w.rows for w in state.written
        },
        "reconciliation": {
            "total_keys": total_keys,
            "matched_keys": matched_keys,
            "unclassified_keys": total_keys - matched_keys,
        },
        "physical_counts": counts,
        "committed_snapshots": len(snapshots),
    }


def _input_versions(
    batches: Sequence[IngestBatchRecord],
    ordered_receipts: Sequence[receipts.Receipt],
    args: argparse.Namespace,
    *,
    with_paths: bool,
) -> list[object]:
    """Input của run; bản đưa vào semantic hash bỏ đường dẫn (đổi theo môi trường)."""
    items: list[object] = []
    for b in batches:
        item: dict[str, object] = {
            "kind": "ingest_batch",
            "query_name": b.query_name,
            "batch_id": b.batch_id,
            "record_count": b.record_count,
        }
        if with_paths:
            item["bronze_path"] = b.bronze_path
        items.append(item)
    items += [
        {"kind": "statement_receipt", "delivery_order": r.delivery_order, "sha256": r.sha256}
        for r in ordered_receipts
    ]
    items.append({"kind": "as_of_time", "value": args.as_of_time})
    items.append({"kind": "business_dates", "value": sorted(args.business_date or [])})
    return items


def run(cfg: AppConfig, args: argparse.Namespace) -> dict[str, object]:
    migrate_up(connect_control_db(cfg.postgres.control))
    store = connect_control_store(cfg.postgres.control)
    spark = build_spark(cfg, "process-day")
    try:
        return run_job(cfg, store, spark, args)
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="n06-process-day-job")
    parser.add_argument("--profile")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--as-of-time", required=True)
    parser.add_argument("--query-name")
    parser.add_argument("--business-date", action="append")
    parser.add_argument(
        "--run-id", help="chỉ dùng cho kiểm thử (run_id đã có object phải bị từ chối)"
    )
    parser.add_argument("--fail-at", choices=FAIL_POINTS, help="chỉ dùng cho kiểm thử")
    args = parser.parse_args(argv)
    try:
        cfg = load_config(profile=args.profile, env=os.environ)
        parse_as_of_time(args.as_of_time)
        for value in args.business_date or []:
            date.fromisoformat(value)
    except (ConfigValidationError, ValueError) as exc:
        message = getattr(exc, "public_message", "tham số không hợp lệ")
        print(f"[process-day] Lỗi: {message}", file=sys.stderr)
        return CONFIG_ERROR_EXIT
    query_name = args.query_name or cfg.kafka.query_name
    if not QUERY_NAME_PATTERN.fullmatch(query_name):
        print("[process-day] Lỗi: query_name không đúng mẫu.", file=sys.stderr)
        return CONFIG_ERROR_EXIT
    try:
        result = run(cfg, args)
    except DataError as exc:
        print(
            ERROR_PREFIX + json.dumps({"code": exc.code, "message": str(exc)}, ensure_ascii=False),
            flush=True,
        )
        return SERVICE_ERROR_EXIT
    except CommitError as exc:
        print(
            ERROR_PREFIX
            + json.dumps({"code": "COMMIT_ERROR", "message": str(exc)}, ensure_ascii=False),
            flush=True,
        )
        return SERVICE_ERROR_EXIT
    except Exception as exc:  # lỗi hạ tầng làm job thất bại, không nuốt
        print(f"[process-day] Job thất bại ({type(exc).__name__}): {exc}", file=sys.stderr)
        return SERVICE_ERROR_EXIT
    print(RESULT_PREFIX + json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
