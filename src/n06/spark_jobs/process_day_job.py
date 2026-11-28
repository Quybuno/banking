"""Job `n06 process-day`: Bronze -> Silver -> đối soát 8 trạng thái -> Gold, commit theo `run_id`.

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
from n06.domain import validate_control_report_data
from n06.ingestion import layout
from n06.ingestion.spark_session import build_spark
from n06.quality import metrics as quality_metrics
from n06.quality import spark_conflicts, spark_metrics, spark_quarantine
from n06.reconciliation import ReconciliationSettings, ReconStatus
from n06.reconciliation import reconcile as reconciliation
from n06.reconciliation.rules import KEY_CONFLICT_COLUMNS
from n06.spark_jobs import plan, receipts, s3_listing, transforms
from n06.storage import (
    BatchCommitCoordinator,
    CommitError,
    ControlStore,
    IngestBatchRecord,
    SnapshotSpec,
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
    cached: list[DataFrame] = field(default_factory=list)

    def persist(self, frame: DataFrame) -> DataFrame:
        """Persist và nhớ để `unpersist` khi run kết thúc, dù thành công hay thất bại."""
        self.cached.append(frame)
        return frame.persist()


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


def _bronze_frame_with_raw(
    spark: SparkSession, batches: Sequence[IngestBatchRecord], entity: str
) -> DataFrame | None:
    frames: list[DataFrame] = []
    for batch in batches:
        uri = layout.bronze_entity_uri(batch.bronze_path, entity)
        if s3_listing.exists(spark, uri):
            frames.append(
                spark.read.parquet(uri)
                .withColumn("_raw_path", F.lit(batch.raw_path))
                .withColumn("_ingestion_date", F.lit(batch.ingestion_date).cast("date"))
            )
    if not frames:
        return None
    result = frames[0]
    for nxt in frames[1:]:
        result = result.unionByName(nxt, allowMissingColumns=True)
    return result


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
    files: DataFrame, ordered: Sequence[receipts.Receipt], uris: dict[str, str]
) -> None:
    """So sha256 và số dòng vật lý của từng file với biên nhận trước khi đọc nội dung."""
    seen = {
        row["path"]: (row["sha256"], int(row["rows"]))
        for row in transforms.file_stats(files).collect()
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


def _load_control_report(
    spark: SparkSession, cfg: AppConfig, dataset_id: str
) -> dict[str, object] | None:
    """Đọc `control_report.json` của nguồn (đặc tả II.4, ADR-0014); không phải ground truth."""
    uri = receipts.control_report_uri(cfg, dataset_id)
    if not s3_listing.exists(spark, uri):
        if cfg.quality.require_control_report:
            raise DataError(
                "CONTROL_REPORT_MISSING", f"Chưa có {uri}; chạy `n06 ingest-statements`."
            )
        return None
    row = transforms.read_binary_files(spark, [uri]).select("content").collect()[0]
    result = validate_control_report_data(bytes(row["content"]))
    if not result.is_valid or result.record is None:
        raise DataError("CONTROL_REPORT_INVALID", f"{uri} không đúng hợp đồng: {result.issues}")
    if result.record["dataset_id"] != dataset_id:
        raise DataError(
            "CONTROL_REPORT_DATASET_MISMATCH",
            f"{uri} có dataset_id={result.record['dataset_id']} khác {dataset_id}.",
        )
    return dict(result.record)


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
    control_report = _load_control_report(spark, cfg, dataset_id)

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
            control_report,
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
    finally:
        for frame in state.cached:
            frame.unpersist()
    return summary


def _build_and_write(
    state: RunState,
    store: ControlStore,
    coordinator: BatchCommitCoordinator,
    batches: Sequence[IngestBatchRecord],
    ordered_receipts: Sequence[receipts.Receipt],
    uris: dict[str, str],
    accounts_uri: str,
    control_report: dict[str, object] | None,
    filter_dates: set[date],
    visible: Sequence[receipts.Receipt],
    args: argparse.Namespace,
) -> dict[str, object]:
    cfg, spark, dataset_id, run_id = state.cfg, state.spark, state.dataset_id, state.run_id
    tz = cfg.business_timezone

    def date_filter(df: DataFrame) -> DataFrame:
        return df.where(F.col("business_date").isin(*filter_dates)) if filter_dates else df

    # --- Silver tài khoản (đọc trước để kiểm UNKNOWN_ACCOUNT và ACCOUNT_CURRENCY_MISMATCH) ---
    quarantine_parts: list[tuple[date, DataFrame, int]] = []
    quarantine_dfs: list[DataFrame] = []
    key_conflict_dfs: list[DataFrame] = []
    fallback_date = min(b.ingestion_date for b in batches)

    account_files = transforms.read_binary_files(spark, [accounts_uri])
    accounts, bad_accounts, conflicting = transforms.silver_accounts(
        spark, account_files, dataset_id=dataset_id, source_file="accounts.jsonl", run_id=run_id
    )
    conflicting = state.persist(conflicting)
    if conflicting.count() > 0:
        quarantine_dfs.append(
            conflicting.select(
                F.col("dataset_id").cast("string").alias("dataset_id"),
                F.lit(None).cast("date").alias("business_date"),
                F.lit(run_id).alias("run_id"),
                F.lit("SILVER_DEDUP").alias("stage"),
                F.lit("account").alias("entity"),
                F.col("account_id").cast("string").alias("business_key"),
                F.lit("BUSINESS_KEY_CONFLICT").alias("error_code"),
                F.array(F.lit("BUSINESS_KEY_CONFLICT")).alias("issue_codes"),
                F.lit("Xung đột khóa tài khoản: cùng account_id khác nội dung").alias(
                    "error_detail"
                ),
                F.lit("accounts.jsonl").alias("source_pointer"),
                F.lit(accounts_uri).alias("raw_reference"),
            )
        )
        accounts = accounts.join(
            conflicting.select("dataset_id", "account_id"),
            ["dataset_id", "account_id"],
            "left_anti",
        )
    bad_accounts = state.persist(bad_accounts)
    if bad_accounts.count() > 0:
        quarantine_dfs.append(
            bad_accounts.select(
                F.coalesce(F.col("dataset_id"), F.lit(dataset_id)).alias("dataset_id"),
                F.lit(None).cast("date").alias("business_date"),
                F.lit(run_id).alias("run_id"),
                F.when(F.col("unparsable"), F.lit("BRONZE_PARSE"))
                .otherwise(F.lit("SILVER_BUSINESS"))
                .alias("stage"),
                F.lit("account").alias("entity"),
                F.col("account_id").cast("string").alias("business_key"),
                F.when(F.col("unparsable"), F.lit("INVALID_JSON"))
                .otherwise(F.lit("SCHEMA_VALUE"))
                .alias("error_code"),
                F.array(
                    F.when(F.col("unparsable"), F.lit("INVALID_JSON")).otherwise(
                        F.lit("SCHEMA_VALUE")
                    )
                ).alias("issue_codes"),
                F.lit("Dòng tài khoản không hợp lệ trong accounts.jsonl").alias("error_detail"),
                F.concat_ws(
                    ":", F.lit("accounts.jsonl"), F.col("source_line").cast("string")
                ).alias("source_pointer"),
                F.lit(accounts_uri).alias("raw_reference"),
            )
        )
    accounts = state.persist(accounts)

    # --- Quarantine từ Bronze parse_error (ADR-0004b Q1: phân vùng theo ingestion_date) ------
    for batch in batches:
        pe_uri = layout.bronze_entity_uri(batch.bronze_path, "parse_error")
        if s3_listing.exists(spark, pe_uri):
            pe_q = state.persist(
                spark_quarantine.bronze_parse_errors_to_quarantine(
                    spark.read.parquet(pe_uri),
                    dataset_id=dataset_id,
                    run_id=run_id,
                    raw_path=batch.raw_path,
                )
            )
            pe_count = pe_q.count()
            if pe_count > 0:
                quarantine_parts.append((batch.ingestion_date, pe_q, pe_count))

    # --- Silver giao dịch và bút toán -------------------------------------------------------
    counts: dict[str, int] = {}
    bronze_scoped: dict[str, DataFrame] = {}  # Bronze của dataset, trước mọi bước lọc
    silver_all: dict[str, DataFrame] = {}  # chưa lọc ngày: đối soát cần toàn bộ dataset
    silver: dict[str, DataFrame] = {}  # sau `--business-date`: phần được ghi
    for entity, bronze_entity, key, payload, build, validate_fn in (
        (
            "transactions",
            "transaction",
            "transaction_id",
            transforms.TRANSACTION_PAYLOAD,
            transforms.silver_transactions,
            spark_quarantine.validate_bronze_transactions_spark,
        ),
        (
            "ledger_entries",
            "ledger_entry",
            "entry_id",
            transforms.LEDGER_PAYLOAD,
            transforms.silver_ledger_entries,
            spark_quarantine.validate_bronze_ledger_spark,
        ),
    ):
        bronze = _bronze_frame_with_raw(spark, batches, bronze_entity)
        if bronze is None:
            raise DataError("NO_BRONZE_DATA", f"Không có Bronze entity={bronze_entity}.")
        bronze_scoped[entity] = bronze.where(
            (F.col("dataset_id") == F.lit(dataset_id))
            | (F.col("envelope_dataset_id") == F.lit(dataset_id))
        )
        valid_bronze, q_df = validate_fn(
            bronze,
            accounts,
            dataset_id=dataset_id,
            business_timezone=tz,
            run_id=run_id,
        )
        q_df = state.persist(q_df)
        if q_df.count() > 0:
            quarantine_dfs.append(q_df)
        grouped, source, dedup_q, dedup_kc = spark_conflicts.split_event_conflicts_spark(
            valid_bronze,
            dataset_id=dataset_id,
            entity=bronze_entity,
            key_col=key,
            payload_cols=payload,
            run_id=run_id,
        )
        grouped = state.persist(grouped)
        source = state.persist(source)
        dedup_q = state.persist(dedup_q)
        dedup_kc = state.persist(dedup_kc)
        if dedup_q.count() > 0:
            quarantine_dfs.append(dedup_q)
        if dedup_kc.count() > 0:
            key_conflict_dfs.append(dedup_kc)
        frame = build(grouped, run_id=run_id)
        silver_all[entity] = state.persist(frame)
        silver[entity] = date_filter(silver_all[entity])
        counts[f"bronze_physical_{entity}"] = int(source.count())

    # Kiểm nhóm sổ cái (LEDGER_INCOMPLETE, LEDGER_UNBALANCED, LEDGER_TRANSACTION_MISMATCH)
    clean_ledger, ledger_grp_q = spark_quarantine.validate_ledger_groups_spark(
        silver_all["transactions"],
        silver_all["ledger_entries"],
        run_id=run_id,
        raw_reference=batches[0].raw_path,
    )
    ledger_grp_q = state.persist(ledger_grp_q)
    if ledger_grp_q.count() > 0:
        quarantine_dfs.append(ledger_grp_q)
        silver_all["ledger_entries"] = state.persist(clean_ledger)
        silver["ledger_entries"] = date_filter(silver_all["ledger_entries"])

    # --- Silver sao kê ----------------------------------------------------------------------
    if not ordered_receipts:
        raise DataError(
            "NO_STATEMENT_RECEIPTS", "Chưa có biên nhận sao kê; chạy `n06 ingest-statements`."
        )
    files = transforms.statement_files(spark, ordered_receipts, uris)
    _statement_integrity(files, ordered_receipts, uris)
    flat, invalid = transforms.silver_statements(
        spark, files, ordered_receipts, uris, dataset_id=dataset_id
    )
    invalid = state.persist(invalid)
    if invalid.count() > 0:
        quarantine_dfs.append(
            invalid.select(
                F.coalesce(F.col("dataset_id"), F.lit(dataset_id)).alias("dataset_id"),
                F.to_date(F.col("business_date"), "yyyy-MM-dd").alias("business_date"),
                F.lit(run_id).alias("run_id"),
                F.when(F.col("unparsable"), F.lit("BRONZE_PARSE"))
                .otherwise(F.lit("SILVER_BUSINESS"))
                .alias("stage"),
                F.lit("statement").alias("entity"),
                F.col("statement_record_id").cast("string").alias("business_key"),
                F.when(F.col("unparsable"), F.lit("INVALID_JSON"))
                .otherwise(F.lit("SCHEMA_VALUE"))
                .alias("error_code"),
                F.array(
                    F.when(F.col("unparsable"), F.lit("INVALID_JSON")).otherwise(
                        F.lit("SCHEMA_VALUE")
                    )
                ).alias("issue_codes"),
                F.lit("Dòng sao kê không hợp lệ").alias("error_detail"),
                F.concat_ws(
                    ":",
                    F.col("source_file"),
                    F.col("source_sha256"),
                    F.col("source_line").cast("string"),
                ).alias("source_pointer"),
                F.col("source_file").alias("raw_reference"),
            )
        )
    flat = state.persist(flat)
    clean_flat, stmt_q, stmt_kc = spark_conflicts.split_statement_conflicts_spark(
        flat, run_id=run_id
    )
    stmt_q = state.persist(stmt_q)
    stmt_kc = state.persist(stmt_kc)
    if stmt_q.count() > 0:
        quarantine_dfs.append(stmt_q)
    if stmt_kc.count() > 0:
        key_conflict_dfs.append(stmt_kc)
    statements = transforms.finish_silver_statements(clean_flat, run_id=run_id)
    bad_stmt_dates = state.persist(transforms.invalid_event_rows(statements, tz))
    if bad_stmt_dates.count() > 0:
        quarantine_dfs.append(
            bad_stmt_dates.select(
                F.col("dataset_id"),
                F.col("business_date"),
                F.lit(run_id).alias("run_id"),
                F.lit("SILVER_BUSINESS").alias("stage"),
                F.lit("statement").alias("entity"),
                F.col("statement_record_id").alias("business_key"),
                F.lit("BUSINESS_DATE_MISMATCH").alias("error_code"),
                F.array(F.lit("BUSINESS_DATE_MISMATCH")).alias("issue_codes"),
                F.lit("sao kê: event_time/business_date lệch ngày theo múi giờ nghiệp vụ").alias(
                    "error_detail"
                ),
                F.concat_ws(
                    ":",
                    F.col("source_file"),
                    F.col("source_sha256"),
                    F.col("source_line").cast("string"),
                ).alias("source_pointer"),
                F.col("source_file").alias("raw_reference"),
            )
        )
        statements = statements.where(
            ~transforms.business_date_mismatch("event_time", "business_date", tz)
            & F.col("event_time").isNotNull()
            & F.col("business_date").isNotNull()
        )
    silver_all["statements"] = state.persist(statements)
    silver["statements"] = date_filter(silver_all["statements"])

    all_dates = sorted(
        {
            d
            for name in ("transactions", "ledger_entries", "statements")
            for d, _, _ in transforms.split_by_date(silver_all[name])
        }
    )
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
    gold_tx = state.persist(
        transforms.gold_fact_transactions(silver["transactions"], run_id=run_id)
    )
    balances_all, balance_overflow = transforms.gold_fact_daily_balances_with_overflow(
        spark,
        accounts,
        silver_all["ledger_entries"],
        business_dates=all_dates,
        run_id=run_id,
    )
    _require_zero(
        balance_overflow,
        "BALANCE_BIGINT_OVERFLOW",
        "Số dư hoặc tổng phát sinh ngày vượt miền BIGINT",
    )
    gold_balances = state.persist(date_filter(balances_all))
    # Đối soát chạy trên toàn bộ giao dịch và sao kê của dataset; `--business-date` chỉ lọc đầu ra.
    prev_recon_snaps = store.list_current_snapshots(
        dataset_id=dataset_id,
        layer=plan.LAYER_GOLD,
        entity="fact_reconciliation",
    )
    prev_recon_paths = [
        s.output_path for s in prev_recon_snaps if s3_listing.exists(spark, s.output_path)
    ]
    if prev_recon_paths:
        previous_recon = spark.read.parquet(*prev_recon_paths)
    else:
        previous_recon = spark.createDataFrame(
            [],
            "dataset_id string, channel string, external_reference string, "
            "first_seen_at timestamp, resolved_at timestamp",
        )
    recon_all = state.persist(
        reconciliation.reconcile_with_diagnostics(
            silver_all["transactions"],
            silver_all["statements"],
            _key_conflicts_for_reconciliation(spark, key_conflict_dfs),
            as_of_time=state.as_of_time,
            settings=ReconciliationSettings.from_config(cfg),
            run_id=run_id,
            input_snapshot_id=plan.input_snapshot_id(
                _input_versions(batches, ordered_receipts, args, with_paths=False)
            ),
            previous_recon=previous_recon,
        )
    )
    # Fact phân vùng theo business_date: dòng thiếu ngày sẽ mất im lặng khi ghi.
    _require_zero(
        recon_all.where(F.col("business_date").isNull()),
        "RECON_BUSINESS_DATE_MISSING",
        "Match key không xác định được business_date",
    )
    recon = state.persist(date_filter(recon_all).select(*reconciliation.RESULT_COLUMNS))

    outputs: dict[tuple[str, str], DataFrame] = {
        (plan.LAYER_SILVER, "transactions"): silver["transactions"],
        (plan.LAYER_SILVER, "ledger_entries"): silver["ledger_entries"],
        (plan.LAYER_SILVER, "statements"): silver["statements"],
        (plan.LAYER_GOLD, "fact_transactions"): gold_tx,
        (plan.LAYER_GOLD, "fact_daily_balances"): gold_balances,
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

    # --- Silver key_conflicts (T31): chỉ ghi nếu có xung đột khóa ----------------------------
    key_conflicts_total = 0
    kc_frames: list[tuple[date, DataFrame, int]] = []
    if key_conflict_dfs:
        unioned_kc = key_conflict_dfs[0]
        for nxt in key_conflict_dfs[1:]:
            unioned_kc = unioned_kc.unionByName(nxt)
        kc_filtered = state.persist(date_filter(unioned_kc))
        kc_frames = list(transforms.split_by_date(kc_filtered))
        if kc_frames:
            state.digests["silver.key_conflicts"] = transforms.entity_digest(kc_filtered)
            key_conflicts_total = write_entity(state, plan.LAYER_SILVER, "key_conflicts", kc_frames)
            total_rows += key_conflicts_total

    # --- Quarantine (T30): chỉ ghi nếu có bản ghi lỗi; không ghi đè lỗi run cũ --------------
    quarantine_by_date: dict[date, list[DataFrame]] = {}
    for pdate, qdf, _ in quarantine_parts:
        quarantine_by_date.setdefault(pdate, []).append(qdf)
    if quarantine_dfs:
        unioned_q = quarantine_dfs[0]
        for nxt in quarantine_dfs[1:]:
            unioned_q = unioned_q.unionByName(nxt)
        for pdate, qdf, _ in spark_quarantine.partition_quarantine_by_date(
            unioned_q, fallback_date=fallback_date
        ):
            quarantine_by_date.setdefault(pdate, []).append(qdf)
    quarantine_total = 0
    q_frames: list[tuple[date, DataFrame, int]] = []
    if quarantine_by_date:
        for pdate in sorted(quarantine_by_date):
            dfs = quarantine_by_date[pdate]
            merged_q = dfs[0]
            for nxt in dfs[1:]:
                merged_q = merged_q.unionByName(nxt)
            merged_q = state.persist(merged_q)
            cnt = merged_q.count()
            if cnt > 0:
                q_frames.append((pdate, merged_q, cnt))
        if q_frames:
            all_q_for_digest = q_frames[0][1]
            for _, nxt, _ in q_frames[1:]:
                all_q_for_digest = all_q_for_digest.unionByName(nxt)
            state.digests["quarantine.records"] = transforms.entity_digest(all_q_for_digest)
            quarantine_total = write_entity(state, plan.LAYER_QUARANTINE, "records", q_frames)
            total_rows += quarantine_total

    # --- Cổng chất lượng và control totals (T32) --------------------------------------------
    # Ghi metrics TRƯỚC khi quyết định, để run bị chặn vẫn còn bằng chứng vì sao.
    evaluation = _evaluate_quality(
        state,
        silver_all=silver_all,
        silver=silver,
        bronze_scoped=bronze_scoped,
        statement_lines=transforms.parse_statement_lines(spark, files, ordered_receipts, uris),
        quarantine_dfs=quarantine_dfs,
        parse_errors=[(day, count) for day, _, count in quarantine_parts],
        quarantine_by_date={day: rows for day, _, rows in q_frames},
        conflicts_by_date={day: rows for day, _, rows in kc_frames},
        control_report=control_report,
        filter_dates=filter_dates,
        default_date=dates[0],
        fallback_date=fallback_date,
    )
    store.upsert_quality_metrics(evaluation.metrics)
    if evaluation.should_block_publish:
        raise DataError(
            "QUALITY_GATE_BLOCKED",
            f"Run {run_id} không publish: " + "; ".join(evaluation.blocking_reasons),
        )

    if args.fail_at == "after_write":  # chỉ cho kiểm thử: run thất bại không bao giờ được đọc
        raise DataError("INJECTED_FAILURE", "Tắt có chủ đích sau khi ghi output, trước commit.")

    semantic_hash = plan.run_semantic_hash(
        dataset_id=dataset_id,
        business_date=dates[0],
        input_versions=_input_versions(batches, ordered_receipts, args, with_paths=False),
        config_hash_value=plan.config_hash(cfg),
        entity_digests=state.digests,
    )
    snapshots = coordinator.commit_run_snapshots(
        run_id=run_id,
        dataset_id=dataset_id,
        snapshots=[
            SnapshotSpec(
                layer=item.layer,
                entity=item.entity,
                business_date=item.business_date,
                output_path=item.path,
            )
            for item in state.written
        ],
        row_count=total_rows,
        semantic_hash=semantic_hash,
    )

    # Đếm trên toàn dataset: `--business-date` chỉ lọc phần được ghi, không lọc đối soát.
    counters = reconciliation.quality_counters(recon_all)
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
            "total_keys": counters["keys"],
            "status_counts": {s.value: counters[s.value] for s in ReconStatus},
            "business_date_disagreements": counters["business_date_disagreements"],
            "age_clamped": counters["age_clamped"],
        },
        "physical_counts": counts,
        "quarantine_records": quarantine_total,
        "key_conflicts": key_conflicts_total,
        "quality": {
            "physical_records": evaluation.physical_records,
            "rejected_records": evaluation.rejected_records,
            "control_groups_checked": len(evaluation.comparisons),
            "control_mismatches": len(evaluation.mismatches),
            "unexplained_mismatches": len(evaluation.unexplained),
        },
        "committed_snapshots": len(snapshots),
    }


def _key_conflicts_for_reconciliation(
    spark: SparkSession, frames: Sequence[DataFrame]
) -> DataFrame:
    """Bằng chứng xung đột khóa của TOÀN BỘ dataset (không lọc ngày); không có thì bảng rỗng."""
    if not frames:
        return spark.createDataFrame(
            [],
            "dataset_id string, business_date date, entity string, channel string, "
            "external_reference string, conflict_code string, source_pointers array<string>",
        )
    merged = frames[0]
    for nxt in frames[1:]:
        merged = merged.unionByName(nxt)
    return merged.select(*KEY_CONFLICT_COLUMNS)


def _evaluate_quality(
    state: RunState,
    *,
    silver_all: dict[str, DataFrame],
    silver: dict[str, DataFrame],
    bronze_scoped: dict[str, DataFrame],
    statement_lines: DataFrame,
    quarantine_dfs: Sequence[DataFrame],
    parse_errors: Sequence[tuple[date, int]],
    quarantine_by_date: dict[date, int],
    conflicts_by_date: dict[date, int],
    control_report: dict[str, object] | None,
    filter_dates: set[date],
    default_date: date,
    fallback_date: date,
) -> quality_metrics.QualityEvaluation:
    """Đếm tầng, control totals và cổng chặn publish theo ngưỡng `[quality]` của cấu hình."""
    quarantine = None
    if quarantine_dfs:
        quarantine = quarantine_dfs[0]
        for nxt in quarantine_dfs[1:]:
            quarantine = quarantine.unionByName(nxt)
        quarantine = state.persist(quarantine)

    layer_counts: list[quality_metrics.LayerCounts] = []
    totals: list[quality_metrics.ControlTotalRow] = []
    for entity, name, key, group in (
        ("transactions", "transaction", "transaction_id", "status"),
        ("ledger_entries", "ledger_entry", "entry_id", "side"),
        ("statements", "statement", "statement_record_id", "status"),
    ):
        is_event = entity in bronze_scoped
        layer_counts += spark_metrics.layer_counts_spark(
            bronze_scoped[entity] if is_event else statement_lines,
            silver_all[entity],
            quarantine,
            entity=name,
            key_col=key,
            pointer=(
                transforms.pointer_column() if is_event else spark_metrics.statement_pointer()
            ),
            fallback_date=fallback_date,
            event_col="event_id" if is_event else None,
        )
        totals += spark_metrics.compute_group_totals_spark(
            silver[entity], entity=name, key_col=key, group_col=group
        )
    if filter_dates:
        layer_counts = [c for c in layer_counts if c.business_date in filter_dates]
    layer_counts += spark_metrics.parse_error_counts(parse_errors)

    rejected: list[quality_metrics.RejectedKey] = []
    if control_report is not None and any(
        not c.matched
        for c in quality_metrics.compare_control_totals(
            totals, control_report, business_dates=filter_dates
        )
    ):
        # Chỉ tìm khóa bị loại khi có chênh lệch cần giải thích.
        for entity, name, key, group in (
            ("transactions", "transaction", "transaction_id", "status"),
            ("ledger_entries", "ledger_entry", "entry_id", "side"),
        ):
            rejected += spark_metrics.rejected_keys_spark(
                bronze_scoped[entity],
                silver_all[entity],
                entity=name,
                key_col=key,
                group_col=group,
            )

    limits = state.cfg.quality
    return quality_metrics.evaluate_quality_gate(
        run_id=state.run_id,
        dataset_id=state.dataset_id,
        default_date=default_date,
        layer_counts=layer_counts,
        totals=totals,
        quarantine_by_date=quarantine_by_date,
        conflicts_by_date=conflicts_by_date,
        control_report=control_report,
        rejected_keys=rejected,
        business_dates=filter_dates,
        thresholds=quality_metrics.QualityThresholds(
            max_quarantine_rate_bps=limits.max_quarantine_rate_bps,
            max_conflict_count=limits.max_conflict_count,
            fail_on_unexplained_control_mismatch=limits.fail_on_unexplained_control_mismatch,
        ),
    )


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
