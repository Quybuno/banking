"""Tầng truy cập Control DB cho giao thức commit batch (TASK T24, ADR-0003)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Protocol

from n06.config import PostgresDbConfig

COMMITTED_RUN_STATUSES = frozenset({"SUCCEEDED", "GOLD_COMMITTED", "MART_PUBLISHED"})
VALID_TARGET_COMMIT_STATUSES = frozenset({"SUCCEEDED", "GOLD_COMMITTED"})


class CommitError(RuntimeError):
    """Lỗi trong giao thức commit batch hoặc truy cập Control DB."""


class RunNotFoundError(CommitError):
    """Không tìm thấy `run_id` trong bảng `runs`."""


class RunStateError(CommitError):
    """Trạng thái của `run_id` không hợp lệ cho thao tác yêu cầu."""


@dataclass(frozen=True)
class RunRecord:
    """Bản ghi một lần chạy batch trong bảng `runs`."""

    run_id: str
    dataset_id: str
    job_name: str
    profile: str
    business_date_from: date | None
    business_date_to: date | None
    status: str
    row_count: int
    input_versions: list[Any]
    config_hash: str
    schema_version: str
    rule_version: str
    code_version: str
    semantic_hash: str | None = None
    error_summary: str | None = None


@dataclass(frozen=True)
class SnapshotRecord:
    """Bản ghi con trỏ snapshot hiện hành trong `current_snapshot`."""

    dataset_id: str
    layer: str
    entity: str
    business_date: date
    run_id: str
    output_path: str
    manifest_path: str | None
    semantic_hash: str | None


@dataclass(frozen=True)
class SnapshotSpec:
    """Thông số một snapshot cần trỏ vào `current_snapshot` trong cùng transaction commit run."""

    layer: str
    entity: str
    business_date: date
    output_path: str
    manifest_path: str | None = None


@dataclass(frozen=True)
class IngestBatchRecord:
    """Bản ghi batch streaming đã commit trong `ingest_batches`."""

    query_name: str
    batch_id: int
    dataset_id: str | None  # streaming ghi NULL (ADR-0004 Q1)
    ingestion_date: date
    topic_offsets: dict[str, Any]
    record_count: int
    raw_path: str
    bronze_path: str
    status: str


@dataclass(frozen=True)
class QualityMetricRecord:
    """Một bản ghi chỉ số chất lượng trong bảng `quality_metrics` (ADR-0003, T32)."""

    run_id: str
    dataset_id: str
    layer: str
    entity: str
    business_date: date
    metric_name: str
    metric_value: int
    status: str = "PASS"
    details: dict[str, Any] = field(default_factory=dict)


class ControlStore(Protocol):
    """Giao diện lưu trữ Control DB cho `BatchCommitCoordinator`."""

    def start_run(self, run: RunRecord) -> RunRecord:
        """Ghi nhận run mới ở trạng thái `RUNNING` (idempotent theo `run_id`)."""

    def commit_run_atomic(
        self,
        *,
        run_id: str,
        dataset_id: str,
        layer: str,
        entity: str,
        business_date: date,
        output_path: str,
        manifest_path: str | None,
        row_count: int,
        semantic_hash: str,
        target_status: str,
        after_run_update_hook: Callable[[], None] | None = None,
    ) -> SnapshotRecord:
        """Cập nhật `runs` và `current_snapshot` trong một transaction nguyên tử."""

    def commit_run_snapshots_atomic(
        self,
        *,
        run_id: str,
        dataset_id: str,
        snapshots: Sequence[SnapshotSpec],
        row_count: int,
        semantic_hash: str,
        target_status: str,
        after_run_update_hook: Callable[[], None] | None = None,
    ) -> list[SnapshotRecord]:
        """Cập nhật `runs` và mọi `current_snapshot` của run trong MỘT transaction nguyên tử."""

    def fail_run(self, *, run_id: str, error_summary: str) -> None:
        """Đánh dấu run `FAILED`; không thay đổi `current_snapshot`."""

    def get_run(self, run_id: str) -> RunRecord | None:
        """Đọc thông tin một run theo `run_id`."""

    def resolve_current_snapshot(
        self,
        *,
        dataset_id: str,
        layer: str,
        entity: str,
        business_date: date,
    ) -> SnapshotRecord | None:
        """Trả về snapshot hiện hành đã commit (bỏ qua run đang chạy hoặc thất bại)."""

    def upsert_ingest_batch(self, batch: IngestBatchRecord) -> None:
        """Ghi một batch streaming đã hoàn tất; chạy lại cùng khóa không tạo dòng mới."""

    def list_committed_ingest_batches(self, *, query_name: str) -> list[IngestBatchRecord]:
        """Liệt kê các `ingest_batches` đã commit của một query, theo `batch_id`."""

    def list_current_snapshots(
        self,
        *,
        dataset_id: str,
        layer: str,
        entity: str | None = None,
        business_date_from: date | None = None,
        business_date_to: date | None = None,
    ) -> list[SnapshotRecord]:
        """Liệt kê snapshot hiện hành đã commit theo `dataset_id`, `layer` và bộ lọc tùy chọn."""

    def mark_run_mart_published(self, *, run_id: str) -> None:
        """Đánh dấu run đã commit là `MART_PUBLISHED` (idempotent nếu đã ở trạng thái này)."""

    def upsert_quality_metrics(self, metrics: Sequence[QualityMetricRecord]) -> None:
        """Ghi các chỉ số chất lượng của một run vào bảng `quality_metrics` (idempotent)."""

    def list_quality_metrics(self, *, run_id: str) -> list[QualityMetricRecord]:
        """Đọc danh sách `quality_metrics` đã lưu của một `run_id`."""


def _scope_key(dataset_id: str, layer: str, entity: str, business_date: date) -> str:
    return f"{dataset_id}|{layer}|{entity}|{business_date.isoformat()}"


def _to_date(value: date | str | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _ingest_record(row: Mapping[str, Any]) -> IngestBatchRecord:
    offsets = row["topic_offsets"]
    ingestion_date = _to_date(row["ingestion_date"])
    assert ingestion_date is not None
    return IngestBatchRecord(
        query_name=row["query_name"],
        batch_id=int(row["batch_id"]),
        dataset_id=row["dataset_id"],
        ingestion_date=ingestion_date,
        topic_offsets=json.loads(offsets) if isinstance(offsets, str) else dict(offsets),
        record_count=int(row["record_count"]),
        raw_path=row["raw_path"],
        bronze_path=row["bronze_path"],
        status=row["status"],
    )


class PsycopgControlStore:
    """Triển khai `ControlStore` dùng kết nối DB-API 2.0 (`psycopg` / `psycopg2`)."""

    def __init__(self, conn_factory: Callable[[], Any]) -> None:
        self._conn_factory = conn_factory

    def start_run(self, run: RunRecord) -> RunRecord:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, dataset_id, config_hash, schema_version,
                           rule_version, code_version
                    FROM runs
                    WHERE run_id = %s
                    FOR UPDATE;
                    """,
                    (run.run_id,),
                )
                existing = cur.fetchone()
                if existing is not None:
                    ex_status, ex_ds, ex_cfg, ex_sch, ex_rule, ex_code = existing
                    if ex_status != "RUNNING":
                        raise RunStateError(
                            f"run_id '{run.run_id}' đã tồn tại với trạng thái '{ex_status}'."
                        )
                    if (ex_ds, ex_cfg, ex_sch, ex_rule, ex_code) != (
                        run.dataset_id,
                        run.config_hash,
                        run.schema_version,
                        run.rule_version,
                        run.code_version,
                    ):
                        raise RunStateError(
                            f"run_id '{run.run_id}' đang RUNNING nhưng metadata không khớp."
                        )
                    conn.commit()
                    loaded = self.get_run(run.run_id)
                    assert loaded is not None
                    return loaded

                cur.execute(
                    """
                    INSERT INTO runs (
                        run_id, dataset_id, job_name, profile,
                        business_date_from, business_date_to, status,
                        row_count, input_versions, config_hash,
                        schema_version, rule_version, code_version
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, 'RUNNING',
                        0, %s::jsonb, %s, %s, %s, %s
                    );
                    """,
                    (
                        run.run_id,
                        run.dataset_id,
                        run.job_name,
                        run.profile,
                        run.business_date_from,
                        run.business_date_to,
                        json.dumps(run.input_versions, ensure_ascii=False),
                        run.config_hash,
                        run.schema_version,
                        run.rule_version,
                        run.code_version,
                    ),
                )
            conn.commit()
            return run
        except CommitError:
            conn.rollback()
            raise
        except Exception as exc:
            conn.rollback()
            raise CommitError(f"start_run thất bại cho '{run.run_id}': {exc}") from exc
        finally:
            conn.close()

    def commit_run_atomic(
        self,
        *,
        run_id: str,
        dataset_id: str,
        layer: str,
        entity: str,
        business_date: date,
        output_path: str,
        manifest_path: str | None,
        row_count: int,
        semantic_hash: str,
        target_status: str,
        after_run_update_hook: Callable[[], None] | None = None,
    ) -> SnapshotRecord:
        return self.commit_run_snapshots_atomic(
            run_id=run_id,
            dataset_id=dataset_id,
            snapshots=[
                SnapshotSpec(
                    layer=layer,
                    entity=entity,
                    business_date=business_date,
                    output_path=output_path,
                    manifest_path=manifest_path,
                )
            ],
            row_count=row_count,
            semantic_hash=semantic_hash,
            target_status=target_status,
            after_run_update_hook=after_run_update_hook,
        )[0]

    def commit_run_snapshots_atomic(
        self,
        *,
        run_id: str,
        dataset_id: str,
        snapshots: Sequence[SnapshotSpec],
        row_count: int,
        semantic_hash: str,
        target_status: str,
        after_run_update_hook: Callable[[], None] | None = None,
    ) -> list[SnapshotRecord]:
        if not snapshots:
            raise CommitError("Cần ít nhất một snapshot để commit.")
        conn = self._conn_factory()
        scopes = sorted(
            {_scope_key(dataset_id, s.layer, s.entity, s.business_date) for s in snapshots}
        )
        try:
            with conn.cursor() as cur:
                # Khóa theo thứ tự cố định để mọi commit nhiều snapshot không deadlock.
                for scope in scopes:
                    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0));", (scope,))

                cur.execute(
                    "SELECT status, semantic_hash FROM runs WHERE run_id = %s FOR UPDATE;",
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise RunNotFoundError(f"Không tìm thấy run_id '{run_id}'.")

                current_status, existing_hash = row
                if current_status in COMMITTED_RUN_STATUSES:
                    if existing_hash != semantic_hash:
                        raise RunStateError(f"run_id '{run_id}' đã commit với semantic_hash khác.")
                elif current_status != "RUNNING":
                    raise RunStateError(
                        f"Không thể commit run_id '{run_id}' từ trạng thái '{current_status}'."
                    )
                else:
                    cur.execute(
                        """
                        UPDATE runs
                        SET status = %s,
                            row_count = %s,
                            semantic_hash = %s,
                            finished_at = NOW()
                        WHERE run_id = %s;
                        """,
                        (target_status, row_count, semantic_hash, run_id),
                    )
                    if after_run_update_hook is not None:
                        after_run_update_hook()

                results: list[SnapshotRecord] = []
                for s in snapshots:
                    cur.execute(
                        """
                        INSERT INTO current_snapshot (
                            dataset_id, layer, entity, business_date,
                            run_id, output_path, manifest_path, semantic_hash
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (dataset_id, layer, entity, business_date) DO UPDATE
                            SET run_id = EXCLUDED.run_id,
                                output_path = EXCLUDED.output_path,
                                manifest_path = EXCLUDED.manifest_path,
                                semantic_hash = EXCLUDED.semantic_hash,
                                updated_at = NOW();
                        """,
                        (
                            dataset_id,
                            s.layer,
                            s.entity,
                            s.business_date,
                            run_id,
                            s.output_path,
                            s.manifest_path,
                            semantic_hash,
                        ),
                    )
                    results.append(
                        SnapshotRecord(
                            dataset_id=dataset_id,
                            layer=s.layer,
                            entity=s.entity,
                            business_date=s.business_date,
                            run_id=run_id,
                            output_path=s.output_path,
                            manifest_path=s.manifest_path,
                            semantic_hash=semantic_hash,
                        )
                    )
            conn.commit()
            return results
        except CommitError:
            conn.rollback()
            raise
        except Exception as exc:
            conn.rollback()
            raise CommitError(
                f"commit_run_snapshots_atomic thất bại cho '{run_id}': {exc}"
            ) from exc
        finally:
            conn.close()

    def fail_run(self, *, run_id: str, error_summary: str) -> None:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM runs WHERE run_id = %s FOR UPDATE;", (run_id,))
                row = cur.fetchone()
                if row is None:
                    raise RunNotFoundError(f"Không tìm thấy run_id '{run_id}'.")
                if row[0] in COMMITTED_RUN_STATUSES:
                    raise RunStateError(f"Không thể đánh dấu FAILED cho run đã commit '{run_id}'.")
                cur.execute(
                    """
                    UPDATE runs
                    SET status = 'FAILED',
                        error_summary = %s,
                        finished_at = NOW()
                    WHERE run_id = %s;
                    """,
                    (error_summary, run_id),
                )
            conn.commit()
        except CommitError:
            conn.rollback()
            raise
        except Exception as exc:
            conn.rollback()
            raise CommitError(f"fail_run thất bại cho '{run_id}': {exc}") from exc
        finally:
            conn.close()

    def get_run(self, run_id: str) -> RunRecord | None:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, dataset_id, job_name, profile,
                           business_date_from, business_date_to, status,
                           row_count, input_versions, config_hash,
                           schema_version, rule_version, code_version,
                           semantic_hash, error_summary
                    FROM runs
                    WHERE run_id = %s;
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
            if row is None:
                return None
            raw_inputs = row[8]
            inputs = json.loads(raw_inputs) if isinstance(raw_inputs, str) else list(raw_inputs)
            return RunRecord(
                run_id=row[0],
                dataset_id=row[1],
                job_name=row[2],
                profile=row[3],
                business_date_from=_to_date(row[4]),
                business_date_to=_to_date(row[5]),
                status=row[6],
                row_count=int(row[7]),
                input_versions=inputs,
                config_hash=row[9],
                schema_version=row[10],
                rule_version=row[11],
                code_version=row[12],
                semantic_hash=row[13],
                error_summary=row[14],
            )
        finally:
            conn.close()

    def resolve_current_snapshot(
        self,
        *,
        dataset_id: str,
        layer: str,
        entity: str,
        business_date: date,
    ) -> SnapshotRecord | None:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cs.dataset_id, cs.layer, cs.entity, cs.business_date,
                           cs.run_id, cs.output_path, cs.manifest_path, cs.semantic_hash
                    FROM current_snapshot cs
                    JOIN runs r ON r.run_id = cs.run_id
                    WHERE cs.dataset_id = %s
                      AND cs.layer = %s
                      AND cs.entity = %s
                      AND cs.business_date = %s
                      AND r.status IN ('SUCCEEDED', 'GOLD_COMMITTED', 'MART_PUBLISHED');
                    """,
                    (dataset_id, layer, entity, business_date),
                )
                row = cur.fetchone()
            if row is None:
                return None
            biz_date = _to_date(row[3])
            assert biz_date is not None
            return SnapshotRecord(
                dataset_id=row[0],
                layer=row[1],
                entity=row[2],
                business_date=biz_date,
                run_id=row[4],
                output_path=row[5],
                manifest_path=row[6],
                semantic_hash=row[7],
            )
        finally:
            conn.close()

    def upsert_ingest_batch(self, batch: IngestBatchRecord) -> None:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingest_batches (
                        query_name, batch_id, dataset_id, ingestion_date,
                        topic_offsets, record_count, raw_path, bronze_path, status
                    ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                    ON CONFLICT (query_name, batch_id) DO UPDATE
                        SET dataset_id = EXCLUDED.dataset_id,
                            ingestion_date = EXCLUDED.ingestion_date,
                            topic_offsets = EXCLUDED.topic_offsets,
                            record_count = EXCLUDED.record_count,
                            raw_path = EXCLUDED.raw_path,
                            bronze_path = EXCLUDED.bronze_path,
                            status = EXCLUDED.status,
                            updated_at = NOW();
                    """,
                    (
                        batch.query_name,
                        batch.batch_id,
                        batch.dataset_id,
                        batch.ingestion_date,
                        json.dumps(batch.topic_offsets),
                        batch.record_count,
                        batch.raw_path,
                        batch.bronze_path,
                        batch.status,
                    ),
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            raise CommitError(
                f"upsert_ingest_batch thất bại cho '{batch.query_name}'/{batch.batch_id}: {exc}"
            ) from exc
        finally:
            conn.close()

    def list_committed_ingest_batches(self, *, query_name: str) -> list[IngestBatchRecord]:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT query_name, batch_id, dataset_id, ingestion_date,
                           topic_offsets, record_count, raw_path, bronze_path, status
                    FROM ingest_batches
                    WHERE query_name = %s AND status = 'COMMITTED'
                    ORDER BY batch_id ASC;
                    """,
                    (query_name,),
                )
                rows = cur.fetchall()
            keys = (
                "query_name", "batch_id", "dataset_id", "ingestion_date",
                "topic_offsets", "record_count", "raw_path", "bronze_path", "status",
            )  # fmt: skip
            return [_ingest_record(dict(zip(keys, row, strict=True))) for row in rows]
        finally:
            conn.close()

    def list_current_snapshots(
        self,
        *,
        dataset_id: str,
        layer: str,
        entity: str | None = None,
        business_date_from: date | None = None,
        business_date_to: date | None = None,
    ) -> list[SnapshotRecord]:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cs.dataset_id, cs.layer, cs.entity, cs.business_date,
                           cs.run_id, cs.output_path, cs.manifest_path, cs.semantic_hash
                    FROM current_snapshot cs
                    JOIN runs r ON r.run_id = cs.run_id
                    WHERE cs.dataset_id = %s
                      AND cs.layer = %s
                      AND r.status IN ('SUCCEEDED', 'GOLD_COMMITTED', 'MART_PUBLISHED')
                      AND (%s::text IS NULL OR cs.entity = %s)
                      AND (%s::date IS NULL OR cs.business_date >= %s)
                      AND (%s::date IS NULL OR cs.business_date <= %s)
                    ORDER BY cs.entity ASC, cs.business_date ASC;
                    """,
                    (
                        dataset_id,
                        layer,
                        entity,
                        entity,
                        business_date_from,
                        business_date_from,
                        business_date_to,
                        business_date_to,
                    ),
                )
                rows = cur.fetchall()
            out: list[SnapshotRecord] = []
            for row in rows:
                biz_date = _to_date(row[3])
                assert biz_date is not None
                out.append(
                    SnapshotRecord(
                        dataset_id=row[0],
                        layer=row[1],
                        entity=row[2],
                        business_date=biz_date,
                        run_id=row[4],
                        output_path=row[5],
                        manifest_path=row[6],
                        semantic_hash=row[7],
                    )
                )
            return out
        finally:
            conn.close()

    def mark_run_mart_published(self, *, run_id: str) -> None:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM runs WHERE run_id = %s FOR UPDATE;", (run_id,))
                existing = cur.fetchone()
                if existing is None:
                    raise RunNotFoundError(f"Không tìm thấy run_id '{run_id}'.")
                status = existing[0]
                if status not in COMMITTED_RUN_STATUSES:
                    raise RunStateError(
                        f"Không thể đánh dấu MART_PUBLISHED cho run '{run_id}' đang ở '{status}'."
                    )
                cur.execute(
                    "UPDATE runs SET status = 'MART_PUBLISHED' WHERE run_id = %s;",
                    (run_id,),
                )
            conn.commit()
        except CommitError:
            conn.rollback()
            raise
        except Exception as exc:
            conn.rollback()
            raise CommitError(f"mark_run_mart_published thất bại cho '{run_id}': {exc}") from exc
        finally:
            conn.close()

    def upsert_quality_metrics(self, metrics: Sequence[QualityMetricRecord]) -> None:
        if not metrics:
            return
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                for m in metrics:
                    cur.execute(
                        """
                        INSERT INTO quality_metrics (
                            run_id, dataset_id, layer, entity, business_date,
                            metric_name, metric_value, status, details
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (run_id, layer, entity, business_date, metric_name) DO UPDATE
                            SET metric_value = EXCLUDED.metric_value,
                                status = EXCLUDED.status,
                                details = EXCLUDED.details;
                        """,
                        (
                            m.run_id,
                            m.dataset_id,
                            m.layer,
                            m.entity,
                            m.business_date,
                            m.metric_name,
                            m.metric_value,
                            m.status,
                            json.dumps(m.details, ensure_ascii=False),
                        ),
                    )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            raise CommitError(f"upsert_quality_metrics thất bại: {exc}") from exc
        finally:
            conn.close()

    def list_quality_metrics(self, *, run_id: str) -> list[QualityMetricRecord]:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, dataset_id, layer, entity, business_date,
                           metric_name, metric_value, status, details
                    FROM quality_metrics
                    WHERE run_id = %s
                    ORDER BY business_date ASC, layer ASC, entity ASC, metric_name ASC;
                    """,
                    (run_id,),
                )
                rows = cur.fetchall()
            out: list[QualityMetricRecord] = []
            for row in rows:
                biz_date = _to_date(row[4])
                assert biz_date is not None
                details = json.loads(row[8]) if isinstance(row[8], str) else dict(row[8] or {})
                out.append(
                    QualityMetricRecord(
                        run_id=row[0],
                        dataset_id=row[1],
                        layer=row[2],
                        entity=row[3],
                        business_date=biz_date,
                        metric_name=row[5],
                        metric_value=int(row[6]),
                        status=row[7],
                        details=details,
                    )
                )
            return out
        finally:
            conn.close()


# Mọi giá trị đi vào SQL bằng MỘT biến JSON của psql, không ghép vào chuỗi câu lệnh (ADR-0012
# quyết định 1). Biến được đặt bằng `\set` ở đầu kịch bản gửi qua **stdin**, nên giá trị không nằm
# trong argv (xem được bằng `ps`, giới hạn độ dài một đối số). Câu lệnh dùng `:'params'`, psql tự
# trích dẫn. psql không thay biến bên trong khối `DO $$ ... $$`, nên khối đó đọc từ bảng tạm.
_PARAMS_TABLE = "CREATE TEMP TABLE n06_params ON COMMIT DROP AS SELECT :'params'::jsonb AS j;"
_PARAMS = "(:'params'::jsonb)"
_COMMITTED_SQL = "('SUCCEEDED', 'GOLD_COMMITTED', 'MART_PUBLISHED')"

_START_RUN_SQL = f"""
BEGIN;
{_PARAMS_TABLE}
DO $$
DECLARE
    v jsonb;
    v_status TEXT;
BEGIN
    SELECT j INTO v FROM n06_params;
    SELECT status INTO v_status FROM runs WHERE run_id = v->>'run_id' FOR UPDATE;
    IF FOUND THEN
        IF v_status <> 'RUNNING' THEN
            RAISE EXCEPTION 'run_id % already exists with status %', v->>'run_id', v_status;
        END IF;
    ELSE
        INSERT INTO runs (
            run_id, dataset_id, job_name, profile, business_date_from, business_date_to,
            status, row_count, input_versions, config_hash, schema_version, rule_version,
            code_version
        ) VALUES (
            v->>'run_id', v->>'dataset_id', v->>'job_name', v->>'profile',
            (v->>'business_date_from')::date, (v->>'business_date_to')::date,
            'RUNNING', 0, v->'input_versions', v->>'config_hash', v->>'schema_version',
            v->>'rule_version', v->>'code_version'
        );
    END IF;
END $$;
COMMIT;
"""

_COMMIT_RUN_SNAPSHOTS_SQL = f"""
BEGIN;
{_PARAMS_TABLE}
SELECT pg_advisory_xact_lock(hashtextextended(scope_item, 0))
FROM n06_params, jsonb_array_elements_text(j->'scopes') AS scope_item;
DO $$
DECLARE
    v jsonb;
    v_status TEXT;
    v_hash TEXT;
BEGIN
    SELECT j INTO v FROM n06_params;
    SELECT status, semantic_hash INTO v_status, v_hash
    FROM runs WHERE run_id = v->>'run_id' FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'run_id % not found', v->>'run_id';
    END IF;

    IF v_status IN {_COMMITTED_SQL} THEN
        IF v_hash IS DISTINCT FROM v->>'semantic_hash' THEN
            RAISE EXCEPTION 'run_id % already committed with different semantic_hash',
                v->>'run_id';
        END IF;
    ELSIF v_status = 'RUNNING' THEN
        UPDATE runs
        SET status = v->>'target_status',
            row_count = (v->>'row_count')::bigint,
            semantic_hash = v->>'semantic_hash',
            finished_at = NOW()
        WHERE run_id = v->>'run_id';
    ELSE
        RAISE EXCEPTION 'cannot commit run_id % from status %', v->>'run_id', v_status;
    END IF;
END $$;

INSERT INTO current_snapshot (
    dataset_id, layer, entity, business_date,
    run_id, output_path, manifest_path, semantic_hash
)
SELECT j->>'dataset_id', s.layer, s.entity, s.business_date,
       j->>'run_id', s.output_path, s.manifest_path, j->>'semantic_hash'
FROM n06_params,
     jsonb_to_recordset(j->'snapshots') AS s(
         layer TEXT,
         entity TEXT,
         business_date DATE,
         output_path TEXT,
         manifest_path TEXT
     )
ON CONFLICT (dataset_id, layer, entity, business_date) DO UPDATE
    SET run_id = EXCLUDED.run_id,
        output_path = EXCLUDED.output_path,
        manifest_path = EXCLUDED.manifest_path,
        semantic_hash = EXCLUDED.semantic_hash,
        updated_at = NOW();
COMMIT;
"""

_FAIL_RUN_SQL = f"""
BEGIN;
{_PARAMS_TABLE}
DO $$
DECLARE
    v jsonb;
    v_status TEXT;
BEGIN
    SELECT j INTO v FROM n06_params;
    SELECT status INTO v_status FROM runs WHERE run_id = v->>'run_id' FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'run_id % not found', v->>'run_id';
    END IF;
    IF v_status IN {_COMMITTED_SQL} THEN
        RAISE EXCEPTION 'cannot fail committed run_id %', v->>'run_id';
    END IF;
    UPDATE runs
    SET status = 'FAILED', error_summary = v->>'error_summary', finished_at = NOW()
    WHERE run_id = v->>'run_id';
END $$;
COMMIT;
"""

_GET_RUN_SQL = f"""
SELECT COALESCE(
    (SELECT row_to_json(t)::text FROM (
        SELECT run_id, dataset_id, job_name, profile,
               business_date_from::text, business_date_to::text, status,
               row_count, input_versions, config_hash,
               schema_version, rule_version, code_version,
               semantic_hash, error_summary
        FROM runs
        WHERE run_id = {_PARAMS}->>'run_id'
    ) t),
    ''
);
"""

_RESOLVE_SNAPSHOT_SQL = f"""
SELECT COALESCE(
    (SELECT row_to_json(t)::text FROM (
        SELECT cs.dataset_id, cs.layer, cs.entity, cs.business_date::text,
               cs.run_id, cs.output_path, cs.manifest_path, cs.semantic_hash
        FROM current_snapshot cs
        JOIN runs r ON r.run_id = cs.run_id
        WHERE cs.dataset_id = {_PARAMS}->>'dataset_id'
          AND cs.layer = {_PARAMS}->>'layer'
          AND cs.entity = {_PARAMS}->>'entity'
          AND cs.business_date = ({_PARAMS}->>'business_date')::date
          AND r.status IN {_COMMITTED_SQL}
    ) t),
    ''
);
"""

_UPSERT_INGEST_SQL = f"""
BEGIN;
{_PARAMS_TABLE}
INSERT INTO ingest_batches (
    query_name, batch_id, dataset_id, ingestion_date,
    topic_offsets, record_count, raw_path, bronze_path, status
)
SELECT j->>'query_name', (j->>'batch_id')::bigint, j->>'dataset_id',
       (j->>'ingestion_date')::date, j->'topic_offsets', (j->>'record_count')::bigint,
       j->>'raw_path', j->>'bronze_path', j->>'status'
FROM n06_params
WHERE true
ON CONFLICT (query_name, batch_id) DO UPDATE
    SET dataset_id = EXCLUDED.dataset_id,
        ingestion_date = EXCLUDED.ingestion_date,
        topic_offsets = EXCLUDED.topic_offsets,
        record_count = EXCLUDED.record_count,
        raw_path = EXCLUDED.raw_path,
        bronze_path = EXCLUDED.bronze_path,
        status = EXCLUDED.status,
        updated_at = NOW();
COMMIT;
"""

_LIST_INGEST_SQL = f"""
SELECT COALESCE(
    (SELECT json_agg(t ORDER BY batch_id ASC)::text FROM (
        SELECT query_name, batch_id, dataset_id, ingestion_date::text,
               topic_offsets, record_count, raw_path, bronze_path, status
        FROM ingest_batches
        WHERE query_name = {_PARAMS}->>'query_name' AND status = 'COMMITTED'
    ) t),
    '[]'
);
"""

_LIST_SNAPSHOTS_SQL = f"""
SELECT COALESCE(
    (SELECT json_agg(t ORDER BY entity ASC, business_date ASC)::text FROM (
        SELECT cs.dataset_id, cs.layer, cs.entity, cs.business_date::text AS business_date,
               cs.run_id, cs.output_path, cs.manifest_path, cs.semantic_hash
        FROM current_snapshot cs
        JOIN runs r ON r.run_id = cs.run_id
        WHERE cs.dataset_id = {_PARAMS}->>'dataset_id'
          AND cs.layer = {_PARAMS}->>'layer'
          AND r.status IN {_COMMITTED_SQL}
          AND (({_PARAMS}->>'entity') IS NULL OR cs.entity = {_PARAMS}->>'entity')
          AND (({_PARAMS}->>'date_from') IS NULL
               OR cs.business_date >= ({_PARAMS}->>'date_from')::date)
          AND (({_PARAMS}->>'date_to') IS NULL
               OR cs.business_date <= ({_PARAMS}->>'date_to')::date)
    ) t),
    '[]'
);
"""

_MARK_MART_PUBLISHED_SQL = f"""
BEGIN;
{_PARAMS_TABLE}
DO $$
DECLARE
    v jsonb;
    v_status TEXT;
BEGIN
    SELECT j INTO v FROM n06_params;
    SELECT status INTO v_status FROM runs WHERE run_id = v->>'run_id' FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'run_id % not found', v->>'run_id';
    END IF;
    IF v_status NOT IN {_COMMITTED_SQL} THEN
        RAISE EXCEPTION 'cannot mark run_id % MART_PUBLISHED from status %',
            v->>'run_id', v_status;
    END IF;
    UPDATE runs SET status = 'MART_PUBLISHED' WHERE run_id = v->>'run_id';
END $$;
COMMIT;
"""

_UPSERT_QUALITY_METRICS_SQL = f"""
BEGIN;
{_PARAMS_TABLE}
INSERT INTO quality_metrics (
    run_id, dataset_id, layer, entity, business_date,
    metric_name, metric_value, status, details
)
SELECT m->>'run_id', m->>'dataset_id', m->>'layer', m->>'entity',
       (m->>'business_date')::date, m->>'metric_name', (m->>'metric_value')::bigint,
       m->>'status', COALESCE(m->'details', '{{}}'::jsonb)
FROM n06_params, jsonb_array_elements(j->'metrics') AS m
ON CONFLICT (run_id, layer, entity, business_date, metric_name) DO UPDATE
    SET metric_value = EXCLUDED.metric_value,
        status = EXCLUDED.status,
        details = EXCLUDED.details;
COMMIT;
"""

_LIST_QUALITY_METRICS_SQL = f"""
SELECT COALESCE(
    (SELECT json_agg(t ORDER BY business_date ASC, layer ASC, entity ASC, metric_name ASC)::text
     FROM (
        SELECT run_id, dataset_id, layer, entity, business_date::text AS business_date,
               metric_name, metric_value, status, details
        FROM quality_metrics
        WHERE run_id = {_PARAMS}->>'run_id'
    ) t),
    '[]'
);
"""

# Runner: `(kịch bản, tuples_only) -> stdout`; kịch bản đã gồm biến tham số.
PsqlRunner = Callable[[str, bool], str]


def _with_params(sql: str, params: Mapping[str, Any]) -> str:
    """Đặt biến `params` bằng `\\set` trên một dòng.

    Trong tham số nháy đơn của meta-command, psql hiểu dấu `\\` kiểu C và `''` là một dấu nháy,
    nên mỗi `\\` và `'` được nhân đôi. JSON không chứa ký tự xuống dòng thô.
    """
    text = json.dumps(params, ensure_ascii=False)
    escaped = text.replace("\\", "\\\\").replace("'", "''")
    return f"\\set params '{escaped}'\n{sql}"


def _iso(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


class PsqlCliControlStore:
    """Triển khai `ControlStore` qua `psql` CLI (hoặc wrapper tùy biến) trong một transaction."""

    def __init__(
        self,
        db_config: PostgresDbConfig,
        *,
        psql_runner: PsqlRunner | None = None,
        psql_bin: str = "psql",
    ) -> None:
        self._db_config = db_config
        self._psql_runner = psql_runner
        self._psql_bin = psql_bin

    def _run_sql(
        self,
        sql: str,
        *,
        tuples_only: bool = False,
        params: Mapping[str, Any] | None = None,
    ) -> str:
        if params is not None:
            sql = _with_params(sql, params)
        if self._psql_runner is not None:
            return self._psql_runner(sql, tuples_only)

        env = {
            **os.environ,
            "PGPASSWORD": self._db_config.password,
            "PGCLIENTENCODING": "UTF8",
        }
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
                encoding="utf-8",
                capture_output=True,
                env=env,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise CommitError(f"psql thất bại (exit {exc.returncode}): {stderr}") from exc
        return proc.stdout

    def start_run(self, run: RunRecord) -> RunRecord:
        self._run_sql(
            _START_RUN_SQL,
            params={
                "run_id": run.run_id,
                "dataset_id": run.dataset_id,
                "job_name": run.job_name,
                "profile": run.profile,
                "business_date_from": _iso(run.business_date_from),
                "business_date_to": _iso(run.business_date_to),
                "input_versions": run.input_versions,
                "config_hash": run.config_hash,
                "schema_version": run.schema_version,
                "rule_version": run.rule_version,
                "code_version": run.code_version,
            },
        )
        loaded = self.get_run(run.run_id)
        if loaded is None:
            raise CommitError(f"Không đọc lại được run '{run.run_id}' sau start_run.")
        return loaded

    def commit_run_atomic(
        self,
        *,
        run_id: str,
        dataset_id: str,
        layer: str,
        entity: str,
        business_date: date,
        output_path: str,
        manifest_path: str | None,
        row_count: int,
        semantic_hash: str,
        target_status: str,
        after_run_update_hook: Callable[[], None] | None = None,
    ) -> SnapshotRecord:
        return self.commit_run_snapshots_atomic(
            run_id=run_id,
            dataset_id=dataset_id,
            snapshots=[
                SnapshotSpec(
                    layer=layer,
                    entity=entity,
                    business_date=business_date,
                    output_path=output_path,
                    manifest_path=manifest_path,
                )
            ],
            row_count=row_count,
            semantic_hash=semantic_hash,
            target_status=target_status,
            after_run_update_hook=after_run_update_hook,
        )[0]

    def commit_run_snapshots_atomic(
        self,
        *,
        run_id: str,
        dataset_id: str,
        snapshots: Sequence[SnapshotSpec],
        row_count: int,
        semantic_hash: str,
        target_status: str,
        after_run_update_hook: Callable[[], None] | None = None,
    ) -> list[SnapshotRecord]:
        if not snapshots:
            raise CommitError("Cần ít nhất một snapshot để commit.")
        if after_run_update_hook is not None:
            # Thực thi hook trước bước upsert bên trong cùng kịch bản transaction không khả thi
            # qua subprocess một lần; gọi hook ngay trước khi commit khối SQL nguyên tử.
            after_run_update_hook()

        scopes = sorted(
            {_scope_key(dataset_id, s.layer, s.entity, s.business_date) for s in snapshots}
        )
        self._run_sql(
            _COMMIT_RUN_SNAPSHOTS_SQL,
            params={
                "scopes": scopes,
                "run_id": run_id,
                "dataset_id": dataset_id,
                "row_count": row_count,
                "semantic_hash": semantic_hash,
                "target_status": target_status,
                "snapshots": [
                    {
                        "layer": s.layer,
                        "entity": s.entity,
                        "business_date": s.business_date.isoformat(),
                        "output_path": s.output_path,
                        "manifest_path": s.manifest_path,
                    }
                    for s in snapshots
                ],
            },
        )
        return [
            SnapshotRecord(
                dataset_id=dataset_id,
                layer=s.layer,
                entity=s.entity,
                business_date=s.business_date,
                run_id=run_id,
                output_path=s.output_path,
                manifest_path=s.manifest_path,
                semantic_hash=semantic_hash,
            )
            for s in snapshots
        ]

    def fail_run(self, *, run_id: str, error_summary: str) -> None:
        self._run_sql(_FAIL_RUN_SQL, params={"run_id": run_id, "error_summary": error_summary})

    def get_run(self, run_id: str) -> RunRecord | None:
        raw = self._run_sql(_GET_RUN_SQL, tuples_only=True, params={"run_id": run_id}).strip()
        if not raw:
            return None
        data = json.loads(raw)
        return RunRecord(
            run_id=data["run_id"],
            dataset_id=data["dataset_id"],
            job_name=data["job_name"],
            profile=data["profile"],
            business_date_from=_to_date(data["business_date_from"]),
            business_date_to=_to_date(data["business_date_to"]),
            status=data["status"],
            row_count=int(data["row_count"]),
            input_versions=list(data["input_versions"]),
            config_hash=data["config_hash"],
            schema_version=data["schema_version"],
            rule_version=data["rule_version"],
            code_version=data["code_version"],
            semantic_hash=data["semantic_hash"],
            error_summary=data["error_summary"],
        )

    def resolve_current_snapshot(
        self,
        *,
        dataset_id: str,
        layer: str,
        entity: str,
        business_date: date,
    ) -> SnapshotRecord | None:
        raw = self._run_sql(
            _RESOLVE_SNAPSHOT_SQL,
            tuples_only=True,
            params={
                "dataset_id": dataset_id,
                "layer": layer,
                "entity": entity,
                "business_date": business_date.isoformat(),
            },
        ).strip()
        if not raw:
            return None
        data = json.loads(raw)
        biz_date = _to_date(data["business_date"])
        assert biz_date is not None
        return SnapshotRecord(
            dataset_id=data["dataset_id"],
            layer=data["layer"],
            entity=data["entity"],
            business_date=biz_date,
            run_id=data["run_id"],
            output_path=data["output_path"],
            manifest_path=data["manifest_path"],
            semantic_hash=data["semantic_hash"],
        )

    def upsert_ingest_batch(self, batch: IngestBatchRecord) -> None:
        self._run_sql(
            _UPSERT_INGEST_SQL,
            params={
                "query_name": batch.query_name,
                "batch_id": batch.batch_id,
                "dataset_id": batch.dataset_id,
                "ingestion_date": batch.ingestion_date.isoformat(),
                "topic_offsets": batch.topic_offsets,
                "record_count": batch.record_count,
                "raw_path": batch.raw_path,
                "bronze_path": batch.bronze_path,
                "status": batch.status,
            },
        )

    def list_committed_ingest_batches(self, *, query_name: str) -> list[IngestBatchRecord]:
        raw = self._run_sql(
            _LIST_INGEST_SQL, tuples_only=True, params={"query_name": query_name}
        ).strip()
        return [_ingest_record(row) for row in json.loads(raw or "[]")]

    def list_current_snapshots(
        self,
        *,
        dataset_id: str,
        layer: str,
        entity: str | None = None,
        business_date_from: date | None = None,
        business_date_to: date | None = None,
    ) -> list[SnapshotRecord]:
        raw = self._run_sql(
            _LIST_SNAPSHOTS_SQL,
            tuples_only=True,
            params={
                "dataset_id": dataset_id,
                "layer": layer,
                "entity": entity,
                "date_from": _iso(business_date_from),
                "date_to": _iso(business_date_to),
            },
        ).strip()
        rows = json.loads(raw or "[]")
        out: list[SnapshotRecord] = []
        for data in rows:
            biz_date = _to_date(data["business_date"])
            assert biz_date is not None
            out.append(
                SnapshotRecord(
                    dataset_id=data["dataset_id"],
                    layer=data["layer"],
                    entity=data["entity"],
                    business_date=biz_date,
                    run_id=data["run_id"],
                    output_path=data["output_path"],
                    manifest_path=data["manifest_path"],
                    semantic_hash=data["semantic_hash"],
                )
            )
        return out

    def mark_run_mart_published(self, *, run_id: str) -> None:
        self._run_sql(_MARK_MART_PUBLISHED_SQL, params={"run_id": run_id})

    def upsert_quality_metrics(self, metrics: Sequence[QualityMetricRecord]) -> None:
        if not metrics:
            return
        payload = [
            {
                "run_id": m.run_id,
                "dataset_id": m.dataset_id,
                "layer": m.layer,
                "entity": m.entity,
                "business_date": m.business_date.isoformat(),
                "metric_name": m.metric_name,
                "metric_value": m.metric_value,
                "status": m.status,
                "details": m.details,
            }
            for m in metrics
        ]
        self._run_sql(_UPSERT_QUALITY_METRICS_SQL, params={"metrics": payload})

    def list_quality_metrics(self, *, run_id: str) -> list[QualityMetricRecord]:
        raw = self._run_sql(
            _LIST_QUALITY_METRICS_SQL, tuples_only=True, params={"run_id": run_id}
        ).strip()
        out: list[QualityMetricRecord] = []
        for data in json.loads(raw or "[]"):
            biz_date = _to_date(data["business_date"])
            assert biz_date is not None
            out.append(
                QualityMetricRecord(
                    run_id=data["run_id"],
                    dataset_id=data["dataset_id"],
                    layer=data["layer"],
                    entity=data["entity"],
                    business_date=biz_date,
                    metric_name=data["metric_name"],
                    metric_value=int(data["metric_value"]),
                    status=data["status"],
                    details=dict(data.get("details") or {}),
                )
            )
        return out


def connect_control_store(db_config: PostgresDbConfig) -> ControlStore:
    """Tạo `ControlStore` từ `load_config().postgres.control`."""
    for module_name in ("psycopg", "psycopg2"):
        try:
            driver = __import__(module_name)

            def _factory(drv: Any = driver) -> Any:
                return drv.connect(
                    host=db_config.host,
                    port=db_config.port,
                    dbname=db_config.database,
                    user=db_config.user,
                    password=db_config.password,
                )

            return PsycopgControlStore(_factory)
        except ImportError:
            continue

    psql_bin = shutil.which("psql")
    if psql_bin is not None:
        return PsqlCliControlStore(db_config, psql_bin=psql_bin)

    raise CommitError(
        "Không tìm thấy driver PostgreSQL (psycopg/psycopg2) hoặc công cụ psql trong PATH."
    )


def make_compose_psql_runner(
    db_config: PostgresDbConfig,
    *,
    docker_bin: str,
    compose_file: Path,
    repo_root: Path,
) -> PsqlRunner:
    """Tạo runner gọi `psql` bên trong container `postgres` của Docker Compose."""

    def _runner(sql: str, tuples_only: bool) -> str:
        cmd = [
            docker_bin,
            "compose",
            "-f",
            str(compose_file),
            "exec",
            "-T",
            "-e",
            f"PGPASSWORD={db_config.password}",
            "-e",
            "PGCLIENTENCODING=UTF8",
            "postgres",
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
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
                cwd=repo_root,
                input=sql,
                text=True,
                encoding="utf-8",
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise CommitError(
                f"psql trong container thất bại (exit {exc.returncode}): {stderr}"
            ) from exc
        return proc.stdout

    return _runner
