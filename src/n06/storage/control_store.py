"""Tầng truy cập Control DB cho giao thức commit batch (TASK T24, ADR-0003)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
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
class IngestBatchRecord:
    """Bản ghi batch streaming đã commit trong `ingest_batches`."""

    query_name: str
    batch_id: int
    dataset_id: str
    ingestion_date: date
    topic_offsets: dict[str, Any]
    record_count: int
    raw_path: str
    bronze_path: str
    status: str


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

    def list_committed_ingest_batches(
        self,
        *,
        dataset_id: str,
        query_name: str | None = None,
    ) -> list[IngestBatchRecord]:
        """Liệt kê các `ingest_batches` đã commit cho `dataset_id`."""


def _scope_key(dataset_id: str, layer: str, entity: str, business_date: date) -> str:
    return f"{dataset_id}|{layer}|{entity}|{business_date.isoformat()}"


def _to_date(value: date | str | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _escape_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_text_or_null(value: str | None) -> str:
    return "NULL" if value is None else _escape_literal(value)


def _sql_date_or_null(value: date | None) -> str:
    return "NULL" if value is None else f"DATE {_escape_literal(value.isoformat())}"


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
        conn = self._conn_factory()
        scope = _scope_key(dataset_id, layer, entity, business_date)
        try:
            with conn.cursor() as cur:
                # Tuần tự hóa mọi commit đồng thời trên cùng (dataset, layer, entity, business_date)
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
                    # Idempotent rerun cùng run_id: cập nhật/đảm bảo snapshot đồng bộ
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
                            layer,
                            entity,
                            business_date,
                            run_id,
                            output_path,
                            manifest_path,
                            semantic_hash,
                        ),
                    )
                    conn.commit()
                    return SnapshotRecord(
                        dataset_id=dataset_id,
                        layer=layer,
                        entity=entity,
                        business_date=business_date,
                        run_id=run_id,
                        output_path=output_path,
                        manifest_path=manifest_path,
                        semantic_hash=semantic_hash,
                    )

                if current_status != "RUNNING":
                    raise RunStateError(
                        f"Không thể commit run_id '{run_id}' từ trạng thái '{current_status}'."
                    )

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
                        layer,
                        entity,
                        business_date,
                        run_id,
                        output_path,
                        manifest_path,
                        semantic_hash,
                    ),
                )
            conn.commit()
            return SnapshotRecord(
                dataset_id=dataset_id,
                layer=layer,
                entity=entity,
                business_date=business_date,
                run_id=run_id,
                output_path=output_path,
                manifest_path=manifest_path,
                semantic_hash=semantic_hash,
            )
        except CommitError:
            conn.rollback()
            raise
        except Exception as exc:
            conn.rollback()
            raise CommitError(f"commit_run_atomic thất bại cho '{run_id}': {exc}") from exc
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

    def list_committed_ingest_batches(
        self,
        *,
        dataset_id: str,
        query_name: str | None = None,
    ) -> list[IngestBatchRecord]:
        conn = self._conn_factory()
        try:
            with conn.cursor() as cur:
                if query_name is None:
                    cur.execute(
                        """
                        SELECT query_name, batch_id, dataset_id, ingestion_date,
                               topic_offsets, record_count, raw_path, bronze_path, status
                        FROM ingest_batches
                        WHERE dataset_id = %s AND status = 'COMMITTED'
                        ORDER BY query_name ASC, batch_id ASC;
                        """,
                        (dataset_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT query_name, batch_id, dataset_id, ingestion_date,
                               topic_offsets, record_count, raw_path, bronze_path, status
                        FROM ingest_batches
                        WHERE dataset_id = %s AND query_name = %s AND status = 'COMMITTED'
                        ORDER BY query_name ASC, batch_id ASC;
                        """,
                        (dataset_id, query_name),
                    )
                rows = cur.fetchall()
            results: list[IngestBatchRecord] = []
            for row in rows:
                offsets = json.loads(row[4]) if isinstance(row[4], str) else dict(row[4])
                ing_date = _to_date(row[3])
                assert ing_date is not None
                results.append(
                    IngestBatchRecord(
                        query_name=row[0],
                        batch_id=int(row[1]),
                        dataset_id=row[2],
                        ingestion_date=ing_date,
                        topic_offsets=offsets,
                        record_count=int(row[5]),
                        raw_path=row[6],
                        bronze_path=row[7],
                        status=row[8],
                    )
                )
            return results
        finally:
            conn.close()


class PsqlCliControlStore:
    """Triển khai `ControlStore` qua `psql` CLI (hoặc wrapper tùy biến) trong một transaction."""

    def __init__(
        self,
        db_config: PostgresDbConfig,
        *,
        psql_runner: Callable[[str, bool], str] | None = None,
        psql_bin: str = "psql",
    ) -> None:
        self._db_config = db_config
        self._psql_runner = psql_runner
        self._psql_bin = psql_bin

    def _run_sql(self, sql: str, *, tuples_only: bool = False) -> str:
        if self._psql_runner is not None:
            return self._psql_runner(sql, tuples_only)

        env = {**os.environ, "PGPASSWORD": self._db_config.password}
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
                capture_output=True,
                env=env,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise CommitError(f"psql thất bại (exit {exc.returncode}): {stderr}") from exc
        return proc.stdout

    def start_run(self, run: RunRecord) -> RunRecord:
        inputs_json = json.dumps(run.input_versions, ensure_ascii=False)
        sql = f"""
        BEGIN;
        DO $$
        DECLARE
            v_status TEXT;
        BEGIN
            SELECT status INTO v_status
            FROM runs WHERE run_id = {_escape_literal(run.run_id)} FOR UPDATE;
            IF FOUND THEN
                IF v_status <> 'RUNNING' THEN
                    RAISE EXCEPTION 'run_id % already exists with status %',
                        {_escape_literal(run.run_id)}, v_status;
                END IF;
            ELSE
                INSERT INTO runs (
                    run_id, dataset_id, job_name, profile,
                    business_date_from, business_date_to, status,
                    row_count, input_versions, config_hash,
                    schema_version, rule_version, code_version
                ) VALUES (
                    {_escape_literal(run.run_id)},
                    {_escape_literal(run.dataset_id)},
                    {_escape_literal(run.job_name)},
                    {_escape_literal(run.profile)},
                    {_sql_date_or_null(run.business_date_from)},
                    {_sql_date_or_null(run.business_date_to)},
                    'RUNNING',
                    0,
                    {_escape_literal(inputs_json)}::jsonb,
                    {_escape_literal(run.config_hash)},
                    {_escape_literal(run.schema_version)},
                    {_escape_literal(run.rule_version)},
                    {_escape_literal(run.code_version)}
                );
            END IF;
        END $$;
        COMMIT;
        """
        self._run_sql(sql)
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
        if after_run_update_hook is not None:
            # Thực thi hook trước bước upsert bên trong cùng kịch bản transaction không khả thi
            # qua subprocess một lần; gọi hook ngay trước khi commit khối SQL nguyên tử.
            after_run_update_hook()

        scope = _scope_key(dataset_id, layer, entity, business_date)
        sql = f"""
        BEGIN;
        SELECT pg_advisory_xact_lock(hashtextextended({_escape_literal(scope)}, 0));
        DO $$
        DECLARE
            v_status TEXT;
            v_hash TEXT;
        BEGIN
            SELECT status, semantic_hash INTO v_status, v_hash
            FROM runs WHERE run_id = {_escape_literal(run_id)} FOR UPDATE;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'run_id % not found', {_escape_literal(run_id)};
            END IF;

            IF v_status IN ('SUCCEEDED', 'GOLD_COMMITTED', 'MART_PUBLISHED') THEN
                IF v_hash IS DISTINCT FROM {_escape_literal(semantic_hash)} THEN
                    RAISE EXCEPTION 'run_id % already committed with different semantic_hash',
                        {_escape_literal(run_id)};
                END IF;
            ELSIF v_status = 'RUNNING' THEN
                UPDATE runs
                SET status = {_escape_literal(target_status)},
                    row_count = {row_count},
                    semantic_hash = {_escape_literal(semantic_hash)},
                    finished_at = NOW()
                WHERE run_id = {_escape_literal(run_id)};
            ELSE
                RAISE EXCEPTION 'cannot commit run_id % from status %',
                    {_escape_literal(run_id)}, v_status;
            END IF;
        END $$;

        INSERT INTO current_snapshot (
            dataset_id, layer, entity, business_date,
            run_id, output_path, manifest_path, semantic_hash
        ) VALUES (
            {_escape_literal(dataset_id)},
            {_escape_literal(layer)},
            {_escape_literal(entity)},
            DATE {_escape_literal(business_date.isoformat())},
            {_escape_literal(run_id)},
            {_escape_literal(output_path)},
            {_sql_text_or_null(manifest_path)},
            {_escape_literal(semantic_hash)}
        )
        ON CONFLICT (dataset_id, layer, entity, business_date) DO UPDATE
            SET run_id = EXCLUDED.run_id,
                output_path = EXCLUDED.output_path,
                manifest_path = EXCLUDED.manifest_path,
                semantic_hash = EXCLUDED.semantic_hash,
                updated_at = NOW();
        COMMIT;
        """
        self._run_sql(sql)
        return SnapshotRecord(
            dataset_id=dataset_id,
            layer=layer,
            entity=entity,
            business_date=business_date,
            run_id=run_id,
            output_path=output_path,
            manifest_path=manifest_path,
            semantic_hash=semantic_hash,
        )

    def fail_run(self, *, run_id: str, error_summary: str) -> None:
        sql = f"""
        BEGIN;
        DO $$
        DECLARE
            v_status TEXT;
        BEGIN
            SELECT status INTO v_status
            FROM runs WHERE run_id = {_escape_literal(run_id)} FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'run_id % not found', {_escape_literal(run_id)};
            END IF;
            IF v_status IN ('SUCCEEDED', 'GOLD_COMMITTED', 'MART_PUBLISHED') THEN
                RAISE EXCEPTION 'cannot fail committed run_id %', {_escape_literal(run_id)};
            END IF;
            UPDATE runs
            SET status = 'FAILED',
                error_summary = {_escape_literal(error_summary)},
                finished_at = NOW()
            WHERE run_id = {_escape_literal(run_id)};
        END $$;
        COMMIT;
        """
        self._run_sql(sql)

    def get_run(self, run_id: str) -> RunRecord | None:
        sql = f"""
        SELECT COALESCE(
            (SELECT row_to_json(t)::text FROM (
                SELECT run_id, dataset_id, job_name, profile,
                       business_date_from::text, business_date_to::text, status,
                       row_count, input_versions, config_hash,
                       schema_version, rule_version, code_version,
                       semantic_hash, error_summary
                FROM runs
                WHERE run_id = {_escape_literal(run_id)}
            ) t),
            ''
        );
        """
        raw = self._run_sql(sql, tuples_only=True).strip()
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
        sql = f"""
        SELECT COALESCE(
            (SELECT row_to_json(t)::text FROM (
                SELECT cs.dataset_id, cs.layer, cs.entity, cs.business_date::text,
                       cs.run_id, cs.output_path, cs.manifest_path, cs.semantic_hash
                FROM current_snapshot cs
                JOIN runs r ON r.run_id = cs.run_id
                WHERE cs.dataset_id = {_escape_literal(dataset_id)}
                  AND cs.layer = {_escape_literal(layer)}
                  AND cs.entity = {_escape_literal(entity)}
                  AND cs.business_date = DATE {_escape_literal(business_date.isoformat())}
                  AND r.status IN ('SUCCEEDED', 'GOLD_COMMITTED', 'MART_PUBLISHED')
            ) t),
            ''
        );
        """
        raw = self._run_sql(sql, tuples_only=True).strip()
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

    def list_committed_ingest_batches(
        self,
        *,
        dataset_id: str,
        query_name: str | None = None,
    ) -> list[IngestBatchRecord]:
        extra_filter = (
            f"AND query_name = {_escape_literal(query_name)}" if query_name is not None else ""
        )
        sql = f"""
        SELECT COALESCE(
            (SELECT json_agg(t ORDER BY query_name ASC, batch_id ASC)::text FROM (
                SELECT query_name, batch_id, dataset_id, ingestion_date::text,
                       topic_offsets, record_count, raw_path, bronze_path, status
                FROM ingest_batches
                WHERE dataset_id = {_escape_literal(dataset_id)}
                  AND status = 'COMMITTED'
                  {extra_filter}
            ) t),
            '[]'
        );
        """
        raw = self._run_sql(sql, tuples_only=True).strip() or "[]"
        rows = json.loads(raw)
        results: list[IngestBatchRecord] = []
        for row in rows:
            ing_date = _to_date(row["ingestion_date"])
            assert ing_date is not None
            results.append(
                IngestBatchRecord(
                    query_name=row["query_name"],
                    batch_id=int(row["batch_id"]),
                    dataset_id=row["dataset_id"],
                    ingestion_date=ing_date,
                    topic_offsets=dict(row["topic_offsets"]),
                    record_count=int(row["record_count"]),
                    raw_path=row["raw_path"],
                    bronze_path=row["bronze_path"],
                    status=row["status"],
                )
            )
        return results


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
) -> Callable[[str, bool], str]:
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
