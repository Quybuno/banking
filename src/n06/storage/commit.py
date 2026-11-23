"""Giao thức commit batch theo `run_id` và `current_snapshot` (mục 8.1, TASK T24)."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable, Sequence
from datetime import date
from typing import Any

from n06.storage.control_store import (
    VALID_TARGET_COMMIT_STATUSES,
    CommitError,
    ControlStore,
    IngestBatchRecord,
    RunRecord,
    SnapshotRecord,
)


def _normalize_date(value: date | str) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def generate_run_id() -> str:
    """Sinh `run_id` mới cho một lần chạy batch."""
    return f"run-{uuid.uuid4().hex}"


def build_run_output_path(
    base_prefix: str,
    *,
    dataset_id: str,
    layer: str,
    entity: str,
    run_id: str,
    business_date: date | str,
) -> str:
    """Tạo đường dẫn output bất biến cô lập theo `run_id` và `business_date`."""
    if not run_id or "/" in run_id:
        raise ValueError(f"run_id không hợp lệ: {run_id!r}")
    clean_prefix = base_prefix.rstrip("/")
    biz_date = _normalize_date(business_date).isoformat()
    return f"{clean_prefix}/{dataset_id}/{layer}/{entity}/run_id={run_id}/business_date={biz_date}"


def compute_semantic_hash(
    *,
    dataset_id: str,
    layer: str,
    entity: str,
    business_date: date | str,
    input_versions: Sequence[Any],
    config_hash: str,
    schema_version: str,
    rule_version: str,
    code_version: str,
    payload_digest: str,
) -> str:
    """Tính semantic hash bất biến theo input, cấu hình và phiên bản (loại trừ `run_id`)."""
    payload = {
        "dataset_id": dataset_id,
        "layer": layer,
        "entity": entity,
        "business_date": _normalize_date(business_date).isoformat(),
        "input_versions": list(input_versions),
        "config_hash": config_hash,
        "schema_version": schema_version,
        "rule_version": rule_version,
        "code_version": code_version,
        "payload_digest": payload_digest,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class BatchCommitCoordinator:
    """Điều phối vòng đời run, commit nguyên tử `current_snapshot` và reader."""

    def __init__(self, store: ControlStore) -> None:
        self._store = store

    def start_run(
        self,
        *,
        dataset_id: str,
        job_name: str,
        profile: str,
        business_date_from: date | str | None,
        business_date_to: date | str | None,
        input_versions: Sequence[Any],
        config_hash: str,
        schema_version: str,
        rule_version: str,
        code_version: str,
        run_id: str | None = None,
    ) -> RunRecord:
        """Khởi tạo một run ở trạng thái `RUNNING` trước khi ghi dữ liệu."""
        resolved_run_id = run_id or generate_run_id()
        from_date = _normalize_date(business_date_from) if business_date_from else None
        to_date = _normalize_date(business_date_to) if business_date_to else None
        record = RunRecord(
            run_id=resolved_run_id,
            dataset_id=dataset_id,
            job_name=job_name,
            profile=profile,
            business_date_from=from_date,
            business_date_to=to_date,
            status="RUNNING",
            row_count=0,
            input_versions=list(input_versions),
            config_hash=config_hash,
            schema_version=schema_version,
            rule_version=rule_version,
            code_version=code_version,
        )
        return self._store.start_run(record)

    def commit_run(
        self,
        *,
        run_id: str,
        dataset_id: str,
        layer: str,
        entity: str,
        business_date: date | str,
        output_path: str,
        row_count: int,
        semantic_hash: str,
        manifest_path: str | None = None,
        target_status: str = "SUCCEEDED",
        after_run_update_hook: Callable[[], None] | None = None,
    ) -> SnapshotRecord:
        """Cập nhật `runs` và trỏ `current_snapshot` sang `run_id` mới trong một transaction."""
        if row_count < 0:
            raise CommitError(f"row_count không được âm: {row_count}")
        if target_status not in VALID_TARGET_COMMIT_STATUSES:
            raise CommitError(f"target_status không hợp lệ: {target_status!r}")
        if not output_path or f"run_id={run_id}" not in output_path:
            raise CommitError(f"output_path phải cô lập theo run_id={run_id}: nhận {output_path!r}")

        biz_date = _normalize_date(business_date)
        return self._store.commit_run_atomic(
            run_id=run_id,
            dataset_id=dataset_id,
            layer=layer,
            entity=entity,
            business_date=biz_date,
            output_path=output_path,
            manifest_path=manifest_path,
            row_count=row_count,
            semantic_hash=semantic_hash,
            target_status=target_status,
            after_run_update_hook=after_run_update_hook,
        )

    def fail_run(self, *, run_id: str, error_summary: str) -> None:
        """Đánh dấu run thất bại mà không chạm tới `current_snapshot`."""
        self._store.fail_run(run_id=run_id, error_summary=error_summary)

    def resolve_current_snapshot(
        self,
        *,
        dataset_id: str,
        layer: str,
        entity: str,
        business_date: date | str,
    ) -> SnapshotRecord | None:
        """Reader: chỉ trả về snapshot đang được `current_snapshot` trỏ tới và đã commit."""
        return self._store.resolve_current_snapshot(
            dataset_id=dataset_id,
            layer=layer,
            entity=entity,
            business_date=_normalize_date(business_date),
        )

    def list_committed_ingest_batches(
        self,
        *,
        dataset_id: str,
        query_name: str | None = None,
    ) -> list[IngestBatchRecord]:
        """Reader: chỉ đọc các `batch_id` đã ghi nhận trong `ingest_batches`."""
        return self._store.list_committed_ingest_batches(
            dataset_id=dataset_id,
            query_name=query_name,
        )
