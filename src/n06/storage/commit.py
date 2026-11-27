"""Giao thức commit batch theo `run_id` và `current_snapshot` (mục 8.1, TASK T24)."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Callable, Iterable, Sequence
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


# Mẫu thành phần đường dẫn theo ADR-0002 ("Quy ước chung").
_DATASET_ID_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
_ENTITY_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


def _run_prefix(tier_prefix: str, *, dataset_id: str, entity: str, run_id: str) -> str:
    """Gốc của một run: `<tier_prefix>/<dataset_id>/<entity>/run_id=<run_id>` (ADR-0002 mục 3).

    `tier_prefix` đã gồm tiền tố của tầng (`paths.silver_prefix`, `gold_prefix` hoặc
    `quarantine_prefix`), có thể là URI `s3a://<bucket>/<prefix>`.
    """
    if not tier_prefix.strip("/"):
        raise ValueError("tier_prefix không được rỗng")
    for name, value, pattern in (
        ("dataset_id", dataset_id, _DATASET_ID_RE),
        ("entity", entity, _ENTITY_RE),
        ("run_id", run_id, _RUN_ID_RE),
    ):
        if not pattern.fullmatch(value):
            raise ValueError(f"{name} không hợp lệ theo ADR-0002: {value!r}")
    return f"{tier_prefix.rstrip('/')}/{dataset_id}/{entity}/run_id={run_id}"


def build_run_output_path(
    tier_prefix: str,
    *,
    dataset_id: str,
    entity: str,
    run_id: str,
    business_date: date | str,
) -> str:
    """Đường dẫn partition bất biến của một run (ADR-0002 mục 3).

    Dạng `<tier_prefix>/<dataset_id>/<entity>/run_id=<run_id>/business_date=<YYYY-MM-DD>`.
    Tầng nằm trong `tier_prefix`, không còn là một đoạn riêng của đường dẫn.
    """
    root = _run_prefix(tier_prefix, dataset_id=dataset_id, entity=entity, run_id=run_id)
    return f"{root}/business_date={_normalize_date(business_date).isoformat()}"


def require_unused_run_prefix(
    list_objects: Callable[[str], Iterable[str]],
    tier_prefix: str,
    *,
    dataset_id: str,
    entity: str,
    run_id: str,
) -> None:
    """Báo lỗi nếu đã có object dưới gốc `run_id=<run_id>/` (ADR-0002 mục 6).

    Phải gọi trước khi ghi bất kỳ object nào của run. `list_objects(prefix)` trả các khóa object
    dưới `prefix`; bản S3 thật do bên ghi dữ liệu (T25) cung cấp. Prefix liệt kê kết thúc bằng
    `/` để `run_id=run-1` không trùng với `run_id=run-10`.
    """
    prefix = _run_prefix(tier_prefix, dataset_id=dataset_id, entity=entity, run_id=run_id) + "/"
    existing = next(iter(list_objects(prefix)), None)
    if existing is not None:
        raise CommitError(
            f"Đường dẫn của run đã có object (ví dụ {existing!r}); không được ghi vào run_id "
            f"đã tồn tại: {prefix}"
        )


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

    def list_committed_ingest_batches(self, *, query_name: str) -> list[IngestBatchRecord]:
        """Reader: chỉ đọc các `batch_id` đã ghi nhận trong `ingest_batches`.

        Lọc theo query vì streaming ghi `dataset_id` NULL (ADR-0004 Q1).
        """
        return self._store.list_committed_ingest_batches(query_name=query_name)
