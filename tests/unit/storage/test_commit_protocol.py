"""Unit tests cho giao thức commit batch (TASK T24).

Expected values (đường dẫn, semantic hash, trạng thái) được tính độc lập
theo đặc tả mục 8.1, không gọi ngược hàm đang test để sinh expected.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable
from datetime import date

import pytest

from n06.storage import (
    COMMITTED_RUN_STATUSES,
    BatchCommitCoordinator,
    CommitError,
    IngestBatchRecord,
    RunRecord,
    RunStateError,
    SnapshotRecord,
    build_run_output_path,
    compute_semantic_hash,
)


class _InMemoryControlStore:
    """ControlStore giả lập trong bộ nhớ có transaction snapshot/rollback và lock."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.runs: dict[str, RunRecord] = {}
        self.snapshots: dict[tuple[str, str, str, date], SnapshotRecord] = {}
        self.ingest_batches: list[IngestBatchRecord] = []

    def start_run(self, run: RunRecord) -> RunRecord:
        with self._lock:
            existing = self.runs.get(run.run_id)
            if existing is not None:
                if existing.status != "RUNNING":
                    raise RunStateError(f"run_id '{run.run_id}' đã ở trạng thái {existing.status}")
                return existing
            self.runs[run.run_id] = run
            return run

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
        with self._lock:
            runs_backup = dict(self.runs)
            snapshots_backup = dict(self.snapshots)
            try:
                existing = self.runs.get(run_id)
                if existing is None:
                    raise CommitError(f"Không tìm thấy run_id '{run_id}'")
                if existing.status in COMMITTED_RUN_STATUSES:
                    if existing.semantic_hash != semantic_hash:
                        raise RunStateError("semantic_hash không khớp khi rerun")
                elif existing.status != "RUNNING":
                    raise RunStateError(f"Trạng thái không hợp lệ: {existing.status}")
                else:
                    self.runs[run_id] = RunRecord(
                        run_id=existing.run_id,
                        dataset_id=existing.dataset_id,
                        job_name=existing.job_name,
                        profile=existing.profile,
                        business_date_from=existing.business_date_from,
                        business_date_to=existing.business_date_to,
                        status=target_status,
                        row_count=row_count,
                        input_versions=existing.input_versions,
                        config_hash=existing.config_hash,
                        schema_version=existing.schema_version,
                        rule_version=existing.rule_version,
                        code_version=existing.code_version,
                        semantic_hash=semantic_hash,
                    )

                if after_run_update_hook is not None:
                    after_run_update_hook()

                snap = SnapshotRecord(
                    dataset_id=dataset_id,
                    layer=layer,
                    entity=entity,
                    business_date=business_date,
                    run_id=run_id,
                    output_path=output_path,
                    manifest_path=manifest_path,
                    semantic_hash=semantic_hash,
                )
                self.snapshots[(dataset_id, layer, entity, business_date)] = snap
                return snap
            except Exception:
                self.runs = runs_backup
                self.snapshots = snapshots_backup
                raise

    def fail_run(self, *, run_id: str, error_summary: str) -> None:
        with self._lock:
            existing = self.runs.get(run_id)
            if existing is None:
                raise CommitError(f"Không tìm thấy run_id '{run_id}'")
            if existing.status in COMMITTED_RUN_STATUSES:
                raise RunStateError("Không thể fail run đã commit")
            self.runs[run_id] = RunRecord(
                run_id=existing.run_id,
                dataset_id=existing.dataset_id,
                job_name=existing.job_name,
                profile=existing.profile,
                business_date_from=existing.business_date_from,
                business_date_to=existing.business_date_to,
                status="FAILED",
                row_count=existing.row_count,
                input_versions=existing.input_versions,
                config_hash=existing.config_hash,
                schema_version=existing.schema_version,
                rule_version=existing.rule_version,
                code_version=existing.code_version,
                semantic_hash=existing.semantic_hash,
                error_summary=error_summary,
            )

    def get_run(self, run_id: str) -> RunRecord | None:
        return self.runs.get(run_id)

    def resolve_current_snapshot(
        self,
        *,
        dataset_id: str,
        layer: str,
        entity: str,
        business_date: date,
    ) -> SnapshotRecord | None:
        snap = self.snapshots.get((dataset_id, layer, entity, business_date))
        if snap is None:
            return None
        run = self.runs.get(snap.run_id)
        if run is None or run.status not in COMMITTED_RUN_STATUSES:
            return None
        return snap

    def list_committed_ingest_batches(
        self,
        *,
        dataset_id: str,
        query_name: str | None = None,
    ) -> list[IngestBatchRecord]:
        return [
            b
            for b in self.ingest_batches
            if b.dataset_id == dataset_id
            and b.status == "COMMITTED"
            and (query_name is None or b.query_name == query_name)
        ]


def test_build_run_output_path_and_independent_semantic_hash() -> None:
    path = build_run_output_path(
        "s3a://n06-test-bucket/lakehouse/",
        dataset_id="ds-alpha",
        layer="silver",
        entity="transactions",
        run_id="run-001",
        business_date="2026-01-01",
    )
    assert path == (
        "s3a://n06-test-bucket/lakehouse/ds-alpha/silver/transactions"
        "/run_id=run-001/business_date=2026-01-01"
    )

    # Expected semantic hash tính độc lập theo chuẩn JSON canonical + SHA-256
    expected_payload = (
        '{"business_date":"2026-01-01","code_version":"abc123","config_hash":"cfg1",'
        '"dataset_id":"ds-alpha","entity":"transactions","input_versions":["b1","b2"],'
        '"layer":"silver","payload_digest":"sha256:deadbeef","rule_version":"1.0.0",'
        '"schema_version":"1.0.0"}'
    )
    expected_hash = hashlib.sha256(expected_payload.encode("utf-8")).hexdigest()

    h1 = compute_semantic_hash(
        dataset_id="ds-alpha",
        layer="silver",
        entity="transactions",
        business_date=date(2026, 1, 1),
        input_versions=["b1", "b2"],
        config_hash="cfg1",
        schema_version="1.0.0",
        rule_version="1.0.0",
        code_version="abc123",
        payload_digest="sha256:deadbeef",
    )
    h2 = compute_semantic_hash(
        dataset_id="ds-alpha",
        layer="silver",
        entity="transactions",
        business_date="2026-01-01",
        input_versions=["b1", "b2"],
        config_hash="cfg1",
        schema_version="1.0.0",
        rule_version="1.0.0",
        code_version="abc123",
        payload_digest="sha256:deadbeef",
    )
    assert h1 == expected_hash
    assert h2 == expected_hash


def test_running_and_failed_runs_never_exposed_by_reader() -> None:
    store = _InMemoryControlStore()
    coord = BatchCommitCoordinator(store)

    # Run 1 thành công
    coord.start_run(
        run_id="run-1",
        dataset_id="ds-1",
        job_name="process_day",
        profile="test",
        business_date_from="2026-01-01",
        business_date_to="2026-01-01",
        input_versions=["b1"],
        config_hash="c1",
        schema_version="1.0.0",
        rule_version="1.0.0",
        code_version="v1",
    )
    assert (
        coord.resolve_current_snapshot(
            dataset_id="ds-1", layer="silver", entity="transactions", business_date="2026-01-01"
        )
        is None
    )

    coord.commit_run(
        run_id="run-1",
        dataset_id="ds-1",
        layer="silver",
        entity="transactions",
        business_date="2026-01-01",
        output_path="lake/ds-1/silver/transactions/run_id=run-1/business_date=2026-01-01",
        row_count=10,
        semantic_hash="hash-1",
    )

    # Run 2 đang chạy và sau đó thất bại -> reader vẫn phải trỏ về run-1
    coord.start_run(
        run_id="run-2",
        dataset_id="ds-1",
        job_name="process_day",
        profile="test",
        business_date_from="2026-01-01",
        business_date_to="2026-01-01",
        input_versions=["b1", "b2"],
        config_hash="c1",
        schema_version="1.0.0",
        rule_version="1.0.0",
        code_version="v1",
    )
    snap_during_run2 = coord.resolve_current_snapshot(
        dataset_id="ds-1", layer="silver", entity="transactions", business_date="2026-01-01"
    )
    assert snap_during_run2 is not None
    assert snap_during_run2.run_id == "run-1"

    coord.fail_run(run_id="run-2", error_summary="boom")
    snap_after_fail = coord.resolve_current_snapshot(
        dataset_id="ds-1", layer="silver", entity="transactions", business_date="2026-01-01"
    )
    assert snap_after_fail is not None
    assert snap_after_fail.run_id == "run-1"
    assert store.get_run("run-2").status == "FAILED"


def test_atomic_rollback_preserves_old_snapshot_and_idempotent_rerun() -> None:
    store = _InMemoryControlStore()
    coord = BatchCommitCoordinator(store)

    coord.start_run(
        run_id="run-1",
        dataset_id="ds-1",
        job_name="process_day",
        profile="test",
        business_date_from="2026-01-01",
        business_date_to="2026-01-01",
        input_versions=["b1"],
        config_hash="c1",
        schema_version="1.0.0",
        rule_version="1.0.0",
        code_version="v1",
    )
    coord.commit_run(
        run_id="run-1",
        dataset_id="ds-1",
        layer="gold",
        entity="fact_transactions",
        business_date="2026-01-01",
        output_path="lake/ds-1/gold/fact_transactions/run_id=run-1/business_date=2026-01-01",
        row_count=5,
        semantic_hash="hash-1",
        target_status="GOLD_COMMITTED",
    )

    # Chạy lại cùng run_id (idempotent) không tạo bản ghi trùng
    rerun_snap = coord.commit_run(
        run_id="run-1",
        dataset_id="ds-1",
        layer="gold",
        entity="fact_transactions",
        business_date="2026-01-01",
        output_path="lake/ds-1/gold/fact_transactions/run_id=run-1/business_date=2026-01-01",
        row_count=5,
        semantic_hash="hash-1",
        target_status="GOLD_COMMITTED",
    )
    assert rerun_snap.run_id == "run-1"
    assert len(store.runs) == 1
    assert len(store.snapshots) == 1

    # Run 2 lỗi giữa transaction (sau khi update runs) -> rollback toàn bộ
    coord.start_run(
        run_id="run-2",
        dataset_id="ds-1",
        job_name="process_day",
        profile="test",
        business_date_from="2026-01-01",
        business_date_to="2026-01-01",
        input_versions=["b2"],
        config_hash="c1",
        schema_version="1.0.0",
        rule_version="1.0.0",
        code_version="v1",
    )

    def _boom() -> None:
        raise RuntimeError("mô phỏng lỗi ghi snapshot")

    with pytest.raises(RuntimeError, match="mô phỏng lỗi"):
        coord.commit_run(
            run_id="run-2",
            dataset_id="ds-1",
            layer="gold",
            entity="fact_transactions",
            business_date="2026-01-01",
            output_path="lake/ds-1/gold/fact_transactions/run_id=run-2/business_date=2026-01-01",
            row_count=7,
            semantic_hash="hash-2",
            after_run_update_hook=_boom,
        )

    assert store.get_run("run-2").status == "RUNNING"
    current = coord.resolve_current_snapshot(
        dataset_id="ds-1", layer="gold", entity="fact_transactions", business_date="2026-01-01"
    )
    assert current is not None
    assert current.run_id == "run-1"
    assert json.loads(json.dumps(current.semantic_hash)) == "hash-1"
