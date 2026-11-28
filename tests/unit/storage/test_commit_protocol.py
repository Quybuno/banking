"""Unit tests cho giao thức commit batch (TASK T24).

Expected values (đường dẫn, semantic hash, trạng thái) được tính độc lập
theo đặc tả mục 8.1, không gọi ngược hàm đang test để sinh expected.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable, Sequence
from datetime import date

import pytest

from n06.storage import (
    COMMITTED_RUN_STATUSES,
    BatchCommitCoordinator,
    CommitError,
    IngestBatchRecord,
    QualityMetricRecord,
    RunRecord,
    RunStateError,
    SnapshotRecord,
    SnapshotSpec,
    build_run_output_path,
    compute_semantic_hash,
    require_unused_run_prefix,
)


class _InMemoryControlStore:
    """ControlStore giả lập trong bộ nhớ có transaction snapshot/rollback và lock."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.runs: dict[str, RunRecord] = {}
        self.snapshots: dict[tuple[str, str, str, date], SnapshotRecord] = {}
        self.ingest_batches: list[IngestBatchRecord] = []
        self.quality_metrics: dict[tuple[str, str, str, date, str], QualityMetricRecord] = {}

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

                out: list[SnapshotRecord] = []
                for s in snapshots:
                    snap = SnapshotRecord(
                        dataset_id=dataset_id,
                        layer=s.layer,
                        entity=s.entity,
                        business_date=s.business_date,
                        run_id=run_id,
                        output_path=s.output_path,
                        manifest_path=s.manifest_path,
                        semantic_hash=semantic_hash,
                    )
                    self.snapshots[(dataset_id, s.layer, s.entity, s.business_date)] = snap
                    out.append(snap)
                return out
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

    def upsert_ingest_batch(self, batch: IngestBatchRecord) -> None:
        with self._lock:
            key = (batch.query_name, batch.batch_id)
            self.ingest_batches = [
                b for b in self.ingest_batches if (b.query_name, b.batch_id) != key
            ] + [batch]

    def list_committed_ingest_batches(self, *, query_name: str) -> list[IngestBatchRecord]:
        return sorted(
            (
                b
                for b in self.ingest_batches
                if b.query_name == query_name and b.status == "COMMITTED"
            ),
            key=lambda b: b.batch_id,
        )

    def list_current_snapshots(
        self,
        *,
        dataset_id: str,
        layer: str,
        entity: str | None = None,
        business_date_from: date | None = None,
        business_date_to: date | None = None,
    ) -> list[SnapshotRecord]:
        out: list[SnapshotRecord] = []
        for snap in self.snapshots.values():
            if snap.dataset_id != dataset_id or snap.layer != layer:
                continue
            if entity is not None and snap.entity != entity:
                continue
            if business_date_from is not None and snap.business_date < business_date_from:
                continue
            if business_date_to is not None and snap.business_date > business_date_to:
                continue
            run = self.runs.get(snap.run_id)
            if run is None or run.status not in COMMITTED_RUN_STATUSES:
                continue
            out.append(snap)
        return sorted(out, key=lambda s: (s.entity, s.business_date))

    def mark_run_mart_published(self, *, run_id: str) -> None:
        with self._lock:
            existing = self.runs.get(run_id)
            if existing is None:
                raise CommitError(f"Không tìm thấy run_id '{run_id}'")
            if existing.status not in COMMITTED_RUN_STATUSES:
                raise RunStateError(f"Trạng thái không hợp lệ: {existing.status}")
            self.runs[run_id] = RunRecord(
                run_id=existing.run_id,
                dataset_id=existing.dataset_id,
                job_name=existing.job_name,
                profile=existing.profile,
                business_date_from=existing.business_date_from,
                business_date_to=existing.business_date_to,
                status="MART_PUBLISHED",
                row_count=existing.row_count,
                input_versions=existing.input_versions,
                config_hash=existing.config_hash,
                schema_version=existing.schema_version,
                rule_version=existing.rule_version,
                code_version=existing.code_version,
                semantic_hash=existing.semantic_hash,
                error_summary=existing.error_summary,
            )

    def upsert_quality_metrics(self, metrics: Sequence[QualityMetricRecord]) -> None:
        with self._lock:
            for m in metrics:
                self.quality_metrics[
                    (m.run_id, m.layer, m.entity, m.business_date, m.metric_name)
                ] = m

    def list_quality_metrics(self, *, run_id: str) -> list[QualityMetricRecord]:
        return sorted(
            (m for m in self.quality_metrics.values() if m.run_id == run_id),
            key=lambda m: (m.business_date, m.layer, m.entity, m.metric_name),
        )


def test_build_run_output_path_follows_adr_0002() -> None:
    # Expected viết tay theo ADR-0002 mục 3, không lấy từ chính hàm đang test.
    path = build_run_output_path(
        "s3a://n06-test-bucket/silver/",
        dataset_id="ds-alpha",
        entity="transactions",
        run_id="run-001",
        business_date="2026-01-01",
    )
    assert path == (
        "s3a://n06-test-bucket/silver/ds-alpha/transactions/run_id=run-001/business_date=2026-01-01"
    )
    # Tầng nằm trong tier_prefix: đổi sang gold không thêm đoạn nào khác.
    gold = build_run_output_path(
        "gold",
        dataset_id="ds-alpha",
        entity="fact_transactions",
        run_id="run-001",
        business_date=date(2026, 1, 1),
    )
    assert gold == "gold/ds-alpha/fact_transactions/run_id=run-001/business_date=2026-01-01"


@pytest.mark.parametrize(
    "overrides",
    [
        {"dataset_id": "DS-alpha"},
        {"dataset_id": "ds"},
        {"entity": "fact-transactions"},
        {"entity": "a/b"},
        {"entity": ""},
        {"run_id": "run/1"},
        {"run_id": ""},
        {"run_id": "-run"},
        {"run_id": "run 1"},
        {"business_date": "2026-13-01"},
        {"tier_prefix": ""},
        {"tier_prefix": "/"},
    ],
)
def test_build_run_output_path_rejects_invalid_components(overrides: dict[str, str]) -> None:
    kwargs: dict[str, str] = {
        "tier_prefix": "silver",
        "dataset_id": "ds-alpha",
        "entity": "transactions",
        "run_id": "run-001",
        "business_date": "2026-01-01",
    }
    kwargs.update(overrides)
    tier_prefix = kwargs.pop("tier_prefix")
    with pytest.raises(ValueError):
        build_run_output_path(tier_prefix, **kwargs)  # type: ignore[arg-type]


def test_semantic_hash_is_independent_of_output_path() -> None:
    # Expected semantic hash tính độc lập theo chuẩn JSON canonical + SHA-256
    expected_payload = (
        '{"business_date":"2026-01-01","code_version":"abc123","config_hash":"cfg1",'
        '"dataset_id":"ds-alpha","entity":"transactions","input_versions":["b1","b2"],'
        '"layer":"silver","payload_digest":"sha256:deadbeef","rule_version":"1.0.0",'
        '"schema_version":"1.0.0"}'
    )
    expected_hash = hashlib.sha256(expected_payload.encode("utf-8")).hexdigest()

    def _hash(business_date: date | str) -> str:
        return compute_semantic_hash(
            dataset_id="ds-alpha",
            layer="silver",
            entity="transactions",
            business_date=business_date,
            input_versions=["b1", "b2"],
            config_hash="cfg1",
            schema_version="1.0.0",
            rule_version="1.0.0",
            code_version="abc123",
            payload_digest="sha256:deadbeef",
        )

    # Cùng input, cấu hình và phiên bản nhưng đường dẫn khác hẳn (run_id và tiền tố khác):
    # semantic hash phải giữ nguyên (đặc tả mục 8.1 bước 6: run_id/đường dẫn là dữ liệu vận hành).
    paths = [
        build_run_output_path(
            "s3a://n06-test-bucket/silver",
            dataset_id="ds-alpha",
            entity="transactions",
            run_id="run-001",
            business_date="2026-01-01",
        ),
        build_run_output_path(
            "s3a://other-bucket/test-silver",
            dataset_id="ds-alpha",
            entity="transactions",
            run_id="run-002",
            business_date=date(2026, 1, 1),
        ),
    ]
    assert paths[0] != paths[1]
    hashes = [_hash("2026-01-01"), _hash(date(2026, 1, 1))]
    assert hashes == [expected_hash, expected_hash]
    assert expected_hash != _hash("2026-01-02")  # business_date có trong hash, đường dẫn thì không


def test_committing_two_runs_with_different_paths_stores_the_same_semantic_hash() -> None:
    """Cùng input, cấu hình, phiên bản nhưng `run_id` và đường dẫn khác: hash lưu như nhau."""
    import inspect

    # Hash không nhận run_id hay đường dẫn làm đầu vào (đặc tả mục 8.1 bước 6).
    parameters = inspect.signature(compute_semantic_hash).parameters
    assert "run_id" not in parameters and "output_path" not in parameters

    expected_payload = (
        '{"business_date":"2026-01-01","code_version":"abc123","config_hash":"cfg1",'
        '"dataset_id":"ds-alpha","entity":"transactions","input_versions":["b1","b2"],'
        '"layer":"silver","payload_digest":"sha256:deadbeef","rule_version":"1.0.0",'
        '"schema_version":"1.0.0"}'
    )
    expected_hash = hashlib.sha256(expected_payload.encode("utf-8")).hexdigest()

    store = _InMemoryControlStore()
    coord = BatchCommitCoordinator(store)
    stored: list[tuple[str, str, str]] = []
    for run_id, tier_prefix in (
        ("run-001", "s3a://n06-test-bucket/silver"),
        ("run-002", "s3a://other-bucket/test-silver"),
    ):
        coord.start_run(
            run_id=run_id,
            dataset_id="ds-alpha",
            job_name="process_day",
            profile="test",
            business_date_from="2026-01-01",
            business_date_to="2026-01-01",
            input_versions=["b1", "b2"],
            config_hash="cfg1",
            schema_version="1.0.0",
            rule_version="1.0.0",
            code_version="abc123",
        )
        output_path = build_run_output_path(
            tier_prefix,
            dataset_id="ds-alpha",
            entity="transactions",
            run_id=run_id,
            business_date="2026-01-01",
        )
        coord.commit_run(
            run_id=run_id,
            dataset_id="ds-alpha",
            layer="silver",
            entity="transactions",
            business_date="2026-01-01",
            output_path=output_path,
            row_count=10,
            semantic_hash=compute_semantic_hash(
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
            ),
        )
        snapshot = coord.resolve_current_snapshot(
            dataset_id="ds-alpha", layer="silver", entity="transactions", business_date="2026-01-01"
        )
        assert snapshot is not None and snapshot.run_id == run_id
        assert snapshot.semantic_hash is not None
        stored.append((snapshot.run_id, snapshot.output_path, snapshot.semantic_hash))

    (run_a, path_a, hash_a), (run_b, path_b, hash_b) = stored
    assert run_a != run_b and path_a != path_b
    assert hash_a == hash_b == expected_hash


def test_require_unused_run_prefix_passes_when_run_has_no_object() -> None:
    seen: list[str] = []

    def lister(prefix: str) -> list[str]:
        seen.append(prefix)
        # Chỉ có object của run khác, kể cả run có tên bắt đầu bằng cùng chuỗi.
        other = "silver/ds-alpha/transactions/run_id=run-10/business_date=2026-01-01/part-0.parquet"
        keys = [other]
        return [k for k in keys if k.startswith(prefix)]

    require_unused_run_prefix(
        lister, "silver", dataset_id="ds-alpha", entity="transactions", run_id="run-1"
    )
    # Prefix liệt kê viết tay theo ADR-0002: kết thúc bằng "/" để run-1 không khớp run-10.
    assert seen == ["silver/ds-alpha/transactions/run_id=run-1/"]


def test_require_unused_run_prefix_rejects_run_that_already_has_objects() -> None:
    existing = "silver/ds-alpha/transactions/run_id=run-1/business_date=2026-01-01/part-0.parquet"

    def lister(prefix: str) -> list[str]:
        return [existing] if existing.startswith(prefix) else []

    with pytest.raises(CommitError, match="đã có object"):
        require_unused_run_prefix(
            lister, "silver", dataset_id="ds-alpha", entity="transactions", run_id="run-1"
        )


def test_require_unused_run_prefix_rejects_invalid_run_id_before_listing() -> None:
    def lister(prefix: str) -> list[str]:
        raise AssertionError("không được liệt kê khi tham số không hợp lệ")

    with pytest.raises(ValueError):
        require_unused_run_prefix(
            lister, "silver", dataset_id="ds-alpha", entity="transactions", run_id="../x"
        )


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


def test_commit_run_snapshots_atomic_never_mixes_runs_on_mid_commit_failure() -> None:
    store = _InMemoryControlStore()
    coord = BatchCommitCoordinator(store)
    for rid in ("run-1", "run-2"):
        coord.start_run(
            run_id=rid,
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

    coord.commit_run_snapshots(
        run_id="run-1",
        dataset_id="ds-1",
        snapshots=[
            SnapshotSpec("silver", "transactions", date(2026, 1, 1), "s/run_id=run-1/t"),
            SnapshotSpec("gold", "fact_transactions", date(2026, 1, 1), "g/run_id=run-1/ft"),
            SnapshotSpec("gold", "fact_reconciliation", date(2026, 1, 1), "g/run_id=run-1/fr"),
        ],
        row_count=10,
        semantic_hash="hash-1",
    )

    def _boom() -> None:
        raise RuntimeError("chết giữa chừng khi commit")

    with pytest.raises(RuntimeError, match="chết giữa chừng"):
        coord.commit_run_snapshots(
            run_id="run-2",
            dataset_id="ds-1",
            snapshots=[
                SnapshotSpec("silver", "transactions", date(2026, 1, 1), "s/run_id=run-2/t"),
                SnapshotSpec("gold", "fact_transactions", date(2026, 1, 1), "g/run_id=run-2/ft"),
                SnapshotSpec("gold", "fact_reconciliation", date(2026, 1, 1), "g/run_id=run-2/fr"),
            ],
            row_count=12,
            semantic_hash="hash-2",
            after_run_update_hook=_boom,
        )

    assert store.get_run("run-2").status == "RUNNING"
    for layer, entity in (
        ("silver", "transactions"),
        ("gold", "fact_transactions"),
        ("gold", "fact_reconciliation"),
    ):
        snap = coord.resolve_current_snapshot(
            dataset_id="ds-1", layer=layer, entity=entity, business_date="2026-01-01"
        )
        assert snap is not None and snap.run_id == "run-1"
