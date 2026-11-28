"""Integration tests cho thư viện commit batch trên PostgreSQL thật (TASK T24).

Kiểm chứng trực tiếp trên `n06_control` (Docker Compose profile `core`):
1. Run `RUNNING` và `FAILED` không bao giờ bị reader đọc; snapshot cũ giữ nguyên.
2. Transaction lỗi giữa chừng -> rollback toàn bộ, `current_snapshot` cũ giữ nguyên.
3. Chạy lại cùng `run_id` -> idempotent, không tạo dòng trùng.
4. Hai run đồng thời cho cùng `(dataset_id, layer, entity, business_date)` -> kết quả xác định,
   không lẫn `(run_id, output_path, manifest_path, semantic_hash)`.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import socket
import threading
import uuid
from datetime import date
from pathlib import Path

import pytest

from n06.cli import parse_dotenv_file
from n06.config import PostgresDbConfig, load_config
from n06.control import DEFAULT_CONTROL_MIGRATIONS_DIR, connect_control_db, migrate_up
from n06.storage import (
    BatchCommitCoordinator,
    CommitError,
    ControlStore,
    PsqlCliControlStore,
    SnapshotSpec,
    connect_control_store,
    make_compose_psql_runner,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ENV_FILE = REPO_ROOT / ".env"


def _load_control_db_config() -> PostgresDbConfig:
    dotenv_vars = parse_dotenv_file(ENV_FILE) if ENV_FILE.is_file() else {}
    env = {
        **dotenv_vars,
        **os.environ,
        "N06_PROFILE": dotenv_vars.get("N06_PROFILE") or os.environ.get("N06_PROFILE", "demo"),
        "N06_OBJECT_STORE_ACCESS_KEY": (
            os.environ.get("N06_OBJECT_STORE_ACCESS_KEY")
            or dotenv_vars.get("N06_OBJECT_STORE_ACCESS_KEY", "test_ak")
        ),
        "N06_OBJECT_STORE_SECRET_KEY": (
            os.environ.get("N06_OBJECT_STORE_SECRET_KEY")
            or dotenv_vars.get("N06_OBJECT_STORE_SECRET_KEY", "test_sk")
        ),
        "N06_POSTGRES_MART_PASSWORD": (
            os.environ.get("N06_POSTGRES_MART_PASSWORD")
            or dotenv_vars.get("N06_POSTGRES_MART_PASSWORD", "test_pw")
        ),
        "N06_POSTGRES_CONTROL_PASSWORD": (
            os.environ.get("N06_POSTGRES_CONTROL_PASSWORD")
            or dotenv_vars.get("N06_POSTGRES_CONTROL_PASSWORD", "test_pw")
        ),
        "N06_POSTGRES_AIRFLOW_PASSWORD": (
            os.environ.get("N06_POSTGRES_AIRFLOW_PASSWORD")
            or dotenv_vars.get("N06_POSTGRES_AIRFLOW_PASSWORD", "test_pw")
        ),
        "N06_POSTGRES_METABASE_PASSWORD": (
            os.environ.get("N06_POSTGRES_METABASE_PASSWORD")
            or dotenv_vars.get("N06_POSTGRES_METABASE_PASSWORD", "test_pw")
        ),
    }
    if "N06_POSTGRES_CONTROL_DB" in env:
        env.setdefault("N06__POSTGRES__CONTROL__DATABASE", env["N06_POSTGRES_CONTROL_DB"])
    if "N06_POSTGRES_CONTROL_USER" in env:
        env.setdefault("N06__POSTGRES__CONTROL__USER", env["N06_POSTGRES_CONTROL_USER"])
    env.setdefault("N06__POSTGRES__CONTROL__HOST", "localhost")
    env.setdefault("N06__POSTGRES__CONTROL__PORT", env.get("N06_POSTGRES_HOST_PORT", "15432"))
    return load_config(profile=env["N06_PROFILE"], env=env).postgres.control


def _control_store_and_sql_or_skip() -> tuple[ControlStore, PsqlCliControlStore | object]:
    cfg = _load_control_db_config()
    try:
        with socket.create_connection((cfg.host, cfg.port), timeout=1.5):
            pass
    except OSError as exc:
        pytest.skip(f"PostgreSQL control DB chưa sẵn sàng tại {cfg.host}:{cfg.port}: {exc}")

    # Đảm bảo schema control đã được migrate
    try:
        mig_exec = connect_control_db(cfg)
        migrate_up(mig_exec, DEFAULT_CONTROL_MIGRATIONS_DIR)
        return connect_control_store(cfg), mig_exec
    except Exception:
        docker_bin = shutil.which("docker")
        if docker_bin is None:
            pytest.skip("Không có psycopg/psql hoặc docker để kết nối Postgres control DB")
        runner = make_compose_psql_runner(
            cfg,
            docker_bin=docker_bin,
            compose_file=COMPOSE_FILE,
            repo_root=REPO_ROOT,
        )
        store = PsqlCliControlStore(cfg, psql_runner=runner)

        class _RunnerExec:
            def execute_in_transaction(self, statements: list[str]) -> None:
                body = "\n".join(s.strip() for s in statements if s.strip())
                if body:
                    runner(f"BEGIN;\n{body}\nCOMMIT;\n", False)

            def query_versions(self, sql: str) -> list[str]:
                out = runner(sql, True)
                return [line.strip() for line in out.splitlines() if line.strip()]

        mig_exec = _RunnerExec()
        migrate_up(mig_exec, DEFAULT_CONTROL_MIGRATIONS_DIR)
        return store, mig_exec


def test_commit_lifecycle_atomicity_rerun_and_concurrency() -> None:
    store, sql_exec = _control_store_and_sql_or_skip()
    coord = BatchCommitCoordinator(store)
    dataset_id = f"t24-it-{uuid.uuid4().hex[:8]}"
    biz_date = date(2026, 1, 15)

    run1 = f"run-1-{uuid.uuid4().hex[:8]}"
    run2 = f"run-2-{uuid.uuid4().hex[:8]}"
    run3 = f"run-3-{uuid.uuid4().hex[:8]}"
    run4 = f"run-4-{uuid.uuid4().hex[:8]}"

    # Đường dẫn viết tay theo ADR-0002 (mục 3): <silver_prefix>/<dataset_id>/<entity>/run_id=<id>/
    # business_date=<d>; manifest nằm ở gốc run. Không dùng build_run_output_path để tính expected.
    root1 = f"s3a://n06-lakehouse/silver/{dataset_id}/transactions/run_id={run1}"
    path1 = f"{root1}/business_date=2026-01-15"
    manifest1 = f"{root1}/_manifest.json"
    root2 = f"s3a://n06-lakehouse/silver/{dataset_id}/transactions/run_id={run2}"
    path2 = f"{root2}/business_date=2026-01-15"
    root3 = f"s3a://n06-lakehouse/silver/{dataset_id}/transactions/run_id={run3}"
    path3 = f"{root3}/business_date=2026-01-15"
    manifest3 = f"{root3}/_manifest.json"
    root4 = f"s3a://n06-lakehouse/silver/{dataset_id}/transactions/run_id={run4}"
    path4 = f"{root4}/business_date=2026-01-15"
    manifest4 = f"{root4}/_manifest.json"

    hash1 = hashlib.sha256(b"payload-1").hexdigest()
    hash3 = hashlib.sha256(b"payload-3").hexdigest()
    hash4 = hashlib.sha256(b"payload-4").hexdigest()

    try:
        # 1. Start run1 (RUNNING) -> reader chưa thấy gì
        coord.start_run(
            run_id=run1,
            dataset_id=dataset_id,
            job_name="process_day",
            profile="test",
            business_date_from=biz_date,
            business_date_to=biz_date,
            input_versions=["batch-1"],
            config_hash="cfg-1",
            schema_version="1.0.0",
            rule_version="1.0.0",
            code_version="dev",
        )
        assert (
            coord.resolve_current_snapshot(
                dataset_id=dataset_id,
                layer="silver",
                entity="transactions",
                business_date=biz_date,
            )
            is None
        )

        # Commit run1 thành công
        coord.commit_run(
            run_id=run1,
            dataset_id=dataset_id,
            layer="silver",
            entity="transactions",
            business_date=biz_date,
            output_path=path1,
            manifest_path=manifest1,
            row_count=100,
            semantic_hash=hash1,
        )
        snap1 = coord.resolve_current_snapshot(
            dataset_id=dataset_id, layer="silver", entity="transactions", business_date=biz_date
        )
        assert snap1 is not None
        assert snap1.run_id == run1
        assert snap1.output_path == path1

        # 2. Chạy lại cùng run1 (idempotent) -> không tạo dòng trùng
        coord.commit_run(
            run_id=run1,
            dataset_id=dataset_id,
            layer="silver",
            entity="transactions",
            business_date=biz_date,
            output_path=path1,
            manifest_path=manifest1,
            row_count=100,
            semantic_hash=hash1,
        )
        counts = sql_exec.query_versions(
            f"SELECT COUNT(*)::text FROM current_snapshot WHERE dataset_id = '{dataset_id}';"
        )
        assert counts == ["1"]

        # 3. Run2 lỗi giữa chừng (sau khi start_run, fail_run + commit lỗi) -> snapshot vẫn là run1
        coord.start_run(
            run_id=run2,
            dataset_id=dataset_id,
            job_name="process_day",
            profile="test",
            business_date_from=biz_date,
            business_date_to=biz_date,
            input_versions=["batch-2"],
            config_hash="cfg-1",
            schema_version="1.0.0",
            rule_version="1.0.0",
            code_version="dev",
        )

        def _abort_tx() -> None:
            raise RuntimeError("injected failure before snapshot pointer update")

        with pytest.raises((RuntimeError, CommitError)):
            coord.commit_run(
                run_id=run2,
                dataset_id=dataset_id,
                layer="silver",
                entity="transactions",
                business_date=biz_date,
                output_path=path2,
                row_count=200,
                semantic_hash=hash3,
                after_run_update_hook=_abort_tx,
            )

        coord.fail_run(run_id=run2, error_summary="simulated job crash")
        snap_after_fail = coord.resolve_current_snapshot(
            dataset_id=dataset_id, layer="silver", entity="transactions", business_date=biz_date
        )
        assert snap_after_fail is not None
        assert snap_after_fail.run_id == run1
        assert snap_after_fail.output_path == path1

        # 4. Hai run đồng thời (run3 và run4) cùng commit cho một phạm vi
        coord.start_run(
            run_id=run3,
            dataset_id=dataset_id,
            job_name="process_day",
            profile="test",
            business_date_from=biz_date,
            business_date_to=biz_date,
            input_versions=["batch-3"],
            config_hash="cfg-1",
            schema_version="1.0.0",
            rule_version="1.0.0",
            code_version="dev",
        )
        coord.start_run(
            run_id=run4,
            dataset_id=dataset_id,
            job_name="process_day",
            profile="test",
            business_date_from=biz_date,
            business_date_to=biz_date,
            input_versions=["batch-4"],
            config_hash="cfg-1",
            schema_version="1.0.0",
            rule_version="1.0.0",
            code_version="dev",
        )

        barrier = threading.Barrier(2)
        errors: list[Exception] = []

        def _commit_worker(rid: str, out_path: str, manifest: str, shash: str, rcount: int) -> None:
            try:
                barrier.wait(timeout=5)
                coord.commit_run(
                    run_id=rid,
                    dataset_id=dataset_id,
                    layer="silver",
                    entity="transactions",
                    business_date=biz_date,
                    output_path=out_path,
                    manifest_path=manifest,
                    row_count=rcount,
                    semantic_hash=shash,
                )
            except Exception as exc:  # pragma: no cover - chỉ dùng báo lỗi thread
                errors.append(exc)

        t3 = threading.Thread(target=_commit_worker, args=(run3, path3, manifest3, hash3, 300))
        t4 = threading.Thread(target=_commit_worker, args=(run4, path4, manifest4, hash4, 400))
        t3.start()
        t4.start()
        t3.join(timeout=15)
        t4.join(timeout=15)

        assert errors == []
        final_snap = coord.resolve_current_snapshot(
            dataset_id=dataset_id, layer="silver", entity="transactions", business_date=biz_date
        )
        assert final_snap is not None
        assert final_snap.run_id in (run3, run4)
        # Kiểm tra không trộn lẫn trường giữa hai run
        if final_snap.run_id == run3:
            assert final_snap.output_path == path3
            assert final_snap.manifest_path == manifest3
            assert final_snap.semantic_hash == hash3
        else:
            assert final_snap.output_path == path4
            assert final_snap.manifest_path == manifest4
            assert final_snap.semantic_hash == hash4
    finally:
        sql_exec.execute_in_transaction(
            [
                f"DELETE FROM current_snapshot WHERE dataset_id = '{dataset_id}';",
                f"DELETE FROM runs WHERE dataset_id = '{dataset_id}';",
            ]
        )


def test_commit_run_snapshots_atomic_both_stores_no_mixing() -> None:
    """Chết giữa chừng khi commit nhiều snapshot -> rollback toàn bộ, không trộn hai run."""
    cfg = _load_control_db_config()
    primary_store, sql_exec = _control_store_and_sql_or_skip()
    stores: list[ControlStore] = [primary_store]
    docker_bin = shutil.which("docker")
    if docker_bin is not None and not isinstance(primary_store, PsqlCliControlStore):
        runner = make_compose_psql_runner(
            cfg,
            docker_bin=docker_bin,
            compose_file=COMPOSE_FILE,
            repo_root=REPO_ROOT,
        )
        stores.append(PsqlCliControlStore(cfg, psql_runner=runner))

    for idx, store in enumerate(stores):
        dataset_id = f"it-atomic-{idx}-{uuid.uuid4().hex[:8]}"
        biz_date = date(2026, 1, 1)
        run1 = f"run-a1-{uuid.uuid4().hex[:8]}"
        run2 = f"run-a2-{uuid.uuid4().hex[:8]}"
        coord = BatchCommitCoordinator(store)
        try:
            for rid in (run1, run2):
                coord.start_run(
                    run_id=rid,
                    dataset_id=dataset_id,
                    job_name="process_day",
                    profile="test",
                    business_date_from=biz_date,
                    business_date_to=biz_date,
                    input_versions=["b1"],
                    config_hash="cfg-1",
                    schema_version="1.0.0",
                    rule_version="1.0.0",
                    code_version="v1",
                )
            snaps1 = [
                SnapshotSpec(
                    "silver",
                    "transactions",
                    biz_date,
                    f"s3a://b/silver/{dataset_id}/transactions/run_id={run1}/business_date=2026-01-01",
                ),
                SnapshotSpec(
                    "gold",
                    "fact_transactions",
                    biz_date,
                    f"s3a://b/gold/{dataset_id}/fact_transactions/run_id={run1}/business_date=2026-01-01",
                ),
                SnapshotSpec(
                    "gold",
                    "fact_reconciliation",
                    biz_date,
                    f"s3a://b/gold/{dataset_id}/fact_reconciliation/run_id={run1}/business_date=2026-01-01",
                ),
            ]
            coord.commit_run_snapshots(
                run_id=run1,
                dataset_id=dataset_id,
                snapshots=snaps1,
                row_count=10,
                semantic_hash="hash-1",
            )

            # Mô phỏng lỗi ngay giữa transaction (sau UPDATE runs, trong lúc ghi snapshot).
            bad_snaps = [
                SnapshotSpec(
                    "silver",
                    "transactions",
                    biz_date,
                    f"s3a://b/silver/{dataset_id}/transactions/run_id={run2}/business_date=2026-01-01",
                ),
                SnapshotSpec(
                    "gold",
                    None,  # type: ignore[arg-type]  # vi phạm NOT NULL ở snapshot thứ 2
                    biz_date,
                    f"s3a://b/gold/{dataset_id}/fact_transactions/run_id={run2}/business_date=2026-01-01",
                ),
            ]
            with pytest.raises(CommitError):
                coord.commit_run_snapshots(
                    run_id=run2,
                    dataset_id=dataset_id,
                    snapshots=bad_snaps,
                    row_count=20,
                    semantic_hash="hash-2",
                )

            run2_rec = store.get_run(run2)
            assert run2_rec is not None and run2_rec.status == "RUNNING"
            for layer, entity in (
                ("silver", "transactions"),
                ("gold", "fact_transactions"),
                ("gold", "fact_reconciliation"),
            ):
                cur = coord.resolve_current_snapshot(
                    dataset_id=dataset_id,
                    layer=layer,
                    entity=entity,
                    business_date=biz_date,
                )
                assert cur is not None and cur.run_id == run1
        finally:
            sql_exec.execute_in_transaction(
                [
                    f"DELETE FROM current_snapshot WHERE dataset_id = '{dataset_id}';",
                    f"DELETE FROM runs WHERE dataset_id = '{dataset_id}';",
                ]
            )
