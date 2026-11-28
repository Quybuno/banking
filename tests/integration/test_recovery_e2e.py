"""Kiểm thử phục hồi E2E (TASK T50, đặc tả I.8.1 & I.13).

Kịch bản trên Kafka, MinIO, PostgreSQL và Spark thật:
1. Producer xác nhận toàn bộ event (`n06 publish`).
2. Streaming (`n06 stream --available-now`):
   - Tắt đột ngột (`SIGKILL`) ở `after_raw` (đang ghi Parquet: Raw xong, Bronze chưa) tại batch 1.
   - Chạy lại, tắt đột ngột (`SIGKILL`) ở `after_upsert` (trước khi commit checkpoint) tại batch 1.
   - Chạy lại tới hết -> `wait-ingestion` xác nhận không mất bản ghi nào, không trùng `batch_id`.
3. Batch (`n06 process-day`):
   - Chết sau khi ghi Parquet (`--fail-at after_write`), trước khi commit `current_snapshot`:
     run `FAILED`, `current_snapshot` không bị chạm tới.
   - Chạy lại thành công 2 lần liên tiếp -> cùng `semantic_hash`, Gold (`fact_transactions`,
     `fact_daily_balances`, `fact_reconciliation`) không nhân đôi và khớp toàn bộ bản ghi nguồn.
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
from typing import Any

import pytest
from test_process_day_clean import REPO_ROOT, Stack, _result, build_stack

from n06.cli import main
from n06.cli.statements import run_ingest_statements

OVERRIDES = {
    "transaction_count": 60,
    "from_date": "2026-01-01",
    "to_date": "2026-01-02",
    "max_offsets": "50",
}
AS_OF_LATE = "2099-01-01T00:00:00Z"


def _gold_counts(stack: Stack, run_id: str, dates: list[str], tmp: Path) -> dict[str, Any]:
    frames = {
        "fact_transactions": {
            "kind": "ids",
            "key": "transaction_id",
            "paths": [
                f"s3a://{stack.bucket}/{stack.cfg.paths.gold_prefix}/{stack.dataset_id}/"
                f"fact_transactions/run_id={run_id}/business_date={d}"
                for d in dates
            ],
        },
        "fact_reconciliation": {
            "kind": "ids",
            "key": "external_reference",
            "paths": [
                f"s3a://{stack.bucket}/{stack.cfg.paths.gold_prefix}/{stack.dataset_id}/"
                f"fact_reconciliation/run_id={run_id}/business_date={d}"
                for d in dates
            ],
        },
    }
    return stack.spark({"action": "read", "frames": frames}, tmp)


def test_t50_stream_and_batch_crash_recovery_no_loss_no_gold_duplicates(
    tmp_path_factory: pytest.TempPathFactory,
    tmp_path: Path,
) -> None:
    with contextlib.contextmanager(build_stack)(
        tmp_path_factory, "generator.demo-clean.json", dict(OVERRIDES)
    ) as stack:
        # 1. Producer xác nhận toàn bộ event.
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("N06__KAFKA__NUM_PARTITIONS", "3")
            out = io.StringIO()
            code = main(
                [
                    "publish",
                    "--profile",
                    "test",
                    "--source-dir",
                    str(stack.source),
                    "--topic",
                    stack.topic,
                    "--json",
                ],
                out=out,
            )
            assert code == 0, out.getvalue()

        # 2a. Chết lúc đang ghi Parquet (`after_raw`, batch 1).
        c1 = stack.job(
            "n06.ingestion.stream_job",
            ["--available-now", "--crash-at", "after_raw", "--crash-batch", "1"],
            timeout=400,
        )
        assert c1.returncode != 0
        assert "after_raw, batch_id=1" in c1.stdout, c1.stderr[-2000:]
        b1 = stack.store.list_committed_ingest_batches(query_name=stack.query)
        assert [b.batch_id for b in b1] == [0]

        # 2b. Chạy lại, chết lúc commit checkpoint (`after_upsert`, batch 1).
        c2 = stack.job(
            "n06.ingestion.stream_job",
            ["--available-now", "--crash-at", "after_upsert", "--crash-batch", "1"],
            timeout=400,
        )
        assert c2.returncode != 0
        assert "after_upsert, batch_id=1" in c2.stdout, c2.stderr[-2000:]
        b2 = stack.store.list_committed_ingest_batches(query_name=stack.query)
        assert [b.batch_id for b in b2] == [0, 1]

        # 2c. Chạy lại tới hết.
        c3 = stack.job("n06.ingestion.stream_job", ["--available-now"], timeout=400)
        assert c3.returncode == 0, c3.stdout[-3000:] + c3.stderr[-3000:]
        b3 = stack.store.list_committed_ingest_batches(query_name=stack.query)
        assert len({b.batch_id for b in b3}) == len(b3) >= 3

        # Xác nhận không mất bản ghi đã được producer xác nhận.
        wait_out = io.StringIO()
        wait_code = main(
            [
                "wait-ingestion",
                "--profile",
                "demo",
                "--dataset-id",
                stack.dataset_id,
                "--source-dir",
                str(stack.source),
                "--query-name",
                stack.query,
                "--timeout-seconds",
                "300",
            ],
            out=wait_out,
        )
        assert wait_code == 0, wait_out.getvalue()

        stmt_out = io.StringIO()
        stmt_code = run_ingest_statements(
            source_dir=stack.source,
            as_of_time=AS_OF_LATE,
            dataset_id=stack.dataset_id,
            profile="test",
            env_file=REPO_ROOT / ".env",
            env=stack.env,
            as_json=True,
            out=stmt_out,
        )
        assert stmt_code == 0, stmt_out.getvalue()

        # 3a. Kill/fail batch job sau khi ghi Parquet (`--fail-at after_write`), trước commit.
        failed = stack.process_day("--fail-at", "after_write", as_of=AS_OF_LATE)
        assert failed.returncode != 0
        assert stack.store.list_current_snapshots(dataset_id=stack.dataset_id, layer="gold") == []

        # 3b. Chạy lại batch job 2 lần liên tiếp -> cùng semantic_hash, Gold không nhân đôi.
        ok1 = stack.process_day(as_of=AS_OF_LATE)
        assert ok1.returncode == 0, ok1.stdout[-3000:] + ok1.stderr[-3000:]
        res1 = _result(ok1, "N06_PROCESS_DAY_RESULT=")

        ok2 = stack.process_day(as_of=AS_OF_LATE)
        assert ok2.returncode == 0, ok2.stdout[-3000:] + ok2.stderr[-3000:]
        res2 = _result(ok2, "N06_PROCESS_DAY_RESULT=")

        assert res1["run_id"] != res2["run_id"]
        assert res1["semantic_hash"] == res2["semantic_hash"]
        assert res2["quality"]["control_mismatches"] == 0

        counts = _gold_counts(stack, res2["run_id"], res2["business_dates"], tmp_path)
        assert counts["fact_transactions"]["rows"] == counts["fact_transactions"]["distinct"] == 60
        assert (
            counts["fact_reconciliation"]["rows"] == counts["fact_reconciliation"]["distinct"] == 60
        )
        assert json.loads(json.dumps(res2["semantic_hash"])) == res1["semantic_hash"]
