"""Integration test: kịch bản nghiệm thu mẫu với `n06 backfill` (TASK T42)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from n06.cli import main
from n06.cli.init_local import parse_dotenv_file
from n06.config import REPO_ROOT, load_config
from n06.storage.object_store import ensure_bucket, target_from_config


def _query_mart_counts(dataset_id: str) -> tuple[int, int]:
    """Đọc số dòng `fact_transactions` và `fact_reconciliation` từ PostgreSQL mart."""
    sql = (
        f"SELECT (SELECT COUNT(*) FROM public.fact_transactions "
        f"WHERE dataset_id = '{dataset_id}'), "
        "(SELECT COUNT(*) FROM public.fact_reconciliation "
        f"WHERE dataset_id = '{dataset_id}');"
    )
    proc = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "n06-postgres-1",
            "psql",
            "-U",
            "n06_mart_user",
            "-d",
            "n06_mart",
            "-tA",
            "-F",
            ",",
            "-c",
            sql,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    parts = proc.stdout.strip().split(",")
    return int(parts[0]), int(parts[1])


def test_backfill_end_to_end_and_idempotent_rerun(tmp_path: Path) -> None:
    env_path = REPO_ROOT / ".env"
    merged_env = parse_dotenv_file(env_path) if env_path.is_file() else {}
    cfg = load_config(profile="default", env=merged_env)
    target = target_from_config(cfg, merged_env)
    assert ensure_bucket(target, cfg.object_store.bucket).ready

    # Sinh dataset sạch quy mô nhỏ (50 giao dịch) từ cấu hình demo-clean
    base_cfg_path = (
        REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / "config" / "generator.demo-clean.json"
    )
    raw = json.loads(base_cfg_path.read_text(encoding="utf-8"))
    dataset_id = "t42-backfill-smoke"
    raw["dataset_id"] = dataset_id
    raw["transaction_count"] = 50
    raw["account_count"] = 20
    gcfg = tmp_path / "gen.json"
    gcfg.write_text(json.dumps(raw), encoding="utf-8")
    out_dir = tmp_path / "data"
    source_dir = out_dir / "source"

    assert main(["generate", "--generator-config", str(gcfg), "--out", str(out_dir)]) == 0

    topic = "n06.t42.backfill.smoke.events"
    qname = "t42_backfill_smoke"
    assert (
        main(
            [
                "publish",
                "--profile",
                "test",
                "--source-dir",
                str(source_dir),
                "--topic",
                topic,
                "--allow-non-empty-topic",
            ]
        )
        == 0
    )

    from n06.cli.ingestion import run_stream, run_wait_ingestion

    stream_env = {
        **merged_env,
        "N06__KAFKA__EVENTS_TOPIC": topic,
        "N06__KAFKA__QUERY_NAME": qname,
    }
    assert (
        run_stream(
            profile="default",
            env_file=env_path,
            query_name=qname,
            available_now=True,
            env=stream_env,
        )
        == 0
    )
    assert (
        run_wait_ingestion(
            dataset_id=dataset_id,
            profile="default",
            env_file=env_path,
            source_dir=source_dir,
            query_name=qname,
            env=stream_env,
        )
        == 0
    )
    assert (
        main(
            [
                "ingest-statements",
                "--profile",
                "default",
                "--source-dir",
                str(source_dir),
                "--dataset-id",
                dataset_id,
                "--as-of-time",
                "2026-01-05T00:00:00Z",
            ]
        )
        == 0
    )

    # Lần 1: n06 backfill
    assert (
        main(
            [
                "backfill",
                "--profile",
                "default",
                "--dataset-id",
                dataset_id,
                "--from-date",
                "2026-01-01",
                "--to-date",
                "2026-01-03",
                "--as-of-time",
                "2026-01-05T00:00:00Z",
                "--query-name",
                qname,
            ]
        )
        == 0
    )
    tx1, recon1 = _query_mart_counts(dataset_id)
    assert tx1 == 50
    assert recon1 == 50

    # Lần 2: chạy lại cùng input/as_of phải giữ nguyên kết quả
    assert (
        main(
            [
                "backfill",
                "--profile",
                "default",
                "--dataset-id",
                dataset_id,
                "--from-date",
                "2026-01-01",
                "--to-date",
                "2026-01-03",
                "--as-of-time",
                "2026-01-05T00:00:00Z",
                "--query-name",
                qname,
            ]
        )
        == 0
    )
    tx2, recon2 = _query_mart_counts(dataset_id)
    assert (tx2, recon2) == (tx1, recon1)
