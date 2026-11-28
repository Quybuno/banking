"""Kiểm thử nghiệm thu M3 (TASK T34): 3 snapshot trên bộ acceptance theo quy trình II.7.

Chuỗi thật: dựng bản sao tạm theo bố cục generator (II.7) -> `n06 publish`
-> `stream --available-now` -> `wait-ingestion` -> từng mốc `as_of_time`
(`early`, `after_cutoff`, `late_corrected`): `ingest-statements --as-of-time`
-> `process-day` 2 lần -> kiểm semantic_hash không đổi
-> `n06 validate --against-ground-truth` PASS -> kiểm status/amount/currency/anchor/tuổi
& control report 7 dòng.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import shutil
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient
from test_process_day_clean import REPO_ROOT, Stack, _result

from n06.cli import main
from n06.cli.init_local import parse_dotenv_file
from n06.cli.object_store import ensure_bucket, target_from_config
from n06.cli.statements import run_ingest_statements
from n06.config import load_config
from n06.reconciliation import ReconStatus
from n06.storage import PsqlCliControlStore, make_compose_psql_runner

COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ACCEPTANCE_DIR = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / "fixtures" / "acceptance"
DATASET_ID = "n06-acceptance-v1"

SNAPSHOTS: tuple[tuple[str, str], ...] = (
    ("early", "2026-01-02T04:00:00Z"),
    ("after_cutoff", "2026-01-02T18:00:00Z"),
    ("late_corrected", "2026-01-03T01:00:00Z"),
)


def _sha256_and_rows(path: Path) -> tuple[str, int]:
    raw = path.read_bytes()
    rows = len([line for line in raw.splitlines() if line.strip()])
    return hashlib.sha256(raw).hexdigest(), rows


def _stage_acceptance_source(target_dir: Path) -> Path:
    """Dựng bản sao tạm của fixture acceptance theo bố cục đầu ra generator (II.7)."""
    source = target_dir / "source"
    (source / "events").mkdir(parents=True, exist_ok=True)
    (source / "statements").mkdir(parents=True, exist_ok=True)
    (source / "manifests").mkdir(parents=True, exist_ok=True)

    shutil.copyfile(ACCEPTANCE_DIR / "accounts.jsonl", source / "accounts.jsonl")
    shutil.copyfile(ACCEPTANCE_DIR / "control_report.json", source / "control_report.json")
    shutil.copyfile(ACCEPTANCE_DIR / "events.jsonl", source / "events" / "events_00000.jsonl")

    rel_files: list[str] = [
        "accounts.jsonl",
        "control_report.json",
        "events/events_00000.jsonl",
    ]
    for batch_path in sorted(ACCEPTANCE_DIR.glob("statement_batch_*.jsonl")):
        dest = source / "statements" / batch_path.name
        shutil.copyfile(batch_path, dest)
        rel_files.append(f"statements/{batch_path.name}")

    deliveries_out: list[str] = []
    for line in (ACCEPTANCE_DIR / "statement_deliveries.jsonl").read_text("utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        rec["file_path"] = f"statements/{Path(rec['file_path']).name}"
        deliveries_out.append(json.dumps(rec, separators=(",", ":"), sort_keys=True))
    (source / "statement_deliveries.jsonl").write_text(
        "\n".join(deliveries_out) + "\n", encoding="utf-8"
    )

    orig_manifest = json.loads((ACCEPTANCE_DIR / "source_manifest.json").read_text("utf-8"))
    base_files = [
        {
            "path": rel,
            "sha256": _sha256_and_rows(source / rel)[0],
            "physical_rows": _sha256_and_rows(source / rel)[1],
        }
        for rel in ("accounts.jsonl", "control_report.json", "events/events_00000.jsonl")
    ]
    base_manifest = {
        "schema_version": orig_manifest["schema_version"],
        "dataset_id": DATASET_ID,
        "source_batch_id": orig_manifest["source_batch_id"],
        "files": base_files,
    }
    (source / "manifests" / "manifest_00000.json").write_text(
        json.dumps(base_manifest, indent=2) + "\n", encoding="utf-8"
    )
    stmt_files = [rel for rel in rel_files if rel.startswith("statements/")]
    for idx, stmt_rel in enumerate(stmt_files, start=1):
        sha, rows = _sha256_and_rows(source / stmt_rel)
        stmt_manifest = {
            "schema_version": orig_manifest["schema_version"],
            "dataset_id": DATASET_ID,
            "source_batch_id": orig_manifest["source_batch_id"],
            "files": [{"path": stmt_rel, "sha256": sha, "physical_rows": rows}],
        }
        (source / "manifests" / f"manifest_{idx:05d}.json").write_text(
            json.dumps(stmt_manifest, indent=2) + "\n", encoding="utf-8"
        )
    return source


def _cleanup_dataset(
    stack: Stack,
    runner: Any,
    tmp_path_factory: pytest.TempPathFactory,
    *,
    delete_topic: bool,
) -> None:
    store_sql = (
        f"DELETE FROM quality_metrics WHERE run_id IN "
        f"(SELECT run_id FROM runs WHERE dataset_id = '{DATASET_ID}');"
        f"DELETE FROM current_snapshot WHERE dataset_id = '{DATASET_ID}';"
        f"DELETE FROM runs WHERE dataset_id = '{DATASET_ID}';"
        f"DELETE FROM ingest_batches WHERE query_name = '{stack.query}';"
    )
    runner(store_sql, False)
    uris = [
        f"s3a://{stack.bucket}/{prefix}"
        for prefix in (
            f"{stack.cfg.paths.raw_prefix}/{stack.query}/",
            f"{stack.cfg.paths.bronze_prefix}/{stack.query}/",
            f"{stack.cfg.paths.checkpoints_prefix}/{stack.query}/",
            f"{stack.cfg.paths.silver_prefix}/{DATASET_ID}/",
            f"{stack.cfg.paths.gold_prefix}/{DATASET_ID}/",
            f"{stack.cfg.paths.statements_prefix}/{DATASET_ID}/",
        )
    ]
    scratch = tmp_path_factory.mktemp("acc-clean")
    stack.spark({"action": "delete", "uris": uris}, scratch)
    if delete_topic:
        admin = AdminClient({"bootstrap.servers": stack.cfg.kafka.bootstrap_servers})
        with contextlib.suppress(Exception):
            for future in admin.delete_topics([stack.topic]).values():
                future.result(timeout=30)


@pytest.fixture(scope="module")
def acceptance_stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    docker = shutil.which("docker")
    assert docker is not None, "Cần Docker CLI để chạy nghiệm thu T34."
    dotenv = parse_dotenv_file(REPO_ROOT / ".env")
    suffix = uuid.uuid4().hex[:10]
    topic, query = f"n06.test.acc.{suffix}", f"it-acc-{suffix}"
    host_env = {**dotenv, **os.environ}
    test_cfg = load_config(profile="test", env=host_env)
    bucket = test_cfg.object_store.bucket
    target = target_from_config(test_cfg, host_env)
    assert ensure_bucket(target, bucket).ready
    demo_prefixes = load_config(profile="demo", env=host_env).paths
    env = {
        **host_env,
        "N06__KAFKA__EVENTS_TOPIC": topic,
        "N06__KAFKA__QUERY_NAME": query,
        "N06__KAFKA__MAX_OFFSETS_PER_TRIGGER": "100",
        "N06__OBJECT_STORE__BUCKET": bucket,
        "N06__PATHS__STATEMENTS_PREFIX": demo_prefixes.statements_prefix,
        "N06__SPARK__SHUFFLE_PARTITIONS": "4",
        "N06__SPARK__EVENT_LOG_ENABLED": "false",
    }
    cfg = load_config(profile="demo", env=env)
    runner = make_compose_psql_runner(
        cfg.postgres.control, docker_bin=docker, compose_file=COMPOSE_FILE, repo_root=REPO_ROOT
    )
    store = PsqlCliControlStore(cfg.postgres.control, psql_runner=runner)
    source = _stage_acceptance_source(tmp_path_factory.mktemp("acc-src"))
    stack = Stack(docker, env, cfg, topic, query, DATASET_ID, bucket, store, runner, target, source)
    _cleanup_dataset(stack, runner, tmp_path_factory, delete_topic=False)

    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("N06__KAFKA__NUM_PARTITIONS", "3")
            out = io.StringIO()
            code = main(
                [
                    "publish",
                    "--profile",
                    "test",
                    "--source-dir",
                    str(source),
                    "--topic",
                    topic,
                    "--json",
                ],
                out=out,
            )
            assert code == 0, out.getvalue()

        streamed = stack.job("n06.ingestion.stream_job", ["--available-now"], timeout=400)
        assert streamed.returncode == 0, streamed.stdout[-3000:] + streamed.stderr[-3000:]

        out = io.StringIO()
        code = main(
            [
                "wait-ingestion",
                "--profile",
                "demo",
                "--dataset-id",
                DATASET_ID,
                "--source-dir",
                str(source),
                "--query-name",
                query,
                "--timeout-seconds",
                "300",
            ],
            out=out,
        )
        assert code == 0, out.getvalue()

        yield stack
    finally:
        _cleanup_dataset(stack, runner, tmp_path_factory, delete_topic=True)


def _snapshot_hashes(store: PsqlCliControlStore) -> dict[tuple[str, str], str]:
    snaps = store.list_current_snapshots(dataset_id=DATASET_ID, layer="gold")
    return {(s.entity, s.business_date.isoformat()): s.semantic_hash for s in snaps}


def test_t34_three_acceptance_snapshots_end_to_end(acceptance_stack: Stack, tmp_path: Path) -> None:
    expected_doc = json.loads((ACCEPTANCE_DIR / "expected_results.json").read_text("utf-8"))
    expected_by_name = {s["name"]: s for s in expected_doc["snapshots"]}

    for label, as_of_str in SNAPSHOTS:
        # II.7: chỉ công bố các lô statement có file_available_at <= as_of_time.
        out = io.StringIO()
        code = run_ingest_statements(
            source_dir=acceptance_stack.source,
            as_of_time=as_of_str,
            dataset_id=DATASET_ID,
            profile="test",
            env_file=REPO_ROOT / ".env",
            env=acceptance_stack.env,
            as_json=True,
            out=out,
        )
        assert code == 0, out.getvalue()

        # Mỗi snapshot chạy 2 lần độc lập từ đầu; semantic_hash của Gold phải giống hệt nhau.
        proc1 = acceptance_stack.process_day(as_of=as_of_str)
        assert proc1.returncode == 0, proc1.stdout[-4000:] + proc1.stderr[-4000:]
        res1 = _result(proc1, "N06_PROCESS_DAY_RESULT=")
        hashes1 = _snapshot_hashes(acceptance_stack.store)

        proc2 = acceptance_stack.process_day(as_of=as_of_str)
        assert proc2.returncode == 0, proc2.stdout[-4000:] + proc2.stderr[-4000:]
        res2 = _result(proc2, "N06_PROCESS_DAY_RESULT=")
        hashes2 = _snapshot_hashes(acceptance_stack.store)

        assert res1["run_id"] != res2["run_id"]
        assert hashes1 == hashes2 and hashes1, (
            f"Snapshot {label} đổi semantic_hash khi chạy lại: {hashes1} vs {hashes2}"
        )

        # Control report 7 dòng khớp qua pipeline, không bản ghi bị loại.
        assert res2["quality"]["control_groups_checked"] == 7
        assert res2["quality"]["control_mismatches"] == 0
        assert res2["quality"]["rejected_records"] == 0

        # Đọc fact_reconciliation thực tế từ MinIO cho run2.
        paths = [
            f"s3a://{acceptance_stack.bucket}/{acceptance_stack.cfg.paths.gold_prefix}/{DATASET_ID}/"
            f"fact_reconciliation/run_id={res2['run_id']}/business_date={day}"
            for day in res2["business_dates"]
        ]
        inspect_dir = tmp_path / f"inspect_{label}"
        inspect_dir.mkdir(parents=True, exist_ok=True)
        rows = acceptance_stack.spark(
            {"action": "read", "frames": {"r": {"kind": "reconciliation_full", "paths": paths}}},
            inspect_dir,
        )["r"]

        actual_file = tmp_path / f"actual_{label}.json"
        actual_file.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

        # 1) `n06 validate --against-ground-truth` PASS (status, anchor, age).
        val_out = io.StringIO()
        val_code = main(
            [
                "validate",
                "--dataset-id",
                DATASET_ID,
                "--against-ground-truth",
                "--as-of-time",
                as_of_str,
                "--fixture-dir",
                str(ACCEPTANCE_DIR),
                "--actual-file",
                str(actual_file),
                "--harness-module",
                "tests.harness.oracle",
                "--json",
            ],
            out=val_out,
        )
        val_payload = json.loads(val_out.getvalue())
        assert val_code == 0, f"n06 validate FAIL cho {label}: {val_payload}"
        assert val_payload["passed"] is True and val_payload["mismatches"] == []

        # 2) So từng khóa với expected_results.json + kiểm amount/currency/status & tuổi (REF-T006).
        exp_snap = expected_by_name[label]
        by_ref = {r["external_reference"]: r for r in rows}
        assert len(by_ref) == expected_doc["counts"]["match_keys"]
        assert {ref: r["recon_status"] for ref, r in by_ref.items()} == exp_snap[
            "expected_status_by_reference"
        ]
        assert {k: v for k, v in res2["reconciliation"]["status_counts"].items() if v} == exp_snap[
            "expected_counts"
        ]

        assert by_ref["REF-T003"]["internal_amount_minor"] == 300
        assert by_ref["REF-T003"]["statement_amount_minor"] == 301
        assert by_ref["REF-T004"]["internal_currency"] == "VND"
        assert by_ref["REF-T004"]["statement_currency"] == "USD"
        assert by_ref["REF-T005"]["internal_status"] == "SUCCESS"
        assert by_ref["REF-T005"]["statement_status"] == "FAILED"
        assert by_ref["REF-X012"]["internal_amount_minor"] is None
        assert by_ref["REF-X012"]["statement_amount_minor"] == 1200
        assert by_ref["REF-X012"]["statement_currency"] == "VND"
        if label == "early":
            assert by_ref["REF-T006"]["recon_status"] == ReconStatus.PENDING.value
            assert by_ref["REF-T006"]["anchor_event_time"] == "2026-01-01T01:00:06Z"
            assert by_ref["REF-T006"]["discrepancy_age_seconds"] == 97194
