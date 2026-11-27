"""Unit test cho `n06 ingest-statements` (T23, ADR-0002 mục 4, đặc tả II.4 & II.7)."""

from __future__ import annotations

import hashlib
import io
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from tests.unit.storage.test_object_store_objects import fake_object_s3

from n06.cli import main
from n06.cli.statements import run_ingest_statements
from n06.config import load_config
from n06.ingestion.statements import (
    StatementIngestConfigError,
    build_receipt_payload,
    build_statement_keys,
    discover_statement_deliveries,
    ingest_statements,
)
from n06.storage.object_store import ObjectIntegrityError, ObjectStoreTarget

SECRETS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "AKIATESTKEYEXAMPLE",
    "N06_OBJECT_STORE_SECRET_KEY": "s3cr3t-value",
    "N06_POSTGRES_MART_PASSWORD": "pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
    "N06_POSTGRES_METABASE_PASSWORD": "pw",
}


def _write_statement_source(
    root: Path,
    *,
    dataset_id: str = "demo-tx-v1",
    batches: list[tuple[int, str, str, int, bytes]],
) -> Path:
    """Tạo thư mục `source/` tối thiểu gồm statements/, manifests/, statement_deliveries.jsonl."""
    source = root / "source"
    (source / "manifests").mkdir(parents=True, exist_ok=True)
    delivery_lines: list[str] = []

    for order, channel, file_avail, window, data in batches:
        rel_data = f"statements/{channel}/statements_w{window:09d}.jsonl"
        data_path = source / rel_data
        data_path.parent.mkdir(parents=True, exist_ok=True)
        data_path.write_bytes(data)

        sha = hashlib.sha256(data).hexdigest()
        rows = data.count(b"\n")
        manifest = {
            "schema_version": "1.0.0",
            "dataset_id": dataset_id,
            "source_batch_id": str(
                uuid.uuid5(uuid.NAMESPACE_DNS, f"{dataset_id}:{channel}:{window}")
            ),
            "files": [{"path": rel_data, "sha256": sha, "physical_rows": rows}],
        }
        rel_manifest = f"manifests/statements_{channel}_w{window:09d}.manifest.json"
        (source / rel_manifest).write_text(json.dumps(manifest) + "\n", encoding="utf-8")

        for idx in range(max(1, rows)):
            delivery = {
                "schema_version": "1.0.0",
                "dataset_id": dataset_id,
                "statement_record_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{order}:{idx}")),
                "file_path": rel_data,
                "file_available_at": file_avail,
                "delivery_order": order,
            }
            delivery_lines.append(json.dumps(delivery))

    (source / "statement_deliveries.jsonl").write_text(
        "\n".join(delivery_lines) + "\n", encoding="utf-8"
    )
    return source


def test_ingest_respects_simulated_clock_upload_order_and_immutability(tmp_path: Path) -> None:
    source = _write_statement_source(
        tmp_path,
        batches=[
            (0, "SIM_CHANNEL_A", "2026-01-01T10:00:00Z", 1, b'{"id":1}\n'),
            (1, "SIM_CHANNEL_B", "2026-01-02T12:00:00Z", 2, b'{"id":2}\n{"id":3}\n'),
            (2, "SIM_CHANNEL_A", "2026-01-03T00:00:00Z", 3, b'{"id":4}\n'),
        ],
    )
    cfg = load_config(profile="test", env=SECRETS)

    with fake_object_s3() as server:
        target = ObjectStoreTarget(
            "http", "127.0.0.1", server.server_address[1], "us-east-1", "AKIA", "SECRET"
        )
        # Mốc sớm: chỉ batch 0 (10:00Z) đủ điều kiện; batch 1 và 2 chưa được phép xuất hiện.
        res1 = ingest_statements(
            cfg, target, source, as_of_time=datetime(2026, 1, 2, 4, 0, 0, tzinfo=UTC)
        )
        assert (
            res1.total_batches,
            res1.eligible_batches,
            res1.uploaded_batches,
            res1.deferred_batches,
        ) == (3, 1, 1, 2)
        put_paths = [path for method, path, _, _ in server.requests if method == "PUT"]
        prefix = f"/{cfg.object_store.bucket}/{cfg.paths.statements_prefix}/demo-tx-v1"
        # Thứ tự bắt buộc: file dữ liệu -> manifest -> biên nhận _ingest/000000000.json.
        assert put_paths == [
            f"{prefix}/statements/SIM_CHANNEL_A/statements_w000000001.jsonl",
            f"{prefix}/manifests/statements_SIM_CHANNEL_A_w000000001.manifest.json",
            f"{prefix}/_ingest/000000000.json",
        ]
        # Tuyệt đối không tải statement_deliveries.jsonl hay lô tương lai.
        assert all("statement_deliveries" not in k for k in server.objects)
        assert f"{prefix}/_ingest/000000001.json" not in server.objects

        # Kiểm nội dung biên nhận.
        receipt_raw, _ = server.objects[f"{prefix}/_ingest/000000000.json"]
        receipt = json.loads(receipt_raw.decode("utf-8"))
        assert receipt["delivery_order"] == 0
        assert receipt["file_available_at"] == "2026-01-01T10:00:00Z"
        assert receipt["file_path"] == "statements/SIM_CHANNEL_A/statements_w000000001.jsonl"
        assert receipt["physical_rows"] == 1
        assert receipt["sha256"] == hashlib.sha256(b'{"id":1}\n').hexdigest()

        # Chạy lại cùng mốc -> idempotent (skipped=1, không gửi thêm PUT).
        puts_before = len(put_paths)
        res2 = ingest_statements(
            cfg, target, source, as_of_time=datetime(2026, 1, 2, 4, 0, 0, tzinfo=UTC)
        )
        assert (res2.uploaded_batches, res2.skipped_batches, res2.deferred_batches) == (0, 1, 2)
        assert len([1 for m, _, _, _ in server.requests if m == "PUT"]) == puts_before

        # Tiến đồng hồ đến đúng 2026-01-02T12:00:00Z (inclusive) -> lô 1 được tải, lô 2 vẫn chờ.
        res3 = ingest_statements(
            cfg, target, source, as_of_time=datetime(2026, 1, 2, 12, 0, 0, tzinfo=UTC)
        )
        assert (
            res3.eligible_batches,
            res3.uploaded_batches,
            res3.skipped_batches,
            res3.deferred_batches,
        ) == (2, 1, 1, 1)
        assert f"{prefix}/_ingest/000000001.json" in server.objects
        assert f"{prefix}/_ingest/000000002.json" not in server.objects

        # Sửa nội dung lô 0 (cùng manifest khớp file mới) -> phải bị từ chối ghi đè vì khác sha256.
        modified = _write_statement_source(
            tmp_path / "tampered",
            batches=[(0, "SIM_CHANNEL_A", "2026-01-01T10:00:00Z", 1, b'{"id":999}\n')],
        )
        with pytest.raises(ObjectIntegrityError):
            ingest_statements(
                cfg, target, modified, as_of_time=datetime(2026, 1, 2, 4, 0, 0, tzinfo=UTC)
            )


def test_discover_rejects_unlisted_file_and_sha_mismatch(tmp_path: Path) -> None:
    source = _write_statement_source(
        tmp_path,
        batches=[(0, "SIM_CHANNEL_A", "2026-01-01T10:00:00Z", 1, b'{"id":1}\n')],
    )
    (source / "statements" / "SIM_CHANNEL_A" / "stray.jsonl").write_bytes(b"{}\n")
    with pytest.raises(StatementIngestConfigError):
        discover_statement_deliveries(source)

    (source / "statements" / "SIM_CHANNEL_A" / "stray.jsonl").unlink()
    (source / "statements" / "SIM_CHANNEL_A" / "statements_w000000001.jsonl").write_bytes(
        b'{"id":2}\n'
    )
    with pytest.raises(StatementIngestConfigError):
        discover_statement_deliveries(source)


def test_keys_and_receipt_payload_format(tmp_path: Path) -> None:
    source = _write_statement_source(
        tmp_path,
        batches=[(42, "SIM_CHANNEL_A", "2026-01-01T10:00:00Z", 7, b'{"id":1}\n')],
    )
    batches = discover_statement_deliveries(source)
    cfg = load_config(profile="test", env=SECRETS)
    data_key, manifest_key, receipt_key = build_statement_keys(cfg, batches[0])
    assert data_key.endswith("/demo-tx-v1/statements/SIM_CHANNEL_A/statements_w000000007.jsonl")
    assert manifest_key.endswith(
        "/demo-tx-v1/manifests/statements_SIM_CHANNEL_A_w000000007.manifest.json"
    )
    assert receipt_key.endswith("/demo-tx-v1/_ingest/000000042.json")
    raw = build_receipt_payload(batches[0])
    assert raw.endswith(b"\n") and json.loads(raw)["delivery_order"] == 42


def test_cli_ingest_statements_help_and_json_output(tmp_path: Path) -> None:
    help_out = io.StringIO()
    assert main(["ingest-statements", "--help"], out=help_out) == 0
    assert "--as-of-time" in help_out.getvalue()

    bad_out = io.StringIO()
    assert (
        run_ingest_statements(
            source_dir=tmp_path, as_of_time="not-a-timestamp", as_json=True, out=bad_out
        )
        == 10
    )

    source = _write_statement_source(
        tmp_path,
        batches=[(0, "SIM_CHANNEL_A", "2026-01-01T10:00:00Z", 1, b'{"id":1}\n')],
    )
    with fake_object_s3() as server:
        out = io.StringIO()
        code = run_ingest_statements(
            source_dir=source,
            as_of_time="2026-01-01T10:00:00Z",
            profile="test",
            env_file=tmp_path / "none.env",
            env={**SECRETS, "N06_MINIO_API_HOST_PORT": str(server.server_address[1])},
            as_json=True,
            out=out,
        )
    assert code == 0
    payload = json.loads(out.getvalue())
    assert payload["uploaded_batches"] == 1 and payload["deferred_batches"] == 0
