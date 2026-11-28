"""`n06 ingest-statements` tải `accounts.jsonl` theo ADR-0004b Q2 (khóa dưới `<dataset_id>/`)."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from n06.config import load_config
from n06.ingestion import statements
from n06.storage.object_store import ObjectStoreTarget

SECRETS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-marker-71",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-marker-72",
    "N06_POSTGRES_MART_PASSWORD": "pw-marker-73",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw-marker-74",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw-marker-75",
    "N06_POSTGRES_METABASE_PASSWORD": "pw-marker-76",
}
DATASET = "demo-tx-v1"


def _source(tmp_path: Path, *, accounts: bytes | None) -> Path:
    source = tmp_path / "source"
    (source / "statements" / "SIM_CHANNEL_A").mkdir(parents=True)
    (source / "manifests").mkdir()
    data = b'{"id":1}\n'
    rel = "statements/SIM_CHANNEL_A/statements_w000000001.jsonl"
    (source / rel).write_bytes(data)
    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": DATASET,
        "source_batch_id": "00000000-0000-0000-0000-000000000001",
        "files": [{"path": rel, "sha256": hashlib.sha256(data).hexdigest(), "physical_rows": 1}],
    }
    (source / "manifests" / "statements_SIM_CHANNEL_A_w000000001.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    delivery = {
        "schema_version": "1.0.0",
        "dataset_id": DATASET,
        "statement_record_id": "00000000-0000-0000-0000-0000000000aa",
        "file_path": rel,
        "file_available_at": "2026-01-01T10:00:00Z",
        "delivery_order": 0,
    }
    (source / "statement_deliveries.jsonl").write_text(json.dumps(delivery) + "\n", "utf-8")
    if accounts is not None:
        (source / "accounts.jsonl").write_bytes(accounts)
    return source


@pytest.fixture
def puts(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Path | bytes]]:
    recorded: list[tuple[str, Path | bytes]] = []

    def file_put(target: Any, bucket: str, key: str, path: Path, **kwargs: Any) -> str:
        recorded.append((key, path))
        return "uploaded"

    def bytes_put(target: Any, bucket: str, key: str, data: bytes, **kwargs: Any) -> str:
        recorded.append((key, data))
        return "uploaded"

    monkeypatch.setattr(statements, "put_object_immutable_file", file_put)
    monkeypatch.setattr(statements, "put_object_immutable_bytes", bytes_put)
    return recorded


def _ingest(source: Path) -> statements.StatementIngestResult:
    cfg = load_config(profile="test", env=SECRETS)
    target = ObjectStoreTarget("http", "127.0.0.1", 9, "us-east-1", "AKIA", "SECRET")
    return statements.ingest_statements(
        cfg, target, source, as_of_time=datetime(2026, 1, 2, tzinfo=UTC)
    )


def test_accounts_file_is_uploaded_under_the_dataset_prefix(
    tmp_path: Path, puts: list[tuple[str, Path | bytes]]
) -> None:
    source = _source(tmp_path, accounts=b'{"account_id":"x"}\n')
    result = _ingest(source)
    prefix = load_config(profile="test", env=SECRETS).paths.statements_prefix
    key = f"{prefix}/{DATASET}/accounts.jsonl"
    assert result.accounts_key == key and result.accounts_status == "uploaded"
    assert (key, source / "accounts.jsonl") in puts
    assert key in result.uploaded_keys
    assert "/_ingest/" not in key  # không phải biên nhận và không bị chặn theo đồng hồ


def test_source_without_accounts_uploads_none(
    tmp_path: Path, puts: list[tuple[str, Path | bytes]]
) -> None:
    result = _ingest(_source(tmp_path, accounts=None))
    assert result.accounts_key is None and result.accounts_status is None
    assert all(not str(key).endswith("accounts.jsonl") for key, _ in puts)
