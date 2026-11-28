"""Biên nhận `_ingest/` và địa chỉ MinIO của sao kê (ADR-0002 mục 4, ADR-0006 D3)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from n06.config import load_config
from n06.spark_jobs import receipts

SECRETS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-marker-71",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-marker-72",
    "N06_POSTGRES_MART_PASSWORD": "pw-marker-73",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw-marker-74",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw-marker-75",
    "N06_POSTGRES_METABASE_PASSWORD": "pw-marker-76",
}
DATASET = "n06-demo-clean"
SHA = "a" * 64


def _receipt(order: int = 0, **override: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "schema_version": "1.0.0",
        "dataset_id": DATASET,
        "source_batch_id": "00000000-0000-0000-0000-000000000001",
        "delivery_order": order,
        "file_available_at": "2026-01-01T18:00:00Z",
        "file_path": f"statements/SIM_CHANNEL_A/statements_w{order:09d}.jsonl",
        "manifest_path": f"manifests/statements_SIM_CHANNEL_A_w{order:09d}.manifest.json",
        "sha256": SHA,
        "physical_rows": 2,
    }
    base.update(override)
    return base


def _parse(order: int = 0, name: str | None = None, **override: Any) -> receipts.Receipt:
    return receipts.parse_receipt(
        name or f"{order:09d}.json",
        (json.dumps(_receipt(order, **override)) + "\n").encode(),
        dataset_id=DATASET,
    )


def test_addresses_follow_adr_0002_section_4() -> None:
    cfg = load_config(profile="default", env=SECRETS)
    assert receipts.receipts_dir_uri(cfg, DATASET) == (
        "s3a://n06-lakehouse/statements/n06-demo-clean/_ingest/"
    )
    assert receipts.statement_object_uri(cfg, DATASET, "statements/SIM_CHANNEL_A/f.jsonl") == (
        "s3a://n06-lakehouse/statements/n06-demo-clean/statements/SIM_CHANNEL_A/f.jsonl"
    )
    assert receipts.accounts_uri(cfg, DATASET) == (
        "s3a://n06-lakehouse/statements/n06-demo-clean/accounts.jsonl"
    )


@pytest.mark.parametrize("bad", ["/etc/passwd", "../x", "a//b", "a/../b"])
def test_object_uri_refuses_unsafe_relative_paths(bad: str) -> None:
    cfg = load_config(profile="default", env=SECRETS)
    with pytest.raises(receipts.ReceiptError):
        receipts.statement_object_uri(cfg, DATASET, bad)


def test_parse_receipt_reads_every_field() -> None:
    parsed = _parse(7)
    assert parsed.delivery_order == 7 and parsed.physical_rows == 2 and parsed.sha256 == SHA
    assert parsed.file_available_at == datetime(2026, 1, 1, 18, 0, 0, tzinfo=UTC)
    assert parsed.file_path.endswith("statements_w000000007.jsonl")


@pytest.mark.parametrize(
    "case",
    [
        {"name": "7.json"},
        {"name": "000000008.json"},  # tên không khớp delivery_order=7
        {"dataset_id": "other-dataset"},
        {"sha256": "xyz"},
        {"physical_rows": "2"},
        {"file_available_at": "2026-01-01 18:00:00"},
        {"file_path": ""},
    ],
)
def test_parse_receipt_rejects_malformed_receipts(case: dict[str, Any]) -> None:
    name = case.pop("name", None)
    with pytest.raises(receipts.ReceiptError):
        _parse(7, name, **case)


def test_parse_receipt_rejects_missing_field_and_non_json() -> None:
    data = _receipt(3)
    del data["manifest_path"]
    with pytest.raises(receipts.ReceiptError, match="thiếu"):
        receipts.parse_receipt("000000003.json", json.dumps(data), dataset_id=DATASET)
    with pytest.raises(receipts.ReceiptError):
        receipts.parse_receipt("000000003.json", b"not json", dataset_id=DATASET)


def test_ordered_sorts_and_rejects_two_receipts_for_one_file() -> None:
    items = [_parse(2), _parse(0), _parse(1)]
    assert [r.delivery_order for r in receipts.ordered(items)] == [0, 1, 2]
    clash = _parse(4, file_path=items[0].file_path)
    with pytest.raises(receipts.ReceiptError, match="Hai biên nhận"):
        receipts.ordered([*items, clash])


def test_visibility_is_inclusive_at_the_exact_instant() -> None:
    early = _parse(0, file_available_at="2026-01-01T18:00:00Z")
    exact = _parse(1, file_available_at="2026-01-02T12:00:00Z")
    later = _parse(2, file_available_at="2026-01-03T00:00:00Z")
    as_of = datetime(2026, 1, 2, 12, 0, 0, tzinfo=UTC)
    assert [r.delivery_order for r in receipts.visible_at([later, exact, early], as_of)] == [0, 1]
    with pytest.raises(ValueError):
        receipts.visible_at([early], datetime(2026, 1, 2))
