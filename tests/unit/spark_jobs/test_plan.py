"""Đường dẫn đầu ra, kiểm run chưa dùng và semantic hash của process-day. Expected viết tay."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import date

import pytest

from n06.config import load_config
from n06.spark_jobs import plan
from n06.storage import CommitError

SECRETS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak-marker-71",
    "N06_OBJECT_STORE_SECRET_KEY": "sk-marker-72",
    "N06_POSTGRES_MART_PASSWORD": "pw-marker-73",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw-marker-74",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw-marker-75",
    "N06_POSTGRES_METABASE_PASSWORD": "pw-marker-76",
}


def _cfg(**override: str):  # type: ignore[no-untyped-def]
    return load_config(profile="default", env={**SECRETS, **override})


def test_output_paths_use_tier_prefix_dataset_entity_and_run_id() -> None:
    cfg = _cfg()
    silver = plan.output_path(
        cfg,
        "silver",
        "transactions",
        dataset_id="n06-demo-clean",
        run_id="run-abc",
        business_date=date(2026, 1, 2),
    )
    assert silver == (
        "s3a://n06-lakehouse/silver/n06-demo-clean/transactions/run_id=run-abc/"
        "business_date=2026-01-02"
    )
    gold = plan.output_path(
        cfg,
        "gold",
        "fact_reconciliation",
        dataset_id="n06-demo-clean",
        run_id="run-abc",
        business_date="2026-01-03",
    )
    assert gold == (
        "s3a://n06-lakehouse/gold/n06-demo-clean/fact_reconciliation/run_id=run-abc/"
        "business_date=2026-01-03"
    )


def test_tier_uri_follows_configured_prefix_and_bucket() -> None:
    cfg = _cfg(N06__PATHS__SILVER_PREFIX="s-x", N06__OBJECT_STORE__BUCKET="other-bucket")
    assert plan.tier_uri(cfg, "silver") == "s3a://other-bucket/s-x"
    assert plan.tier_uri(cfg, "quarantine") == "s3a://other-bucket/quarantine"
    with pytest.raises(ValueError):
        plan.tier_uri(cfg, "bronze")


def test_every_output_has_a_distinct_layer_entity_pair() -> None:
    assert len(set(plan.OUTPUTS)) == len(plan.OUTPUTS) == 8
    assert {entity for layer, entity in plan.OUTPUTS if layer == "gold"} == {
        "fact_transactions",
        "fact_reconciliation",
    }
    assert ("silver", "key_conflicts") in plan.OUTPUTS
    assert ("quarantine", "records") in plan.OUTPUTS


def test_claim_checks_every_run_root_with_a_trailing_slash_before_any_write() -> None:
    asked: list[str] = []

    def empty(prefix: str) -> Iterator[str]:
        asked.append(prefix)
        return iter(())

    plan.claim_run_prefixes(_cfg(), empty, dataset_id="n06-demo-clean", run_id="run-1")
    assert asked == [
        "s3a://n06-lakehouse/silver/n06-demo-clean/transactions/run_id=run-1/",
        "s3a://n06-lakehouse/silver/n06-demo-clean/ledger_entries/run_id=run-1/",
        "s3a://n06-lakehouse/silver/n06-demo-clean/statements/run_id=run-1/",
        "s3a://n06-lakehouse/silver/n06-demo-clean/accounts/run_id=run-1/",
        "s3a://n06-lakehouse/silver/n06-demo-clean/key_conflicts/run_id=run-1/",
        "s3a://n06-lakehouse/gold/n06-demo-clean/fact_transactions/run_id=run-1/",
        "s3a://n06-lakehouse/gold/n06-demo-clean/fact_reconciliation/run_id=run-1/",
        "s3a://n06-lakehouse/quarantine/n06-demo-clean/records/run_id=run-1/",
    ]


def test_claim_refuses_a_run_that_already_has_an_object_in_any_entity() -> None:
    def listing(prefix: str) -> Iterator[str]:
        if "fact_transactions" in prefix:
            yield prefix + "part-0.parquet"

    with pytest.raises(CommitError, match="đã có object"):
        plan.claim_run_prefixes(_cfg(), listing, dataset_id="n06-demo-clean", run_id="run-1")


def test_run_id_prefix_is_not_confused_with_a_longer_run_id() -> None:
    asked: list[str] = []

    def listing(prefix: str) -> Iterator[str]:
        asked.append(prefix)
        # Một run khác (`run-10`) đã có object, nhưng prefix của `run-1` kết thúc bằng `/`.
        return iter(["x"] if prefix.endswith("/run_id=run-10/") else [])

    plan.claim_run_prefixes(_cfg(), listing, dataset_id="n06-demo-clean", run_id="run-1")
    assert all(prefix.endswith("/run_id=run-1/") for prefix in asked)


def _hash(**override: object) -> str:
    args: dict[str, object] = {
        "dataset_id": "n06-demo-clean",
        "business_date": date(2026, 1, 1),
        "input_versions": [{"kind": "ingest_batch", "batch_id": 0}],
        "config_hash_value": plan.config_hash(_cfg()),
        "entity_digests": {"silver.transactions": "10:1:2", "gold.fact_reconciliation": "5:3:4"},
    }
    args.update(override)
    return plan.run_semantic_hash(**args)  # type: ignore[arg-type]


def test_semantic_hash_is_stable_and_ignores_digest_key_order() -> None:
    first = _hash()
    assert first == _hash()
    reordered = {"gold.fact_reconciliation": "5:3:4", "silver.transactions": "10:1:2"}
    assert first == _hash(entity_digests=reordered)
    assert len(first) == 64


@pytest.mark.parametrize(
    "override",
    [
        {"entity_digests": {"silver.transactions": "10:1:3", "gold.fact_reconciliation": "5:3:4"}},
        {"input_versions": [{"kind": "ingest_batch", "batch_id": 1}]},
        {"config_hash_value": "x" * 64},
    ],
)
def test_semantic_hash_changes_with_content_input_or_config(override: dict[str, object]) -> None:
    assert _hash(**override) != _hash()


def test_config_hash_covers_only_business_configuration() -> None:
    base = plan.config_hash(_cfg())
    expected = hashlib.sha256(b'{"business_timezone":"Asia/Ho_Chi_Minh"}').hexdigest()
    assert base == expected
    assert base == plan.config_hash(_cfg(N06__OBJECT_STORE__BUCKET="elsewhere"))
    assert base == plan.config_hash(_cfg(N06__SPARK__CORES="4"))
