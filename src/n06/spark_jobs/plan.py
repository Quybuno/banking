"""Đường dẫn đầu ra, kiểm run chưa dùng và semantic hash của `process-day` (ADR-0002 mục 3, 6).

Module thuần, không nhập PySpark. Mọi đường dẫn đi qua `build_run_output_path` và mọi run phải qua
`require_unused_run_prefix` trước khi ghi object đầu tiên.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Sequence
from datetime import date
from typing import Any
from urllib.parse import urlunsplit

from n06.config import AppConfig
from n06.reconciliation.rules import RECON_RULE_VERSION
from n06.storage import build_run_output_path, compute_semantic_hash, require_unused_run_prefix

LAYER_SILVER = "silver"
LAYER_GOLD = "gold"
LAYER_QUARANTINE = "quarantine"
SILVER_ENTITIES = ("transactions", "ledger_entries", "statements", "accounts", "key_conflicts")
GOLD_ENTITIES = ("fact_transactions", "fact_reconciliation")
QUARANTINE_ENTITIES = ("records",)
# Thứ tự ghi và commit; dimension bổ sung và fact_daily_balances là việc của task khác.
OUTPUTS: tuple[tuple[str, str], ...] = tuple(
    [(LAYER_SILVER, entity) for entity in SILVER_ENTITIES]
    + [(LAYER_GOLD, entity) for entity in GOLD_ENTITIES]
    + [(LAYER_QUARANTINE, entity) for entity in QUARANTINE_ENTITIES]
)

SCHEMA_VERSION = "silver-gold-1.0.0"
# Tăng khi đổi logic biến đổi; vào semantic hash (đặc tả I.8.1 batch bước 5).
CODE_VERSION = "process-day-3"
RUN_LEVEL_LAYER = "silver+gold"
RUN_LEVEL_ENTITY = "process-day"


def tier_uri(cfg: AppConfig, layer: str) -> str:
    """`s3a://<bucket>/<paths.<layer>_prefix>` (ADR-0002 quy ước chung và mục 3)."""
    if layer not in (LAYER_SILVER, LAYER_GOLD, LAYER_QUARANTINE):
        raise ValueError(f"Tầng không được ghi bởi process-day: {layer!r}")
    prefix = getattr(cfg.paths, f"{layer}_prefix").strip("/")
    return urlunsplit(("s3a", cfg.object_store.bucket, "/" + prefix, "", ""))


def output_path(
    cfg: AppConfig,
    layer: str,
    entity: str,
    *,
    dataset_id: str,
    run_id: str,
    business_date: date | str,
) -> str:
    return build_run_output_path(
        tier_uri(cfg, layer),
        dataset_id=dataset_id,
        entity=entity,
        run_id=run_id,
        business_date=business_date,
    )


def claim_run_prefixes(
    cfg: AppConfig,
    list_objects: Callable[[str], Iterable[str]],
    *,
    dataset_id: str,
    run_id: str,
) -> None:
    """Kiểm mọi gốc `run_id=<id>/` của run còn trống, trước khi ghi bất kỳ object nào.

    `list_objects` là bản liệt kê S3 thật (xem `n06.spark_jobs.s3_listing`). Ném `CommitError`
    ngay khi một gốc đã có object.
    """
    for layer, entity in OUTPUTS:
        require_unused_run_prefix(
            list_objects,
            tier_uri(cfg, layer),
            dataset_id=dataset_id,
            entity=entity,
            run_id=run_id,
        )


def config_hash(cfg: AppConfig) -> str:
    """Băm phần cấu hình nghiệp vụ ảnh hưởng kết quả; không gồm endpoint, bucket hay executor."""
    payload = {
        "business_timezone": cfg.business_timezone,
        # Bốn tham số quyết định trạng thái đối soát (ADR-0006 Q10).
        "allowed_lateness_seconds": cfg.dataset.allowed_lateness_seconds,
        "reconciliation_statuses": list(cfg.reconciliation.statuses),
        "reconciliation_channels": list(cfg.reconciliation.channels),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def input_snapshot_id(input_versions: Sequence[Any]) -> str:
    """Định danh tập input của một lần đối soát (đặc tả I.7.3).

    Chỉ phụ thuộc dữ liệu đầu vào: bỏ `as_of_time` và bộ lọc `business_date` của lần chạy.
    """
    run_options = ("as_of_time", "business_dates")
    items = [
        i for i in input_versions if not (isinstance(i, dict) and i.get("kind") in run_options)
    ]
    text = json.dumps(items, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "in-" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def run_semantic_hash(
    *,
    dataset_id: str,
    business_date: date | str,
    input_versions: Sequence[Any],
    config_hash_value: str,
    entity_digests: dict[str, str],
) -> str:
    """Semantic hash của cả run: cùng input, cấu hình và phiên bản thì cùng giá trị.

    Một run commit nhiều `current_snapshot` qua `commit_run`, và `commit_run` đòi mọi lần commit của
    cùng `run_id` mang cùng `semantic_hash`; vì vậy giá trị này bao cả run, không riêng một entity.
    `entity_digests` ghi `<tầng>.<entity>` -> chuỗi băm nội dung đã loại cột `run_id`.
    """
    payload_digest = hashlib.sha256(
        json.dumps(entity_digests, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return compute_semantic_hash(
        dataset_id=dataset_id,
        layer=RUN_LEVEL_LAYER,
        entity=RUN_LEVEL_ENTITY,
        business_date=business_date,
        input_versions=list(input_versions),
        config_hash=config_hash_value,
        schema_version=SCHEMA_VERSION,
        rule_version=RECON_RULE_VERSION,
        code_version=CODE_VERSION,
        payload_digest=payload_digest,
    )
