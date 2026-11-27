"""Đường dẫn Raw, Bronze, checkpoint và các giá trị suy từ một microbatch (ADR-0002, ADR-0004).

Module thuần, không nhập PySpark.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from urllib.parse import urlunsplit
from zoneinfo import ZoneInfo

from n06.config import AppConfig

_SCHEME = "s3a"  # ADR-0002: đường dẫn ghi vào bảng control là URI S3A của bucket cấu hình


def _uri(cfg: AppConfig, *parts: str) -> str:
    joined = "/".join(part.strip("/") for part in parts)
    return urlunsplit((_SCHEME, cfg.object_store.bucket, f"/{joined}/", "", ""))


def _batch_parts(query_name: str, ingestion_date: date, batch_id: int) -> tuple[str, str, str]:
    if batch_id < 0:
        raise ValueError(f"batch_id phải >= 0, nhận {batch_id}")
    return query_name, f"ingestion_date={ingestion_date.isoformat()}", f"batch_id={batch_id}"


def raw_batch_uri(cfg: AppConfig, query_name: str, ingestion_date: date, batch_id: int) -> str:
    return _uri(cfg, cfg.paths.raw_prefix, *_batch_parts(query_name, ingestion_date, batch_id))


def bronze_batch_uri(cfg: AppConfig, query_name: str, ingestion_date: date, batch_id: int) -> str:
    return _uri(cfg, cfg.paths.bronze_prefix, *_batch_parts(query_name, ingestion_date, batch_id))


def bronze_entity_uri(bronze_uri: str, entity: str) -> str:
    return f"{bronze_uri}entity={entity}/"


def checkpoint_uri(cfg: AppConfig, query_name: str) -> str:
    return _uri(cfg, cfg.paths.checkpoints_prefix, query_name)


def ingestion_date_of(min_kafka_timestamp: datetime, timezone: str) -> date:
    """Ngày theo múi giờ nghiệp vụ của Kafka timestamp nhỏ nhất trong batch (ADR-0002 điểm 3)."""
    if min_kafka_timestamp.tzinfo is None:
        raise ValueError("Kafka timestamp phải kèm múi giờ")
    return min_kafka_timestamp.astimezone(ZoneInfo(timezone)).date()


def topic_offsets(ranges: Iterable[tuple[str, int, int, int]]) -> dict[str, dict[str, list[int]]]:
    """`(topic, partition, offset nhỏ nhất, offset lớn nhất)` thành dạng của ADR-0004 Q3:
    `{"<topic>": {"<partition>": [<đầu>, <cuối, không tính>]}}`.
    """
    result: dict[str, dict[str, list[int]]] = {}
    for topic, partition, first, last in sorted(ranges):
        result.setdefault(topic, {})[str(partition)] = [int(first), int(last) + 1]
    return result
