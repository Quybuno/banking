"""Đo chỉ số lưu trữ theo tầng (ADR-0007 B3, I.14.2)."""

from __future__ import annotations

import math
import os
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from n06.config import AppConfig
from n06.storage.object_store import ObjectStoreTarget, _send_request, target_from_config


@dataclass(frozen=True)
class LayerStorageMetrics:
    """Thống kê dung lượng và phân bố kích thước file cho một tầng lưu trữ."""

    layer: str
    object_count: int
    total_bytes: int
    file_size_min: int
    file_size_p50: float
    file_size_p95: float
    file_size_max: int
    tiny_file_count: int


def _percentile(sorted_values: Sequence[int], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    pos = (len(sorted_values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_values[lo])
    frac = pos - lo
    return round(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac, 2)


def summarize_file_sizes(
    layer: str, sizes: Sequence[int], *, tiny_file_bytes: int
) -> LayerStorageMetrics:
    """Tính min, p50, p95, max và số tiny file từ danh sách kích thước (byte)."""
    if not sizes:
        return LayerStorageMetrics(
            layer=layer,
            object_count=0,
            total_bytes=0,
            file_size_min=0,
            file_size_p50=0.0,
            file_size_p95=0.0,
            file_size_max=0,
            tiny_file_count=0,
        )
    ordered = sorted(sizes)
    return LayerStorageMetrics(
        layer=layer,
        object_count=len(ordered),
        total_bytes=sum(ordered),
        file_size_min=ordered[0],
        file_size_p50=_percentile(ordered, 0.50),
        file_size_p95=_percentile(ordered, 0.95),
        file_size_max=ordered[-1],
        tiny_file_count=sum(1 for s in ordered if s < tiny_file_bytes),
    )


def measure_local_directory(
    layer: str, directory: Path, *, tiny_file_bytes: int
) -> LayerStorageMetrics:
    """Đo dung lượng mọi file trong thư mục cục bộ."""
    if not directory.is_dir():
        return summarize_file_sizes(layer, (), tiny_file_bytes=tiny_file_bytes)
    sizes = [p.stat().st_size for p in directory.rglob("*") if p.is_file()]
    return summarize_file_sizes(layer, sizes, tiny_file_bytes=tiny_file_bytes)


def list_s3_object_sizes(target: ObjectStoreTarget, bucket: str, prefix: str) -> list[int]:
    """Liệt kê kích thước mọi object dưới `prefix` bằng S3 ListObjectsV2."""
    sizes: list[int] = []
    continuation: str | None = None
    norm_prefix = prefix.strip("/")
    if norm_prefix:
        norm_prefix = f"{norm_prefix}/"

    while True:
        query: dict[str, str] = {"list-type": "2", "prefix": norm_prefix}
        if continuation is not None:
            query["continuation-token"] = continuation
        resp = _send_request(target, method="GET", path=f"/{bucket}", query=query)
        if resp.status >= 400:
            break
        root = ET.fromstring(resp.body)
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}", 1)[0] + "}"
        for contents in root.findall(f"{ns}Contents"):
            size_el = contents.find(f"{ns}Size")
            if size_el is not None and size_el.text and size_el.text.isdigit():
                sizes.append(int(size_el.text))
        trunc_el = root.find(f"{ns}IsTruncated")
        next_el = root.find(f"{ns}NextContinuationToken")
        if (
            trunc_el is not None
            and trunc_el.text == "true"
            and next_el is not None
            and next_el.text
        ):
            continuation = next_el.text
        else:
            break
    return sizes


def measure_object_store_layers(
    cfg: AppConfig, *, env: Mapping[str, str] | None = None
) -> list[LayerStorageMetrics]:
    """Đo các tầng lưu trữ trên object store theo `cfg.paths`."""
    target = target_from_config(cfg, env if env is not None else os.environ)
    layers = (
        ("raw", cfg.paths.raw_prefix),
        ("bronze", cfg.paths.bronze_prefix),
        ("silver", cfg.paths.silver_prefix),
        ("gold", cfg.paths.gold_prefix),
        ("quarantine", cfg.paths.quarantine_prefix),
        ("benchmark", cfg.paths.benchmark_prefix),
    )
    results: list[LayerStorageMetrics] = []
    for layer_name, prefix in layers:
        try:
            sizes = list_s3_object_sizes(target, cfg.object_store.bucket, prefix)
        except Exception:
            sizes = []
        results.append(
            summarize_file_sizes(layer_name, sizes, tiny_file_bytes=cfg.benchmark.tiny_file_bytes)
        )
    return results
