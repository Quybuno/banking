"""Biên nhận sao kê `_ingest/` và địa chỉ file sao kê trên MinIO (ADR-0002 mục 4, ADR-0006 D3).

Module thuần, không nhập PySpark. Pipeline phát hiện file sao kê bằng cách liệt kê `_ingest/`,
không quét thư mục theo ngày và không đọc `statement_deliveries.jsonl` hay ground truth.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlunsplit

from n06.config import AppConfig
from n06.domain import parse_utc_timestamp

RECEIPT_NAME = re.compile(r"^(\d{9})\.json$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED = (
    "schema_version",
    "dataset_id",
    "delivery_order",
    "file_available_at",
    "file_path",
    "manifest_path",
    "sha256",
    "physical_rows",
)


class ReceiptError(ValueError):
    """Biên nhận sao kê sai định dạng hoặc không nhất quán (lỗi toàn vẹn, không được bỏ qua)."""


@dataclass(frozen=True)
class Receipt:
    dataset_id: str
    delivery_order: int
    file_available_at: datetime  # UTC
    file_available_at_text: str
    file_path: str
    manifest_path: str
    sha256: str
    physical_rows: int


def _base_key(cfg: AppConfig, dataset_id: str) -> str:
    return f"{cfg.paths.statements_prefix.strip('/')}/{dataset_id}"


def _uri(cfg: AppConfig, key: str, *, directory: bool = False) -> str:
    return urlunsplit(
        ("s3a", cfg.object_store.bucket, "/" + key + ("/" if directory else ""), "", "")
    )


def receipts_dir_uri(cfg: AppConfig, dataset_id: str) -> str:
    """Thư mục `_ingest/` của dataset: `s3a://<bucket>/<statements_prefix>/<dataset_id>/_ingest/`."""
    return _uri(cfg, f"{_base_key(cfg, dataset_id)}/_ingest", directory=True)


def statement_object_uri(cfg: AppConfig, dataset_id: str, relative_path: str) -> str:
    """URI của file dữ liệu hoặc manifest, ghép từ đường dẫn tương đối trong biên nhận."""
    parts = relative_path.split("/")
    if relative_path.startswith("/") or ".." in parts or "" in parts:
        raise ReceiptError(f"Đường dẫn trong biên nhận không hợp lệ: {relative_path!r}")
    return _uri(cfg, f"{_base_key(cfg, dataset_id)}/{relative_path}")


def accounts_uri(cfg: AppConfig, dataset_id: str) -> str:
    """URI của `accounts.jsonl` (ADR-0004b Q2)."""
    return _uri(cfg, f"{_base_key(cfg, dataset_id)}/accounts.jsonl")


def parse_receipt(name: str, content: bytes | str, *, dataset_id: str) -> Receipt:
    """Đọc một biên nhận; tên tệp phải khớp `delivery_order` và `dataset_id` phải khớp."""
    match = RECEIPT_NAME.fullmatch(name)
    if match is None:
        raise ReceiptError(f"Tên tệp trong _ingest/ không đúng mẫu <9 chữ số>.json: {name!r}")
    text = content.decode("utf-8") if isinstance(content, bytes) else content
    try:
        data = json.loads(text)
    except ValueError as exc:
        raise ReceiptError(f"Biên nhận {name} không phải JSON hợp lệ.") from exc
    if not isinstance(data, dict):
        raise ReceiptError(f"Biên nhận {name} phải là một đối tượng JSON.")
    missing = [field for field in _REQUIRED if field not in data]
    if missing:
        raise ReceiptError(f"Biên nhận {name} thiếu trường {missing}.")
    order, rows = data["delivery_order"], data["physical_rows"]
    if type(order) is not int or type(rows) is not int or order < 0 or rows < 0:
        raise ReceiptError(f"Biên nhận {name}: delivery_order và physical_rows phải là số nguyên.")
    if order != int(match.group(1)):
        raise ReceiptError(f"Biên nhận {name}: delivery_order={order} không khớp tên tệp.")
    if data["dataset_id"] != dataset_id:
        raise ReceiptError(f"Biên nhận {name}: dataset_id khác {dataset_id!r}.")
    sha = data["sha256"]
    if not isinstance(sha, str) or _SHA256.fullmatch(sha) is None:
        raise ReceiptError(f"Biên nhận {name}: sha256 không hợp lệ.")
    for field in ("file_path", "manifest_path", "file_available_at"):
        if not isinstance(data[field], str) or not data[field]:
            raise ReceiptError(f"Biên nhận {name}: {field} phải là chuỗi không rỗng.")
    try:
        available = parse_utc_timestamp(data["file_available_at"])
    except ValueError as exc:
        raise ReceiptError(f"Biên nhận {name}: file_available_at không hợp lệ.") from exc
    return Receipt(
        dataset_id=dataset_id,
        delivery_order=order,
        file_available_at=available,
        file_available_at_text=data["file_available_at"],
        file_path=data["file_path"],
        manifest_path=data["manifest_path"],
        sha256=sha,
        physical_rows=rows,
    )


def ordered(receipts: Iterable[Receipt]) -> list[Receipt]:
    """Sắp theo `delivery_order`; hai biên nhận cùng `file_path` là lỗi toàn vẹn."""
    result = sorted(receipts, key=lambda r: r.delivery_order)
    seen: set[str] = set()
    for receipt in result:
        if receipt.file_path in seen:
            raise ReceiptError(f"Hai biên nhận cùng trỏ tới file {receipt.file_path!r}.")
        seen.add(receipt.file_path)
    return result


def visible_at(receipts: Iterable[Receipt], as_of_time: datetime) -> list[Receipt]:
    """Biên nhận có `file_available_at <= as_of_time` (ADR-0006 D3; tại đúng mốc là có mặt)."""
    if as_of_time.tzinfo is None:
        raise ValueError("as_of_time phải kèm múi giờ")
    return [r for r in ordered(receipts) if r.file_available_at <= as_of_time]
