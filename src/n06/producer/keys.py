"""Chọn key Kafka cho từng dòng events (Phần II mục 4, ADR-0010 quyết định 4).

Key chỉ ảnh hưởng phân vùng; value luôn là đúng byte của dòng trong file. Dòng hỏng (trống, JSON
sai, thiếu trường, event_type lạ) luôn là `SourceDataError` nêu file và số dòng, cho cả hai
chiến lược.
"""

from __future__ import annotations

import json
from typing import Any

from n06.producer.errors import OrphanLedgerError, PublishConfigError, SourceDataError

TRANSACTION_ID = "transaction_id"
ACCOUNT_FROM = "account_from"
SUPPORTED_STRATEGIES = (TRANSACTION_ID, ACCOUNT_FROM)

_TRANSACTION_EVENT = "TRANSACTION_CREATED"
_LEDGER_EVENT = "LEDGER_ENTRY_CREATED"


def _encode_key(text: str, where: str) -> bytes:
    """Mã hóa key UTF-8; ký tự không mã hóa được (ví dụ surrogate lẻ) là lỗi dữ liệu nguồn."""
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise SourceDataError(
            f"Giá trị key không mã hóa được UTF-8 tại {where} (ký tự không hợp lệ): {exc.reason}."
        ) from exc


def _required_text(container: dict[str, Any], name: str, where: str) -> str:
    value = container.get(name)
    if not isinstance(value, str) or not value:
        raise SourceDataError(
            f"Thiếu hoặc sai kiểu trường {name!r} (cần chuỗi không rỗng) tại {where}."
        )
    return value


class KeySelector:
    """Trả key (bytes) cho từng dòng events; `start_file` xóa trạng thái khi sang shard mới."""

    def __init__(self, strategy: str) -> None:
        if strategy not in SUPPORTED_STRATEGIES:
            raise PublishConfigError(
                "Chiến lược key (producer.key_strategy) không được hỗ trợ; chọn một trong "
                f"{SUPPORTED_STRATEGIES}."
            )
        self._strategy = strategy
        self._account_from_by_transaction: dict[str, str] = {}

    def start_file(self) -> None:
        """Ledger luôn cùng shard với transaction nên map chỉ sống trong một file events."""
        self._account_from_by_transaction.clear()

    def select(self, line: bytes, *, file_path: str, line_number: int) -> bytes:
        where = f"{file_path} dòng {line_number}"
        if not line.strip():
            raise SourceDataError(f"Dòng trống tại {where}.")
        try:
            envelope = json.loads(line)
        except (ValueError, RecursionError) as exc:
            # ValueError gồm JSONDecodeError, UnicodeDecodeError và giới hạn số nguyên quá dài;
            # RecursionError là JSON lồng quá sâu. Tất cả là lỗi dữ liệu nguồn (mã 10).
            raise SourceDataError(f"JSON hỏng hoặc vượt giới hạn tại {where}: {exc}") from exc
        if not isinstance(envelope, dict):
            raise SourceDataError(f"Dòng không phải một object JSON tại {where}.")
        event_type = envelope.get("event_type")
        if event_type not in (_TRANSACTION_EVENT, _LEDGER_EVENT):
            raise SourceDataError(f"event_type {event_type!r} không được hỗ trợ tại {where}.")
        payload = envelope.get("payload")
        if not isinstance(payload, dict):
            raise SourceDataError(f"Thiếu hoặc sai kiểu trường 'payload' tại {where}.")
        transaction_id = _required_text(payload, "transaction_id", where)
        if self._strategy == TRANSACTION_ID:
            return _encode_key(transaction_id, where)

        if event_type == _TRANSACTION_EVENT:
            account_from = _required_text(payload, "account_from", where)
            self._account_from_by_transaction[transaction_id] = account_from
            return _encode_key(account_from, where)
        account_from_known = self._account_from_by_transaction.get(transaction_id)
        if account_from_known is None:
            raise OrphanLedgerError(
                "Dữ liệu nguồn không nhất quán (không phải lỗi cấu hình): ledger event "
                f"event_id={envelope.get('event_id')} của transaction_id={transaction_id} không "
                f"có transaction event trước nó trong cùng file {file_path} "
                f"(dòng {line_number}). Không thể chọn key theo {ACCOUNT_FROM}."
            )
        return _encode_key(account_from_known, where)
