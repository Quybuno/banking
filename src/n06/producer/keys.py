"""Chọn key Kafka cho từng dòng events (Phần II mục 4, ADR-0010 quyết định 4).

Key chỉ ảnh hưởng phân vùng; value luôn là đúng byte của dòng trong file.
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


class KeySelector:
    """Trả key (bytes) cho từng dòng events; `start_file` xóa trạng thái khi sang shard mới."""

    def __init__(self, strategy: str) -> None:
        if strategy not in SUPPORTED_STRATEGIES:
            raise PublishConfigError(
                f"Chiến lược key {strategy!r} không được hỗ trợ; chọn một trong "
                f"{SUPPORTED_STRATEGIES}."
            )
        self._strategy = strategy
        self._account_from_by_transaction: dict[str, str] = {}

    def start_file(self) -> None:
        """Ledger luôn cùng shard với transaction nên map chỉ sống trong một file events."""
        self._account_from_by_transaction.clear()

    def select(self, line: bytes, *, file_path: str, line_number: int) -> bytes:
        envelope: dict[str, Any] = json.loads(line)
        payload = envelope["payload"]
        transaction_id = str(payload["transaction_id"])
        if self._strategy == TRANSACTION_ID:
            return transaction_id.encode("utf-8")

        event_type = envelope["event_type"]
        if event_type == _TRANSACTION_EVENT:
            account_from = str(payload["account_from"])
            self._account_from_by_transaction[transaction_id] = account_from
            return account_from.encode("utf-8")
        if event_type == _LEDGER_EVENT:
            account_from = self._account_from_by_transaction.get(transaction_id)
            if account_from is None:
                raise OrphanLedgerError(
                    "Dữ liệu nguồn không nhất quán (không phải lỗi cấu hình): ledger event "
                    f"event_id={envelope['event_id']} của transaction_id={transaction_id} không "
                    f"có transaction event trước nó trong cùng file {file_path} "
                    f"(dòng {line_number}). Không thể chọn key theo {ACCOUNT_FROM}."
                )
            return account_from.encode("utf-8")
        raise SourceDataError(
            f"event_type {event_type!r} không được hỗ trợ tại {file_path} dòng {line_number}."
        )
