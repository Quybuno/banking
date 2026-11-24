"""Chọn key Kafka (Phần II mục 4; ADR-0010 quyết định 4)."""

from __future__ import annotations

import pytest

from n06.producer.errors import OrphanLedgerError, PublishConfigError
from n06.producer.keys import KeySelector

from .conftest import ledger_event, line_of, tx_event

TX, A_FROM, A_TO = "tx-1", "acc-from", "acc-to"


def _select(selector: KeySelector, event: dict, line: int = 1) -> bytes:
    return selector.select(line_of(event), file_path="events/f.jsonl", line_number=line)


def test_default_key_is_transaction_id_for_both_event_types() -> None:
    selector = KeySelector("transaction_id")
    assert _select(selector, tx_event(TX, A_FROM, A_TO)) == b"tx-1"
    # Không cần transaction event trước: key mặc định chỉ dùng transaction_id.
    assert _select(selector, ledger_event(TX, "CREDIT", A_TO)) == b"tx-1"


def test_account_from_key_for_transaction_and_both_ledger_sides() -> None:
    selector = KeySelector("account_from")
    selector.start_file()
    assert _select(selector, tx_event(TX, A_FROM, A_TO)) == b"acc-from"
    # Cả DEBIT lẫn CREDIT dùng account_from của transaction (CREDIT ghi vào account_to).
    assert _select(selector, ledger_event(TX, "DEBIT", A_FROM)) == b"acc-from"
    assert _select(selector, ledger_event(TX, "CREDIT", A_TO)) == b"acc-from"


def test_replay_transaction_event_keeps_the_same_key() -> None:
    selector = KeySelector("account_from")
    selector.start_file()
    first = _select(selector, tx_event(TX, A_FROM, A_TO, event_id="e1"))
    again = _select(selector, tx_event(TX, A_FROM, A_TO, event_id="e2"))
    assert first == again == b"acc-from"


def test_map_is_cleared_when_next_file_starts() -> None:
    selector = KeySelector("account_from")
    selector.start_file()
    _select(selector, tx_event(TX, A_FROM, A_TO))
    selector.start_file()  # sang shard mới
    with pytest.raises(OrphanLedgerError):
        _select(selector, ledger_event(TX, "DEBIT", A_FROM), line=7)


def test_orphan_ledger_error_message_is_explicit_about_source_data() -> None:
    selector = KeySelector("account_from")
    selector.start_file()
    event = ledger_event("tx-orphan", "DEBIT", A_FROM, event_id="ev-123")
    with pytest.raises(OrphanLedgerError) as info:
        selector.select(line_of(event), file_path="events/events_batch_00003.jsonl", line_number=42)
    message = str(info.value)
    assert "dữ liệu nguồn không nhất quán" in message.lower()
    assert "không phải lỗi cấu hình" in message
    for token in ("ev-123", "tx-orphan", "events/events_batch_00003.jsonl", "42"):
        assert token in message


def test_unsupported_strategy_is_rejected() -> None:
    with pytest.raises(PublishConfigError):
        KeySelector("ledger_account_id")
