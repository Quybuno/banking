"""Envelope Kafka từ kịch bản gốc kèm replay / missing_internal (Phần II mục 5.3)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from n06.generator.params import DatasetParams


def _envelope(
    params: DatasetParams,
    *,
    ordinal: int,
    event_type: str,
    discriminator: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    event_time = str(payload["event_time"])
    return {
        "schema_version": params.contract_version,
        "event_id": params.uuid("event", ordinal, discriminator),
        "event_type": event_type,
        "dataset_id": params.dataset_id,
        "source_batch_id": params.events_source_batch_id(params.event_shard(ordinal)),
        "event_time": event_time,
        "emitted_at": event_time,
        "payload": dict(payload),
    }


def build_event_envelopes(
    tx: Mapping[str, Any],
    ledger_entries: list[dict[str, Any]],
    ordinal: int,
    fault: str | None,
    params: DatasetParams,
) -> list[dict[str, Any]]:
    """Danh sách envelope được công bố cho một giao dịch, theo thứ tự phát."""
    if fault == "missing_internal":
        return []
    tx_event = _envelope(
        params,
        ordinal=ordinal,
        event_type="TRANSACTION_CREATED",
        discriminator="TRANSACTION_CREATED:original",
        payload=tx,
    )
    events = [tx_event]
    if fault == "event_replay":
        events.append(dict(tx_event))
    elif fault == "business_replay":
        events.append(
            _envelope(
                params,
                ordinal=ordinal,
                event_type="TRANSACTION_CREATED",
                discriminator="TRANSACTION_CREATED:business-replay",
                payload=tx,
            )
        )
    for entry in ledger_entries:
        events.append(
            _envelope(
                params,
                ordinal=ordinal,
                event_type="LEDGER_ENTRY_CREATED",
                discriminator=f"LEDGER_ENTRY_CREATED:{entry['side']}",
                payload=entry,
            )
        )
    return events
