"""Đọc lại kịch bản gốc của T12 theo thứ tự `(event_time, ordinal)` kèm ordinal (TASK T13).

Bản ghi transaction của T12 không mang ordinal (schema cấm trường lạ), nên ordinal được dựng lại
bằng các hàm công khai và đối chiếu `transaction_id` để phát hiện lệch.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from datetime import date
from pathlib import Path
from typing import Any

from n06.generator.determinism import deterministic_uuid
from n06.generator.external_sort import event_time_ordinal_key, external_sort_jsonl
from n06.generator.scenario import compute_event_timestamp


class CanonicalMismatchError(RuntimeError):
    """Kịch bản gốc không khớp với ordinal dựng lại."""


def iter_jsonl_files(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    yield json.loads(line)


def _ordinal_records(config: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
    from_date = date.fromisoformat(str(config["from_date"]))
    num_days = (date.fromisoformat(str(config["to_date"])) - from_date).days + 1
    for ordinal in range(int(config["transaction_count"])):
        _, event_time = compute_event_timestamp(
            contract_version=str(config["contract_version"]),
            seed=int(config["seed"]),
            ordinal=ordinal,
            from_date=from_date,
            num_days=num_days,
            business_timezone=str(config["business_timezone"]),
        )
        yield {
            "event_time": event_time,
            "ordinal": ordinal,
            "transaction_id": deterministic_uuid(
                seed_namespace=str(config["seed_namespace"]),
                contract_version=str(config["contract_version"]),
                dataset_id=str(config["dataset_id"]),
                seed=int(config["seed"]),
                entity="transaction",
                ordinal=ordinal,
                discriminator="original",
            ),
        }


def iter_canonical(
    config: Mapping[str, Any],
    transaction_files: Iterable[Path],
    ledger_entry_files: Iterable[Path],
    *,
    chunk_rows: int,
) -> Iterator[tuple[int, dict[str, Any], list[dict[str, Any]]]]:
    """Trả `(ordinal, transaction, ledger_entries)`; SUCCESS có đúng DEBIT rồi CREDIT."""
    ordinals = external_sort_jsonl(
        _ordinal_records(config), key_fn=event_time_ordinal_key, chunk_rows=chunk_rows
    )
    txs = iter_jsonl_files(transaction_files)
    ledgers = iter_jsonl_files(ledger_entry_files)
    for index, tx in zip(ordinals, txs, strict=True):
        if (
            index["transaction_id"] != tx["transaction_id"]
            or index["event_time"] != tx["event_time"]
        ):
            raise CanonicalMismatchError(
                f"Ordinal {index['ordinal']} không khớp transaction_id trong kịch bản gốc."
            )
        entries: list[dict[str, Any]] = []
        if tx["status"] == "SUCCESS":
            entries = [next(ledgers), next(ledgers)]
            sides = [e["side"] for e in entries]
            ids = {e["transaction_id"] for e in entries}
            if sides != ["DEBIT", "CREDIT"] or ids != {tx["transaction_id"]}:
                raise CanonicalMismatchError(
                    f"Ledger của ordinal {index['ordinal']} không đúng thứ tự DEBIT/CREDIT."
                )
        yield int(index["ordinal"]), tx, entries
    if next(ledgers, None) is not None:
        raise CanonicalMismatchError("Còn ledger entry thừa sau khi hết giao dịch.")
