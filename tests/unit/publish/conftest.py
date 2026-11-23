"""Fixture và client giả cho test `n06 publish` (TASK T20). Không dùng generator để dựng nguồn."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from n06.producer.errors import BrokerUnavailableError

DATASET_ID = "n06-unit-test"
SCHEMA_VERSION = "1.0.0"


def tx_event(tx: str, account_from: str, account_to: str, *, event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": "TRANSACTION_CREATED",
        "payload": {
            "transaction_id": tx,
            "account_from": account_from,
            "account_to": account_to,
            "amount_minor": 100,
        },
    }


def ledger_event(tx: str, side: str, account: str, *, event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": "LEDGER_ENTRY_CREATED",
        "payload": {
            "entry_id": str(uuid.uuid4()),
            "transaction_id": tx,
            "ledger_account_id": account,
            "side": side,
            "amount_minor": 100,
        },
    }


def line_of(event: dict) -> bytes:
    return json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha_rows(data: bytes) -> tuple[str, int]:
    return hashlib.sha256(data).hexdigest(), data.count(b"\n")


@pytest.fixture
def make_source(tmp_path: Path) -> Callable[..., Path]:
    """Dựng thư mục nguồn nhỏ bằng tay: events theo shard + manifest (sha256 tính bằng hashlib)."""

    def build(shards: list[list[bytes]], *, name: str = "source") -> Path:
        root = tmp_path / name
        (root / "events").mkdir(parents=True)
        (root / "manifests").mkdir()
        (root / "statements" / "CH").mkdir(parents=True)
        accounts = b'{"account_id":"a"}\n{"account_id":"b"}\n'
        report = b'{"totals":[]}\n'
        (root / "accounts.jsonl").write_bytes(accounts)
        (root / "control_report.json").write_bytes(report)
        for index, lines in enumerate(shards):
            data = b"".join(line + b"\n" for line in lines)
            rel = f"events/events_batch_{index:05d}.jsonl"
            (root / rel).write_bytes(data)
            files = [(rel, data)]
            if index == 0:
                files += [("accounts.jsonl", accounts), ("control_report.json", report)]
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "dataset_id": DATASET_ID,
                "source_batch_id": str(uuid.uuid4()),
                "files": [
                    {"path": p, "sha256": _sha_rows(d)[0], "physical_rows": _sha_rows(d)[1]}
                    for p, d in sorted(files)
                ],
            }
            (root / "manifests" / f"events_batch_{index:05d}.manifest.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )
        statement = b'{"statement_record_id":"s"}\n'
        (root / "statements" / "CH" / "statements_w000000001.jsonl").write_bytes(statement)
        stmt_manifest = {
            "schema_version": SCHEMA_VERSION,
            "dataset_id": DATASET_ID,
            "source_batch_id": str(uuid.uuid4()),
            "files": [
                {
                    "path": "statements/CH/statements_w000000001.jsonl",
                    "sha256": _sha_rows(statement)[0],
                    "physical_rows": 1,
                }
            ],
        }
        (root / "manifests" / "statements_CH_w000000001.manifest.json").write_text(
            json.dumps(stmt_manifest), encoding="utf-8"
        )
        return root

    return build


class FakeProducer:
    """Producer giả: ghi lại produce, gọi callback khi poll/flush, có thể giả lỗi."""

    def __init__(
        self,
        *,
        buffer_errors: int = 0,
        always_buffer_full: bool = False,
        fail_delivery_at: int | None = None,
        undelivered_after_flush: int = 0,
    ) -> None:
        self.buffer_errors = buffer_errors
        self.always_buffer_full = always_buffer_full
        self.fail_delivery_at = fail_delivery_at
        self.undelivered_after_flush = undelivered_after_flush
        self.calls: list[tuple[str, bytes, bytes]] = []
        self.polls: list[float] = []
        self._pending: list[tuple[Any, bytes]] = []
        self.flush_timeouts: list[float] = []

    def produce(self, topic: str, *, value: bytes, key: bytes, on_delivery: Any) -> None:
        """Cùng chữ ký từ khóa với `confluent_kafka.Producer.produce` (value, key, on_delivery)."""
        if self.always_buffer_full:
            raise BufferError("queue full")
        if self.buffer_errors > 0:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        index = len(self.calls)
        self.calls.append((topic, key, value))
        self._pending.append((on_delivery, value, index))  # type: ignore[arg-type]

    def _drain(self) -> None:
        for callback, _value, index in self._pending:  # type: ignore[misc]
            if self.fail_delivery_at is not None and index == self.fail_delivery_at:
                callback("delivery failed", None)
            else:
                callback(None, object())
        self._pending.clear()

    def poll(self, timeout: float) -> int:
        self.polls.append(timeout)
        self._drain()
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_timeouts.append(timeout)
        self._drain()
        return self.undelivered_after_flush


class FakeBroker:
    def __init__(
        self,
        *,
        partitions: int | None = None,
        end_offsets: dict[int, int] | None = None,
        unavailable: bool = False,
    ) -> None:
        self.partitions = partitions
        self.offsets = end_offsets if end_offsets is not None else {}
        self.unavailable = unavailable
        self.created: list[tuple[str, int, int]] = []
        self.calls: list[str] = []

    def partition_count(self, topic: str) -> int | None:
        self.calls.append("partition_count")
        if self.unavailable:
            raise BrokerUnavailableError("Không kết nối được broker giả")
        return self.partitions

    def create_topic(self, topic: str, partitions: int, replication_factor: int) -> None:
        self.calls.append("create_topic")
        self.created.append((topic, partitions, replication_factor))
        self.partitions = partitions

    def end_offsets(self, topic: str) -> dict[int, int]:
        self.calls.append("end_offsets")
        return dict(self.offsets)
