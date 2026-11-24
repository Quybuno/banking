"""Mọi đường lỗi phải ra đúng mã thoát, không traceback và không gửi dở dang (review T20)."""

from __future__ import annotations

import hashlib
import io
import json
import math
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from n06.cli.publish import run_publish
from n06.producer.errors import (
    DeliveryError,
    OrphanLedgerError,
    PublishConfigError,
    SourceDataError,
)
from n06.producer.kafka_client import build_producer_config
from n06.producer.keys import KeySelector
from n06.producer.publisher import PublishSettings, publish_events
from n06.producer.source import discover_event_files

from .conftest import FakeBroker, FakeProducer, ledger_event, line_of, tx_event

Factory = Callable[..., Path]


@pytest.fixture(autouse=True)
def _secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "N06_OBJECT_STORE_ACCESS_KEY",
        "N06_OBJECT_STORE_SECRET_KEY",
        "N06_POSTGRES_MART_PASSWORD",
        "N06_POSTGRES_CONTROL_PASSWORD",
        "N06_POSTGRES_AIRFLOW_PASSWORD",
        "N06_POSTGRES_METABASE_PASSWORD",
    ):
        monkeypatch.setenv(name, "unit-test-secret")


def _good() -> list[list[bytes]]:
    return [
        [
            line_of(tx_event("t1", "a", "b")),
            line_of(ledger_event("t1", "DEBIT", "a")),
            line_of(ledger_event("t1", "CREDIT", "b")),
        ],
        [line_of(tx_event("t2", "b", "a")), line_of(ledger_event("t2", "DEBIT", "b"))],
    ]


def _settings(**overrides: Any) -> PublishSettings:
    values: dict[str, Any] = {
        "topic": "t.events",
        "key_strategy": "transaction_id",
        "rate": 0,
        "partitions": 1,
        "replication_factor": 1,
        "allow_non_empty_topic": False,
        "flush_timeout_seconds": 30.0,
    }
    values.update(overrides)
    return PublishSettings(**values)


def _rewrite_second_shard(root: Path, data: bytes) -> None:
    """Ghi lại file events shard 1 và cập nhật manifest cho nhất quán (sha256, số dòng LF)."""
    rel = "events/events_batch_00001.jsonl"
    (root / rel).write_bytes(data)
    manifest_path = root / "manifests" / "events_batch_00001.manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    manifest["files"] = [
        {
            "path": rel,
            "sha256": hashlib.sha256(data).hexdigest(),
            "physical_rows": data.count(b"\n"),
        }
    ]
    manifest_path.write_text(json.dumps(manifest), "utf-8")


# --- Key selector: dữ liệu hỏng phải là SourceDataError, không phải KeyError/JSONDecodeError ---


@pytest.mark.parametrize("strategy", ["transaction_id", "account_from"])
@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"   ",
        b"{not json",
        b"[1,2,3]",
        b'{"event_type":"TRANSACTION_CREATED"}',
        b'{"event_type":"TRANSACTION_CREATED","payload":{}}',
        b'{"event_type":"TRANSACTION_CREATED","payload":{"transaction_id":5}}',
        b'{"event_type":"SOMETHING_ELSE","payload":{"transaction_id":"t"}}',
        b'{"payload":{"transaction_id":"t"}}',
    ],
)
def test_malformed_lines_raise_source_data_error(strategy: str, raw: bytes) -> None:
    selector = KeySelector(strategy)
    selector.start_file()
    with pytest.raises(SourceDataError) as info:
        selector.select(raw, file_path="events/f.jsonl", line_number=9)
    assert "events/f.jsonl" in str(info.value) and "9" in str(info.value)


def test_transaction_event_without_account_from_is_source_data_error() -> None:
    selector = KeySelector("account_from")
    selector.start_file()
    raw = b'{"event_type":"TRANSACTION_CREATED","payload":{"transaction_id":"t"}}'
    with pytest.raises(SourceDataError, match="account_from"):
        selector.select(raw, file_path="events/f.jsonl", line_number=1)


# --- Kiểm toàn bộ nguồn trước khi gửi message đầu tiên ---

BAD_SECOND_SHARD = {
    "blank_line": line_of(tx_event("t2", "b", "a")) + b"\n\n",
    "broken_json": b"{broken json\n",
    "missing_transaction_id": (
        b'{"event_type":"TRANSACTION_CREATED","payload":{"account_from":"a"}}\n'
    ),
    "oversized_line": b'{"event_type":"TRANSACTION_CREATED","payload":{"transaction_id":"'
    + b"x" * 1_100_000
    + b'","account_from":"a"}}\n',
    "no_final_lf": line_of(tx_event("t2", "b", "a")),
}


@pytest.mark.parametrize("case", sorted(BAD_SECOND_SHARD))
def test_bad_source_is_rejected_before_first_message_is_sent(
    make_source: Factory, case: str
) -> None:
    root = make_source(_good())
    _rewrite_second_shard(root, BAD_SECOND_SHARD[case])
    producer, broker = FakeProducer(), FakeBroker(partitions=1)
    with pytest.raises(SourceDataError):
        publish_events(
            discover_event_files(root), settings=_settings(), broker=broker, producer=producer
        )
    assert producer.calls == [] and broker.calls == []  # shard 0 hợp lệ cũng chưa được gửi


def test_orphan_ledger_in_second_shard_is_rejected_before_sending(make_source: Factory) -> None:
    root = make_source(_good())
    _rewrite_second_shard(root, line_of(ledger_event("t1", "DEBIT", "a")) + b"\n")
    producer = FakeProducer()
    with pytest.raises(OrphanLedgerError):
        publish_events(
            discover_event_files(root),
            settings=_settings(key_strategy="account_from"),
            broker=FakeBroker(partitions=1),
            producer=producer,
        )
    assert producer.calls == []


def test_unknown_event_type_is_rejected_for_both_strategies(make_source: Factory) -> None:
    root = make_source(_good())
    _rewrite_second_shard(root, b'{"event_type":"X","payload":{"transaction_id":"t2"}}\n')
    for strategy in ("transaction_id", "account_from"):
        with pytest.raises(SourceDataError):
            publish_events(
                discover_event_files(root),
                settings=_settings(key_strategy=strategy),
                broker=FakeBroker(partitions=1),
                producer=FakeProducer(),
            )


def test_broken_manifest_json_is_source_data_error(make_source: Factory) -> None:
    root = make_source(_good())
    (root / "manifests" / "events_batch_00001.manifest.json").write_text("{oops", "utf-8")
    with pytest.raises(SourceDataError, match="manifest"):
        discover_event_files(root)


def test_unreadable_event_file_is_source_data_error(make_source: Factory) -> None:
    root = make_source(_good())
    path = root / "events" / "events_batch_00001.jsonl"
    source = discover_event_files(root)
    path.chmod(0)
    try:
        with pytest.raises(SourceDataError):
            publish_events(
                source,
                settings=_settings(),
                broker=FakeBroker(partitions=1),
                producer=FakeProducer(),
            )
    finally:
        path.chmod(0o644)


@pytest.mark.parametrize(
    "bad_path", ["events/../outside.jsonl", "events//../../x.jsonl", "/etc/hosts"]
)
def test_manifest_path_outside_source_dir_is_rejected(make_source: Factory, bad_path: str) -> None:
    root = make_source(_good())
    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": "n06-unit-test",
        "source_batch_id": str(uuid.uuid4()),
        "files": [{"path": bad_path, "sha256": "0" * 64, "physical_rows": 1}],
    }
    (root / "manifests" / "zz.manifest.json").write_text(json.dumps(manifest), "utf-8")
    with pytest.raises(SourceDataError, match="ngoài thư mục nguồn|không hợp lệ"):
        discover_event_files(root, events_dirname="events" if bad_path != "/etc/hosts" else "/etc")


# --- KafkaException và cấu hình producer ---


def test_sync_kafka_exception_from_produce_is_delivery_error_and_queue_is_flushed(
    make_source: Factory,
) -> None:
    source = discover_event_files(make_source(_good()))
    producer = FakeProducer(sync_error_at=2)
    with pytest.raises(DeliveryError, match="MSG_SIZE_TOO_LARGE"):
        publish_events(
            source, settings=_settings(), broker=FakeBroker(partitions=1), producer=producer
        )
    assert producer.flush_timeouts  # không bỏ rơi message còn trong hàng đợi producer


@pytest.mark.parametrize("retries", [0, -1])
def test_retries_below_one_is_rejected_for_idempotent_producer(retries: int) -> None:
    with pytest.raises(PublishConfigError, match="retries"):
        build_producer_config(bootstrap_servers="h:1", acks="all", retries=retries, batch_size=1)


@pytest.mark.parametrize("timeout", [0, -1, math.inf, math.nan])
def test_non_positive_or_non_finite_flush_timeout_exits_10(
    tmp_path: Path, make_source: Factory, timeout: float
) -> None:
    out = io.StringIO()
    env_file = tmp_path / "empty.env"
    env_file.write_text("", "utf-8")
    code = run_publish(
        profile="test",
        env_file=env_file,
        source_dir=make_source(_good()),
        flush_timeout_seconds=timeout,
        out=out,
        broker_factory=lambda _b: FakeBroker(partitions=1),
        producer_factory=lambda _c: FakeProducer(),
    )
    assert code == 10 and "flush" in out.getvalue()


def test_kafka_exception_creating_producer_exits_10(tmp_path: Path, make_source: Factory) -> None:
    def factory(_config: dict[str, Any]) -> Any:
        raise KafkaException(KafkaError(KafkaError._INVALID_ARG))

    out = io.StringIO()
    env_file = tmp_path / "empty.env"
    env_file.write_text("", "utf-8")
    code = run_publish(
        profile="test",
        env_file=env_file,
        source_dir=make_source(_good()),
        out=out,
        broker_factory=lambda _b: FakeBroker(partitions=1),
        producer_factory=factory,
    )
    assert code == 10


# --- Topic không rỗng: thông báo phải nêu cách dọn; in số message đã có trước khi gửi ---


def test_non_empty_refusal_explains_how_to_clean_the_topic(make_source: Factory) -> None:
    source = discover_event_files(make_source(_good()))
    with pytest.raises(PublishConfigError) as info:
        publish_events(
            source,
            settings=_settings(),
            broker=FakeBroker(partitions=1, end_offsets={0: 4}),
            producer=FakeProducer(),
        )
    message = str(info.value)
    assert "xóa topic" in message and "--topic" in message and "--allow-non-empty-topic" in message


class _ProbeProducer(FakeProducer):
    """Ghi lại những gì đã in ra stdout tại thời điểm message đầu tiên được gửi."""

    def __init__(self, out: io.StringIO) -> None:
        super().__init__()
        self._out = out
        self.printed_at_first_produce: str | None = None

    def produce(self, topic: str, *, value: bytes, key: bytes, on_delivery: Any) -> None:
        if self.printed_at_first_produce is None:
            self.printed_at_first_produce = self._out.getvalue()
        super().produce(topic, value=value, key=key, on_delivery=on_delivery)


def test_text_mode_prints_preexisting_messages_before_sending(
    tmp_path: Path, make_source: Factory
) -> None:
    out = io.StringIO()
    producer = _ProbeProducer(out)
    env_file = tmp_path / "empty.env"
    env_file.write_text("", "utf-8")
    code = run_publish(
        profile="test",
        env_file=env_file,
        source_dir=make_source(_good()),
        allow_non_empty_topic=True,
        out=out,
        broker_factory=lambda _b: FakeBroker(partitions=1, end_offsets={0: 7}),
        producer_factory=lambda _c: producer,
    )
    assert code == 0
    assert producer.printed_at_first_produce is not None
    assert "đã có 7 message" in producer.printed_at_first_produce
