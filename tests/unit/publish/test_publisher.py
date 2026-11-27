"""Vòng gửi: byte value, key, thứ tự, chạy lại, lỗi gửi (ADR-0010)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from n06.producer.errors import (
    BrokerUnavailableError,
    DeliveryError,
    OrphanLedgerError,
    PublishConfigError,
    SourceDataError,
)
from n06.producer.kafka_client import build_producer_config
from n06.producer.publisher import PublishSettings, publish_events
from n06.producer.source import discover_event_files

from .conftest import FakeBroker, FakeProducer, ledger_event, line_of, tx_event


def _settings(**overrides: object) -> PublishSettings:
    values: dict[str, object] = {
        "topic": "t.events",
        "key_strategy": "transaction_id",
        "rate": 0,
        "partitions": 3,
        "replication_factor": 1,
        "allow_non_empty_topic": False,
        "flush_timeout_seconds": 30.0,
    }
    values.update(overrides)
    return PublishSettings(**values)  # type: ignore[arg-type]


def _build_shards() -> list[list[bytes]]:
    replay = line_of(tx_event("t1", "a", "b", event_id="e1"))
    return [
        [
            replay,
            replay,  # event_replay: cùng bytes
            line_of(ledger_event("t1", "DEBIT", "a")),
            line_of(ledger_event("t1", "CREDIT", "b")),
        ],
        [
            line_of(tx_event("t2", "b", "a")),
            line_of(tx_event("t2", "b", "a")),  # business replay: event_id khác
            line_of(ledger_event("t2", "DEBIT", "b")),
        ],
    ]


_SHARDS = _build_shards()  # một lần: event_id/entry_id ngẫu nhiên phải cố định trong suốt test


def _shards() -> list[list[bytes]]:
    return _SHARDS


@pytest.fixture
def source(make_source: Callable[..., Path]):
    return discover_event_files(make_source(_shards()))


def _all_lines() -> list[bytes]:
    return [line for shard in _shards() for line in shard]


def _run(source, **kwargs):  # type: ignore[no-untyped-def]
    producer = kwargs.pop("producer", None) or FakeProducer()
    broker = kwargs.pop("broker", None) or FakeBroker(partitions=3)
    stats = publish_events(
        source,
        settings=kwargs.pop("settings", _settings()),
        broker=broker,
        producer=producer,
        **kwargs,
    )
    return stats, producer, broker


def test_values_are_exact_file_bytes_in_file_order_including_replays(source) -> None:  # type: ignore[no-untyped-def]
    stats, producer, _ = _run(source)
    values = [v for _, _, v in producer.calls]
    assert values == [line for shard in _shards() for line in shard]
    assert all(not v.endswith(b"\n") for v in values)
    assert stats.messages_delivered == stats.messages_expected == 7


def test_changing_key_strategy_changes_keys_but_never_values(make_source) -> None:  # type: ignore[no-untyped-def]
    source = discover_event_files(make_source(_shards()))
    by_tx = _run(source, settings=_settings(key_strategy="transaction_id"))[1]
    by_acc = _run(source, settings=_settings(key_strategy="account_from"))[1]
    assert [v for _, _, v in by_tx.calls] == [v for _, _, v in by_acc.calls]
    assert [k for _, k, _ in by_tx.calls] == [b"t1", b"t1", b"t1", b"t1", b"t2", b"t2", b"t2"]
    # account_from: t1 gửi từ a; t2 gửi từ b; ledger CREDIT của t1 cũng theo "a".
    assert [k for _, k, _ in by_acc.calls] == [b"a", b"a", b"a", b"a", b"b", b"b", b"b"]


def test_orphan_ledger_in_a_shard_stops_before_sending_anything_wrong(make_source) -> None:  # type: ignore[no-untyped-def]
    shards = [[line_of(tx_event("t1", "a", "b"))], [line_of(ledger_event("t1", "DEBIT", "a"))]]
    source = discover_event_files(make_source(shards))
    producer = FakeProducer()
    with pytest.raises(OrphanLedgerError, match="không nhất quán"):
        _run(source, producer=producer, settings=_settings(key_strategy="account_from"))
    assert producer.calls == []  # kiểm xong toàn bộ dữ liệu và key trước khi gửi message đầu tiên


def test_missing_topic_is_created_with_configured_partitions(source) -> None:  # type: ignore[no-untyped-def]
    broker = FakeBroker(partitions=None)
    _run(source, broker=broker, settings=_settings(partitions=6, replication_factor=1))
    assert broker.created == [("t.events", 6, 1)]


def test_existing_topic_with_other_partition_count_is_rejected(source) -> None:  # type: ignore[no-untyped-def]
    producer = FakeProducer()
    with pytest.raises(PublishConfigError, match="partition"):
        _run(source, broker=FakeBroker(partitions=1), producer=producer)
    assert producer.calls == []


def test_non_empty_topic_is_refused_by_default(source) -> None:  # type: ignore[no-untyped-def]
    producer = FakeProducer()
    broker = FakeBroker(partitions=3, end_offsets={0: 5, 1: 0, 2: 2})
    with pytest.raises(PublishConfigError) as info:
        _run(source, broker=broker, producer=producer)
    assert "t.events" in str(info.value) and "7" in str(info.value)
    assert "--allow-non-empty-topic" in str(info.value)
    assert producer.calls == []


def test_non_empty_topic_is_allowed_only_with_explicit_flag(source) -> None:  # type: ignore[no-untyped-def]
    broker = FakeBroker(partitions=3, end_offsets={0: 5, 1: 0, 2: 2})
    stats, producer, _ = _run(source, broker=broker, settings=_settings(allow_non_empty_topic=True))
    assert len(producer.calls) == 7
    assert stats.preexisting_messages == 7 and stats.allow_non_empty_topic is True


def test_empty_topic_needs_no_flag(source) -> None:  # type: ignore[no-untyped-def]
    stats, _, _ = _run(source, broker=FakeBroker(partitions=3, end_offsets={0: 0, 1: 0, 2: 0}))
    assert stats.preexisting_messages == 0


def test_integrity_failure_happens_before_any_broker_call(make_source) -> None:  # type: ignore[no-untyped-def]
    root = make_source(_shards())
    path = root / "events" / "events_batch_00000.jsonl"
    path.write_bytes(path.read_bytes() + b"{}\n")
    broker, producer = FakeBroker(partitions=3), FakeProducer()
    with pytest.raises(SourceDataError):
        _run(discover_event_files(root), broker=broker, producer=producer)
    assert broker.calls == [] and producer.calls == []


def test_unavailable_broker_surfaces_as_broker_error(source) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(BrokerUnavailableError):
        _run(source, broker=FakeBroker(unavailable=True))


def test_buffer_full_retries_with_the_same_bytes(source) -> None:  # type: ignore[no-untyped-def]
    producer = FakeProducer(buffer_errors=3)
    stats, producer, _ = _run(source, producer=producer)
    assert stats.messages_delivered == 7
    assert [v for _, _, v in producer.calls] == _all_lines()
    assert 0.5 in producer.polls  # đã poll chờ khi hàng đợi đầy


def test_buffer_full_forever_fails_with_delivery_error(source) -> None:  # type: ignore[no-untyped-def]
    class Clock:
        now = 0.0

        def __call__(self) -> float:
            Clock.now += 10.0
            return Clock.now

    with pytest.raises(DeliveryError, match="Hàng đợi"):
        _run(source, producer=FakeProducer(always_buffer_full=True), clock=Clock())


def test_delivery_error_stops_early_and_is_reported(source) -> None:  # type: ignore[no-untyped-def]
    producer = FakeProducer(fail_delivery_at=2)
    with pytest.raises(DeliveryError, match="MSG_TIMED_OUT"):
        _run(source, producer=producer)
    assert len(producer.calls) < 7


def test_messages_left_after_flush_is_a_delivery_error(source) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(DeliveryError, match="còn 2 chưa giao"):
        _run(source, producer=FakeProducer(undelivered_after_flush=2))


def test_rate_limit_is_applied_with_injected_clock(source) -> None:  # type: ignore[no-untyped-def]
    sleeps: list[float] = []
    now = [0.0]

    def clock() -> float:
        return now[0]

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    stats, _, _ = _run(source, settings=_settings(rate=7), clock=clock, sleep=sleep)
    assert len(sleeps) == 6  # 7 message, message đầu gửi ngay
    assert sum(sleeps) == pytest.approx(6 / 7)
    assert stats.target_rate == 7
    assert stats.achieved_rate == pytest.approx(7 / (6 / 7), rel=0.01)


class _TimedProducer(FakeProducer):
    """Đồng hồ giả: thời gian chỉ trôi khi publisher chờ qua `producer.poll` (đường chờ thật,
    không tiêm `sleep`). Ghi lại thời điểm gửi từng message."""

    def __init__(self) -> None:
        super().__init__()
        self.now = 0.0
        self.sent_at: list[float] = []

    def clock(self) -> float:
        return self.now

    def produce(self, topic: str, *, value: bytes, key: bytes, on_delivery: Any) -> None:
        self.sent_at.append(self.now)
        super().produce(topic, value=value, key=key, on_delivery=on_delivery)

    def poll(self, timeout: float) -> int:
        if timeout > 0:
            self.now += max(timeout, 1e-9)  # bước tối thiểu để đồng hồ giả luôn tiến
        return super().poll(timeout)


def test_no_message_is_sent_before_its_slot(source) -> None:  # type: ignore[no-untyped-def]
    producer = _TimedProducer()
    stats, _, _ = _run(source, settings=_settings(rate=7), producer=producer, clock=producer.clock)
    # 7 message ở 7/s: message thứ k (từ 0) không được gửi trước k/7 giây.
    assert len(producer.sent_at) == 7
    for k, sent_at in enumerate(producer.sent_at):
        assert sent_at >= k / 7 - 1e-6, (k, producer.sent_at)
    assert stats.elapsed_seconds >= 6 / 7 - 1e-6


def test_zero_rate_never_waits(source) -> None:  # type: ignore[no-untyped-def]
    producer = _TimedProducer()
    stats, _, _ = _run(source, settings=_settings(rate=0), producer=producer, clock=producer.clock)
    assert producer.sent_at == [0.0] * 7
    assert producer.now == 0.0 and all(timeout == 0 for timeout in producer.polls)
    assert stats.target_rate == 0 and stats.messages_delivered == 7


def test_producer_config_is_idempotent_and_rejects_other_acks() -> None:
    config = build_producer_config(bootstrap_servers="h:1", acks="all", retries=5, batch_size=16384)
    assert config["enable.idempotence"] is True and config["acks"] == "all"
    assert config["retries"] == 5 and config["bootstrap.servers"] == "h:1"
    with pytest.raises(PublishConfigError, match="idempotent"):
        build_producer_config(bootstrap_servers="h:1", acks="1", retries=5, batch_size=1)
