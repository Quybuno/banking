"""`ConfluentBroker` với admin/consumer giả: lỗi tạm thời, vĩnh viễn, ánh xạ lỗi (review T20)."""

from __future__ import annotations

from concurrent.futures import Future
from types import SimpleNamespace
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from n06.producer.errors import BrokerUnavailableError, PublishConfigError
from n06.producer.kafka_client import ConfluentBroker


def _topic(error: int | None, partitions: int = 3) -> Any:
    return SimpleNamespace(
        error=None if error is None else KafkaError(error),
        partitions={i: object() for i in range(partitions)},
    )


class FakeAdmin:
    def __init__(self, scripted: list[dict[str, Any]], create_result: Any = None) -> None:
        self._scripted = scripted
        self.list_calls = 0
        self.create_result = create_result

    def list_topics(self, timeout: float) -> Any:
        index = min(self.list_calls, len(self._scripted) - 1)
        self.list_calls += 1
        return SimpleNamespace(topics=self._scripted[index])

    def create_topics(self, new_topics: list[Any]) -> dict[str, Future[None]]:
        future: Future[None] = Future()
        if isinstance(self.create_result, BaseException):
            future.set_exception(self.create_result)
        else:
            future.set_result(None)
        return {new_topics[0].topic: future}


class Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _broker(admin: Any, consumer: Any = None, clock: Clock | None = None) -> ConfluentBroker:
    clock = clock or Clock()
    return ConfluentBroker(
        "h:1",
        admin=admin,
        consumer_factory=(lambda: consumer),
        clock=clock,
        sleep=clock.sleep,
    )


def test_missing_topic_is_none() -> None:
    assert _broker(FakeAdmin([{}])).partition_count("t") is None


def test_unknown_topic_error_means_not_existing() -> None:
    admin = FakeAdmin([{"t": _topic(KafkaError.UNKNOWN_TOPIC_OR_PART)}])
    assert _broker(admin).partition_count("t") is None


def test_transient_metadata_error_is_retried_then_succeeds() -> None:
    admin = FakeAdmin(
        [
            {"t": _topic(KafkaError.LEADER_NOT_AVAILABLE)},
            {"t": _topic(KafkaError.LEADER_NOT_AVAILABLE)},
            {"t": _topic(None, partitions=6)},
        ]
    )
    assert _broker(admin).partition_count("t") == 6
    assert admin.list_calls == 3


def test_persistent_transient_metadata_error_never_assumes_topic_is_missing() -> None:
    admin = FakeAdmin([{"t": _topic(KafkaError.LEADER_NOT_AVAILABLE)}])
    with pytest.raises(BrokerUnavailableError, match="metadata"):
        _broker(admin).partition_count("t")


def test_end_offsets_of_missing_topic_is_an_error_not_empty() -> None:
    with pytest.raises(BrokerUnavailableError):
        _broker(FakeAdmin([{}])).end_offsets("t")


class FakeConsumer:
    def __init__(self, errors: list[int], high: int = 5) -> None:
        self.errors = list(errors)
        self.high = high
        self.calls = 0
        self.closed = False

    def get_watermark_offsets(self, partition: Any, timeout: float) -> tuple[int, int]:
        self.calls += 1
        if self.errors:
            raise KafkaException(KafkaError(self.errors.pop(0)))
        return 0, self.high

    def close(self) -> None:
        self.closed = True


def test_high_watermark_retries_transient_errors_then_returns() -> None:
    consumer = FakeConsumer([KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE])
    clock = Clock()
    broker = _broker(FakeAdmin([{"t": _topic(None, partitions=1)}]), consumer, clock)
    assert broker.end_offsets("t") == {0: 5}
    assert consumer.calls == 3 and len(clock.sleeps) == 2 and consumer.closed


def test_high_watermark_non_transient_error_is_not_retried() -> None:
    consumer = FakeConsumer([KafkaError.TOPIC_AUTHORIZATION_FAILED])
    broker = _broker(FakeAdmin([{"t": _topic(None, partitions=1)}]), consumer)
    with pytest.raises(BrokerUnavailableError):
        broker.end_offsets("t")
    assert consumer.calls == 1 and consumer.closed


def test_high_watermark_gives_up_after_deadline() -> None:
    consumer = FakeConsumer([KafkaError.NOT_LEADER_FOR_PARTITION] * 10_000)
    broker = _broker(FakeAdmin([{"t": _topic(None, partitions=1)}]), consumer)
    with pytest.raises(BrokerUnavailableError):
        broker.end_offsets("t")
    assert consumer.closed


def test_replication_factor_above_broker_count_is_a_config_error() -> None:
    error = KafkaException(KafkaError(KafkaError.INVALID_REPLICATION_FACTOR))
    broker = _broker(FakeAdmin([{}], create_result=error))
    with pytest.raises(PublishConfigError, match="replication"):
        broker.create_topic("t", 3, 3)


def test_create_topic_timeout_is_broker_unavailable() -> None:
    broker = _broker(FakeAdmin([{}], create_result=TimeoutError()))
    with pytest.raises(BrokerUnavailableError):
        broker.create_topic("t", 3, 1)


def test_create_topic_already_exists_is_tolerated() -> None:
    error = KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS))
    _broker(FakeAdmin([{}], create_result=error)).create_topic("t", 3, 1)


def test_end_offsets_waits_until_a_just_created_topic_shows_in_metadata() -> None:
    """Metadata của broker có thể chậm hơn lệnh tạo topic: hai lần đọc đầu chưa thấy topic."""
    admin = FakeAdmin([{}, {}, {"t": _topic(None, partitions=1)}])
    clock = Clock()

    assert _broker(admin, FakeConsumer([], high=0), clock).end_offsets("t") == {0: 0}
    assert admin.list_calls == 3
    assert len(clock.sleeps) == 2


def test_end_offsets_gives_up_on_a_topic_that_never_appears() -> None:
    clock = Clock()
    with pytest.raises(BrokerUnavailableError, match="không tồn tại"):
        _broker(FakeAdmin([{}]), clock=clock).end_offsets("t")
    # Chờ có hạn: đúng bằng hạn chờ leader mặc định (20 s) cộng tối đa một bước thử lại.
    assert 20.0 <= clock.now <= 20.5
