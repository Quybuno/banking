"""Vòng gửi events lên Kafka bằng producer idempotent (TASK T20, ADR-0010)."""

from __future__ import annotations

import math
import time
from array import array
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from n06.producer.errors import DeliveryError, PublishConfigError
from n06.producer.kafka_client import BrokerAdmin, ProducerLike
from n06.producer.keys import KeySelector
from n06.producer.rate import RateLimiter
from n06.producer.source import EventSource, iter_lines, verify_event_files

_BUFFER_FULL_POLL_SECONDS = 0.5


@dataclass(frozen=True)
class PublishSettings:
    topic: str
    key_strategy: str
    rate: int
    partitions: int
    replication_factor: int
    allow_non_empty_topic: bool
    flush_timeout_seconds: float


@dataclass(frozen=True)
class PublishStats:
    topic: str
    key_strategy: str
    partitions: int
    files: int
    messages_expected: int
    messages_delivered: int
    bytes_delivered: int
    preexisting_messages: int
    allow_non_empty_topic: bool
    target_rate: int
    achieved_rate: float
    elapsed_seconds: float
    latency_ms_p50: float
    latency_ms_p95: float
    latency_ms_p99: float

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


class _Delivery:
    """Gom kết quả giao nhận từ callback của producer."""

    def __init__(self) -> None:
        self.delivered = 0
        self.bytes = 0
        self.errors: list[str] = []
        self.latencies_ns = array("q")

    def callback(self, started_ns: int, size: int) -> Callable[[Any, Any], None]:
        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                self.errors.append(str(err))
                return
            self.delivered += 1
            self.bytes += size
            self.latencies_ns.append(time.perf_counter_ns() - started_ns)

        return on_delivery


def _percentile_ms(sorted_ns: list[int], fraction: float) -> float:
    if not sorted_ns:
        return 0.0
    index = max(0, math.ceil(fraction * len(sorted_ns)) - 1)
    return sorted_ns[index] / 1_000_000


def prepare_topic(broker: BrokerAdmin, settings: PublishSettings) -> int:
    """Tạo topic nếu chưa có, kiểm số partition và quy tắc chạy lại; trả số message hiện có."""
    existing = broker.partition_count(settings.topic)
    if existing is None:
        broker.create_topic(settings.topic, settings.partitions, settings.replication_factor)
    elif existing != settings.partitions:
        raise PublishConfigError(
            f"Topic {settings.topic} đã tồn tại với {existing} partition, cấu hình yêu cầu "
            f"{settings.partitions} (kafka.num_partitions)."
        )
    preexisting = sum(broker.end_offsets(settings.topic).values())
    if preexisting > 0 and not settings.allow_non_empty_topic:
        raise PublishConfigError(
            f"Topic {settings.topic} đã có {preexisting} message; từ chối publish để không gửi "
            "trùng toàn bộ dữ liệu. Dùng --allow-non-empty-topic nếu cố ý phát lại."
        )
    return preexisting


def publish_events(
    source: EventSource,
    *,
    settings: PublishSettings,
    broker: BrokerAdmin,
    producer: ProducerLike,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> PublishStats:
    """Kiểm manifest, chuẩn bị topic, gửi từng dòng theo thứ tự file rồi flush và đo."""
    verify_event_files(source)
    selector = KeySelector(settings.key_strategy)
    limiter = RateLimiter(settings.rate, clock=clock, sleep=sleep)
    preexisting = prepare_topic(broker, settings)

    delivery = _Delivery()
    started = clock()
    for item in source.files:
        selector.start_file()
        for number, value in iter_lines(item):
            key = selector.select(value, file_path=item.relative_path, line_number=number)
            limiter.acquire()
            _produce(producer, settings, key, value, delivery, clock)
            producer.poll(0)
            if delivery.errors:
                break
        if delivery.errors:
            break

    remaining = producer.flush(settings.flush_timeout_seconds)
    elapsed = max(clock() - started, 1e-9)
    if delivery.errors or remaining > 0 or delivery.delivered != source.expected_messages:
        first = delivery.errors[0] if delivery.errors else "không có"
        raise DeliveryError(
            f"Giao nhận không hoàn tất: đã giao {delivery.delivered}/{source.expected_messages} "
            f"message, còn {remaining} chưa giao sau flush, {len(delivery.errors)} lỗi "
            f"(lỗi đầu tiên: {first})."
        )

    latencies = sorted(delivery.latencies_ns)
    return PublishStats(
        topic=settings.topic,
        key_strategy=settings.key_strategy,
        partitions=settings.partitions,
        files=len(source.files),
        messages_expected=source.expected_messages,
        messages_delivered=delivery.delivered,
        bytes_delivered=delivery.bytes,
        preexisting_messages=preexisting,
        allow_non_empty_topic=settings.allow_non_empty_topic,
        target_rate=settings.rate,
        achieved_rate=delivery.delivered / elapsed,
        elapsed_seconds=elapsed,
        latency_ms_p50=_percentile_ms(latencies, 0.50),
        latency_ms_p95=_percentile_ms(latencies, 0.95),
        latency_ms_p99=_percentile_ms(latencies, 0.99),
    )


def _produce(
    producer: ProducerLike,
    settings: PublishSettings,
    key: bytes,
    value: bytes,
    delivery: _Delivery,
    clock: Callable[[], float],
) -> None:
    """Gửi một message; hàng đợi đầy thì poll rồi gửi lại cùng đối tượng byte, có hạn chót."""
    callback = delivery.callback(time.perf_counter_ns(), len(value))
    deadline = clock() + settings.flush_timeout_seconds
    while True:
        try:
            producer.produce(settings.topic, value=value, key=key, on_delivery=callback)
            return
        except BufferError:
            if clock() > deadline:
                raise DeliveryError(
                    "Hàng đợi producer đầy quá thời hạn flush; broker không nhận message."
                ) from None
            producer.poll(_BUFFER_FULL_POLL_SECONDS)
