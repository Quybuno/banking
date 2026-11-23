"""Cầu nối tới `confluent-kafka` (ADR-0010): admin, đếm offset và producer idempotent."""

from __future__ import annotations

import time
import uuid
from typing import Any, Protocol

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from n06.producer.errors import BrokerUnavailableError, PublishConfigError

CLIENT_ID = "n06-publish"
METADATA_TIMEOUT_SECONDS = 10.0
LEADER_WAIT_SECONDS = 20.0
# Topic vừa tạo cần một lúc để bầu leader; các lỗi này là tạm thời và được thử lại.
_TRANSIENT_ERRORS = frozenset(
    {
        KafkaError.NOT_LEADER_FOR_PARTITION,
        KafkaError.LEADER_NOT_AVAILABLE,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError._UNKNOWN_PARTITION,
    }
)


class ProducerLike(Protocol):
    def produce(self, topic: str, *, value: bytes, key: bytes, on_delivery: Any) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class BrokerAdmin(Protocol):
    def partition_count(self, topic: str) -> int | None:
        """Số partition của topic, hoặc None nếu topic chưa tồn tại."""

    def create_topic(self, topic: str, partitions: int, replication_factor: int) -> None: ...

    def end_offsets(self, topic: str) -> dict[int, int]:
        """High watermark của từng partition."""


def build_producer_config(
    *, bootstrap_servers: str, acks: str, retries: int, batch_size: int
) -> dict[str, Any]:
    """Cấu hình producer idempotent. `acks` khác `all` mâu thuẫn với idempotence nên bị từ chối."""
    if acks != "all":
        raise PublishConfigError(
            f"producer.acks={acks!r} không tương thích với producer idempotent; yêu cầu 'all'."
        )
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": CLIENT_ID,
        "enable.idempotence": True,
        "acks": acks,
        "retries": retries,
        "batch.size": batch_size,
    }


def make_producer(config: dict[str, Any]) -> Producer:
    return Producer(config)


class ConfluentBroker:
    """Triển khai `BrokerAdmin` bằng AdminClient và Consumer (chỉ đọc watermark)."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._bootstrap = bootstrap_servers
        self._admin = AdminClient({"bootstrap.servers": bootstrap_servers, "client.id": CLIENT_ID})

    def partition_count(self, topic: str) -> int | None:
        try:
            metadata = self._admin.list_topics(timeout=METADATA_TIMEOUT_SECONDS)
        except KafkaException as exc:
            raise BrokerUnavailableError(
                f"Không kết nối được broker {self._bootstrap}: {exc}"
            ) from exc
        found = metadata.topics.get(topic)
        if found is None or (found.error is not None and found.error.code() != KafkaError.NO_ERROR):
            return None
        return len(found.partitions)

    def create_topic(self, topic: str, partitions: int, replication_factor: int) -> None:
        futures = self._admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)]
        )
        try:
            futures[topic].result(timeout=METADATA_TIMEOUT_SECONDS)
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                return
            raise BrokerUnavailableError(f"Không tạo được topic {topic}: {exc}") from exc

    def end_offsets(self, topic: str) -> dict[int, int]:
        count = self.partition_count(topic)
        if count is None:
            return {}
        consumer = Consumer(
            {
                "bootstrap.servers": self._bootstrap,
                "group.id": f"{CLIENT_ID}-probe-{uuid.uuid4()}",
                "enable.auto.commit": False,
                "client.id": CLIENT_ID,
            }
        )
        try:
            result: dict[int, int] = {}
            for partition in range(count):
                result[partition] = self._high_watermark(consumer, topic, partition)
            return result
        except KafkaException as exc:
            raise BrokerUnavailableError(f"Không đọc được offset của topic {topic}: {exc}") from exc
        finally:
            consumer.close()

    @staticmethod
    def _high_watermark(consumer: Consumer, topic: str, partition: int) -> int:
        deadline = time.monotonic() + LEADER_WAIT_SECONDS
        while True:
            try:
                _, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=METADATA_TIMEOUT_SECONDS
                )
                return int(high)
            except KafkaException as exc:
                error = exc.args[0] if exc.args else None
                transient = error is not None and error.code() in _TRANSIENT_ERRORS
                if not transient or time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
