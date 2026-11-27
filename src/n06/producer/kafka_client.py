"""Cầu nối tới `confluent-kafka` (ADR-0010): admin, đếm offset và producer idempotent."""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable
from typing import Any, Protocol

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from n06.producer.errors import BrokerUnavailableError, PublishConfigError

CLIENT_ID = "n06-publish"
# Mặc định của thư viện; `n06 publish` luôn truyền giá trị từ `config/` (producer.*), và
# `tests/unit/publish/test_review_fixes.py` khẳng định hai bên không lệch nhau.
DEFAULT_METADATA_TIMEOUT_SECONDS = 10.0
DEFAULT_LEADER_WAIT_SECONDS = 20.0
RETRY_SLEEP_SECONDS = 0.2
# Giới hạn kích thước message (key + value) của producer; librdkafka từ chối message lớn hơn
# `message.max.bytes` ngay khi gọi produce(). Chừa biên cho phần overhead của bản ghi.
DEFAULT_MAX_MESSAGE_BYTES = 1_000_000
MESSAGE_OVERHEAD_MARGIN_BYTES = 1024
# Topic vừa tạo cần một lúc để bầu leader; các lỗi này là tạm thời và được thử lại có hạn.
_TRANSIENT_ERRORS = frozenset(
    {
        KafkaError.NOT_LEADER_FOR_PARTITION,
        KafkaError.LEADER_NOT_AVAILABLE,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError._UNKNOWN_PARTITION,
    }
)
# Lỗi tạo topic do cấu hình không hợp lệ với cụm broker (ví dụ replication_factor > số broker).
_CONFIG_ERRORS = frozenset(
    {
        KafkaError.INVALID_REPLICATION_FACTOR,
        KafkaError.INVALID_PARTITIONS,
        KafkaError.INVALID_CONFIG,
        KafkaError.INVALID_REQUEST,
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
    *,
    bootstrap_servers: str,
    acks: str,
    retries: int,
    batch_size: int,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
) -> dict[str, Any]:
    """Cấu hình producer idempotent; `acks` khác `all` hoặc `retries < 1` bị từ chối."""
    if acks != "all":
        raise PublishConfigError(
            "producer.acks không tương thích với producer idempotent; yêu cầu 'all'."
        )
    if retries < 1:
        raise PublishConfigError(
            "producer.retries không tương thích với producer idempotent; yêu cầu "
            "retries >= 1 (librdkafka từ chối khởi tạo)."
        )
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": CLIENT_ID,
        "enable.idempotence": True,
        "acks": acks,
        "retries": retries,
        "batch.size": batch_size,
        "message.max.bytes": max_message_bytes,
    }


_PROPERTY_PATTERN = re.compile(r'configuration property "([a-z0-9._]+)"', re.IGNORECASE)


def rejected_producer_message(exc: BaseException) -> str:
    """Thông báo khi librdkafka từ chối cấu hình: chỉ nêu tên thuộc tính.

    Nội dung lỗi gốc có thể trích lại giá trị bị từ chối nên không in.
    """
    names = _PROPERTY_PATTERN.findall(str(exc))
    where = f" ở thuộc tính {names[-1]}" if names else ""
    return (
        f"Cấu hình producer bị từ chối{where} ({type(exc).__name__}); kiểm mục producer.* và "
        "kafka.* trong cấu hình."
    )


def make_producer(config: dict[str, Any]) -> Producer:
    """Tạo producer thật; cấu hình bị librdkafka từ chối là lỗi cấu hình (mã 10)."""
    try:
        return Producer(config)
    except (KafkaException, ValueError, TypeError) as exc:
        raise PublishConfigError(rejected_producer_message(exc)) from exc


def _default_consumer_factory(bootstrap_servers: str) -> Callable[[], Consumer]:
    def factory() -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"{CLIENT_ID}-probe-{uuid.uuid4()}",
                "enable.auto.commit": False,
                "client.id": CLIENT_ID,
            }
        )

    return factory


class ConfluentBroker:
    """Triển khai `BrokerAdmin` bằng AdminClient và Consumer (chỉ đọc watermark).

    `admin`, `consumer_factory`, `clock`, `sleep` có thể thay thế để kiểm thử.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        admin: Any = None,
        consumer_factory: Callable[[], Any] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        metadata_timeout_seconds: float = DEFAULT_METADATA_TIMEOUT_SECONDS,
        leader_wait_seconds: float = DEFAULT_LEADER_WAIT_SECONDS,
    ) -> None:
        self._metadata_timeout = metadata_timeout_seconds
        self._leader_wait = leader_wait_seconds
        self._bootstrap = bootstrap_servers
        self._admin = (
            admin
            if admin is not None
            else AdminClient({"bootstrap.servers": bootstrap_servers, "client.id": CLIENT_ID})
        )
        self._consumer_factory = consumer_factory or _default_consumer_factory(bootstrap_servers)
        self._clock = clock
        self._sleep = sleep

    def partition_count(self, topic: str) -> int | None:
        """None chỉ khi broker xác nhận topic không tồn tại; lỗi metadata tạm thời được thử lại
        có hạn rồi dừng (không bao giờ coi là "topic rỗng")."""
        deadline = self._clock() + self._leader_wait
        while True:
            try:
                metadata = self._admin.list_topics(timeout=self._metadata_timeout)
            except KafkaException as exc:
                raise BrokerUnavailableError(
                    f"Không kết nối được broker {self._bootstrap}: {exc}"
                ) from exc
            found = metadata.topics.get(topic)
            if found is None:
                return None
            code = found.error.code() if found.error is not None else KafkaError.NO_ERROR
            if code == KafkaError.NO_ERROR:
                return len(found.partitions)
            if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return None
            if code in _TRANSIENT_ERRORS and self._clock() <= deadline:
                self._sleep(RETRY_SLEEP_SECONDS)
                continue
            if code == KafkaError.TOPIC_EXCEPTION:
                raise BrokerUnavailableError(
                    "Broker từ chối tên topic ở kafka.events_topic (TOPIC_EXCEPTION)."
                )
            raise BrokerUnavailableError(
                f"Không lấy được metadata của topic {topic} (lỗi {found.error})."
            )

    def create_topic(self, topic: str, partitions: int, replication_factor: int) -> None:
        futures = self._admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)]
        )
        try:
            futures[topic].result(timeout=self._metadata_timeout)
        except KafkaException as exc:
            code = exc.args[0].code() if exc.args else None
            if code == KafkaError.TOPIC_ALREADY_EXISTS:
                return
            if code in _CONFIG_ERRORS:
                raise PublishConfigError(
                    "Cụm broker từ chối tạo topic; kiểm kafka.events_topic, "
                    "kafka.num_partitions, kafka.replication_factor "
                    f"({exc.args[0].name()})."
                ) from exc
            if code == KafkaError.TOPIC_EXCEPTION:
                raise BrokerUnavailableError(
                    "Broker từ chối tên topic ở kafka.events_topic (TOPIC_EXCEPTION)."
                ) from exc
            raise BrokerUnavailableError(f"Không tạo được topic {topic}: {exc}") from exc
        except TimeoutError as exc:
            raise BrokerUnavailableError(f"Hết giờ khi tạo topic {topic}.") from exc

    def _expected_partition_count(self, topic: str) -> int:
        """Số partition của topic phải đã tồn tại.

        Lệnh tạo topic trả về trước khi metadata của broker kịp có topic mới, nên "chưa thấy"
        được thử lại tới hạn `leader_wait_seconds` rồi mới coi là lỗi.
        """
        deadline = self._clock() + self._leader_wait
        while True:
            count = self.partition_count(topic)
            if count is not None:
                return count
            if self._clock() >= deadline:
                raise BrokerUnavailableError(
                    f"Topic {topic} không tồn tại khi đọc offset (đã chờ {self._leader_wait:g} s)."
                )
            self._sleep(RETRY_SLEEP_SECONDS)

    def end_offsets(self, topic: str) -> dict[int, int]:
        count = self._expected_partition_count(topic)
        consumer = None
        try:
            # Dựng consumer cũng có thể ném KafkaException: phải nằm trong khối này để thành mã 14.
            consumer = self._consumer_factory()
            return {
                partition: self._high_watermark(consumer, topic, partition)
                for partition in range(count)
            }
        except KafkaException as exc:
            raise BrokerUnavailableError(f"Không đọc được offset của topic {topic}: {exc}") from exc
        finally:
            if consumer is not None:
                consumer.close()

    def _high_watermark(self, consumer: Any, topic: str, partition: int) -> int:
        deadline = self._clock() + self._leader_wait
        while True:
            try:
                _, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=self._metadata_timeout
                )
                return int(high)
            except KafkaException as exc:
                error = exc.args[0] if exc.args else None
                transient = error is not None and error.code() in _TRANSIENT_ERRORS
                if not transient or self._clock() > deadline:
                    raise
                self._sleep(RETRY_SLEEP_SECONDS)
