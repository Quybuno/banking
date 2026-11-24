"""`confluent_kafka.Producer` thật chạy offline: bắt sai chữ ký và cấu hình (review T20).

Producer được tạo với `bootstrap.servers=localhost:1` (không có broker); `produce()` chỉ đưa
message vào hàng đợi nên không cần kết nối. Test này sẽ bắt lỗi chữ ký `produce` từng xảy ra ở bản
đầu (tham số thứ ba là partition, không phải key) mà client giả không bắt được.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException, Producer

from n06.producer.errors import DeliveryError, PublishConfigError
from n06.producer.kafka_client import (
    DEFAULT_MAX_MESSAGE_BYTES,
    MESSAGE_OVERHEAD_MARGIN_BYTES,
    build_producer_config,
    make_producer,
)
from n06.producer.publisher import PublishSettings, _Delivery, _produce


def _config(**overrides: Any) -> dict[str, Any]:
    config = build_producer_config(
        bootstrap_servers="localhost:1", acks="all", retries=5, batch_size=16384
    )
    config.update(overrides)
    return config


@pytest.fixture
def producer() -> Iterator[Producer]:
    real = make_producer(_config())
    yield real
    real.purge()
    real.flush(0)


def _settings() -> PublishSettings:
    return PublishSettings(
        topic="offline.topic",
        key_strategy="transaction_id",
        rate=0,
        partitions=1,
        replication_factor=1,
        allow_non_empty_topic=False,
        flush_timeout_seconds=5.0,
    )


def test_make_producer_hands_our_idempotent_config_to_the_real_client() -> None:
    """Producer thật nhận đúng cấu hình idempotent do `build_producer_config` sinh ra.

    Chứng minh idempotence có hiệu lực trên broker nằm ở
    `tests/integration/test_publish_kafka.py::test_real_producer_is_idempotent_on_the_broker_*`.
    """
    from unittest.mock import patch

    config = _config()
    with patch("n06.producer.kafka_client.Producer") as constructor:
        make_producer(config)
    (passed,), _ = constructor.call_args
    assert passed["enable.idempotence"] is True
    assert passed["acks"] == "all"
    assert passed["retries"] >= 1
    assert passed is config or passed == config


def test_publisher_produce_call_matches_real_signature(producer: Producer) -> None:
    delivery = _Delivery()
    _produce(producer, _settings(), b"key-1", b'{"v":1}', delivery, lambda: 0.0)
    # Message đã vào hàng đợi (chưa cần broker): purge sẽ báo đúng một lần qua callback.
    producer.purge()
    producer.flush(5)
    assert len(delivery.errors) == 1 and "PURGE" in delivery.errors[0]


def test_positional_key_would_have_been_a_partition_error(producer: Producer) -> None:
    """Chữ ký thật: tham số thứ ba là partition. Gọi sai kiểu như bản đầu thì phải lỗi."""
    with pytest.raises(TypeError):
        producer.produce("offline.topic", b"key", b"value", lambda e, m: None)  # type: ignore[misc]


def test_oversized_message_raises_kafka_exception_synchronously_and_is_delivery_error(
    producer: Producer,
) -> None:
    value = b"x" * (DEFAULT_MAX_MESSAGE_BYTES + 1)
    with pytest.raises(KafkaException) as info:
        producer.produce("offline.topic", value=value, key=b"k", on_delivery=lambda e, m: None)
    assert info.value.args[0].code() == KafkaError.MSG_SIZE_TOO_LARGE
    with pytest.raises(DeliveryError, match="MSG_SIZE_TOO_LARGE"):
        _produce(producer, _settings(), b"k", value, _Delivery(), lambda: 0.0)


def test_message_at_our_preflight_threshold_is_accepted_by_the_real_producer(
    producer: Producer,
) -> None:
    key = b"k"
    value = b"x" * (DEFAULT_MAX_MESSAGE_BYTES - MESSAGE_OVERHEAD_MARGIN_BYTES - len(key))
    producer.produce("offline.topic", value=value, key=key, on_delivery=lambda e, m: None)


def test_invalid_producer_configuration_becomes_config_error() -> None:
    with pytest.raises(PublishConfigError):
        make_producer(
            {"bootstrap.servers": "localhost:1", "enable.idempotence": True, "retries": 0}
        )
    with pytest.raises(PublishConfigError):
        make_producer({"bootstrap.servers": "localhost:1", "no.such.option": 1})
