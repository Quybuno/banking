"""Test bổ sung T20 sau review độc lập: dữ liệu nguồn xấu, lỗi giữa lúc gửi, độ trễ, cấu hình.

Expected viết tay theo hành vi yêu cầu (lỗi dữ liệu nguồn là mã 10, không traceback; độ trễ
giao nhận không cộng khoảng chờ giới hạn tốc độ).
"""

from __future__ import annotations

import io
import json
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from n06.cli.publish import run_publish
from n06.config import load_config
from n06.producer import kafka_client
from n06.producer.errors import BrokerUnavailableError, SourceDataError
from n06.producer.kafka_client import (
    DEFAULT_LEADER_WAIT_SECONDS,
    DEFAULT_MAX_MESSAGE_BYTES,
    DEFAULT_METADATA_TIMEOUT_SECONDS,
    ConfluentBroker,
    build_producer_config,
)
from n06.producer.keys import KeySelector
from n06.producer.publisher import PublishSettings, publish_events
from n06.producer.source import discover_event_files, verify_event_files

from .conftest import FakeBroker, FakeProducer, line_of, tx_event

TX = "TRANSACTION_CREATED"
CREDENTIALS = {
    "N06_OBJECT_STORE_ACCESS_KEY": "ak",
    "N06_OBJECT_STORE_SECRET_KEY": "sk",
    "N06_POSTGRES_MART_PASSWORD": "pw",
    "N06_POSTGRES_CONTROL_PASSWORD": "pw",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "pw",
    "N06_POSTGRES_METABASE_PASSWORD": "pw",
}


def _settings(**overrides: object) -> PublishSettings:
    values: dict[str, object] = {
        "topic": "t.events",
        "key_strategy": "transaction_id",
        "rate": 0,
        "partitions": 1,
        "replication_factor": 1,
        "allow_non_empty_topic": False,
        "flush_timeout_seconds": 30.0,
    }
    values.update(overrides)
    return PublishSettings(**values)  # type: ignore[arg-type]


# --- Dữ liệu nguồn xấu phải là SourceDataError (mã 10), không phải traceback --------------------


def _event_line(payload: str) -> bytes:
    return f'{{"event_type":"{TX}","payload":{payload}}}'.encode()


@pytest.mark.parametrize("strategy", ["transaction_id", "account_from"])
def test_lone_surrogate_in_key_field_is_source_data_error(strategy: str) -> None:
    raw = _event_line('{"transaction_id":"\\ud800","account_from":"\\ud800"}')
    with pytest.raises(SourceDataError, match="không mã hóa được UTF-8") as caught:
        KeySelector(strategy).select(raw, file_path="events/x.jsonl", line_number=7)
    assert "events/x.jsonl dòng 7" in str(caught.value)


def test_integer_beyond_python_digit_limit_is_source_data_error() -> None:
    raw = _event_line('{"transaction_id":"t1","n":' + "9" * 5000 + "}")
    with pytest.raises(SourceDataError, match="JSON hỏng hoặc vượt giới hạn"):
        KeySelector("transaction_id").select(raw, file_path="f", line_number=1)


def test_deeply_nested_json_is_source_data_error() -> None:
    raw = ("[" * 100_000 + "]" * 100_000).encode()
    with pytest.raises(SourceDataError, match="JSON hỏng hoặc vượt giới hạn"):
        KeySelector("transaction_id").select(raw, file_path="f", line_number=1)


def test_verification_pass_reports_bad_key_as_source_data_error(
    make_source: Callable[..., Path],
) -> None:
    bad = _event_line('{"transaction_id":"\\ud800"}')
    source = discover_event_files(make_source([[bad]]))
    with pytest.raises(SourceDataError, match="UTF-8"):
        verify_event_files(source)


def test_manifest_with_huge_integer_is_source_data_error(
    make_source: Callable[..., Path],
) -> None:
    root = make_source([[line_of(tx_event("t1", "a", "b"))]])
    manifest = next((root / "manifests").glob("*.json"))
    manifest.write_text('{"x":' + "9" * 5000 + "}", encoding="utf-8")
    with pytest.raises(SourceDataError, match="không đọc được hoặc hỏng"):
        discover_event_files(root)


def test_cli_maps_bad_source_data_to_exit_10_without_traceback(
    tmp_path: Path, make_source: Callable[..., Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in CREDENTIALS.items():
        monkeypatch.setenv(name, value)
    root = make_source([[_event_line('{"transaction_id":"\\ud800"}')]])
    env_file = tmp_path / "e.env"
    env_file.write_text("", encoding="utf-8")
    out = io.StringIO()
    code = run_publish(
        profile="test",
        env_file=env_file,
        source_dir=root,
        out=out,
        broker_factory=lambda _bootstrap: FakeBroker(partitions=1),
        producer_factory=lambda _config: FakeProducer(),
        as_json=True,
    )
    payload = json.loads(out.getvalue())
    assert code == 10 and payload["ok"] is False and "UTF-8" in payload["error"]


# --- Lỗi đọc file giữa lúc đang gửi ------------------------------------------------------------


def test_os_error_during_send_flushes_and_reports_delivered_count(
    make_source: Callable[..., Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    source = discover_event_files(
        make_source([[line_of(tx_event(f"t{i}", "a", "b")) for i in range(3)]])
    )
    real_iter = __import__("n06.producer.publisher", fromlist=["iter_lines"]).iter_lines

    def breaking(item: Any) -> Iterator[tuple[int, bytes]]:
        for number, value in real_iter(item):
            if number == 2:
                raise PermissionError("file bị đổi quyền giữa chừng")
            yield number, value

    monkeypatch.setattr("n06.producer.publisher.iter_lines", breaking)

    class DeliversOnlyOnFlush(FakeProducer):
        """Callback chỉ chạy ở flush (như broker chậm): lỗi giao nhận đến sau khi OSError xảy ra."""

        def poll(self, timeout: float) -> int:
            self.polls.append(timeout)
            return 0

    producer = DeliversOnlyOnFlush(fail_delivery_at=0)
    with pytest.raises(SourceDataError) as caught:
        publish_events(
            source, settings=_settings(), broker=FakeBroker(partitions=1), producer=producer
        )
    text = str(caught.value)
    assert "Không đọc được dữ liệu nguồn khi đang gửi" in text
    assert "Đã giao 0 message trước khi dừng" in text
    assert "Lỗi giao nhận đầu tiên" in text  # lỗi đến trong lần flush không bị nuốt
    assert "Topic có thể đã nhận một phần" in text
    assert producer.flush_timeouts == [30.0]  # đã flush, không bỏ rơi hàng đợi


# --- Độ trễ không cộng khoảng chờ giới hạn tốc độ ----------------------------------------------


class _FakeTime:
    """Đồng hồ giả: thời gian chỉ trôi khi có ai đó chờ (`sleep`, hoặc `poll` có thời hạn)."""

    def __init__(self) -> None:
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    def clock_ns(self) -> int:
        return round(self.now * 1_000_000_000)

    def sleep(self, seconds: float) -> None:
        self.now += max(seconds, 1e-9)


class _AckAfterMillisecond:
    """Producer giả: broker ack sau 1 ms của đồng hồ giả, callback chỉ chạy ở lần poll sau đó."""

    def __init__(self, fake: _FakeTime) -> None:
        self._fake = fake
        self._pending: list[tuple[Any, float]] = []
        self._polls = 0

    def produce(self, topic: str, *, value: bytes, key: bytes, on_delivery: Any) -> None:
        self._pending.append((on_delivery, self._fake.now + 0.001))

    def poll(self, timeout: float) -> int:
        # Vòng chờ không tiến đồng hồ phải làm test fail, không được treo.
        self._polls += 1
        assert self._polls < 10_000, "vòng chờ không kết thúc"
        if timeout > 0:
            self._fake.sleep(timeout)
        ready = [item for item in self._pending if item[1] <= self._fake.now]
        self._pending = [item for item in self._pending if item[1] > self._fake.now]
        for callback, _ in ready:
            callback(None, object())
        return len(ready)

    def flush(self, timeout: float) -> int:
        while self._pending:
            self.poll(0.001)
        return 0


def test_delivery_latency_is_not_inflated_by_the_rate_limit_wait(
    make_source: Callable[..., Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    # rate 20/s: message thứ k gửi lúc k * 50 ms. Khi chờ nhịp, publisher poll từng lát 5 ms nên
    # callback của ack (sau 1 ms) chạy ở cuối lát đầu tiên: độ trễ 5 ms. Message cuối được giao
    # trong flush sau 1 ms. Bản cũ chờ bằng sleep nên độ trễ xấp xỉ 50 ms.
    fake = _FakeTime()
    monkeypatch.setattr(time, "sleep", fake.sleep)
    monkeypatch.setattr(time, "perf_counter_ns", fake.clock_ns)
    source = discover_event_files(
        make_source([[line_of(tx_event(f"t{i}", "a", "b")) for i in range(6)]])
    )
    stats = publish_events(
        source,
        settings=_settings(rate=20),
        broker=FakeBroker(partitions=1),
        producer=_AckAfterMillisecond(fake),  # type: ignore[arg-type]
        clock=fake.clock,
    )
    assert stats.messages_delivered == 6
    # Sáu độ trễ: 1 ms (message cuối) và năm lần 5 ms.
    assert stats.latency_ms_p50 == pytest.approx(5.0, abs=0.01)
    assert stats.latency_ms_p99 == pytest.approx(5.0, abs=0.01)
    # Message cuối gửi lúc 5 * 50 ms = 250 ms, giao lúc 251 ms.
    assert stats.elapsed_seconds == pytest.approx(0.251, abs=1e-6)
    assert stats.achieved_rate == pytest.approx(6 / 0.251, rel=1e-4)


# --- Dựng consumer thất bại là mã 14 -----------------------------------------------------------


def test_consumer_factory_failure_is_broker_unavailable() -> None:
    class Metadata:
        class _Topic:
            error = None
            partitions = {0: object()}

        topics = {"t": _Topic()}

    class Admin:
        def list_topics(self, timeout: float) -> Metadata:
            return Metadata()

    def failing_consumer() -> Any:
        raise KafkaException(KafkaError(KafkaError._TRANSPORT))

    broker = ConfluentBroker("h:1", admin=Admin(), consumer_factory=failing_consumer)
    with pytest.raises(BrokerUnavailableError, match="Không đọc được offset"):
        broker.end_offsets("t")


# --- Giá trị triển khai nằm trong config, mặc định của thư viện không được lệch ----------------


def _cfg():  # type: ignore[no-untyped-def]
    return load_config(profile="test", env=CREDENTIALS)


def test_library_defaults_equal_the_configuration_defaults() -> None:
    producer = _cfg().producer
    assert producer.max_message_bytes == DEFAULT_MAX_MESSAGE_BYTES
    assert producer.metadata_timeout_seconds == DEFAULT_METADATA_TIMEOUT_SECONDS
    assert producer.leader_wait_seconds == DEFAULT_LEADER_WAIT_SECONDS
    assert producer.flush_timeout_seconds == 120


def test_producer_limits_are_overridable_through_the_environment() -> None:
    cfg = load_config(
        profile="test",
        env={
            **CREDENTIALS,
            "N06__PRODUCER__FLUSH_TIMEOUT_SECONDS": "7",
            "N06__PRODUCER__MAX_MESSAGE_BYTES": "5000",
            "N06__PRODUCER__METADATA_TIMEOUT_SECONDS": "3",
            "N06__PRODUCER__LEADER_WAIT_SECONDS": "4",
        },
    )
    p = cfg.producer
    assert (p.flush_timeout_seconds, p.max_message_bytes) == (7, 5000)
    assert (p.metadata_timeout_seconds, p.leader_wait_seconds) == (3, 4)


def test_producer_config_takes_message_limit_from_the_argument() -> None:
    config = build_producer_config(
        bootstrap_servers="h:1", acks="all", retries=5, batch_size=1, max_message_bytes=5000
    )
    assert config["message.max.bytes"] == 5000


def test_cli_passes_configured_limits_to_the_producer_and_the_publisher(
    tmp_path: Path, make_source: Callable[..., Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in CREDENTIALS.items():
        monkeypatch.setenv(name, value)
    root = make_source([[line_of(tx_event("t1", "a", "b"))]])
    env_file = tmp_path / "e.env"
    env_file.write_text("", encoding="utf-8")
    seen: dict[str, Any] = {}

    def producer_factory(config: dict[str, Any]) -> FakeProducer:
        seen["max_bytes"] = config["message.max.bytes"]
        producer = FakeProducer()
        seen["producer"] = producer
        return producer

    code = run_publish(
        profile="test",
        env_file=env_file,
        source_dir=root,
        out=io.StringIO(),
        broker_factory=lambda _bootstrap: FakeBroker(partitions=1),
        producer_factory=producer_factory,
        config_overrides={
            "producer.max_message_bytes": 123456,
            "producer.flush_timeout_seconds": 11,
        },
    )
    assert code == 0 and seen["max_bytes"] == 123456
    assert seen["producer"].flush_timeouts[-1] == 11.0  # từ cấu hình khi không có cờ dòng lệnh

    seen.clear()
    code = run_publish(
        profile="test",
        env_file=env_file,
        source_dir=root,
        out=io.StringIO(),
        broker_factory=lambda _bootstrap: FakeBroker(partitions=1),
        producer_factory=producer_factory,
        config_overrides={"producer.flush_timeout_seconds": 11},
        flush_timeout_seconds=3.5,
        topic="t2.events",
    )
    assert code == 0 and seen["producer"].flush_timeouts[-1] == 3.5  # cờ dòng lệnh thắng cấu hình


def test_module_still_exposes_no_hardcoded_timeouts_besides_the_defaults() -> None:
    assert not hasattr(kafka_client, "METADATA_TIMEOUT_SECONDS")
    assert not hasattr(kafka_client, "LEADER_WAIT_SECONDS")


def test_symlink_pointing_outside_the_source_directory_is_rejected(
    tmp_path: Path, make_source: Callable[..., Path]
) -> None:
    import hashlib
    import os
    import uuid

    root = make_source([[line_of(tx_event("t1", "a", "b"))]])
    outside = tmp_path / "outside.jsonl"
    data = line_of(tx_event("evil", "a", "b")) + b"\n"
    outside.write_bytes(data)
    link = root / "events" / "events_batch_00009.jsonl"
    os.symlink(outside, link)
    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": "n06-unit-test",
        "source_batch_id": str(uuid.uuid4()),
        "files": [
            {
                "path": "events/events_batch_00009.jsonl",
                "sha256": hashlib.sha256(data).hexdigest(),
                "physical_rows": 1,
            }
        ],
    }
    (root / "manifests" / "evil.manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SourceDataError):
        discover_event_files(root)


# --- Tên file có dãy chữ số bất thường (review vòng hai) ---------------------------------------


def _add_listed_event(root: Path, name: str, data: bytes) -> None:
    """Thêm một mục manifest trỏ tới `events/<name>` mà không cần file đó tồn tại được trên đĩa."""
    import hashlib
    import uuid

    manifest = {
        "schema_version": "1.0.0",
        "dataset_id": "n06-unit-test",
        "source_batch_id": str(uuid.uuid4()),
        "files": [
            {
                "path": f"events/{name}",
                "sha256": hashlib.sha256(data).hexdigest(),
                "physical_rows": 1,
            }
        ],
    }
    (root / "manifests" / f"extra_{uuid.uuid4().hex}.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def test_manifest_path_with_huge_digit_run_exits_10_without_traceback(
    tmp_path: Path, make_source: Callable[..., Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in CREDENTIALS.items():
        monkeypatch.setenv(name, value)
    line = line_of(tx_event("t1", "a", "b"))
    root = make_source([[line]])
    _add_listed_event(root, "e_" + "9" * 5000 + ".jsonl", line + b"\n")
    env_file = tmp_path / "e.env"
    env_file.write_text("", encoding="utf-8")
    producer = FakeProducer()
    out = io.StringIO()
    code = run_publish(
        profile="test",
        env_file=env_file,
        source_dir=root,
        out=out,
        broker_factory=lambda _bootstrap: FakeBroker(partitions=1),
        producer_factory=lambda _config: producer,
        as_json=True,
    )
    payload = json.loads(out.getvalue())
    assert code == 10 and payload["ok"] is False, out.getvalue()
    assert producer.calls == []  # chưa gửi message nào


def test_names_differing_only_in_leading_zeros_have_a_fixed_order(
    make_source: Callable[..., Path],
) -> None:
    line = line_of(tx_event("t1", "a", "b"))
    root = make_source([[line]])
    data = line + b"\n"
    for name in ("batch_1.jsonl", "batch_01.jsonl", "batch_2.jsonl", "batch_10.jsonl"):
        (root / "events" / name).write_bytes(data)
        _add_listed_event(root, name, data)
    order = [f.relative_path for f in discover_event_files(root).files]
    batches = [p for p in order if p.startswith("events/batch_")]
    assert batches == [
        "events/batch_01.jsonl",
        "events/batch_1.jsonl",
        "events/batch_2.jsonl",
        "events/batch_10.jsonl",
    ]
