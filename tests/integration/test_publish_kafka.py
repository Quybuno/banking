"""Integration test `n06 publish` trên Kafka thật (TASK T20, docker compose --profile core up -d).

Không bị skip: nếu Kafka không chạy thì test FAIL. Mỗi test dùng topic riêng và xóa trong
`finally`. Expected được tính độc lập: đọc lại file events/manifest và đếm bằng consumer riêng
(group khác, đọc từ offset đầu tới high watermark), không dùng mã của `n06.producer`.
"""

from __future__ import annotations

import io
import json
import os
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from n06.cli import main
from n06.cli.init_local import parse_dotenv_file
from n06.config import load_config
from n06.generator import build_dataset

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIR = REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1"
PARTITIONS = 3


def _env() -> dict[str, str]:
    merged: dict[str, str] = {}
    env_file = REPO_ROOT / ".env"
    if env_file.is_file():
        merged.update(parse_dotenv_file(env_file))
    merged.update(os.environ)
    return merged


@pytest.fixture(scope="module")
def bootstrap() -> str:
    cfg = load_config(profile="test", env=_env())
    admin = AdminClient({"bootstrap.servers": cfg.kafka.bootstrap_servers})
    try:
        admin.list_topics(timeout=15)
    except KafkaException as exc:  # không skip: hạ tầng thiếu là lỗi của môi trường kiểm thử
        pytest.fail(
            f"Kafka không chạy tại {cfg.kafka.bootstrap_servers} "
            f"(docker compose --profile core up -d): {exc}"
        )
    return cfg.kafka.bootstrap_servers


@pytest.fixture(scope="module")
def source_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    cfg = json.loads((CONTRACT_DIR / "config" / "generator.demo-faults.json").read_text("utf-8"))
    cfg.update(dataset_id="n06-t20-integration", transaction_count=1500, account_count=30)
    root = tmp_path_factory.mktemp("t20_dataset")
    build_dataset(cfg, root, validate_outputs=False)
    return root / "source"


@pytest.fixture
def topic(bootstrap: str) -> Iterator[str]:
    name = f"n06.test.publish.{uuid.uuid4().hex[:12]}"
    yield name
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.delete_topics([name])
    for future in futures.values():
        try:
            future.result(timeout=15)
        except KafkaException:
            pass  # topic có thể chưa được tạo (test lỗi sớm)


@pytest.fixture(autouse=True)
def _partitions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("N06__KAFKA__NUM_PARTITIONS", str(PARTITIONS))


def _publish(source_dir: Path, topic: str, *extra: str) -> tuple[int, dict[str, Any]]:
    out = io.StringIO()
    code = main(
        ["publish", "--profile", "test", "--source-dir", str(source_dir), "--topic", topic]
        + list(extra)
        + ["--json"],
        out=out,
    )
    return code, json.loads(out.getvalue().strip().splitlines()[-1])


def _consume_all(bootstrap: str, topic: str) -> list[tuple[int, int, bytes, bytes]]:
    """Consumer riêng: `(partition, offset, key, value)` của mọi message tới high watermark."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"n06-test-verify-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=15)
        partitions = sorted(metadata.topics[topic].partitions)
        highs = {}
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
            highs[p] = (low, high)
        consumer.assign([TopicPartition(topic, p, highs[p][0]) for p in partitions])
        remaining = {p: high - low for p, (low, high) in highs.items()}
        messages: list[tuple[int, int, bytes, bytes]] = []
        deadline = time.monotonic() + 60
        while any(v > 0 for v in remaining.values()):
            assert time.monotonic() < deadline, f"hết giờ khi đọc topic, còn {remaining}"
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            assert msg.error() is None, msg.error()
            messages.append((msg.partition(), msg.offset(), msg.key(), msg.value()))
            remaining[msg.partition()] -= 1
        return messages
    finally:
        consumer.close()


def _event_lines(source_dir: Path) -> list[bytes]:
    """Dòng events theo thứ tự file, tìm bằng manifest (không dùng mã của producer)."""
    paths: list[str] = []
    expected_rows = 0
    for manifest_path in sorted((source_dir / "manifests").glob("*.json")):
        manifest = json.loads(manifest_path.read_text("utf-8"))
        for item in manifest["files"]:
            if item["path"].startswith("events/"):
                paths.append(item["path"])
                expected_rows += item["physical_rows"]
    lines: list[bytes] = []
    for rel in sorted(paths):
        lines += [ln for ln in (source_dir / rel).read_bytes().split(b"\n") if ln]
    assert len(lines) == expected_rows  # manifest khớp file
    return lines


def test_default_key_count_matches_manifest_and_bytes_identical(
    bootstrap: str, source_dir: Path, topic: str
) -> None:
    lines = _event_lines(source_dir)
    code, stats = _publish(source_dir, topic)
    assert code == 0, stats
    messages = _consume_all(bootstrap, topic)

    assert len(messages) == len(lines) == stats["messages_expected"] == stats["messages_delivered"]
    assert Counter(m[3] for m in messages) == Counter(lines)  # byte y hệt, kể cả replay
    duplicates = sum(c - 1 for c in Counter(lines).values() if c > 1)
    truth = json.loads(
        (source_dir.parent / "ground_truth" / "fault_counts.json").read_text("utf-8")
    )["counts"]
    assert duplicates == truth["event_replay"] > 0  # replay không bị loại ở bước publish
    for _, _, key, value in messages:
        assert key == json.loads(value)["payload"]["transaction_id"].encode("utf-8")
    # Thứ tự trong một key (một partition) giữ nguyên thứ tự file.
    by_key: dict[bytes, list[bytes]] = {}
    for _, _, key, value in sorted(messages, key=lambda m: (m[0], m[1])):
        by_key.setdefault(key, []).append(value)
    in_file: dict[bytes, list[bytes]] = {}
    for line in lines:
        in_file.setdefault(json.loads(line)["payload"]["transaction_id"].encode(), []).append(line)
    assert by_key == in_file


def test_account_from_key_colocates_each_transaction_and_keeps_values(
    bootstrap: str, source_dir: Path, topic: str
) -> None:
    lines = _event_lines(source_dir)
    account_from: dict[str, str] = {}
    for line in lines:
        env = json.loads(line)
        if env["event_type"] == "TRANSACTION_CREATED":
            account_from[env["payload"]["transaction_id"]] = env["payload"]["account_from"]

    code, stats = _publish(source_dir, topic, "--key-strategy", "account_from")
    assert code == 0, stats
    messages = _consume_all(bootstrap, topic)
    assert len(messages) == len(lines)
    assert Counter(m[3] for m in messages) == Counter(lines)  # đổi key không đổi value

    partitions_by_tx: dict[str, set[int]] = {}
    keys_changed = 0
    for partition, _, key, value in messages:
        payload = json.loads(value)["payload"]
        tx = payload["transaction_id"]
        assert key == account_from[tx].encode("utf-8")  # kể cả ledger CREDIT
        partitions_by_tx.setdefault(tx, set()).add(partition)
        keys_changed += key != tx.encode("utf-8")
    assert keys_changed == len(messages)
    # Transaction, replay và hai ledger của nó nằm trên cùng một partition.
    assert all(len(p) == 1 for p in partitions_by_tx.values())
    assert len({next(iter(p)) for p in partitions_by_tx.values()}) > 1  # dùng nhiều partition


def test_topic_is_created_with_configured_partitions(
    bootstrap: str, source_dir: Path, topic: str
) -> None:
    assert _publish(source_dir, topic)[0] == 0
    metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=15)
    assert len(metadata.topics[topic].partitions) == PARTITIONS


def test_rerun_is_refused_unless_flag_and_count_is_exact(
    bootstrap: str, source_dir: Path, topic: str
) -> None:
    total = len(_event_lines(source_dir))
    assert _publish(source_dir, topic)[0] == 0
    assert len(_consume_all(bootstrap, topic)) == total

    code, result = _publish(source_dir, topic)
    assert code == 10 and "--allow-non-empty-topic" in result["error"]
    assert len(_consume_all(bootstrap, topic)) == total  # từ chối thì topic không đổi

    code, stats = _publish(source_dir, topic, "--allow-non-empty-topic")
    assert code == 0 and stats["preexisting_messages"] == total
    assert len(_consume_all(bootstrap, topic)) == 2 * total


def test_rate_is_controlled_and_measured(bootstrap: str, source_dir: Path, topic: str) -> None:
    total = len(_event_lines(source_dir))
    rate = 3000
    code, limited = _publish(source_dir, topic, "--rate", str(rate))
    assert code == 0 and limited["target_rate"] == rate
    assert limited["messages_delivered"] == total
    # Message thứ k không được gửi trước k/rate giây: thời gian tối thiểu (total-1)/rate.
    assert limited["elapsed_seconds"] >= (total - 1) / rate * 0.98
    assert limited["achieved_rate"] <= rate * 1.03
    assert limited["latency_ms_p50"] <= limited["latency_ms_p95"] <= limited["latency_ms_p99"]

    code, free = _publish(source_dir, topic + ".free", "--rate", "0")
    try:
        assert code == 0 and free["target_rate"] == 0
        assert free["achieved_rate"] > limited["achieved_rate"]
    finally:
        admin = AdminClient({"bootstrap.servers": bootstrap})  # giữ tham chiếu tới khi xong
        futures = admin.delete_topics([topic + ".free"])
        for future in futures.values():
            future.result(timeout=15)


def test_unreachable_broker_exits_14(
    source_dir: Path, topic: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("N06__KAFKA__BOOTSTRAP_SERVERS", "localhost:1")
    code, result = _publish(source_dir, topic)
    assert code == 14 and "broker" in result["error"].lower()


def test_bad_source_is_rejected_before_any_message_reaches_the_topic(
    bootstrap: str, source_dir: Path, tmp_path: Path, topic: str
) -> None:
    """Dòng trống ở giữa shard (manifest vẫn nhất quán): thoát 10 và topic chưa có message nào."""
    import hashlib
    import shutil

    broken = tmp_path / "broken_source"
    shutil.copytree(source_dir, broken)
    manifest_path = next(
        m for m in sorted((broken / "manifests").glob("events_*.json")) if "00000" in m.name
    )
    manifest = json.loads(manifest_path.read_text("utf-8"))
    rel = next(f["path"] for f in manifest["files"] if f["path"].startswith("events/"))
    lines = (broken / rel).read_bytes().split(b"\n")
    lines.insert(len(lines) // 2, b"")  # dòng trống ở giữa file
    data = b"\n".join(lines)
    (broken / rel).write_bytes(data)
    for item in manifest["files"]:
        if item["path"] == rel:
            item["sha256"] = hashlib.sha256(data).hexdigest()
            item["physical_rows"] = data.count(b"\n")
    manifest_path.write_text(json.dumps(manifest), "utf-8")

    code, result = _publish(broken, topic)
    assert code == 10 and "Dòng trống" in result["error"]
    metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=15)
    assert topic not in metadata.topics  # thất bại trước khi tạo topic hay gửi message


def _idempotence_state_on_broker(
    bootstrap: str, topic: str, config: dict[str, Any]
) -> dict[str, Any]:
    """Gửi một message rồi đọc thống kê `eos` của librdkafka; trả khối `eos` ở lần đọc cuối
    (rỗng nếu producer không bật idempotence)."""
    from n06.producer.kafka_client import make_producer

    stats: list[dict[str, Any]] = []
    producer = make_producer(
        {
            **config,
            "statistics.interval.ms": 200,
            "stats_cb": lambda raw: stats.append(json.loads(raw)),
        }
    )
    delivered: list[Any] = []
    producer.produce(
        topic, value=b"idempotence-probe", key=b"k", on_delivery=lambda e, m: delivered.append(e)
    )
    # Thống kê đến theo chu kỳ: sau khi giao xong, chờ thêm tối đa 3 giây cho tới khi trạng thái
    # idempotent là "Assigned" (producer đối chứng không bao giờ tới đó nên chờ hết 3 giây).
    deadline = time.monotonic() + 20
    settle_until: float | None = None
    while time.monotonic() < deadline:
        producer.poll(0.2)
        latest = [s["eos"] for s in stats if s.get("eos")]
        if latest and latest[-1].get("idemp_state") == "Assigned":
            break
        if delivered and settle_until is None:
            settle_until = time.monotonic() + 3
        if settle_until is not None and time.monotonic() > settle_until:
            break
    producer.flush(10)
    assert delivered == [None], f"Message không được giao: {delivered}"
    eos = [s["eos"] for s in stats if s.get("eos")]
    return eos[-1] if eos else {}  # producer không idempotent thì librdkafka không báo khối eos


def test_real_producer_is_idempotent_on_the_broker_and_control_is_not(
    bootstrap: str, topic: str
) -> None:
    """Bằng chứng idempotence có hiệu lực thật: broker đã cấp Producer ID cho producer của
    `n06 publish`, còn producer đối chứng (không bật idempotence) thì không."""
    from n06.producer.kafka_client import ConfluentBroker, build_producer_config

    ConfluentBroker(bootstrap).create_topic(topic, 1, 1)
    ours = _idempotence_state_on_broker(
        bootstrap,
        topic,
        build_producer_config(bootstrap_servers=bootstrap, acks="all", retries=5, batch_size=16384),
    )
    assert ours["idemp_state"] == "Assigned", ours
    assert ours["producer_id"] >= 0, ours

    control = _idempotence_state_on_broker(
        bootstrap,
        topic,
        {"bootstrap.servers": bootstrap, "enable.idempotence": False, "acks": "all"},
    )
    assert control.get("idemp_state") != "Assigned" and control.get("producer_id", -1) == -1, (
        control
    )
    assert control == {}, f"Producer đối chứng không được có khối eos: {control}"
