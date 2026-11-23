"""`n06 publish`: ánh xạ lỗi sang mã thoát và ghi đè cấu hình (TASK T20)."""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from n06.cli import ExitCode, main
from n06.cli.publish import run_publish

from .conftest import FakeBroker, FakeProducer, ledger_event, line_of, tx_event


@pytest.fixture(autouse=True)
def _secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cấu hình T01 bắt buộc secret (không có trong git); dùng giá trị giả chỉ cho test."""
    for name in (
        "N06_OBJECT_STORE_ACCESS_KEY",
        "N06_OBJECT_STORE_SECRET_KEY",
        "N06_POSTGRES_MART_PASSWORD",
        "N06_POSTGRES_CONTROL_PASSWORD",
        "N06_POSTGRES_AIRFLOW_PASSWORD",
        "N06_POSTGRES_METABASE_PASSWORD",
    ):
        monkeypatch.setenv(name, "unit-test-secret")


def _shards() -> list[list[bytes]]:
    return [
        [
            line_of(tx_event("t1", "a", "b")),
            line_of(ledger_event("t1", "DEBIT", "a")),
            line_of(ledger_event("t1", "CREDIT", "b")),
        ]
    ]


def _run(
    tmp_path: Path,
    source: Path,
    broker: FakeBroker,
    producer: FakeProducer,
    **kwargs: Any,
) -> tuple[int, str]:
    out = io.StringIO()
    env_file = tmp_path / "empty.env"
    env_file.write_text("", encoding="utf-8")
    code = run_publish(
        profile="test",
        env_file=env_file,
        source_dir=source,
        out=out,
        broker_factory=lambda _bootstrap: broker,
        producer_factory=lambda _config: producer,
        **kwargs,
    )
    return code, out.getvalue()


def test_success_prints_json_stats(tmp_path: Path, make_source: Callable[..., Path]) -> None:
    source = make_source(_shards())
    producer = FakeProducer()
    code, text = _run(
        tmp_path,
        source,
        FakeBroker(partitions=1),
        producer,
        topic="unit.topic",
        key_strategy="account_from",
        rate=0,
        as_json=True,
    )
    result = json.loads(text)
    assert code == int(ExitCode.OK)
    assert result["ok"] is True and result["topic"] == "unit.topic"
    assert result["key_strategy"] == "account_from"
    assert result["messages_delivered"] == result["messages_expected"] == 3
    assert [k for _, k, _ in producer.calls] == [b"a", b"a", b"a"]
    assert {t for t, _, _ in producer.calls} == {"unit.topic"}


def test_defaults_come_from_configuration(tmp_path: Path, make_source: Callable[..., Path]) -> None:
    producer = FakeProducer()
    code, _ = _run(tmp_path, make_source(_shards()), FakeBroker(partitions=1), producer)
    assert code == 0
    assert {t for t, _, _ in producer.calls} == {"n06.test.events.v1"}  # config/test.toml
    assert [k for _, k, _ in producer.calls] == [b"t1"] * 3  # key mặc định


def test_non_empty_topic_exits_10_unless_flag(
    tmp_path: Path, make_source: Callable[..., Path]
) -> None:
    source = make_source(_shards())
    broker = FakeBroker(partitions=1, end_offsets={0: 3})
    producer = FakeProducer()
    code, text = _run(tmp_path, source, broker, producer)
    assert code == int(ExitCode.CONFIG_ERROR) == 10
    assert "--allow-non-empty-topic" in text and producer.calls == []
    code, _ = _run(tmp_path, source, broker, producer, allow_non_empty_topic=True)
    assert code == 0 and len(producer.calls) == 3


def test_orphan_ledger_exits_10_with_source_data_message(
    tmp_path: Path, make_source: Callable[..., Path]
) -> None:
    shards = [[line_of(tx_event("t1", "a", "b"))], [line_of(ledger_event("t1", "DEBIT", "a"))]]
    code, text = _run(
        tmp_path,
        make_source(shards),
        FakeBroker(partitions=1),
        FakeProducer(),
        key_strategy="account_from",
    )
    assert code == 10
    assert "dữ liệu nguồn không nhất quán" in text.lower()
    assert "không phải lỗi cấu hình" in text


def test_unreachable_broker_exits_14(tmp_path: Path, make_source: Callable[..., Path]) -> None:
    code, _ = _run(tmp_path, make_source(_shards()), FakeBroker(unavailable=True), FakeProducer())
    assert code == int(ExitCode.SERVICE_ERROR) == 14


def test_delivery_failure_exits_15(tmp_path: Path, make_source: Callable[..., Path]) -> None:
    code, text = _run(
        tmp_path,
        make_source(_shards()),
        FakeBroker(partitions=1),
        FakeProducer(fail_delivery_at=1),
    )
    assert code == int(ExitCode.KAFKA_DELIVERY_ERROR) == 15
    assert "Giao nhận không hoàn tất" in text


def test_bad_acks_and_bad_key_strategy_exit_10(
    tmp_path: Path, make_source: Callable[..., Path]
) -> None:
    source = make_source(_shards())
    code, _ = _run(
        tmp_path,
        source,
        FakeBroker(partitions=1),
        FakeProducer(),
        config_overrides={"producer.acks": "1"},
    )
    assert code == 10
    code, _ = _run(tmp_path, source, FakeBroker(partitions=1), FakeProducer(), key_strategy="bogus")
    assert code == 10


def test_negative_rate_is_a_config_error(tmp_path: Path, make_source: Callable[..., Path]) -> None:
    code, _ = _run(
        tmp_path, make_source(_shards()), FakeBroker(partitions=1), FakeProducer(), rate=-5
    )
    assert code == 10


def test_publish_subcommand_is_registered_with_help() -> None:
    out = io.StringIO()
    assert main(["publish", "--help"], out=out) == 0
    text = out.getvalue()
    for flag in ("--topic", "--rate", "--key-strategy", "--allow-non-empty-topic", "--source-dir"):
        assert flag in text
