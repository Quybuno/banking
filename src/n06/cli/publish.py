"""Lệnh `n06 publish`: đọc events từ nguồn do generator sinh và gửi lên Kafka (TASK T20).

Tham chiếu: docs/spec.md Phần II mục 4; docs/adr/0010-kafka-client-va-quy-tac-publish.md.
Mọi endpoint, topic, tốc độ và chiến lược key lấy từ cấu hình T01 và có thể ghi đè bằng tham số.
"""

from __future__ import annotations

import json
import math
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, TextIO

from confluent_kafka import KafkaException

from n06.cli.doctor import ExitCode
from n06.cli.init_local import parse_dotenv_file
from n06.config import REPO_ROOT, AppConfig, ConfigValidationError, load_config
from n06.producer.errors import (
    BrokerUnavailableError,
    DeliveryError,
    PublishConfigError,
    PublishError,
)
from n06.producer.kafka_client import (
    BrokerAdmin,
    ConfluentBroker,
    ProducerLike,
    build_producer_config,
    make_producer,
)
from n06.producer.publisher import PublishSettings, PublishStats, publish_events
from n06.producer.source import (
    DEFAULT_EVENTS_DIRNAME,
    DEFAULT_MANIFESTS_DIRNAME,
    DEFAULT_SOURCE_DIRNAME,
    discover_event_files,
)

DEFAULT_FLUSH_TIMEOUT_SECONDS = 120.0

BrokerFactory = Callable[[str], BrokerAdmin]
ProducerFactory = Callable[[dict[str, Any]], ProducerLike]


def _load_app_config(
    profile: str | None,
    env_file: Path | None,
    overrides: dict[str, Any],
) -> AppConfig:
    resolved_env_file = env_file if env_file is not None else (REPO_ROOT / ".env")
    merged_env: dict[str, str] = {}
    if resolved_env_file.is_file():
        merged_env.update(parse_dotenv_file(resolved_env_file))
    merged_env.update(os.environ)
    return load_config(profile=profile, env=merged_env, cli_overrides=overrides)


def _format_text(stats: PublishStats) -> str:
    return (
        f"[publish] topic={stats.topic} key={stats.key_strategy} partitions={stats.partitions}\n"
        f"[publish] đã giao {stats.messages_delivered}/{stats.messages_expected} message "
        f"({stats.files} file, {stats.bytes_delivered} byte) trong {stats.elapsed_seconds:.3f}s\n"
        f"[publish] tốc độ mục tiêu={stats.target_rate}/s đạt={stats.achieved_rate:.1f}/s; "
        f"độ trễ giao nhận p50={stats.latency_ms_p50:.2f}ms p95={stats.latency_ms_p95:.2f}ms "
        f"p99={stats.latency_ms_p99:.2f}ms\n"
    )


def run_publish(
    *,
    profile: str | None = None,
    env_file: Path | None = None,
    source_dir: Path | None = None,
    topic: str | None = None,
    rate: int | None = None,
    key_strategy: str | None = None,
    allow_non_empty_topic: bool = False,
    manifests_dir: str = DEFAULT_MANIFESTS_DIRNAME,
    events_dir: str = DEFAULT_EVENTS_DIRNAME,
    flush_timeout_seconds: float = DEFAULT_FLUSH_TIMEOUT_SECONDS,
    as_json: bool = False,
    out: TextIO | None = None,
    broker_factory: BrokerFactory = ConfluentBroker,
    producer_factory: ProducerFactory = make_producer,
    config_overrides: dict[str, Any] | None = None,
) -> int:
    """Thực thi `n06 publish`; trả mã thoát theo bảng `ExitCode` (10 cấu hình/dữ liệu nguồn,
    14 không kết nối được broker, 15 lỗi giao nhận)."""
    stdout = out if out is not None else sys.stdout

    def fail(code: ExitCode, message: str) -> int:
        if as_json:
            stdout.write(json.dumps({"ok": False, "error": message}, ensure_ascii=False) + "\n")
        else:
            stdout.write(f"[publish] Lỗi: {message}\n")
        return int(code)

    if not (math.isfinite(flush_timeout_seconds) and flush_timeout_seconds > 0):
        return fail(
            ExitCode.CONFIG_ERROR,
            "--flush-timeout-seconds (thời gian chờ flush) phải > 0 và hữu hạn, "
            f"nhận {flush_timeout_seconds!r}.",
        )

    overrides: dict[str, Any] = dict(config_overrides or {})
    if topic is not None:
        overrides["kafka.events_topic"] = topic
    if rate is not None:
        overrides["producer.rate"] = rate
    if key_strategy is not None:
        overrides["producer.key_strategy"] = key_strategy
    try:
        cfg = _load_app_config(profile, env_file, overrides)
    except ConfigValidationError as exc:
        return fail(ExitCode.CONFIG_ERROR, str(exc))

    resolved_source = (
        source_dir
        if source_dir is not None
        else Path(cfg.paths.generator_output_dir) / DEFAULT_SOURCE_DIRNAME
    )
    settings = PublishSettings(
        topic=cfg.kafka.events_topic,
        key_strategy=cfg.producer.key_strategy,
        rate=cfg.producer.rate,
        partitions=cfg.kafka.num_partitions,
        replication_factor=cfg.kafka.replication_factor,
        allow_non_empty_topic=allow_non_empty_topic,
        flush_timeout_seconds=flush_timeout_seconds,
    )
    try:
        producer_config = build_producer_config(
            bootstrap_servers=cfg.kafka.bootstrap_servers,
            acks=cfg.producer.acks,
            retries=cfg.producer.retries,
            batch_size=cfg.producer.batch_size,
        )
        source = discover_event_files(
            resolved_source, manifests_dirname=manifests_dir, events_dirname=events_dir
        )
        try:
            producer = producer_factory(producer_config)
        except KafkaException as exc:
            raise PublishConfigError(f"Cấu hình producer bị từ chối: {exc}") from exc
        stats = publish_events(
            source,
            settings=settings,
            broker=broker_factory(cfg.kafka.bootstrap_servers),
            producer=producer,
            report=None if as_json else stdout.write,
        )
    except PublishConfigError as exc:
        return fail(ExitCode.CONFIG_ERROR, str(exc))
    except BrokerUnavailableError as exc:
        return fail(ExitCode.SERVICE_ERROR, str(exc))
    except DeliveryError as exc:
        return fail(ExitCode.KAFKA_DELIVERY_ERROR, str(exc))
    except PublishError as exc:
        return fail(ExitCode.CONFIG_ERROR, str(exc))
    except KafkaException as exc:  # lưới an toàn: lỗi Kafka chưa được bọc ở lớp dưới
        return fail(ExitCode.KAFKA_DELIVERY_ERROR, f"Lỗi Kafka: {exc}")
    except OSError as exc:  # lưới an toàn: lỗi đọc file nguồn chưa được bọc
        return fail(ExitCode.CONFIG_ERROR, f"Không đọc được dữ liệu nguồn: {exc}")

    if as_json:
        stdout.write(json.dumps({"ok": True, **stats.to_dict()}, ensure_ascii=False) + "\n")
    else:
        stdout.write(_format_text(stats))
    return int(ExitCode.OK)
