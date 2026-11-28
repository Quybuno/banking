"""Registry workload cho `n06 benchmark` (ADR-0007 B1, B4)."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from n06.cli.doctor import ExitCode
from n06.cli.generate import run_generate
from n06.cli.ingestion import run_stream
from n06.cli.publish import run_publish
from n06.config import REPO_ROOT, AppConfig, load_config
from n06.observability.sampler import default_process_tree_rss_reader
from n06.storage.object_store import resolve_host_endpoint


class WorkloadNotRegisteredError(LookupError):
    """Ném khi thí nghiệm hoặc workload chưa được đăng ký trên nhánh hiện tại."""


@dataclass(frozen=True)
class WorkloadResult:
    """Kết quả thực thi một lần chạy workload."""

    exit_code: int
    status: str  # "ok" | "error" | "invalid"
    phase_seconds: dict[str, float]
    records_processed: int
    throughput_entity: str
    throughput_scope: str
    correctness_ok: bool
    unique_key_count: int | None = None
    totals_by_currency: dict[str, int] = field(default_factory=dict)
    status_counts: dict[str, int] = field(default_factory=dict)
    semantic_hash: str | None = None
    oracle_match: bool | None = None
    error_type: str | None = None
    error_message: str | None = None
    extra_metrics: list[tuple[str, float | int | str, str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class DatasetPrepResult:
    """Kết quả chuẩn bị dataset (tách generation_seconds khỏi wall_seconds theo I.14.2)."""

    exit_code: int
    dataset_dir: Path
    source_dir: Path
    generation_seconds: float
    generator_peak_rss_mb: float
    manifest_path: str
    manifest_sha256: str
    transaction_count: int
    seed: int
    error_message: str | None = None


WorkloadFn = Callable[
    [AppConfig, DatasetPrepResult, str, int, Mapping[str, Any]],
    WorkloadResult,
]


@dataclass(frozen=True)
class WorkloadSpec:
    """Mô tả một workload đã đăng ký."""

    experiment: str
    variable_name: str
    variable2_name: str | None
    ram_scope: str
    description: str
    runner: WorkloadFn


class WorkloadRegistry:
    """Quản lý ánh xạ mã thí nghiệm -> workload thật."""

    def __init__(self) -> None:
        self._specs: dict[str, WorkloadSpec] = {}

    def register(self, spec: WorkloadSpec) -> None:
        self._specs[spec.experiment.upper()] = spec

    def get(self, experiment: str) -> WorkloadSpec:
        key = experiment.upper()
        if key not in self._specs:
            raise WorkloadNotRegisteredError(
                f"Chưa có workload cho thí nghiệm {key} trên nhánh hiện tại."
            )
        return self._specs[key]

    def available_experiments(self) -> list[str]:
        return sorted(self._specs.keys())


def _find_manifest(source_dir: Path) -> tuple[str, str]:
    """Tìm file manifest đầu tiên trong `source_dir` và tính sha256."""
    import hashlib

    if not source_dir.is_dir():
        return "", ""
    manifests = sorted(source_dir.rglob("*manifest*.json"))
    if not manifests:
        return str(source_dir), ""
    first = manifests[0]
    digest = hashlib.sha256(first.read_bytes()).hexdigest()
    return str(first), digest


def prepare_benchmark_dataset(
    cfg: AppConfig,
    *,
    generator_config: Path,
    out_dir: Path,
    transactions: int,
    seed: int,
    env_file: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> DatasetPrepResult:
    """Sinh dữ liệu một lần cho mức tải và đo riêng `generation_seconds` (I.14.2)."""
    temp_cfg_dir = out_dir.parent
    temp_cfg_dir.mkdir(parents=True, exist_ok=True)
    raw_gcfg = json.loads(generator_config.read_text(encoding="utf-8"))
    raw_gcfg["dataset_id"] = f"{cfg.dataset.dataset_id}-{transactions}-{seed}"
    raw_gcfg["seed"] = seed
    raw_gcfg["transaction_count"] = transactions
    raw_gcfg["account_count"] = max(50, transactions // 200)
    patched_cfg_path = temp_cfg_dir / f"generator-{transactions}-{seed}.json"
    patched_cfg_path.write_text(json.dumps(raw_gcfg, indent=2), encoding="utf-8")

    rss_before = default_process_tree_rss_reader()
    t0 = time.monotonic()
    code = run_generate(
        generator_config=patched_cfg_path,
        out_dir=out_dir,
        transactions=transactions,
        profile=cfg.profile,
        env_file=env_file,
        env=env,
    )
    gen_seconds = round(time.monotonic() - t0, 4)
    rss_after = default_process_tree_rss_reader()
    peak_rss = max(rss_before, rss_after)

    source_dir = out_dir / "source"
    manifest_path, manifest_sha = _find_manifest(source_dir)
    return DatasetPrepResult(
        exit_code=code,
        dataset_dir=out_dir,
        source_dir=source_dir,
        generation_seconds=gen_seconds,
        generator_peak_rss_mb=peak_rss,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        transaction_count=transactions,
        seed=seed,
        error_message=None if code == 0 else f"n06 generate exited with code {code}",
    )


def _resolve_host_kafka_bootstrap(env: Mapping[str, str]) -> str:
    _, host, _ = resolve_host_endpoint(env)
    host_cfg = load_config(profile="test", env=env)
    default_port = host_cfg.kafka.bootstrap_servers.partition(":")[2] or "19092"
    port = env.get("N06_KAFKA_HOST_PORT", default_port).strip() or default_port
    return f"{host}:{port}"


def _run_ingest_stages(
    cfg: AppConfig,
    dataset: DatasetPrepResult,
    run_id: str,
    level: int,
    params: Mapping[str, Any],
) -> WorkloadResult:
    """Chạy thật `publish` + `stream --available-now` với topic cô lập theo `run_id`."""
    topic = f"{cfg.kafka.events_topic}.{run_id}".replace(":", "-").lower()[:120]
    query_name = f"bench_{run_id}".replace("-", "_").lower()[:60]
    partitions = int(params.get("kafka_partitions", cfg.kafka.num_partitions))
    env = dict(params.get("env") or os.environ)
    env_file = params.get("env_file")
    host_bootstrap = _resolve_host_kafka_bootstrap(env)

    t_pub0 = time.monotonic()
    pub_code = run_publish(
        profile=cfg.profile,
        env_file=env_file,
        source_dir=dataset.source_dir,
        topic=topic,
        rate=cfg.producer.rate,
        key_strategy=cfg.producer.key_strategy,
        allow_non_empty_topic=True,
        config_overrides={
            "kafka.num_partitions": partitions,
            "kafka.bootstrap_servers": host_bootstrap,
        },
    )
    pub_sec = round(time.monotonic() - t_pub0, 4)
    if pub_code != int(ExitCode.OK):
        return WorkloadResult(
            exit_code=pub_code,
            status="error",
            phase_seconds={"publish": pub_sec},
            records_processed=0,
            throughput_entity="event",
            throughput_scope="ingest",
            correctness_ok=False,
            error_type="PublishError",
            error_message=f"n06 publish failed with exit code {pub_code}",
        )

    t_stream0 = time.monotonic()
    stream_code = run_stream(
        profile=cfg.profile,
        env_file=env_file,
        query_name=query_name,
        available_now=True,
        env={
            **env,
            "N06__KAFKA__EVENTS_TOPIC": topic,
            "N06__KAFKA__QUERY_NAME": query_name,
        },
    )
    stream_sec = round(time.monotonic() - t_stream0, 4)
    ingest_sec = round(pub_sec + stream_sec, 4)

    if stream_code != int(ExitCode.OK):
        return WorkloadResult(
            exit_code=stream_code,
            status="error",
            phase_seconds={"publish": pub_sec, "stream": stream_sec, "ingest": ingest_sec},
            records_processed=0,
            throughput_entity="event",
            throughput_scope="ingest",
            correctness_ok=False,
            error_type="StreamError",
            error_message=f"n06 stream failed with exit code {stream_code}",
        )

    return WorkloadResult(
        exit_code=0,
        status="ok",
        phase_seconds={"publish": pub_sec, "stream": stream_sec, "ingest": ingest_sec},
        records_processed=dataset.transaction_count,
        throughput_entity="transaction",
        throughput_scope=(
            "ingest (process-day pending T25)" if params.get("experiment") == "E1" else "ingest"
        ),
        correctness_ok=True,
    )


def default_workload_registry() -> WorkloadRegistry:
    """Registry mặc định với các workload thật hiện có trên nhánh `main`."""
    registry = WorkloadRegistry()
    registry.register(
        WorkloadSpec(
            experiment="E1",
            variable_name="transaction_count",
            variable2_name=None,
            ram_scope="host",
            description=(
                "Quy mô giao dịch (hiện chạy ingest thật; process-day nối khi T25 vào main)"
            ),
            runner=_run_ingest_stages,
        )
    )
    registry.register(
        WorkloadSpec(
            experiment="E2",
            variable_name="kafka_partitions",
            variable2_name=None,
            ram_scope="host",
            description="Ingest Raw/Bronze với số partition Kafka thay đổi, producer rate=0",
            runner=_run_ingest_stages,
        )
    )
    return registry


def default_generator_config_path() -> Path:
    return (
        REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / "config" / "generator.demo-faults.json"
    )
