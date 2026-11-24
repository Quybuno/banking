"""Hệ thống nạp, gộp 4 tầng và kiểm tra cấu hình có kiểu cho N06 (TASK T01).

Thứ tự ưu tiên (thấp đến cao):
1. File mặc định: config/default.toml
2. File theo profile: config/<profile>.toml
3. Biến môi trường: N06_* và N06__<SECTION>__<KEY>
4. Tham số ghi đè từ CLI: cli_overrides
"""

from __future__ import annotations

import copy
import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_DIR = REPO_ROOT / "config"

SUPPORTED_BUSINESS_TIMEZONE = "Asia/Ho_Chi_Minh"
VALID_KEY_STRATEGIES = ("transaction_id", "account_from")
VALID_CURRENCIES = ("VND", "USD")
POSTGRES_ROLES = ("mart", "control", "airflow", "metabase")
DATASET_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Danh sách đường dẫn trường nhạy cảm chỉ được phép lấy từ biến môi trường, cấm đặt trong file TOML.
SECRET_FIELD_PATHS = frozenset(
    {
        "object_store.access_key",
        "object_store.secret_key",
        "postgres.mart.password",
        "postgres.control.password",
        "postgres.airflow.password",
        "postgres.metabase.password",
    }
)

# Ánh xạ tên biến môi trường tường minh trong .env.example sang đường dẫn trường cấu hình.
EXPLICIT_ENV_MAPPING: dict[str, str] = {
    "N06_PROFILE": "profile",
    "N06_BUSINESS_TIMEZONE": "business_timezone",
    "N06_OBJECT_STORE_ACCESS_KEY": "object_store.access_key",
    "N06_OBJECT_STORE_SECRET_KEY": "object_store.secret_key",
    "N06_POSTGRES_MART_PASSWORD": "postgres.mart.password",
    "N06_POSTGRES_CONTROL_PASSWORD": "postgres.control.password",
    "N06_POSTGRES_AIRFLOW_PASSWORD": "postgres.airflow.password",
    "N06_POSTGRES_METABASE_PASSWORD": "postgres.metabase.password",
}

# Kiểu mong đợi cho các trường lá để hỗ trợ parse biến môi trường và kiểm tra kiểu nghiêm ngặt.
FIELD_TYPES: dict[str, type] = {
    "profile": str,
    "business_timezone": str,
    "services.airflow_enabled": bool,
    "services.metabase_enabled": bool,
    "kafka.bootstrap_servers": str,
    "kafka.events_topic": str,
    "kafka.num_partitions": int,
    "kafka.replication_factor": int,
    "kafka.consumer_group": str,
    "kafka.max_offsets_per_trigger": int,
    "kafka.trigger_interval_seconds": int,
    "object_store.endpoint": str,
    "object_store.region": str,
    "object_store.bucket": str,
    "object_store.path_style_access": bool,
    "object_store.access_key": str,
    "object_store.secret_key": str,
    "postgres.mart.host": str,
    "postgres.mart.port": int,
    "postgres.mart.database": str,
    "postgres.mart.user": str,
    "postgres.mart.password": str,
    "postgres.control.host": str,
    "postgres.control.port": int,
    "postgres.control.database": str,
    "postgres.control.user": str,
    "postgres.control.password": str,
    "postgres.airflow.host": str,
    "postgres.airflow.port": int,
    "postgres.airflow.database": str,
    "postgres.airflow.user": str,
    "postgres.airflow.password": str,
    "postgres.metabase.host": str,
    "postgres.metabase.port": int,
    "postgres.metabase.database": str,
    "postgres.metabase.user": str,
    "postgres.metabase.password": str,
    "spark.app_name": str,
    "spark.master": str,
    "spark.shuffle_partitions": int,
    "spark.cores": int,
    "spark.driver_memory": str,
    "spark.executor_memory": str,
    "spark.aqe_enabled": bool,
    "spark.target_file_size_mb": int,
    "spark.event_log_enabled": bool,
    "spark.event_log_dir": str,
    "producer.rate": int,
    "producer.key_strategy": str,
    "producer.acks": str,
    "producer.retries": int,
    "producer.batch_size": int,
    "producer.flush_timeout_seconds": int,
    "producer.max_message_bytes": int,
    "producer.metadata_timeout_seconds": int,
    "producer.leader_wait_seconds": int,
    "dataset.dataset_id": str,
    "dataset.seed": int,
    "dataset.from_date": str,
    "dataset.to_date": str,
    "dataset.transaction_count": int,
    "dataset.account_count": int,
    "dataset.currency": str,
    "dataset.allowed_lateness_seconds": int,
    "paths.raw_prefix": str,
    "paths.bronze_prefix": str,
    "paths.silver_prefix": str,
    "paths.gold_prefix": str,
    "paths.quarantine_prefix": str,
    "paths.statements_prefix": str,
    "paths.checkpoints_prefix": str,
    "paths.benchmark_prefix": str,
    "paths.generator_output_dir": str,
    "paths.reports_dir": str,
    "limits.max_transactions": int,
    "limits.min_free_disk_gb_reserve": int,
    "limits.generator_bytes_per_tx": int,
    "limits.pipeline_bytes_per_tx": int,
    "limits.disk_probe_path": str,
}


class ConfigValidationError(ValueError):
    """Lỗi validate cấu hình, luôn mang đường dẫn trường (field_path) và giá trị sai."""

    def __init__(self, field_path: str, invalid_value: Any, reason: str) -> None:
        self.field_path = field_path
        self.invalid_value = invalid_value
        self.reason = reason
        super().__init__(
            f"Lỗi cấu hình tại trường '{field_path}' (giá trị={invalid_value!r}): {reason}"
        )


@dataclass(frozen=True)
class ServicesConfig:
    airflow_enabled: bool
    metabase_enabled: bool


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    events_topic: str
    num_partitions: int
    replication_factor: int
    consumer_group: str
    max_offsets_per_trigger: int
    trigger_interval_seconds: int


@dataclass(frozen=True)
class ObjectStoreConfig:
    endpoint: str
    region: str
    bucket: str
    path_style_access: bool
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class PostgresDbConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class PostgresConfig:
    mart: PostgresDbConfig
    control: PostgresDbConfig
    airflow: PostgresDbConfig
    metabase: PostgresDbConfig


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    master: str
    shuffle_partitions: int
    cores: int
    driver_memory: str
    executor_memory: str
    aqe_enabled: bool
    target_file_size_mb: int
    event_log_enabled: bool
    event_log_dir: str


@dataclass(frozen=True)
class ProducerConfig:
    rate: int
    key_strategy: str
    acks: str
    retries: int
    batch_size: int
    flush_timeout_seconds: int
    max_message_bytes: int
    metadata_timeout_seconds: int
    leader_wait_seconds: int


@dataclass(frozen=True)
class DatasetConfig:
    dataset_id: str
    seed: int
    from_date: str
    to_date: str
    transaction_count: int
    account_count: int
    currency: str
    allowed_lateness_seconds: int


@dataclass(frozen=True)
class PathsConfig:
    raw_prefix: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    quarantine_prefix: str
    statements_prefix: str
    checkpoints_prefix: str
    benchmark_prefix: str
    generator_output_dir: str
    reports_dir: str


@dataclass(frozen=True)
class LimitsConfig:
    """Giới hạn theo dung lượng máy (CFG-scale-guard). GB = 10^9 byte, KB = 10^3 byte.

    `max_transactions`, `pipeline_bytes_per_tx` và `disk_probe_path` là tùy chọn: `None` nghĩa là
    không đặt (pipeline_bytes_per_tx = None nghĩa là phần pipeline CHƯA ĐO).
    """

    max_transactions: int | None
    min_free_disk_gb_reserve: int
    generator_bytes_per_tx: int
    pipeline_bytes_per_tx: int | None
    disk_probe_path: str | None


@dataclass(frozen=True)
class AppConfig:
    profile: str
    business_timezone: str
    services: ServicesConfig
    kafka: KafkaConfig
    object_store: ObjectStoreConfig
    postgres: PostgresConfig
    spark: SparkConfig
    producer: ProducerConfig
    dataset: DatasetConfig
    paths: PathsConfig
    limits: LimitsConfig


def _deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """Gộp đệ quy dict override đè lên base."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _set_dotted(target: dict[str, Any], dotted_path: str, value: Any) -> None:
    """Gán giá trị vào dict lồng nhau theo đường dẫn dấu chấm (ví dụ 'producer.key_strategy')."""
    parts = dotted_path.split(".")
    curr = target
    for part in parts[:-1]:
        node = curr.get(part)
        if not isinstance(node, dict):
            node = {}
            curr[part] = node
        curr = node
    curr[parts[-1]] = value


def _get_dotted(source: Mapping[str, Any], dotted_path: str) -> tuple[bool, Any]:
    """Lấy giá trị theo đường dẫn dấu chấm; trả về (found, value)."""
    curr: Any = source
    for part in dotted_path.split("."):
        if not isinstance(curr, Mapping) or part not in curr:
            return False, None
        curr = curr[part]
    return True, curr


def _flatten_paths(data: Mapping[str, Any], prefix: str = "") -> set[str]:
    """Trả về tập tất cả đường dẫn lá dạng dấu chấm trong một dict lồng nhau."""
    paths: set[str] = set()
    for key, value in data.items():
        full_key = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            paths.update(_flatten_paths(value, full_key))
        else:
            paths.add(full_key)
    return paths


def _check_no_secrets_in_file_data(file_data: Mapping[str, Any], source_name: str) -> None:
    """Chặn cứng việc đặt password hoặc secret_key trong file cấu hình TOML."""
    for path in _flatten_paths(file_data):
        lower_path = path.lower()
        if (
            path in SECRET_FIELD_PATHS
            or lower_path.endswith(".password")
            or lower_path.endswith(".secret_key")
        ):
            _, val = _get_dotted(file_data, path)
            raise ConfigValidationError(
                path,
                val,
                f"Credential không được phép khai báo trong file cấu hình ({source_name}); "
                "chỉ được lấy từ biến môi trường.",
            )


def _coerce_env_value(field_path: str, raw_val: str) -> Any:
    """Chuyển chuỗi từ biến môi trường sang kiểu mục tiêu; giữ nguyên nếu sai kiểu."""
    expected_type = FIELD_TYPES.get(field_path)
    if expected_type is int:
        stripped = raw_val.strip()
        if re.fullmatch(r"[+-]?\d+", stripped):
            return int(stripped)
        return raw_val
    if expected_type is bool:
        lower = raw_val.strip().lower()
        if lower in ("true", "1", "yes"):
            return True
        if lower in ("false", "0", "no"):
            return False
        return raw_val
    return raw_val


def _extract_env_overrides(env: Mapping[str, str]) -> dict[str, Any]:
    """Trích xuất các biến môi trường N06_* và N06__<SECTION>__<KEY> thành dict cấu hình."""
    overrides: dict[str, Any] = {}
    for env_key, dotted_path in EXPLICIT_ENV_MAPPING.items():
        if env_key in env:
            _set_dotted(overrides, dotted_path, _coerce_env_value(dotted_path, env[env_key]))

    for env_key, raw_val in env.items():
        if not env_key.startswith("N06__"):
            continue
        raw_parts = [part.lower() for part in env_key[len("N06__") :].split("__") if part]
        if not raw_parts:
            continue
        dotted_path = ".".join(raw_parts)
        _set_dotted(overrides, dotted_path, _coerce_env_value(dotted_path, raw_val))
    return overrides


def _normalize_cli_overrides(cli_overrides: Mapping[str, Any]) -> dict[str, Any]:
    """Chuẩn hóa cli_overrides hỗ trợ cả nested dict và dotted key."""
    normalized: dict[str, Any] = {}
    for key, value in cli_overrides.items():
        if "." in key:
            _set_dotted(normalized, key, value)
        elif isinstance(value, Mapping):
            sub = _normalize_cli_overrides(value)
            existing = normalized.get(key)
            if isinstance(existing, dict):
                normalized[key] = _deep_merge(existing, sub)
            else:
                normalized[key] = sub
        else:
            normalized[key] = copy.deepcopy(value)
    return normalized


def _load_toml_file(path: Path, field_context: str) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigValidationError(
            field_context,
            str(path),
            f"Không tìm thấy file cấu hình '{path}'.",
        )
    try:
        content = path.read_bytes()
        return tomllib.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ConfigValidationError(
            field_context,
            str(path),
            f"File TOML không hợp lệ: {exc}",
        ) from exc


def _require_typed(data: Mapping[str, Any], field_path: str) -> Any:
    """Lấy trường bắt buộc và kiểm tra đúng kiểu dữ liệu (không cho bool trượt qua int)."""
    found, value = _get_dotted(data, field_path)
    if not found or value is None:
        raise ConfigValidationError(field_path, None, "Thiếu trường cấu hình bắt buộc.")

    expected_type = FIELD_TYPES[field_path]
    if expected_type is int:
        if type(value) is not int:
            raise ConfigValidationError(
                field_path,
                value,
                f"Sai kiểu dữ liệu: yêu cầu int, nhận {type(value).__name__}.",
            )
    elif expected_type is bool:
        if type(value) is not bool:
            raise ConfigValidationError(
                field_path,
                value,
                f"Sai kiểu dữ liệu: yêu cầu bool, nhận {type(value).__name__}.",
            )
    elif expected_type is str:
        if not isinstance(value, str):
            raise ConfigValidationError(
                field_path,
                value,
                f"Sai kiểu dữ liệu: yêu cầu str, nhận {type(value).__name__}.",
            )
        if not value.strip():
            raise ConfigValidationError(field_path, value, "Giá trị chuỗi không được để trống.")
    return value


def _optional_typed(data: Mapping[str, Any], field_path: str) -> Any:
    """Lấy trường tùy chọn: vắng mặt trả `None`; có mặt thì kiểm kiểu như trường bắt buộc."""
    found, value = _get_dotted(data, field_path)
    if not found or value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None  # biến môi trường rỗng (`N06__LIMITS__X=`) nghĩa là không đặt
    return _require_typed(data, field_path)


def _require_min_int(field_path: str, value: int, minimum: int) -> int:
    if value < minimum:
        raise ConfigValidationError(
            field_path,
            value,
            f"Giá trị phải >= {minimum}.",
        )
    return value


def _parse_iso_date(field_path: str, value: str) -> date:
    if not ISO_DATE_PATTERN.fullmatch(value):
        raise ConfigValidationError(
            field_path,
            value,
            "Ngày phải có định dạng chuẩn YYYY-MM-DD.",
        )
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ConfigValidationError(
            field_path,
            value,
            f"Ngày không hợp lệ: {exc}",
        ) from exc


def validate_config_dict(merged: Mapping[str, Any]) -> AppConfig:
    """Kiểm tra toàn bộ ràng buộc kiểu và nghiệp vụ trên dict cấu hình đã gộp."""
    all_paths = _flatten_paths(merged)
    unknown_paths = sorted(all_paths - set(FIELD_TYPES.keys()))
    if unknown_paths:
        first_unknown = unknown_paths[0]
        _, bad_val = _get_dotted(merged, first_unknown)
        raise ConfigValidationError(
            first_unknown,
            bad_val,
            "Trường cấu hình không xác định trong schema.",
        )

    profile = _require_typed(merged, "profile")
    business_timezone = _require_typed(merged, "business_timezone")
    if business_timezone != SUPPORTED_BUSINESS_TIMEZONE:
        raise ConfigValidationError(
            "business_timezone",
            business_timezone,
            f"Hợp đồng v1 chỉ chấp nhận '{SUPPORTED_BUSINESS_TIMEZONE}'.",
        )

    services = ServicesConfig(
        airflow_enabled=_require_typed(merged, "services.airflow_enabled"),
        metabase_enabled=_require_typed(merged, "services.metabase_enabled"),
    )

    kafka = KafkaConfig(
        bootstrap_servers=_require_typed(merged, "kafka.bootstrap_servers"),
        events_topic=_require_typed(merged, "kafka.events_topic"),
        num_partitions=_require_min_int(
            "kafka.num_partitions",
            _require_typed(merged, "kafka.num_partitions"),
            1,
        ),
        replication_factor=_require_min_int(
            "kafka.replication_factor",
            _require_typed(merged, "kafka.replication_factor"),
            1,
        ),
        consumer_group=_require_typed(merged, "kafka.consumer_group"),
        max_offsets_per_trigger=_require_min_int(
            "kafka.max_offsets_per_trigger",
            _require_typed(merged, "kafka.max_offsets_per_trigger"),
            1,
        ),
        trigger_interval_seconds=_require_min_int(
            "kafka.trigger_interval_seconds",
            _require_typed(merged, "kafka.trigger_interval_seconds"),
            1,
        ),
    )

    object_store = ObjectStoreConfig(
        endpoint=_require_typed(merged, "object_store.endpoint"),
        region=_require_typed(merged, "object_store.region"),
        bucket=_require_typed(merged, "object_store.bucket"),
        path_style_access=_require_typed(merged, "object_store.path_style_access"),
        access_key=_require_typed(merged, "object_store.access_key"),
        secret_key=_require_typed(merged, "object_store.secret_key"),
    )

    pg_configs: dict[str, PostgresDbConfig] = {}
    seen_databases: dict[str, str] = {}
    seen_users: dict[str, str] = {}
    for role in POSTGRES_ROLES:
        prefix = f"postgres.{role}"
        host = _require_typed(merged, f"{prefix}.host")
        port = _require_typed(merged, f"{prefix}.port")
        if not 1 <= port <= 65535:
            raise ConfigValidationError(
                f"{prefix}.port",
                port,
                "Port phải nằm trong khoảng 1..65535.",
            )
        database = _require_typed(merged, f"{prefix}.database")
        user = _require_typed(merged, f"{prefix}.user")
        password = _require_typed(merged, f"{prefix}.password")

        if database in seen_databases:
            other_role = seen_databases[database]
            raise ConfigValidationError(
                f"{prefix}.database",
                database,
                f"PostgreSQL database của '{role}' không được trùng với '{other_role}'.",
            )
        seen_databases[database] = role

        if user in seen_users:
            other_role = seen_users[user]
            raise ConfigValidationError(
                f"{prefix}.user",
                user,
                f"PostgreSQL user của '{role}' không được trùng với '{other_role}'.",
            )
        seen_users[user] = role

        pg_configs[role] = PostgresDbConfig(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )

    postgres = PostgresConfig(
        mart=pg_configs["mart"],
        control=pg_configs["control"],
        airflow=pg_configs["airflow"],
        metabase=pg_configs["metabase"],
    )

    spark = SparkConfig(
        app_name=_require_typed(merged, "spark.app_name"),
        master=_require_typed(merged, "spark.master"),
        shuffle_partitions=_require_min_int(
            "spark.shuffle_partitions",
            _require_typed(merged, "spark.shuffle_partitions"),
            1,
        ),
        cores=_require_min_int(
            "spark.cores",
            _require_typed(merged, "spark.cores"),
            1,
        ),
        driver_memory=_require_typed(merged, "spark.driver_memory"),
        executor_memory=_require_typed(merged, "spark.executor_memory"),
        aqe_enabled=_require_typed(merged, "spark.aqe_enabled"),
        target_file_size_mb=_require_min_int(
            "spark.target_file_size_mb",
            _require_typed(merged, "spark.target_file_size_mb"),
            1,
        ),
        event_log_enabled=_require_typed(merged, "spark.event_log_enabled"),
        event_log_dir=_require_typed(merged, "spark.event_log_dir"),
    )

    rate = _require_min_int(
        "producer.rate",
        _require_typed(merged, "producer.rate"),
        0,
    )
    key_strategy = _require_typed(merged, "producer.key_strategy")
    if key_strategy not in VALID_KEY_STRATEGIES:
        raise ConfigValidationError(
            "producer.key_strategy",
            key_strategy,
            f"Giá trị ngoài enum cho phép {VALID_KEY_STRATEGIES}.",
        )
    producer = ProducerConfig(
        rate=rate,
        key_strategy=key_strategy,
        acks=_require_typed(merged, "producer.acks"),
        retries=_require_min_int(
            "producer.retries",
            _require_typed(merged, "producer.retries"),
            0,
        ),
        batch_size=_require_min_int(
            "producer.batch_size",
            _require_typed(merged, "producer.batch_size"),
            1,
        ),
        flush_timeout_seconds=_require_min_int(
            "producer.flush_timeout_seconds",
            _require_typed(merged, "producer.flush_timeout_seconds"),
            1,
        ),
        max_message_bytes=_require_min_int(
            "producer.max_message_bytes",
            _require_typed(merged, "producer.max_message_bytes"),
            2048,
        ),
        metadata_timeout_seconds=_require_min_int(
            "producer.metadata_timeout_seconds",
            _require_typed(merged, "producer.metadata_timeout_seconds"),
            1,
        ),
        leader_wait_seconds=_require_min_int(
            "producer.leader_wait_seconds",
            _require_typed(merged, "producer.leader_wait_seconds"),
            1,
        ),
    )

    dataset_id = _require_typed(merged, "dataset.dataset_id")
    if not DATASET_ID_PATTERN.fullmatch(dataset_id):
        raise ConfigValidationError(
            "dataset.dataset_id",
            dataset_id,
            "dataset_id không khớp mẫu '^[a-z][a-z0-9-]{2,63}$'.",
        )
    seed = _require_min_int("dataset.seed", _require_typed(merged, "dataset.seed"), 0)
    from_date_str = _require_typed(merged, "dataset.from_date")
    to_date_str = _require_typed(merged, "dataset.to_date")
    from_date_val = _parse_iso_date("dataset.from_date", from_date_str)
    to_date_val = _parse_iso_date("dataset.to_date", to_date_str)
    if from_date_val > to_date_val:
        raise ConfigValidationError(
            "dataset.from_date",
            from_date_str,
            f"from_date ({from_date_str}) không được lớn hơn to_date ({to_date_str}).",
        )

    transaction_count = _require_min_int(
        "dataset.transaction_count",
        _require_typed(merged, "dataset.transaction_count"),
        1,
    )
    account_count = _require_min_int(
        "dataset.account_count",
        _require_typed(merged, "dataset.account_count"),
        2,
    )
    currency = _require_typed(merged, "dataset.currency")
    if currency not in VALID_CURRENCIES:
        raise ConfigValidationError(
            "dataset.currency",
            currency,
            f"Tiền tệ ngoài danh sách hỗ trợ {VALID_CURRENCIES}.",
        )
    allowed_lateness_seconds = _require_min_int(
        "dataset.allowed_lateness_seconds",
        _require_typed(merged, "dataset.allowed_lateness_seconds"),
        1,
    )

    dataset = DatasetConfig(
        dataset_id=dataset_id,
        seed=seed,
        from_date=from_date_str,
        to_date=to_date_str,
        transaction_count=transaction_count,
        account_count=account_count,
        currency=currency,
        allowed_lateness_seconds=allowed_lateness_seconds,
    )

    paths = PathsConfig(
        raw_prefix=_require_typed(merged, "paths.raw_prefix"),
        bronze_prefix=_require_typed(merged, "paths.bronze_prefix"),
        silver_prefix=_require_typed(merged, "paths.silver_prefix"),
        gold_prefix=_require_typed(merged, "paths.gold_prefix"),
        quarantine_prefix=_require_typed(merged, "paths.quarantine_prefix"),
        statements_prefix=_require_typed(merged, "paths.statements_prefix"),
        checkpoints_prefix=_require_typed(merged, "paths.checkpoints_prefix"),
        benchmark_prefix=_require_typed(merged, "paths.benchmark_prefix"),
        generator_output_dir=_require_typed(merged, "paths.generator_output_dir"),
        reports_dir=_require_typed(merged, "paths.reports_dir"),
    )

    max_transactions = _optional_typed(merged, "limits.max_transactions")
    pipeline_bytes_per_tx = _optional_typed(merged, "limits.pipeline_bytes_per_tx")
    limits = LimitsConfig(
        max_transactions=(
            None
            if max_transactions is None
            else _require_min_int("limits.max_transactions", max_transactions, 1)
        ),
        min_free_disk_gb_reserve=_require_min_int(
            "limits.min_free_disk_gb_reserve",
            _require_typed(merged, "limits.min_free_disk_gb_reserve"),
            0,
        ),
        generator_bytes_per_tx=_require_min_int(
            "limits.generator_bytes_per_tx",
            _require_typed(merged, "limits.generator_bytes_per_tx"),
            1,
        ),
        pipeline_bytes_per_tx=(
            None
            if pipeline_bytes_per_tx is None
            else _require_min_int("limits.pipeline_bytes_per_tx", pipeline_bytes_per_tx, 1)
        ),
        disk_probe_path=_optional_typed(merged, "limits.disk_probe_path"),
    )

    operational_prefixes = [
        paths.raw_prefix,
        paths.bronze_prefix,
        paths.silver_prefix,
        paths.gold_prefix,
        paths.quarantine_prefix,
        paths.statements_prefix,
        paths.checkpoints_prefix,
        paths.benchmark_prefix,
    ]
    if len(set(operational_prefixes)) != len(operational_prefixes):
        raise ConfigValidationError(
            "paths",
            operational_prefixes,
            "Các prefix tầng dữ liệu trên object store không được trùng nhau.",
        )

    return AppConfig(
        profile=profile,
        business_timezone=business_timezone,
        services=services,
        kafka=kafka,
        object_store=object_store,
        postgres=postgres,
        spark=spark,
        producer=producer,
        dataset=dataset,
        paths=paths,
        limits=limits,
    )


def load_config(
    *,
    profile: str | None = None,
    config_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
    cli_overrides: Mapping[str, Any] | None = None,
) -> AppConfig:
    """Nạp, gộp 4 tầng (default < profile < env < cli_overrides) và validate cấu hình."""
    resolved_config_dir = config_dir if config_dir is not None else DEFAULT_CONFIG_DIR
    resolved_env: Mapping[str, str] = env if env is not None else os.environ
    normalized_cli = _normalize_cli_overrides(cli_overrides) if cli_overrides else {}
    env_overrides = _extract_env_overrides(resolved_env)

    # 1. Tầng thấp nhất: default.toml
    default_path = resolved_config_dir / "default.toml"
    base_data = _load_toml_file(default_path, "config.default")
    _check_no_secrets_in_file_data(base_data, default_path.name)

    # Thứ tự chọn profile: default < env < tham số profile < cli_overrides["profile"]
    selected_profile = base_data.get("profile", "default")
    if "profile" in env_overrides:
        selected_profile = env_overrides["profile"]
    if profile is not None:
        selected_profile = profile
    if "profile" in normalized_cli:
        selected_profile = normalized_cli["profile"]

    if not isinstance(selected_profile, str) or not selected_profile.strip():
        raise ConfigValidationError(
            "profile",
            selected_profile,
            "Tên profile phải là chuỗi hợp lệ.",
        )

    # 2. Tầng 2: <profile>.toml (nếu khác "default")
    merged = base_data
    if selected_profile != "default":
        profile_path = resolved_config_dir / f"{selected_profile}.toml"
        profile_data = _load_toml_file(profile_path, "profile")
        _check_no_secrets_in_file_data(profile_data, profile_path.name)
        merged = _deep_merge(merged, profile_data)

    # 3. Tầng 3: Biến môi trường
    merged = _deep_merge(merged, env_overrides)

    # 4. Tầng 4 (cao nhất): Tham số CLI ghi đè
    if profile is not None and "profile" not in normalized_cli:
        normalized_cli = _deep_merge(normalized_cli, {"profile": profile})
    merged = _deep_merge(merged, normalized_cli)

    return validate_config_dict(merged)
