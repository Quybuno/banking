"""Publish mart: staging, kiểm tra, rồi thay thế phạm vi ngày trong MỘT transaction (ADR-0005 §4).

Luồng (đặc tả Phần I mục 8.2):
1. `create_staging`: tạo bảng staging `stg_<bảng>_<hex>` (tên từ danh sách cho phép, không từ chuỗi
   người dùng) bằng `CREATE TABLE ... AS SELECT * ... WITH NO DATA`, nên không có ràng buộc và việc
   kiểm tra PK/null/trùng chạy được trên dữ liệu thô.
2. `stage_rows` (hoặc nạp bằng đường khác, ví dụ Spark JDBC vào staging, rồi gọi `publish_staged`).
3. `validate_staging`: không null PK/cột bắt buộc, không trùng PK, đúng `dataset_id` và khoảng ngày,
   `opening + credit - debit = closing`. Lỗi thì không đụng vào mart.
4. `publish_staged`: một transaction upsert 4 dimension, với mỗi fact `DELETE` đúng
   `(dataset_id, business_date trong khoảng)` rồi `INSERT` từ staging, ghi `mart_publish_metadata`.
   Lỗi bất kỳ giữa chừng thì không COMMIT và mart giữ nguyên bản cũ.

Giá trị dữ liệu luôn đi qua biến psql `:'params'` (JSON), không ghép vào câu SQL. Tiền là số
nguyên Python (`int`), kiểm miền trước khi gửi.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from n06.warehouse.runner import SqlRunner, with_params
from n06.warehouse.schema import (
    ALL_TABLES,
    DIMENSION_TABLES,
    FACT_TABLES,
    INTEGER_RANGES,
    TABLES_BY_NAME,
    TableSpec,
)

DATASET_ID_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
STAGING_NAME_RE = re.compile(r"^stg_[a-z_]{1,40}_[0-9a-f]{16}$")

DEFAULT_STAGE_CHUNK_ROWS = 2000

_PARAM_DATASET = "(:'params'::jsonb ->> 'dataset_id')"
_PARAM_FROM = "((:'params'::jsonb ->> 'date_from')::date)"
_PARAM_TO = "((:'params'::jsonb ->> 'date_to')::date)"


class MartPublishError(RuntimeError):
    """Lỗi trong quá trình publish mart."""


class MartValidationError(MartPublishError):
    """Staging không đạt kiểm tra; mart chưa bị thay đổi."""

    def __init__(self, problems: Sequence[str]) -> None:
        self.problems = list(problems)
        super().__init__("Staging không đạt kiểm tra: " + "; ".join(self.problems))


@dataclass(frozen=True)
class PublishRequest:
    """Phạm vi một lần publish: dataset, Gold run và khoảng `business_date` (đóng hai đầu)."""

    dataset_id: str
    run_id: str
    business_date_from: date
    business_date_to: date
    source_watermark_at: datetime | None = None

    def __post_init__(self) -> None:
        if not DATASET_ID_RE.fullmatch(self.dataset_id):
            raise MartPublishError(f"dataset_id không hợp lệ: {self.dataset_id!r}")
        if not RUN_ID_RE.fullmatch(self.run_id):
            raise MartPublishError(f"run_id không hợp lệ: {self.run_id!r}")
        for name, value in (("from", self.business_date_from), ("to", self.business_date_to)):
            if type(value) is not date:
                raise MartPublishError(f"business_date_{name} phải là date")
        if self.business_date_from > self.business_date_to:
            raise MartPublishError("business_date_from phải <= business_date_to")
        wm = self.source_watermark_at
        if wm is not None and (wm.tzinfo is None or wm.utcoffset() is None):
            raise MartPublishError("source_watermark_at phải có múi giờ")

    def params(self) -> dict[str, Any]:
        wm = self.source_watermark_at
        return {
            "dataset_id": self.dataset_id,
            "run_id": self.run_id,
            "date_from": self.business_date_from.isoformat(),
            "date_to": self.business_date_to.isoformat(),
            "source_watermark_at": None if wm is None else wm.astimezone(UTC).isoformat(),
        }


@dataclass(frozen=True)
class PublishResult:
    """Số dòng của staging đã được publish, theo bảng."""

    dataset_id: str
    run_id: str
    rows_by_table: Mapping[str, int]


def staging_table_name(table: str, run_id: str) -> str:
    """Tên staging `stg_<bảng>_<hex>`; `table` thuộc danh sách bảng mart, hex suy từ `run_id`."""
    if table not in TABLES_BY_NAME:
        raise MartPublishError(f"Bảng không thuộc danh sách cho phép: {table!r}")
    if not RUN_ID_RE.fullmatch(run_id):
        raise MartPublishError(f"run_id không hợp lệ: {run_id!r}")
    safe_run_hex = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
    name = f"stg_{table}_{safe_run_hex}"
    if not STAGING_NAME_RE.fullmatch(name):  # phòng thủ: tên cuối cùng luôn là identifier an toàn
        raise MartPublishError(f"Tên staging không hợp lệ: {name!r}")
    return name


def check_bigint(value: object, name: str) -> int:
    """Kiểm miền BIGINT cho tiền: phải là `int` (không bool, không float) trong [-2^63, 2^63-1]."""
    return _check_integer(value, name, "BIGINT")


def _check_integer(value: object, name: str, sql_type: str) -> int:
    if type(value) is not int:
        raise MartPublishError(f"{name}: cần số nguyên, nhận {type(value).__name__}")
    low, high = INTEGER_RANGES[sql_type]
    if not low <= value <= high:
        raise MartPublishError(f"{name}: {value} ngoài miền {sql_type}")
    return value


def _json_value(table: str, column: str, sql_type: str, value: object) -> object:
    label = f"{table}.{column}"
    if value is None:
        return None
    if sql_type in INTEGER_RANGES:
        return _check_integer(value, label, sql_type)
    if sql_type == "TEXT":
        if not isinstance(value, str) or "\x00" in value:
            raise MartPublishError(f"{label}: cần chuỗi không chứa NUL")
        return value
    if sql_type == "DATE":
        if type(value) is not date:
            raise MartPublishError(f"{label}: cần date")
        return value.isoformat()
    if sql_type == "TIMESTAMPTZ":
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise MartPublishError(f"{label}: cần datetime có múi giờ")
        return value.astimezone(UTC).isoformat()
    if sql_type == "TEXT[]":
        if not isinstance(value, (list, tuple)) or not all(
            isinstance(item, str) and "\x00" not in item for item in value
        ):
            raise MartPublishError(f"{label}: cần danh sách chuỗi")
        return list(value)
    raise MartPublishError(f"{label}: kiểu không hỗ trợ {sql_type}")


def normalize_rows(spec: TableSpec, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, object]]:
    """Kiểm kiểu, đổi dòng sang giá trị JSON; cột thiếu thành NULL (staging báo nếu bắt buộc)."""
    types = {col.name: col.sql_type for col in spec.columns}
    out: list[dict[str, object]] = []
    for row in rows:
        unknown = set(row) - set(types)
        if unknown:
            raise MartPublishError(f"{spec.name}: cột không có trong schema: {sorted(unknown)}")
        out.append(
            {
                name: _json_value(spec.name, name, sql_type, row.get(name))
                for name, sql_type in types.items()
            }
        )
    return out


def _cols(spec: TableSpec) -> str:
    return ", ".join(spec.column_names)


class MartPublisher:
    """Publish dữ liệu Gold vào mart qua một `SqlRunner` (psql)."""

    def __init__(self, runner: SqlRunner, *, chunk_rows: int = DEFAULT_STAGE_CHUNK_ROWS) -> None:
        if chunk_rows <= 0:
            raise MartPublishError("chunk_rows phải > 0")
        self._runner = runner
        self._chunk_rows = chunk_rows

    # ------------------------------------------------------------------ nội bộ
    def _run(self, sql: str, *, tuples_only: bool = False) -> str:
        try:
            return self._runner(sql, tuples_only)
        except Exception as exc:
            raise MartPublishError(f"Thực thi SQL mart thất bại: {exc}") from exc

    def _staging(self, request: PublishRequest, spec: TableSpec) -> str:
        return staging_table_name(spec.name, request.run_id)

    # ----------------------------------------------------------------- staging
    def create_staging(self, request: PublishRequest) -> None:
        """Tạo (làm mới) đủ 7 bảng staging rỗng cho `request.run_id`."""
        statements = ["BEGIN;"]
        for spec in ALL_TABLES:
            stg = self._staging(request, spec)
            statements.append(f"DROP TABLE IF EXISTS {stg};")
            statements.append(
                f"CREATE UNLOGGED TABLE {stg} AS SELECT * FROM {spec.name} WITH NO DATA;"
            )
        statements.append("COMMIT;")
        self._run("\n".join(statements))

    def drop_staging(self, request: PublishRequest) -> None:
        """Xóa các bảng staging của `request.run_id` (an toàn khi chưa tồn tại)."""
        drops = [f"DROP TABLE IF EXISTS {self._staging(request, spec)};" for spec in ALL_TABLES]
        self._run("BEGIN;\n" + "\n".join(drops) + "\nCOMMIT;")

    def stage_rows(
        self, request: PublishRequest, table: str, rows: Sequence[Mapping[str, Any]]
    ) -> int:
        """Nạp `rows` vào staging của `table` theo từng lô; trả về số dòng đã nạp.

        Dùng cho khối lượng nhỏ (test, dimension). Khối lượng lớn nên nạp staging bằng đường khác
        (ví dụ Spark JDBC vào đúng tên staging) rồi gọi `publish_staged`.
        """
        spec = TABLES_BY_NAME.get(table)
        if spec is None:
            raise MartPublishError(f"Bảng không thuộc danh sách cho phép: {table!r}")
        stg = self._staging(request, spec)
        normalized = normalize_rows(spec, rows)
        for start in range(0, len(normalized), self._chunk_rows):
            chunk = normalized[start : start + self._chunk_rows]
            sql = with_params(
                f"INSERT INTO {stg} ({_cols(spec)})\n"
                f"SELECT {_cols(spec)} FROM jsonb_populate_recordset(NULL::{stg}, "
                "(:'params'::jsonb -> 'rows'));\n",
                {"rows": chunk},
            )
            self._run(sql)
        return len(normalized)

    # --------------------------------------------------------------- kiểm tra
    def _validation_sql(self, request: PublishRequest) -> str:
        parts: list[str] = []
        for spec in ALL_TABLES:
            stg = self._staging(request, spec)
            name = spec.name
            key = ", ".join(spec.primary_key)
            null_key = " OR ".join(f"{c} IS NULL" for c in spec.primary_key)
            required = [
                c.name for c in spec.columns if not c.nullable and c.name not in spec.primary_key
            ]
            parts.append(f"SELECT '{name}', 'rows', count(*) FROM {stg}")
            parts.append(
                f"SELECT '{name}', 'null_primary_key', count(*) FROM {stg} WHERE {null_key}"
            )
            if required:
                null_req = " OR ".join(f"{c} IS NULL" for c in required)
                parts.append(
                    f"SELECT '{name}', 'null_required_column', count(*) FROM {stg} WHERE {null_req}"
                )
            parts.append(
                f"SELECT '{name}', 'duplicate_primary_key', count(*) FROM "
                f"(SELECT 1 FROM {stg} GROUP BY {key} HAVING count(*) > 1) AS dup"
            )
            if spec.dataset_scoped:
                parts.append(
                    f"SELECT '{name}', 'wrong_dataset_id', count(*) FROM {stg} "
                    f"WHERE dataset_id IS DISTINCT FROM {_PARAM_DATASET}"
                )
            if spec.kind == "fact":
                parts.append(
                    f"SELECT '{name}', 'outside_business_date_range', count(*) FROM {stg} "
                    f"WHERE business_date IS NULL OR business_date < {_PARAM_FROM} "
                    f"OR business_date > {_PARAM_TO}"
                )
                parts.append(
                    f"SELECT '{name}', 'date_key_mismatch', count(*) FROM {stg} "
                    "WHERE date_key IS DISTINCT FROM to_char(business_date, 'YYYYMMDD')::int"
                )
            if name == "fact_daily_balances":
                parts.append(
                    f"SELECT '{name}', 'balance_invariant_violation', count(*) FROM {stg} "
                    "WHERE opening_balance_minor::numeric + credit_minor::numeric "
                    "- debit_minor::numeric IS DISTINCT FROM closing_balance_minor::numeric"
                )
        return with_params("\nUNION ALL\n".join(parts) + ";\n", request.params())

    def validate_staging(self, request: PublishRequest) -> dict[str, int]:
        """Kiểm tra staging; trả về số dòng theo bảng hoặc ném `MartValidationError`."""
        output = self._run(self._validation_sql(request), tuples_only=True)
        counts: dict[str, int] = {}
        problems: list[str] = []
        for line in output.splitlines():
            if not line.strip():
                continue
            table, check, raw = line.strip().split("|")
            value = int(raw)
            if check == "rows":
                counts[table] = value
            elif value > 0:
                problems.append(f"{table}: {check} = {value}")
        if set(counts) != {spec.name for spec in ALL_TABLES}:
            raise MartPublishError("Kết quả kiểm tra staging thiếu bảng")
        if problems:
            raise MartValidationError(problems)
        return counts

    # ----------------------------------------------------------------- publish
    def _transaction_sql(self, request: PublishRequest) -> str:
        lines = ["BEGIN;"]
        for spec in DIMENSION_TABLES:
            stg = self._staging(request, spec)
            key = ", ".join(spec.primary_key)
            updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in spec.non_key_columns)
            lines.append(
                f"INSERT INTO {spec.name} ({_cols(spec)}) SELECT {_cols(spec)} FROM {stg}\n"
                f"ON CONFLICT ({key}) DO UPDATE SET {updates};"
            )
        for spec in FACT_TABLES:
            stg = self._staging(request, spec)
            lines.append(
                f"DELETE FROM {spec.name} WHERE dataset_id = {_PARAM_DATASET} "
                f"AND business_date BETWEEN {_PARAM_FROM} AND {_PARAM_TO};"
            )
            lines.append(
                f"INSERT INTO {spec.name} ({_cols(spec)}) SELECT {_cols(spec)} FROM {stg};"
            )
        lines.append(
            "INSERT INTO mart_publish_metadata (dataset_id, business_date_from, business_date_to,\n"
            "    gold_run_id, mart_published_at, source_watermark_at)\n"
            f"VALUES ({_PARAM_DATASET}, {_PARAM_FROM}, {_PARAM_TO},\n"
            "    (:'params'::jsonb ->> 'run_id'), clock_timestamp(),\n"
            "    (:'params'::jsonb ->> 'source_watermark_at')::timestamptz)\n"
            "ON CONFLICT (dataset_id, gold_run_id, business_date_from, business_date_to)\n"
            "DO UPDATE SET mart_published_at = EXCLUDED.mart_published_at,\n"
            "    source_watermark_at = EXCLUDED.source_watermark_at;"
        )
        lines.append("COMMIT;")
        return with_params("\n".join(lines) + "\n", request.params())

    def publish_staged(self, request: PublishRequest) -> PublishResult:
        """Kiểm tra staging đã nạp rồi publish trong một transaction. Không xóa staging."""
        counts = self.validate_staging(request)
        self._run(self._transaction_sql(request))
        return PublishResult(request.dataset_id, request.run_id, counts)

    def publish(
        self, request: PublishRequest, tables: Mapping[str, Sequence[Mapping[str, Any]]]
    ) -> PublishResult:
        """Nạp `tables` (đủ 7 bảng, có thể rỗng), kiểm tra, publish; luôn dọn staging."""
        expected = {spec.name for spec in ALL_TABLES}
        if set(tables) != expected:
            raise MartPublishError(f"Cần đúng các bảng {sorted(expected)}; nhận {sorted(tables)}")
        # Kiểm kiểu toàn bộ trước khi chạm vào database.
        for spec in ALL_TABLES:
            normalize_rows(spec, tables[spec.name])
        self.create_staging(request)
        try:
            for spec in ALL_TABLES:
                self.stage_rows(request, spec.name, tables[spec.name])
            return self.publish_staged(request)
        finally:
            self.drop_staging(request)
