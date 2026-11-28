"""Logic thuần cho `n06 load-mart` (T26, ADR-0005 §4): lập kế hoạch đọc snapshot và dựng bảng mart.

Module này không import `pyspark` để chạy được unit test nhanh trên host. Job Spark thật nằm ở
`n06.warehouse.load_mart_job`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from n06.storage.control_store import SnapshotRecord
from n06.warehouse.schema import FACT_TABLES, TableSpec

GOLD_FACT_ENTITIES: tuple[str, ...] = tuple(spec.name for spec in FACT_TABLES)

_CHANNEL_LABELS: Mapping[str, str] = {
    "SIM_CHANNEL_A": "Kênh thanh toán mô phỏng A",
    "SIM_CHANNEL_B": "Kênh thanh toán mô phỏng B",
}

_TX_TYPE_DESCRIPTIONS: Mapping[str, str] = {
    "TRANSFER": "Chuyển khoản giữa hai tài khoản",
    "PAYMENT": "Thanh toán giao dịch",
}


class LoadMartError(RuntimeError):
    """Lỗi nghiệp vụ hoặc thiếu dữ liệu đầu vào khi nạp mart."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class LoadPlan:
    """Kế hoạch đọc Gold/Silver đã commit từ `current_snapshot` để publish vào mart."""

    dataset_id: str
    business_date_from: date
    business_date_to: date
    gold_run_id: str
    gold_run_ids: tuple[str, ...]
    fact_paths: Mapping[str, tuple[str, ...]]
    account_paths: tuple[str, ...]
    snapshots_count: int


def build_dim_date_rows(dates: Iterable[date]) -> list[dict[str, object]]:
    """Tạo các dòng `dim_date` theo ADR-0005 (ISO weekday: 1=Thứ 2 .. 7=Chủ nhật)."""
    unique_dates = sorted(set(dates))
    rows: list[dict[str, object]] = []
    for d in unique_dates:
        if type(d) is not date:
            raise LoadMartError("INVALID_DATE", f"Cần giá trị date cho dim_date, nhận {type(d)}")
        rows.append(
            {
                "date_key": d.year * 10000 + d.month * 100 + d.day,
                "calendar_date": d,
                "year": d.year,
                "quarter": (d.month - 1) // 3 + 1,
                "month": d.month,
                "day": d.day,
                "day_of_week": d.isoweekday(),
            }
        )
    return rows


def build_dim_channel_rows(channels: Iterable[str]) -> list[dict[str, object]]:
    """Tạo các dòng `dim_channel` từ tập `channel` có mặt trong dữ liệu Gold."""
    return [
        {"channel": ch, "channel_label": _CHANNEL_LABELS.get(ch, ch)}
        for ch in sorted(set(channels))
    ]


def build_dim_transaction_type_rows(tx_types: Iterable[str]) -> list[dict[str, object]]:
    """Tạo các dòng `dim_transaction_type` từ tập `transaction_type` có mặt trong dữ liệu Gold."""
    return [
        {"transaction_type": t, "description": _TX_TYPE_DESCRIPTIONS.get(t, t)}
        for t in sorted(set(tx_types))
    ]


def _normalize_timestamp(value: object) -> object:
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def spark_row_to_mart_row(spec: TableSpec, row: Mapping[str, Any]) -> dict[str, object]:
    """Chuyển một dòng đọc từ Parquet (Spark Row dict) sang dict đúng cột của `spec`.

    - Bỏ cột phụ không thuộc bảng mart (ví dụ `source_file`, `business_date` từ `silver.accounts`).
    - Cột `nullable=True` bị thiếu trong Parquet (ví dụ `input_snapshot_id` ở T25) điền `None`.
    - `datetime` naive từ Spark UTC session được gắn `tzinfo=UTC` để phù hợp `TIMESTAMPTZ`.
    """
    out: dict[str, object] = {}
    for col in spec.columns:
        if col.name in row:
            val = row[col.name]
            if col.sql_type == "TIMESTAMPTZ":
                val = _normalize_timestamp(val)
            out[col.name] = val
        elif col.nullable:
            out[col.name] = None
        else:
            raise LoadMartError(
                "MISSING_COLUMN",
                f"Parquet cho bảng {spec.name} thiếu cột bắt buộc {col.name!r}.",
            )
    return out


def select_publish_plan(
    gold_snapshots: Sequence[SnapshotRecord],
    account_snapshots: Sequence[SnapshotRecord],
    *,
    dataset_id: str,
    date_from: date | None = None,
    date_to: date | None = None,
) -> LoadPlan:
    """Chọn đường dẫn Parquet đã commit từ `current_snapshot` cho phạm vi cần publish.

    - Chỉ nhận snapshot của đúng `dataset_id` và `layer == 'gold'` cho các bảng fact (và
      `dim_account` nếu đã có ở Gold).
    - Nếu chưa có `gold.dim_account`, dùng snapshot ngày mới nhất của `silver.accounts` trong
      phạm vi (hoặc mới nhất hiện có của dataset) vì `silver.accounts` là ảnh toàn bộ tài khoản.
    """
    if date_from is not None and date_to is not None and date_from > date_to:
        raise LoadMartError("INVALID_DATE_RANGE", "from_date phải <= to_date.")

    gold_facts = [
        s
        for s in gold_snapshots
        if s.dataset_id == dataset_id
        and s.layer == "gold"
        and s.entity in GOLD_FACT_ENTITIES
        and (date_from is None or s.business_date >= date_from)
        and (date_to is None or s.business_date <= date_to)
    ]
    if not gold_facts:
        raise LoadMartError(
            "NO_GOLD_SNAPSHOTS",
            f"Không tìm thấy snapshot Gold đã commit nào cho dataset {dataset_id!r}"
            + (
                f" trong khoảng {date_from or '..'}..{date_to or '..'}."
                if (date_from or date_to)
                else "."
            ),
        )

    all_dates = [s.business_date for s in gold_facts]
    effective_from = date_from if date_from is not None else min(all_dates)
    effective_to = date_to if date_to is not None else max(all_dates)

    fact_paths: dict[str, tuple[str, ...]] = {}
    for entity in GOLD_FACT_ENTITIES:
        paths = tuple(
            s.output_path
            for s in sorted(
                (snap for snap in gold_facts if snap.entity == entity),
                key=lambda snap: snap.business_date,
            )
        )
        fact_paths[entity] = paths

    gold_dim_accounts = [
        s
        for s in gold_snapshots
        if s.dataset_id == dataset_id
        and s.layer == "gold"
        and s.entity == "dim_account"
        and effective_from <= s.business_date <= effective_to
    ]
    if gold_dim_accounts:
        latest_acc = max(gold_dim_accounts, key=lambda s: s.business_date)
        account_paths = (latest_acc.output_path,)
    else:
        silver_accounts = [
            s
            for s in account_snapshots
            if s.dataset_id == dataset_id and s.layer == "silver" and s.entity == "accounts"
        ]
        in_range = [s for s in silver_accounts if effective_from <= s.business_date <= effective_to]
        chosen_pool = in_range or silver_accounts
        if chosen_pool:
            latest_acc = max(chosen_pool, key=lambda s: s.business_date)
            account_paths = (latest_acc.output_path,)
        else:
            account_paths = ()

    # Lấy run_id đại diện từ snapshot ngày mới nhất; ghi nhận mọi run_id Gold duy nhất để cập nhật
    # trạng thái `MART_PUBLISHED` trong Control DB sau khi commit mart thành công.
    latest_fact = max(gold_facts, key=lambda s: (s.business_date, s.run_id))
    unique_runs = tuple(dict.fromkeys(s.run_id for s in gold_facts))

    return LoadPlan(
        dataset_id=dataset_id,
        business_date_from=effective_from,
        business_date_to=effective_to,
        gold_run_id=latest_fact.run_id,
        gold_run_ids=unique_runs,
        fact_paths=fact_paths,
        account_paths=account_paths,
        snapshots_count=len(gold_facts) + len(account_paths),
    )
