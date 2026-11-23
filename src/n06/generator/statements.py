"""Sao kê kênh mô phỏng: biến đổi lỗi, mốc D, chia file theo cửa sổ (Phần II mục 5.2, 5.3)."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from n06.domain import MAX_AMOUNT_MINOR, parse_utc_timestamp
from n06.generator.params import DatasetParams

_UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_CURRENCY_SWAP = {"VND": "USD", "USD": "VND"}


def format_utc(moment: datetime) -> str:
    """Biểu diễn UTC dạng `YYYY-MM-DDTHH:mm:ssZ`, không phần giây lẻ."""
    return moment.astimezone(UTC).strftime(_UTC_FORMAT)


def deadline_d(business_date: str, business_timezone: str) -> datetime:
    """Mốc D: 00:00:00 theo múi giờ nghiệp vụ của ngày sau `business_date`, đổi sang UTC."""
    next_day = date.fromisoformat(business_date) + timedelta(days=1)
    local_midnight = datetime(
        next_day.year, next_day.month, next_day.day, tzinfo=ZoneInfo(business_timezone)
    )
    return local_midnight.astimezone(UTC)


def statement_available_at(tx: Mapping[str, Any], fault: str | None, params: DatasetParams) -> str:
    if fault == "late_within_deadline":
        delay = params.late_within_delay_seconds
        base = deadline_d(str(tx["business_date"]), params.business_timezone)
    elif fault == "late_after_deadline":
        delay = params.late_after_delay_seconds
        base = deadline_d(str(tx["business_date"]), params.business_timezone)
    else:
        delay = params.normal_statement_delay_seconds
        base = parse_utc_timestamp(str(tx["event_time"]))
    return format_utc(base + timedelta(seconds=delay))


def build_statement_rows(
    tx: Mapping[str, Any], ordinal: int, fault: str | None, params: DatasetParams
) -> list[tuple[dict[str, Any], str]]:
    """Trả `[(dòng sao kê, kind)]`; rỗng nếu `missing_external`. kind là original/duplicate."""
    if fault == "missing_external":
        return []
    amount = int(tx["amount_minor"])
    currency = str(tx["currency"])
    status = str(tx["status"])
    if fault == "amount_mismatch":
        amount = amount - 1 if amount >= MAX_AMOUNT_MINOR else amount + 1
    elif fault == "currency_mismatch":
        currency = _CURRENCY_SWAP[currency]
    elif fault == "status_mismatch":
        status = "FAILED" if status == "SUCCESS" else status

    def row(discriminator: str) -> dict[str, Any]:
        return {
            "schema_version": params.contract_version,
            "dataset_id": params.dataset_id,
            "statement_record_id": params.uuid("statement_record", ordinal, discriminator),
            "external_reference": str(tx["external_reference"]),
            "channel": str(tx["channel"]),
            "amount_minor": amount,
            "currency": currency,
            "status": status,
            "event_time": str(tx["event_time"]),
            "business_date": str(tx["business_date"]),
            "available_at": statement_available_at(tx, fault, params),
        }

    rows = [(row("original"), "original")]
    if fault == "statement_duplicate":
        rows.append((row("duplicate"), "duplicate"))
    return rows


def statement_window(available_at: str, window_seconds: int) -> tuple[int, str]:
    """`w = floor(epoch(available_at)/W)`; `file_available_at = (w+1)*W`."""
    epoch = int(parse_utc_timestamp(available_at).timestamp())
    window = epoch // window_seconds
    file_available = datetime.fromtimestamp((window + 1) * window_seconds, tz=UTC)
    return window, format_utc(file_available)
