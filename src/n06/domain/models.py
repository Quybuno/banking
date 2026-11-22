"""Domain models bất biến và hằng số hợp đồng dữ liệu 1.0.0 (TASK T10).

Tham chiếu: docs/spec.md Phần I mục 4, Phần II mục 1, 2, 3, 4.
Quy tắc:
- Tiền luôn dùng số nguyên đơn vị nhỏ nhất (`amount_minor`, `opening_balance_minor`,
  `amount_minor_total`).
- Cấm dùng `float` hoặc `bool` cho các trường số nguyên tiền tệ.
- Các enum trạng thái và hằng số nghiệp vụ cố định được định nghĩa tập trung.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

CONTRACT_VERSION = "1.0.0"
BUSINESS_TIMEZONE_NAME = "Asia/Ho_Chi_Minh"

MIN_AMOUNT_MINOR = 1
MAX_AMOUNT_MINOR = 1_000_000_000_000  # 10^12
MIN_OPENING_BALANCE_MINOR = 0
MAX_OPENING_BALANCE_MINOR = 9_000_000_000_000_000  # 9 * 10^15


class Currency(StrEnum):
    """Tiền tệ hợp đồng v1.0.0 (VND exponent 0, USD exponent 2)."""

    VND = "VND"
    USD = "USD"


CURRENCY_EXPONENTS: dict[str, int] = {
    Currency.VND.value: 0,
    Currency.USD.value: 2,
}


class AccountType(StrEnum):
    """Loại tài khoản trong hợp đồng v1.0.0."""

    CUSTOMER_DEPOSIT = "CUSTOMER_DEPOSIT"


class TransactionStatus(StrEnum):
    """Trạng thái kết thúc của giao dịch (không có trạng thái trung gian trong input)."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class TransactionType(StrEnum):
    """Loại giao dịch nội bộ trong hợp đồng v1.0.0."""

    INTERNAL_TRANSFER = "INTERNAL_TRANSFER"


class Channel(StrEnum):
    """Hai kênh đối chiếu giả lập trong hợp đồng v1.0.0."""

    SIM_CHANNEL_A = "SIM_CHANNEL_A"
    SIM_CHANNEL_B = "SIM_CHANNEL_B"


class LedgerSide(StrEnum):
    """Hai vế hạch toán bút toán sổ cái (DEBIT bên gửi, CREDIT bên nhận)."""

    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class EventType(StrEnum):
    """Loại sự kiện trong event envelope."""

    TRANSACTION_CREATED = "TRANSACTION_CREATED"
    LEDGER_ENTRY_CREATED = "LEDGER_ENTRY_CREATED"


class ControlEntity(StrEnum):
    """Loại thực thể trong control_report.json."""

    TRANSACTION = "transaction"
    LEDGER_ENTRY = "ledger_entry"


class AccountSelectionMode(StrEnum):
    """Chế độ chọn cặp tài khoản của generator."""

    UNIFORM_DISTINCT_PAIR = "UNIFORM_DISTINCT_PAIR"
    HOT_ACCOUNT_V1 = "HOT_ACCOUNT_V1"


def _require_strict_int(value: Any, field_name: str) -> int:
    """Đảm bảo giá trị là kiểu `int` thuần túy (loại trừ `bool` và `float`)."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(
            f"Trường '{field_name}' phải là số nguyên (int), không chấp nhận {type(value).__name__}"
        )
    return value


@dataclass(frozen=True)
class MatchKey:
    """Khóa đối soát chuẩn theo Phần II mục 1.9: `(dataset_id, channel, external_reference)`."""

    dataset_id: str
    channel: str
    external_reference: str


@dataclass(frozen=True)
class Account:
    """Bản ghi tài khoản và số dư đầu kỳ (`account.schema.json`)."""

    schema_version: str
    dataset_id: str
    account_id: str
    currency: str
    opening_balance_minor: int
    opening_business_date: str
    account_type: str

    def __post_init__(self) -> None:
        _require_strict_int(self.opening_balance_minor, "opening_balance_minor")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Account:
        return cls(
            schema_version=str(data["schema_version"]),
            dataset_id=str(data["dataset_id"]),
            account_id=str(data["account_id"]),
            currency=str(data["currency"]),
            opening_balance_minor=_require_strict_int(
                data["opening_balance_minor"], "opening_balance_minor"
            ),
            opening_business_date=str(data["opening_business_date"]),
            account_type=str(data["account_type"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "account_id": self.account_id,
            "currency": self.currency,
            "opening_balance_minor": self.opening_balance_minor,
            "opening_business_date": self.opening_business_date,
            "account_type": self.account_type,
        }


@dataclass(frozen=True)
class Transaction:
    """Bản ghi giao dịch chuyển khoản nội bộ (`transaction.schema.json`)."""

    schema_version: str
    dataset_id: str
    transaction_id: str
    external_reference: str
    account_from: str
    account_to: str
    amount_minor: int
    currency: str
    status: str
    transaction_type: str
    channel: str
    event_time: str
    business_date: str

    def __post_init__(self) -> None:
        _require_strict_int(self.amount_minor, "amount_minor")

    @property
    def match_key(self) -> MatchKey:
        return MatchKey(
            dataset_id=self.dataset_id,
            channel=self.channel,
            external_reference=self.external_reference,
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Transaction:
        return cls(
            schema_version=str(data["schema_version"]),
            dataset_id=str(data["dataset_id"]),
            transaction_id=str(data["transaction_id"]),
            external_reference=str(data["external_reference"]),
            account_from=str(data["account_from"]),
            account_to=str(data["account_to"]),
            amount_minor=_require_strict_int(data["amount_minor"], "amount_minor"),
            currency=str(data["currency"]),
            status=str(data["status"]),
            transaction_type=str(data["transaction_type"]),
            channel=str(data["channel"]),
            event_time=str(data["event_time"]),
            business_date=str(data["business_date"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "transaction_id": self.transaction_id,
            "external_reference": self.external_reference,
            "account_from": self.account_from,
            "account_to": self.account_to,
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "status": self.status,
            "transaction_type": self.transaction_type,
            "channel": self.channel,
            "event_time": self.event_time,
            "business_date": self.business_date,
        }


@dataclass(frozen=True)
class LedgerEntry:
    """Bản ghi bút toán sổ cái (`ledger_entry.schema.json`)."""

    schema_version: str
    dataset_id: str
    entry_id: str
    transaction_id: str
    ledger_account_id: str
    side: str
    amount_minor: int
    currency: str
    event_time: str
    business_date: str

    def __post_init__(self) -> None:
        _require_strict_int(self.amount_minor, "amount_minor")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> LedgerEntry:
        return cls(
            schema_version=str(data["schema_version"]),
            dataset_id=str(data["dataset_id"]),
            entry_id=str(data["entry_id"]),
            transaction_id=str(data["transaction_id"]),
            ledger_account_id=str(data["ledger_account_id"]),
            side=str(data["side"]),
            amount_minor=_require_strict_int(data["amount_minor"], "amount_minor"),
            currency=str(data["currency"]),
            event_time=str(data["event_time"]),
            business_date=str(data["business_date"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "entry_id": self.entry_id,
            "transaction_id": self.transaction_id,
            "ledger_account_id": self.ledger_account_id,
            "side": self.side,
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "event_time": self.event_time,
            "business_date": self.business_date,
        }


@dataclass(frozen=True)
class ExternalStatement:
    """Bản ghi sao kê đối tác kênh thanh toán (`statement.schema.json`)."""

    schema_version: str
    dataset_id: str
    statement_record_id: str
    external_reference: str
    channel: str
    amount_minor: int
    currency: str
    status: str
    event_time: str
    business_date: str
    available_at: str

    def __post_init__(self) -> None:
        _require_strict_int(self.amount_minor, "amount_minor")

    @property
    def match_key(self) -> MatchKey:
        return MatchKey(
            dataset_id=self.dataset_id,
            channel=self.channel,
            external_reference=self.external_reference,
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ExternalStatement:
        return cls(
            schema_version=str(data["schema_version"]),
            dataset_id=str(data["dataset_id"]),
            statement_record_id=str(data["statement_record_id"]),
            external_reference=str(data["external_reference"]),
            channel=str(data["channel"]),
            amount_minor=_require_strict_int(data["amount_minor"], "amount_minor"),
            currency=str(data["currency"]),
            status=str(data["status"]),
            event_time=str(data["event_time"]),
            business_date=str(data["business_date"]),
            available_at=str(data["available_at"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "statement_record_id": self.statement_record_id,
            "external_reference": self.external_reference,
            "channel": self.channel,
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "status": self.status,
            "event_time": self.event_time,
            "business_date": self.business_date,
            "available_at": self.available_at,
        }


@dataclass(frozen=True)
class EventEnvelope:
    """Phong bì sự kiện Kafka (`event_envelope.schema.json`)."""

    schema_version: str
    event_id: str
    event_type: str
    dataset_id: str
    source_batch_id: str
    event_time: str
    emitted_at: str
    payload: Transaction | LedgerEntry

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EventEnvelope:
        event_type = str(data["event_type"])
        raw_payload = data["payload"]
        if not isinstance(raw_payload, Mapping):
            raise TypeError("Trường 'payload' trong EventEnvelope phải là object")
        if event_type == EventType.TRANSACTION_CREATED.value:
            payload_obj: Transaction | LedgerEntry = Transaction.from_dict(raw_payload)
        elif event_type == EventType.LEDGER_ENTRY_CREATED.value:
            payload_obj = LedgerEntry.from_dict(raw_payload)
        else:
            raise ValueError(f"event_type không hợp lệ: {event_type!r}")

        return cls(
            schema_version=str(data["schema_version"]),
            event_id=str(data["event_id"]),
            event_type=event_type,
            dataset_id=str(data["dataset_id"]),
            source_batch_id=str(data["source_batch_id"]),
            event_time=str(data["event_time"]),
            emitted_at=str(data["emitted_at"]),
            payload=payload_obj,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "dataset_id": self.dataset_id,
            "source_batch_id": self.source_batch_id,
            "event_time": self.event_time,
            "emitted_at": self.emitted_at,
            "payload": self.payload.to_dict(),
        }


@dataclass(frozen=True)
class StatementDelivery:
    """Lịch công bố file sao kê (`statement_delivery.schema.json`)."""

    schema_version: str
    dataset_id: str
    statement_record_id: str
    file_path: str
    file_available_at: str
    delivery_order: int

    def __post_init__(self) -> None:
        _require_strict_int(self.delivery_order, "delivery_order")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> StatementDelivery:
        return cls(
            schema_version=str(data["schema_version"]),
            dataset_id=str(data["dataset_id"]),
            statement_record_id=str(data["statement_record_id"]),
            file_path=str(data["file_path"]),
            file_available_at=str(data["file_available_at"]),
            delivery_order=_require_strict_int(data["delivery_order"], "delivery_order"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "statement_record_id": self.statement_record_id,
            "file_path": self.file_path,
            "file_available_at": self.file_available_at,
            "delivery_order": self.delivery_order,
        }


@dataclass(frozen=True)
class ManifestFileItem:
    """Thông tin một file vật lý trong `source_manifest.json`."""

    path: str
    sha256: str
    physical_rows: int

    def __post_init__(self) -> None:
        _require_strict_int(self.physical_rows, "physical_rows")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ManifestFileItem:
        return cls(
            path=str(data["path"]),
            sha256=str(data["sha256"]),
            physical_rows=_require_strict_int(data["physical_rows"], "physical_rows"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "physical_rows": self.physical_rows,
        }


@dataclass(frozen=True)
class SourceManifest:
    """Manifest kiểm tra toàn vẹn nguồn (`source_manifest.schema.json`)."""

    schema_version: str
    dataset_id: str
    source_batch_id: str
    files: tuple[ManifestFileItem, ...]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SourceManifest:
        raw_files = data["files"]
        if not isinstance(raw_files, list):
            raise TypeError("Trường 'files' trong SourceManifest phải là list")
        return cls(
            schema_version=str(data["schema_version"]),
            dataset_id=str(data["dataset_id"]),
            source_batch_id=str(data["source_batch_id"]),
            files=tuple(ManifestFileItem.from_dict(item) for item in raw_files),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "source_batch_id": self.source_batch_id,
            "files": [item.to_dict() for item in self.files],
        }


@dataclass(frozen=True)
class ControlReportRow:
    """Một dòng tổng kiểm soát theo `(entity, business_date, currency, status_or_side)`."""

    entity: str
    business_date: str
    currency: str
    status_or_side: str
    record_count: int
    amount_minor_total: int

    def __post_init__(self) -> None:
        _require_strict_int(self.record_count, "record_count")
        _require_strict_int(self.amount_minor_total, "amount_minor_total")

    @property
    def sort_key(self) -> tuple[str, str, str, str]:
        return (self.entity, self.business_date, self.currency, self.status_or_side)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ControlReportRow:
        return cls(
            entity=str(data["entity"]),
            business_date=str(data["business_date"]),
            currency=str(data["currency"]),
            status_or_side=str(data["status_or_side"]),
            record_count=_require_strict_int(data["record_count"], "record_count"),
            amount_minor_total=_require_strict_int(
                data["amount_minor_total"], "amount_minor_total"
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity,
            "business_date": self.business_date,
            "currency": self.currency,
            "status_or_side": self.status_or_side,
            "record_count": self.record_count,
            "amount_minor_total": self.amount_minor_total,
        }


@dataclass(frozen=True)
class ControlReport:
    """Báo cáo tổng kiểm soát nguồn (`control_report.schema.json`)."""

    schema_version: str
    dataset_id: str
    totals: tuple[ControlReportRow, ...]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ControlReport:
        raw_totals = data["totals"]
        if not isinstance(raw_totals, list):
            raise TypeError("Trường 'totals' trong ControlReport phải là list")
        return cls(
            schema_version=str(data["schema_version"]),
            dataset_id=str(data["dataset_id"]),
            totals=tuple(ControlReportRow.from_dict(item) for item in raw_totals),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "totals": [row.to_dict() for row in self.totals],
        }
