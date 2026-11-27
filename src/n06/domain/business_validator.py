"""Validator nghiệp vụ (semantic validation) ngoài JSON Schema theo hợp đồng 1.0.0 (TASK T10).

Tham chiếu: docs/spec.md Phần II mục 1, 3, 4, 8.
Kiểm tra các ràng buộc ngữ nghĩa và liên kết giữa các bản ghi:
- `BUSINESS_DATE_MISMATCH`: `business_date` bằng ngày của `event_time` theo `Asia/Ho_Chi_Minh`.
- `SAME_ACCOUNT_TRANSFER`: `account_from != account_to`.
- `UNKNOWN_ACCOUNT`: `account_from`, `account_to`, `ledger_account_id` tồn tại trong `accounts`.
- `ACCOUNT_CURRENCY_MISMATCH`: currency của account phải khớp currency của giao dịch / ledger.
- `ENVELOPE_PAYLOAD_MISMATCH`: `dataset_id` và `event_time` của envelope phải trùng với `payload`;
  `event_type` phải khớp loại entity trong `payload`; `emitted_at >= event_time`;
  `source_batch_id` khớp manifest nguồn (khi có).
- `EVENT_ID_CONFLICT`: Cùng `event_id` nhưng khác nội dung envelope/payload bất biến.
- `BUSINESS_KEY_CONFLICT`: Cùng business ID (`transaction_id` / `entry_id` / `statement_record_id`)
  nhưng khác business payload; trích xuất `MatchKey` để đối soát báo `DUPLICATE_OR_CONFLICT`.
- `LEDGER_INCOMPLETE`: Giao dịch `SUCCESS` thiếu vế (`DEBIT` hoặc `CREDIT`) sau khi input đã đủ.
- `LEDGER_UNBALANCED`: Hai vế `DEBIT` và `CREDIT` không cân bằng (`debit != credit`).
- `LEDGER_TRANSACTION_MISMATCH`: Giao dịch `FAILED` nhưng có ledger, hoặc ledger lệch
  `account_from`/`account_to`/`amount_minor`/`currency`/`event_time`/`business_date` của giao dịch.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from n06.domain.errors import ErrorCode, ValidationIssue, ValidationResult
from n06.domain.models import (
    BUSINESS_TIMEZONE_NAME,
    Account,
    ControlEntity,
    ControlReport,
    Currency,
    EventEnvelope,
    EventType,
    ExternalStatement,
    LedgerEntry,
    LedgerSide,
    MatchKey,
    SourceManifest,
    StatementDelivery,
    Transaction,
    TransactionStatus,
)
from n06.domain.schema_validator import (
    ContractSchemaRegistry,
    check_format,
    get_default_schema_registry,
    parse_and_validate_json_schema,
)

_HO_CHI_MINH_TZ = ZoneInfo(BUSINESS_TIMEZONE_NAME)


def parse_utc_timestamp(ts_str: str) -> datetime:
    """Chuyển chuỗi timestamp chuẩn UTC `YYYY-MM-DDTHH:mm:ssZ` sang `datetime`."""
    if not check_format(ts_str, "date-time"):
        raise ValueError(f"Chuỗi timestamp không đúng chuẩn UTC 'YYYY-MM-DDTHH:mm:ssZ': {ts_str!r}")
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def derive_business_date(
    event_time_utc: str,
    tz_name: str = BUSINESS_TIMEZONE_NAME,
) -> str:
    """Tính `business_date` (`YYYY-MM-DD`) từ `event_time` UTC theo múi giờ nghiệp vụ."""
    dt_utc = parse_utc_timestamp(event_time_utc)
    tz = _HO_CHI_MINH_TZ if tz_name == BUSINESS_TIMEZONE_NAME else ZoneInfo(tz_name)
    return dt_utc.astimezone(tz).date().isoformat()


def _expected_business_date(event_time_utc: str, tz_name: str) -> str | None:
    """`business_date` kỳ vọng, hoặc None khi đổi múi giờ vượt miền lịch (năm 9999 cuối năm).

    None không bằng chuỗi ngày nào nên record nhận `BUSINESS_DATE_MISMATCH` thay vì làm ném.
    """
    try:
        return derive_business_date(event_time_utc, tz_name=tz_name)
    except OverflowError:
        return None


def _normalize_json_for_comparison(obj: Mapping[str, Any]) -> str:
    """Chuẩn hóa dict thành chuỗi JSON đã sắp xếp khóa để so sánh bất biến nội dung."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _resolve_account_currency(account_obj: Account | Mapping[str, Any]) -> str:
    if isinstance(account_obj, Account):
        return account_obj.currency
    return str(account_obj["currency"])


def _build_account_lookup(
    accounts: Mapping[str, Account | Mapping[str, Any]]
    | Iterable[Account | Mapping[str, Any]]
    | None,
) -> dict[str, Account | Mapping[str, Any]] | None:
    if accounts is None:
        return None
    if isinstance(accounts, Mapping):
        return dict(accounts)
    lookup: dict[str, Account | Mapping[str, Any]] = {}
    for item in accounts:
        acc_id = item.account_id if isinstance(item, Account) else str(item["account_id"])
        lookup[acc_id] = item
    return lookup


def validate_account_record(
    raw_or_dict: str | bytes | Mapping[str, Any],
    *,
    registry: ContractSchemaRegistry | None = None,
) -> ValidationResult:
    """Validate bản ghi `account` theo JSON Schema Draft 2020-12 và trả về `Account` model."""
    schema_res = parse_and_validate_json_schema(raw_or_dict, "account", registry=registry)
    if not schema_res.is_valid or schema_res.record is None:
        return schema_res
    model = Account.from_dict(schema_res.record)
    return ValidationResult.ok(record=schema_res.record, model=model)


def validate_transaction_record(
    raw_or_dict: str | bytes | Mapping[str, Any],
    *,
    accounts: Mapping[str, Account | Mapping[str, Any]]
    | Iterable[Account | Mapping[str, Any]]
    | None = None,
    business_timezone: str = BUSINESS_TIMEZONE_NAME,
    registry: ContractSchemaRegistry | None = None,
) -> ValidationResult:
    """Validate bản ghi `transaction` (cả JSON Schema lẫn ràng buộc semantic và thứ tự ưu tiên).

    Nếu bản ghi vi phạm nhiều chỗ (ví dụ vừa sai schema vừa sai `business_date` hoặc
    `account_from == account_to`), toàn bộ mã lỗi đều được thu thập vào `issue_codes`
    và `primary_error` được chọn theo đúng thứ tự ưu tiên ở Phần II mục 8.
    """
    schema_res = parse_and_validate_json_schema(raw_or_dict, "transaction", registry=registry)
    if schema_res.record is None:
        return schema_res

    record = schema_res.record
    issues: list[ValidationIssue] = list(schema_res.issues)
    account_map = _build_account_lookup(accounts)

    # 1. Kiểm tra BUSINESS_DATE_MISMATCH nếu event_time và business_date có định dạng hợp lệ
    ev_time = record.get("event_time")
    biz_date = record.get("business_date")
    if (
        isinstance(ev_time, str)
        and isinstance(biz_date, str)
        and check_format(ev_time, "date-time")
        and check_format(biz_date, "date")
    ):
        expected_biz_date = _expected_business_date(ev_time, business_timezone)
        if biz_date != expected_biz_date:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.BUSINESS_DATE_MISMATCH,
                    field_path="$.business_date",
                    message=(
                        f"business_date={biz_date!r} không khớp ngày của event_time={ev_time!r} "
                        f"theo múi giờ {business_timezone} (kỳ vọng {expected_biz_date!r})."
                    ),
                )
            )

    # 2. Kiểm tra SAME_ACCOUNT_TRANSFER
    acc_from = record.get("account_from")
    acc_to = record.get("account_to")
    if isinstance(acc_from, str) and isinstance(acc_to, str) and acc_from == acc_to:
        issues.append(
            ValidationIssue(
                code=ErrorCode.SAME_ACCOUNT_TRANSFER,
                field_path="$.account_to",
                message=f"Tài khoản gửi và nhận không được trùng nhau ({acc_from!r}).",
            )
        )

    # 3. Kiểm tra UNKNOWN_ACCOUNT và ACCOUNT_CURRENCY_MISMATCH khi có tập accounts tham chiếu
    if account_map is not None and isinstance(acc_from, str) and isinstance(acc_to, str):
        missing_accounts = [
            field_name
            for field_name, acc_id in (("account_from", acc_from), ("account_to", acc_to))
            if acc_id not in account_map
        ]
        for field_name in missing_accounts:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.UNKNOWN_ACCOUNT,
                    field_path=f"$.{field_name}",
                    message=f"Tài khoản {record[field_name]!r} tại '{field_name}' không tồn tại.",
                )
            )

        tx_currency = record.get("currency")
        if (
            not missing_accounts
            and isinstance(tx_currency, str)
            and tx_currency in (Currency.VND.value, Currency.USD.value)
        ):
            for field_name, acc_id in (("account_from", acc_from), ("account_to", acc_to)):
                acc_currency = _resolve_account_currency(account_map[acc_id])
                if acc_currency != tx_currency:
                    issues.append(
                        ValidationIssue(
                            code=ErrorCode.ACCOUNT_CURRENCY_MISMATCH,
                            field_path=f"$.{field_name}",
                            message=(
                                f"Currency của tài khoản {acc_id!r} ({acc_currency!r}) "
                                f"khác currency của giao dịch ({tx_currency!r})."
                            ),
                        )
                    )

    if issues:
        return ValidationResult.from_issues(issues, record=record)

    return ValidationResult.ok(record=record, model=Transaction.from_dict(record))


def validate_ledger_entry_record(
    raw_or_dict: str | bytes | Mapping[str, Any],
    *,
    accounts: Mapping[str, Account | Mapping[str, Any]]
    | Iterable[Account | Mapping[str, Any]]
    | None = None,
    transactions: Mapping[str, Transaction | Mapping[str, Any]] | None = None,
    business_timezone: str = BUSINESS_TIMEZONE_NAME,
    registry: ContractSchemaRegistry | None = None,
) -> ValidationResult:
    """Validate một bản ghi `ledger_entry` đơn lẻ theo JSON Schema và ngữ nghĩa cơ bản."""
    schema_res = parse_and_validate_json_schema(raw_or_dict, "ledger_entry", registry=registry)
    if schema_res.record is None:
        return schema_res

    record = schema_res.record
    issues: list[ValidationIssue] = list(schema_res.issues)
    account_map = _build_account_lookup(accounts)

    ev_time = record.get("event_time")
    biz_date = record.get("business_date")
    if (
        isinstance(ev_time, str)
        and isinstance(biz_date, str)
        and check_format(ev_time, "date-time")
        and check_format(biz_date, "date")
    ):
        expected_biz_date = _expected_business_date(ev_time, business_timezone)
        if biz_date != expected_biz_date:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.BUSINESS_DATE_MISMATCH,
                    field_path="$.business_date",
                    message=(
                        f"business_date={biz_date!r} không khớp ngày của event_time={ev_time!r} "
                        f"(kỳ vọng {expected_biz_date!r})."
                    ),
                )
            )

    ledger_acc_id = record.get("ledger_account_id")
    if account_map is not None and isinstance(ledger_acc_id, str):
        if ledger_acc_id not in account_map:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.UNKNOWN_ACCOUNT,
                    field_path="$.ledger_account_id",
                    message=f"Tài khoản ledger {ledger_acc_id!r} không tồn tại trong accounts.",
                )
            )
        else:
            acc_curr = _resolve_account_currency(account_map[ledger_acc_id])
            entry_curr = record.get("currency")
            if isinstance(entry_curr, str) and acc_curr != entry_curr:
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.ACCOUNT_CURRENCY_MISMATCH,
                        field_path="$.ledger_account_id",
                        message=(
                            f"Currency của tài khoản ledger {ledger_acc_id!r} ({acc_curr!r}) "
                            f"khác currency của bút toán ({entry_curr!r})."
                        ),
                    )
                )

    tx_id = record.get("transaction_id")
    if transactions is not None and isinstance(tx_id, str):
        if tx_id not in transactions:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.LEDGER_TRANSACTION_MISMATCH,
                    field_path="$.transaction_id",
                    message=f"Bút toán ledger tham chiếu transaction_id={tx_id!r} không tồn tại.",
                )
            )

    if issues:
        return ValidationResult.from_issues(issues, record=record)

    return ValidationResult.ok(record=record, model=LedgerEntry.from_dict(record))


def validate_statement_record(
    raw_or_dict: str | bytes | Mapping[str, Any],
    *,
    business_timezone: str = BUSINESS_TIMEZONE_NAME,
    registry: ContractSchemaRegistry | None = None,
) -> ValidationResult:
    """Validate bản ghi `statement` (`external_statements`) theo Schema và ngữ nghĩa thời gian."""
    schema_res = parse_and_validate_json_schema(raw_or_dict, "statement", registry=registry)
    if schema_res.record is None:
        return schema_res

    record = schema_res.record
    issues: list[ValidationIssue] = list(schema_res.issues)

    ev_time = record.get("event_time")
    biz_date = record.get("business_date")
    avail_at = record.get("available_at")

    if (
        isinstance(ev_time, str)
        and isinstance(biz_date, str)
        and check_format(ev_time, "date-time")
        and check_format(biz_date, "date")
    ):
        expected_biz_date = _expected_business_date(ev_time, business_timezone)
        if biz_date != expected_biz_date:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.BUSINESS_DATE_MISMATCH,
                    field_path="$.business_date",
                    message=(
                        f"business_date={biz_date!r} của sao kê không khớp event_time={ev_time!r} "
                        f"(kỳ vọng {expected_biz_date!r})."
                    ),
                )
            )

    if (
        isinstance(ev_time, str)
        and isinstance(avail_at, str)
        and check_format(ev_time, "date-time")
        and check_format(avail_at, "date-time")
    ):
        if parse_utc_timestamp(avail_at) < parse_utc_timestamp(ev_time):
            issues.append(
                ValidationIssue(
                    code=ErrorCode.SCHEMA_VALUE,
                    field_path="$.available_at",
                    message=(
                        f"Thời điểm available_at ({avail_at}) không được nhỏ hơn "
                        f"event_time ({ev_time})."
                    ),
                )
            )

    if issues:
        return ValidationResult.from_issues(issues, record=record)

    return ValidationResult.ok(record=record, model=ExternalStatement.from_dict(record))


def validate_event_envelope(
    raw_or_dict: str | bytes | Mapping[str, Any],
    *,
    accounts: Mapping[str, Account | Mapping[str, Any]]
    | Iterable[Account | Mapping[str, Any]]
    | None = None,
    allowed_source_batch_ids: set[str] | None = None,
    business_timezone: str = BUSINESS_TIMEZONE_NAME,
    registry: ContractSchemaRegistry | None = None,
) -> ValidationResult:
    """Validate `event_envelope` cùng `payload` bên trong theo Phần II mục 3 và mục 8."""
    reg = registry if registry is not None else get_default_schema_registry()
    schema_res = parse_and_validate_json_schema(raw_or_dict, "event_envelope", registry=reg)
    if schema_res.record is None:
        return schema_res

    env_record = schema_res.record
    issues: list[ValidationIssue] = list(schema_res.issues)

    event_type = env_record.get("event_type")
    payload = env_record.get("payload")

    if isinstance(payload, dict):
        # Kiểm tra sự tương ứng giữa event_type của envelope và hình dạng của payload
        if event_type == EventType.TRANSACTION_CREATED.value:
            if "entry_id" in payload and "transaction_id" in payload and "side" in payload:
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.ENVELOPE_PAYLOAD_MISMATCH,
                        field_path="$.event_type",
                        message="event_type là TRANSACTION_CREATED nhưng payload là ledger_entry.",
                    )
                )
            else:
                tx_res = validate_transaction_record(
                    payload,
                    accounts=accounts,
                    business_timezone=business_timezone,
                    registry=reg,
                )
                issues.extend(tx_res.issues)
        elif event_type == EventType.LEDGER_ENTRY_CREATED.value:
            if "account_from" in payload or "external_reference" in payload:
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.ENVELOPE_PAYLOAD_MISMATCH,
                        field_path="$.event_type",
                        message="event_type là LEDGER_ENTRY_CREATED nhưng payload là transaction.",
                    )
                )
            else:
                le_res = validate_ledger_entry_record(
                    payload,
                    accounts=accounts,
                    business_timezone=business_timezone,
                    registry=reg,
                )
                issues.extend(le_res.issues)

        # Envelope dataset_id và event_time phải trùng với payload (Phần II mục 3)
        env_ds = env_record.get("dataset_id")
        pay_ds = payload.get("dataset_id")
        if isinstance(env_ds, str) and isinstance(pay_ds, str) and env_ds != pay_ds:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.ENVELOPE_PAYLOAD_MISMATCH,
                    field_path="$.dataset_id",
                    message=(
                        f"dataset_id của envelope ({env_ds!r}) không khớp "
                        f"dataset_id của payload ({pay_ds!r})."
                    ),
                )
            )

        env_time = env_record.get("event_time")
        pay_time = payload.get("event_time")
        if isinstance(env_time, str) and isinstance(pay_time, str) and env_time != pay_time:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.ENVELOPE_PAYLOAD_MISMATCH,
                    field_path="$.event_time",
                    message=(
                        f"event_time của envelope ({env_time!r}) không khớp "
                        f"event_time của payload ({pay_time!r})."
                    ),
                )
            )

    # Kiểm tra emitted_at >= event_time
    env_time = env_record.get("event_time")
    emitted_at = env_record.get("emitted_at")
    if (
        isinstance(env_time, str)
        and isinstance(emitted_at, str)
        and check_format(env_time, "date-time")
        and check_format(emitted_at, "date-time")
    ):
        if parse_utc_timestamp(emitted_at) < parse_utc_timestamp(env_time):
            issues.append(
                ValidationIssue(
                    code=ErrorCode.ENVELOPE_PAYLOAD_MISMATCH,
                    field_path="$.emitted_at",
                    message=(
                        f"emitted_at ({emitted_at}) không được nhỏ hơn event_time ({env_time})."
                    ),
                )
            )

    # Kiểm tra source_batch_id nằm trong manifest nguồn nếu được cung cấp
    src_batch_id = env_record.get("source_batch_id")
    if (
        allowed_source_batch_ids is not None
        and isinstance(src_batch_id, str)
        and src_batch_id not in allowed_source_batch_ids
    ):
        issues.append(
            ValidationIssue(
                code=ErrorCode.ENVELOPE_PAYLOAD_MISMATCH,
                field_path="$.source_batch_id",
                message=(
                    f"source_batch_id={src_batch_id!r} không thuộc tập manifest nguồn được phép."
                ),
            )
        )

    if issues:
        return ValidationResult.from_issues(issues, record=env_record)

    return ValidationResult.ok(record=env_record, model=EventEnvelope.from_dict(env_record))


def validate_transaction_ledger_group(
    transaction: Transaction | Mapping[str, Any],
    ledger_entries: Sequence[LedgerEntry | Mapping[str, Any]],
) -> ValidationResult:
    """Kiểm tra tính đầy đủ, cân bằng và khớp nối của nhóm bút toán ledger cho 1 giao dịch.

    Tham chiếu: docs/spec.md Phần II mục 1.3, mục 3, mục 8:
    - Giao dịch `FAILED`: phải có đúng 0 ledger entry (nếu có -> `LEDGER_TRANSACTION_MISMATCH`).
    - Giao dịch `SUCCESS` (sau khi phạm vi input đã đủ):
      * Không đủ 2 vế `DEBIT` và `CREDIT` -> `LEDGER_INCOMPLETE`.
      * Có nhiều hơn 2 dòng hoặc trùng side -> `LEDGER_TRANSACTION_MISMATCH`.
      * Tổng tiền vế `DEBIT` != vế `CREDIT` -> `LEDGER_UNBALANCED`.
      * Bút toán không khớp `transaction_id`, `account_from`/`account_to`, `amount_minor`,
        `currency`, `event_time`, `business_date` của giao dịch -> `LEDGER_TRANSACTION_MISMATCH`.
    """
    tx = transaction if isinstance(transaction, Transaction) else Transaction.from_dict(transaction)
    entries = [
        e if isinstance(e, LedgerEntry) else LedgerEntry.from_dict(e) for e in ledger_entries
    ]
    issues: list[ValidationIssue] = []

    if tx.status == TransactionStatus.FAILED.value:
        if entries:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.LEDGER_TRANSACTION_MISMATCH,
                    field_path="$.ledger_entries",
                    message=(
                        f"Giao dịch FAILED ({tx.transaction_id}) không được có bút toán ledger "
                        f"(nhận {len(entries)} dòng)."
                    ),
                )
            )
        return ValidationResult.from_issues(issues, record=tx.to_dict(), model=tx)

    # Giao dịch SUCCESS: bắt buộc đủ 2 vế DEBIT và CREDIT
    sides_present = {e.side for e in entries}
    if len(entries) < 2 or sides_present != {LedgerSide.DEBIT.value, LedgerSide.CREDIT.value}:
        issues.append(
            ValidationIssue(
                code=ErrorCode.LEDGER_INCOMPLETE,
                field_path="$.ledger_entries",
                message=(
                    f"Giao dịch SUCCESS ({tx.transaction_id}) thiếu vế sổ cái: "
                    f"có {len(entries)} dòng với sides={sorted(sides_present)} "
                    "(yêu cầu đủ DEBIT và CREDIT)."
                ),
            )
        )
        return ValidationResult.from_issues(issues, record=tx.to_dict())

    if len(entries) > 2:
        issues.append(
            ValidationIssue(
                code=ErrorCode.LEDGER_TRANSACTION_MISMATCH,
                field_path="$.ledger_entries",
                message=(
                    f"Giao dịch SUCCESS ({tx.transaction_id}) có quá 2 dòng ledger "
                    f"({len(entries)} dòng)."
                ),
            )
        )

    debit_entries = [e for e in entries if e.side == LedgerSide.DEBIT.value]
    credit_entries = [e for e in entries if e.side == LedgerSide.CREDIT.value]
    debit_sum = sum(e.amount_minor for e in debit_entries)
    credit_sum = sum(e.amount_minor for e in credit_entries)

    if debit_sum != credit_sum:
        issues.append(
            ValidationIssue(
                code=ErrorCode.LEDGER_UNBALANCED,
                field_path="$.ledger_entries",
                message=(
                    f"Bút toán ledger của giao dịch {tx.transaction_id} không cân bằng: "
                    f"DEBIT={debit_sum} != CREDIT={credit_sum}."
                ),
            )
        )

    for idx, entry in enumerate(entries):
        expected_account = (
            tx.account_from if entry.side == LedgerSide.DEBIT.value else tx.account_to
        )
        if entry.transaction_id != tx.transaction_id:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.LEDGER_TRANSACTION_MISMATCH,
                    field_path=f"$.ledger_entries[{idx}].transaction_id",
                    message=(
                        f"entry {entry.entry_id} có transaction_id={entry.transaction_id!r} "
                        f"khác {tx.transaction_id!r}."
                    ),
                )
            )
        if entry.ledger_account_id != expected_account:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.LEDGER_TRANSACTION_MISMATCH,
                    field_path=f"$.ledger_entries[{idx}].ledger_account_id",
                    message=(
                        f"Vế {entry.side} của entry {entry.entry_id} trỏ tới tài khoản "
                        f"{entry.ledger_account_id!r}, kỳ vọng {expected_account!r}."
                    ),
                )
            )
        if entry.amount_minor != tx.amount_minor:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.LEDGER_TRANSACTION_MISMATCH,
                    field_path=f"$.ledger_entries[{idx}].amount_minor",
                    message=(
                        f"Vế {entry.side} của entry {entry.entry_id} có amount_minor="
                        f"{entry.amount_minor} khác transaction.amount_minor={tx.amount_minor}."
                    ),
                )
            )
        if entry.currency != tx.currency:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.LEDGER_TRANSACTION_MISMATCH,
                    field_path=f"$.ledger_entries[{idx}].currency",
                    message=(
                        f"Entry {entry.entry_id} có currency={entry.currency!r} "
                        f"khác transaction.currency={tx.currency!r}."
                    ),
                )
            )
        if entry.event_time != tx.event_time or entry.business_date != tx.business_date:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.LEDGER_TRANSACTION_MISMATCH,
                    field_path=f"$.ledger_entries[{idx}].event_time",
                    message=(
                        f"Entry {entry.entry_id} lệch event_time/business_date so với giao dịch."
                    ),
                )
            )

    if issues:
        return ValidationResult.from_issues(issues, record=tx.to_dict())

    return ValidationResult.ok(record=tx.to_dict(), model=(tx, tuple(entries)))


@dataclass(frozen=True)
class KeyConflictRecord:
    """Thông tin xung đột ID để đẩy sang `key_conflicts` phục vụ `DUPLICATE_OR_CONFLICT`."""

    error_code: ErrorCode
    entity_type: str
    conflicting_id: str
    match_key: MatchKey | None
    detail: str


class ConflictTracker:
    """Bộ theo dõi phát hiện replay hợp lệ so với `EVENT_ID_CONFLICT` và `BUSINESS_KEY_CONFLICT`.

    Tham chiếu: docs/spec.md Phần II mục 3 và mục 8:
    - Cùng `event_id` và cùng canonical JSON của envelope -> event replay hợp lệ (dedup).
    - Cùng `event_id` nhưng khác nội dung envelope -> `EVENT_ID_CONFLICT`.
    - Khác `event_id`, cùng business ID (`transaction_id` hoặc `entry_id`) và cùng canonical JSON
      của business payload -> business replay hợp lệ (dedup).
    - Cùng business ID (hoặc cùng `(channel, external_reference)` đối với transaction)
      nhưng khác business payload -> `BUSINESS_KEY_CONFLICT`.
    """

    def __init__(self) -> None:
        self._events_by_id: dict[str, str] = {}
        self._business_by_key: dict[tuple[str, str], str] = {}
        self._tx_ref_to_id: dict[tuple[str, str, str], str] = {}
        self.conflicts: list[KeyConflictRecord] = []

    @staticmethod
    def _extract_match_key(payload: Mapping[str, Any]) -> MatchKey | None:
        ds = payload.get("dataset_id")
        ch = payload.get("channel")
        ref = payload.get("external_reference")
        if isinstance(ds, str) and isinstance(ch, str) and isinstance(ref, str):
            return MatchKey(dataset_id=ds, channel=ch, external_reference=ref)
        return None

    def check_and_register_envelope(
        self,
        envelope: EventEnvelope | Mapping[str, Any],
    ) -> ValidationResult:
        """Kiểm tra một event envelope với lịch sử đã gặp để phân biệt replay và conflict."""
        env_dict = envelope.to_dict() if isinstance(envelope, EventEnvelope) else dict(envelope)
        event_id = str(env_dict["event_id"])
        event_type = str(env_dict["event_type"])
        payload = dict(env_dict["payload"])
        match_key = self._extract_match_key(payload)

        canonical_env = _normalize_json_for_comparison(env_dict)
        canonical_payload = _normalize_json_for_comparison(payload)

        issues: list[ValidationIssue] = []

        # 1. Kiểm tra xung đột trên event_id
        if event_id in self._events_by_id:
            if self._events_by_id[event_id] != canonical_env:
                msg = (
                    f"Xung đột EVENT_ID_CONFLICT: event_id={event_id!r} có nội dung khác bản trước."
                )
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.EVENT_ID_CONFLICT,
                        field_path="$.event_id",
                        message=msg,
                    )
                )
                self.conflicts.append(
                    KeyConflictRecord(
                        error_code=ErrorCode.EVENT_ID_CONFLICT,
                        entity_type=event_type,
                        conflicting_id=event_id,
                        match_key=match_key,
                        detail=msg,
                    )
                )
                return ValidationResult.from_issues(issues, record=env_dict)
            # Cùng event_id và cùng nội dung -> event replay hợp lệ
            return ValidationResult.ok(record=env_dict)

        self._events_by_id[event_id] = canonical_env

        # 2. Kiểm tra xung đột trên business key của payload
        if event_type == EventType.TRANSACTION_CREATED.value:
            biz_id = str(payload["transaction_id"])
            biz_key = ("transaction", biz_id)
        else:
            biz_id = str(payload["entry_id"])
            biz_key = ("ledger_entry", biz_id)

        if biz_key in self._business_by_key:
            if self._business_by_key[biz_key] != canonical_payload:
                msg = (
                    f"Xung đột BUSINESS_KEY_CONFLICT: {biz_key[0]} ID={biz_id!r} "
                    "có payload khác với bản ghi đã tồn tại."
                )
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.BUSINESS_KEY_CONFLICT,
                        field_path="$.payload",
                        message=msg,
                    )
                )
                self.conflicts.append(
                    KeyConflictRecord(
                        error_code=ErrorCode.BUSINESS_KEY_CONFLICT,
                        entity_type=biz_key[0],
                        conflicting_id=biz_id,
                        match_key=match_key,
                        detail=msg,
                    )
                )
                return ValidationResult.from_issues(issues, record=env_dict)
            # Khác event_id nhưng cùng business payload -> business replay hợp lệ
            return ValidationResult.ok(record=env_dict)

        # Kiểm tra thêm tính duy nhất của (dataset_id, channel, external_reference) cho transaction
        if event_type == EventType.TRANSACTION_CREATED.value and match_key is not None:
            ref_tuple = (match_key.dataset_id, match_key.channel, match_key.external_reference)
            existing_tx_id = self._tx_ref_to_id.get(ref_tuple)
            if existing_tx_id is not None and existing_tx_id != biz_id:
                msg = (
                    f"Xung đột BUSINESS_KEY_CONFLICT: external_reference="
                    f"{match_key.external_reference!r} trên kênh {match_key.channel!r} "
                    f"thuộc về hai transaction_id khác nhau ({existing_tx_id!r} và {biz_id!r})."
                )
                issues.append(
                    ValidationIssue(
                        code=ErrorCode.BUSINESS_KEY_CONFLICT,
                        field_path="$.payload.external_reference",
                        message=msg,
                    )
                )
                self.conflicts.append(
                    KeyConflictRecord(
                        error_code=ErrorCode.BUSINESS_KEY_CONFLICT,
                        entity_type="transaction",
                        conflicting_id=match_key.external_reference,
                        match_key=match_key,
                        detail=msg,
                    )
                )
                return ValidationResult.from_issues(issues, record=env_dict)
            self._tx_ref_to_id[ref_tuple] = biz_id

        self._business_by_key[biz_key] = canonical_payload
        return ValidationResult.ok(record=env_dict)


def validate_control_report_data(
    raw_or_dict: str | bytes | Mapping[str, Any],
    *,
    registry: ContractSchemaRegistry | None = None,
) -> ValidationResult:
    """Validate `control_report.json` theo schema và quy tắc dòng ở Phần II mục 4."""
    schema_res = parse_and_validate_json_schema(raw_or_dict, "control_report", registry=registry)
    if not schema_res.is_valid or schema_res.record is None:
        return schema_res

    report = ControlReport.from_dict(schema_res.record)
    issues: list[ValidationIssue] = []

    seen_keys: list[tuple[str, str, str, str]] = []
    for idx, row in enumerate(report.totals):
        if row.record_count < 1:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.SCHEMA_VALUE,
                    field_path=f"$.totals[{idx}].record_count",
                    message="control_report chỉ được xuất các tổ hợp có record_count >= 1.",
                )
            )
        if row.entity == ControlEntity.TRANSACTION.value and row.status_or_side not in (
            TransactionStatus.SUCCESS.value,
            TransactionStatus.FAILED.value,
        ):
            issues.append(
                ValidationIssue(
                    code=ErrorCode.SCHEMA_VALUE,
                    field_path=f"$.totals[{idx}].status_or_side",
                    message=(
                        f"Dòng transaction trong control_report có status_or_side="
                        f"{row.status_or_side!r} không hợp lệ."
                    ),
                )
            )
        if row.entity == ControlEntity.LEDGER_ENTRY.value and row.status_or_side not in (
            LedgerSide.DEBIT.value,
            LedgerSide.CREDIT.value,
        ):
            issues.append(
                ValidationIssue(
                    code=ErrorCode.SCHEMA_VALUE,
                    field_path=f"$.totals[{idx}].status_or_side",
                    message=(
                        f"Dòng ledger_entry trong control_report có status_or_side="
                        f"{row.status_or_side!r} không hợp lệ."
                    ),
                )
            )
        seen_keys.append(row.sort_key)

    if len(set(seen_keys)) != len(seen_keys) or seen_keys != sorted(seen_keys):
        issues.append(
            ValidationIssue(
                code=ErrorCode.SCHEMA_VALUE,
                field_path="$.totals",
                message=(
                    "Các dòng trong control_report phải duy nhất và sắp xếp tăng dần theo "
                    "(entity, business_date, currency, status_or_side)."
                ),
            )
        )

    if issues:
        return ValidationResult.from_issues(issues, record=schema_res.record)

    return ValidationResult.ok(record=schema_res.record, model=report)


def validate_generator_config_data(
    raw_or_dict: str | bytes | Mapping[str, Any],
    *,
    registry: ContractSchemaRegistry | None = None,
) -> ValidationResult:
    """Validate cấu hình generator (`generator.*.json`) theo schema và ràng buộc liên trường."""
    schema_res = parse_and_validate_json_schema(raw_or_dict, "generator_config", registry=registry)
    if not schema_res.is_valid or schema_res.record is None:
        return schema_res

    cfg = schema_res.record
    issues: list[ValidationIssue] = []

    if cfg["from_date"] > cfg["to_date"]:
        issues.append(
            ValidationIssue(
                code=ErrorCode.SCHEMA_VALUE,
                field_path="$.from_date",
                message=(
                    f"from_date ({cfg['from_date']}) không được lớn hơn to_date ({cfg['to_date']})."
                ),
            )
        )

    if cfg["amount_min_minor"] > cfg["amount_max_minor"]:
        issues.append(
            ValidationIssue(
                code=ErrorCode.AMOUNT_OUT_OF_RANGE,
                field_path="$.amount_min_minor",
                message="amount_min_minor không được lớn hơn amount_max_minor.",
            )
        )

    weights = cfg["channel_weights_basis_points"]
    if sum(weights.values()) != 10000:
        issues.append(
            ValidationIssue(
                code=ErrorCode.SCHEMA_VALUE,
                field_path="$.channel_weights_basis_points",
                message="Tổng trọng số channel_weights_basis_points phải bằng đúng 10000.",
            )
        )

    fault_sum = sum(
        v for k, v in cfg["faults"].items() if k.endswith("_basis_points") and isinstance(v, int)
    )
    if fault_sum > 10000:
        issues.append(
            ValidationIssue(
                code=ErrorCode.SCHEMA_VALUE,
                field_path="$.faults",
                message=f"Tổng basis_points của các lỗi ({fault_sum}) không được vượt quá 10000.",
            )
        )

    late_within = cfg["late_within_delay_after_day_end_seconds"]
    window_sec = cfg["statement_file_window_seconds"]
    allowed_late = cfg["allowed_lateness_seconds"]
    late_after = cfg["late_after_delay_after_day_end_seconds"]
    if not (late_within + window_sec < allowed_late <= late_after):
        issues.append(
            ValidationIssue(
                code=ErrorCode.SCHEMA_VALUE,
                field_path="$.late_within_delay_after_day_end_seconds",
                message=(
                    "Ràng buộc độ trễ sao kê vi phạm: yêu cầu "
                    "late_within + statement_file_window < allowed_lateness <= late_after."
                ),
            )
        )

    if cfg["account_selection"] == "HOT_ACCOUNT_V1":
        if not (
            1 <= cfg["hot_account_count"] < cfg["account_count"]
            and cfg["hot_account_basis_points"] > 0
        ):
            issues.append(
                ValidationIssue(
                    code=ErrorCode.SCHEMA_VALUE,
                    field_path="$.hot_account_count",
                    message=(
                        "Với HOT_ACCOUNT_V1, yêu cầu 1 <= hot_account_count < account_count "
                        "và hot_account_basis_points > 0."
                    ),
                )
            )
    else:
        if cfg["hot_account_count"] != 0 or cfg["hot_account_basis_points"] != 0:
            issues.append(
                ValidationIssue(
                    code=ErrorCode.SCHEMA_VALUE,
                    field_path="$.hot_account_count",
                    message=(
                        "Với UNIFORM_DISTINCT_PAIR, hot_account_count và "
                        "hot_account_basis_points phải bằng 0."
                    ),
                )
            )

    if issues:
        return ValidationResult.from_issues(issues, record=cfg)

    return ValidationResult.ok(record=cfg, model=cfg)


def validate_record(
    raw_or_dict: str | bytes | Mapping[str, Any],
    entity: str,
    *,
    accounts: Mapping[str, Account | Mapping[str, Any]]
    | Iterable[Account | Mapping[str, Any]]
    | None = None,
    transactions: Mapping[str, Transaction | Mapping[str, Any]] | None = None,
    allowed_source_batch_ids: set[str] | None = None,
    business_timezone: str = BUSINESS_TIMEZONE_NAME,
    registry: ContractSchemaRegistry | None = None,
) -> ValidationResult:
    """Điểm vào tổng quát để validate bất kỳ loại bản ghi nào theo `entity`."""
    if entity == "account":
        return validate_account_record(raw_or_dict, registry=registry)
    if entity == "transaction":
        return validate_transaction_record(
            raw_or_dict,
            accounts=accounts,
            business_timezone=business_timezone,
            registry=registry,
        )
    if entity == "ledger_entry":
        return validate_ledger_entry_record(
            raw_or_dict,
            accounts=accounts,
            transactions=transactions,
            business_timezone=business_timezone,
            registry=registry,
        )
    if entity in ("statement", "external_statement"):
        return validate_statement_record(
            raw_or_dict,
            business_timezone=business_timezone,
            registry=registry,
        )
    if entity == "event_envelope":
        return validate_event_envelope(
            raw_or_dict,
            accounts=accounts,
            allowed_source_batch_ids=allowed_source_batch_ids,
            business_timezone=business_timezone,
            registry=registry,
        )
    if entity == "statement_delivery":
        res = parse_and_validate_json_schema(raw_or_dict, "statement_delivery", registry=registry)
        if res.is_valid and res.record is not None:
            return ValidationResult.ok(
                record=res.record, model=StatementDelivery.from_dict(res.record)
            )
        return res
    if entity == "source_manifest":
        res = parse_and_validate_json_schema(raw_or_dict, "source_manifest", registry=registry)
        if res.is_valid and res.record is not None:
            return ValidationResult.ok(
                record=res.record, model=SourceManifest.from_dict(res.record)
            )
        return res
    if entity == "control_report":
        return validate_control_report_data(raw_or_dict, registry=registry)
    if entity == "generator_config":
        return validate_generator_config_data(raw_or_dict, registry=registry)

    return parse_and_validate_json_schema(raw_or_dict, entity, registry=registry)
