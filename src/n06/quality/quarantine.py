"""Chuẩn hóa và phân loại bản ghi lỗi vào `quarantine.records` (TASK T30, ADR-0004b §3.6).

Module thuần Python (không import PySpark) tái dùng trực tiếp bộ kiểm định T10 (`n06.domain`)
và bộ phân loại Bronze (`n06.ingestion.records`), bảo đảm mọi bản ghi từ negative suite (kể cả
JSON hỏng, UTF-8 hỏng, lỗi schema, lỗi nghiệp vụ, lỗi cân bằng sổ cái) đều có đường tới
quarantine với đúng `error_code`, `issue_codes`, `error_detail`, `source_pointer`, `raw_reference`.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from n06.domain import (
    Account,
    ErrorCode,
    LedgerEntry,
    Transaction,
    sort_issue_codes,
    validate_account_record,
    validate_event_envelope,
    validate_statement_record,
    validate_transaction_ledger_group,
)
from n06.domain.models import BUSINESS_TIMEZONE_NAME
from n06.ingestion import records as bronze_records

STAGE_BRONZE_PARSE = "BRONZE_PARSE"
STAGE_SILVER_BUSINESS = "SILVER_BUSINESS"
STAGE_SILVER_DEDUP = "SILVER_DEDUP"

MAX_ERROR_DETAIL_CHARS = 4000

QUARANTINE_COLUMNS: tuple[str, ...] = (
    "dataset_id",
    "business_date",
    "run_id",
    "stage",
    "entity",
    "business_key",
    "error_code",
    "issue_codes",
    "error_detail",
    "source_pointer",
    "raw_reference",
)


@dataclass(frozen=True)
class QuarantineRecord:
    """Một bản ghi trong bảng `quarantine.records` theo ADR-0004b §3.6."""

    dataset_id: str | None
    business_date: date | None
    run_id: str
    stage: str
    entity: str | None
    business_key: str | None
    error_code: str
    issue_codes: tuple[str, ...]
    error_detail: str
    source_pointer: str
    raw_reference: str

    def as_dict(self) -> dict[str, object]:
        return {
            "dataset_id": self.dataset_id,
            "business_date": self.business_date,
            "run_id": self.run_id,
            "stage": self.stage,
            "entity": self.entity,
            "business_key": self.business_key,
            "error_code": self.error_code,
            "issue_codes": list(self.issue_codes),
            "error_detail": self.error_detail,
            "source_pointer": self.source_pointer,
            "raw_reference": self.raw_reference,
        }


def _safe_date(raw: object) -> date | None:
    if isinstance(raw, date):
        return raw
    if isinstance(raw, str):
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None
    return None


def _format_detail(issues: Sequence[Any]) -> str:
    text = "; ".join(f"{i.code.value}@{i.field_path}: {i.message}" for i in issues)
    return text.encode("utf-8", errors="replace").decode("utf-8")[:MAX_ERROR_DETAIL_CHARS]


def _extract_entity_and_key(record: Mapping[str, Any] | None) -> tuple[str | None, str | None]:
    if not isinstance(record, Mapping):
        return None, None
    event_type = record.get("event_type")
    payload = record.get("payload")
    if event_type == "TRANSACTION_CREATED":
        key = payload.get("transaction_id") if isinstance(payload, Mapping) else None
        return "transaction", key if isinstance(key, str) else None
    if event_type == "LEDGER_ENTRY_CREATED":
        key = payload.get("entry_id") if isinstance(payload, Mapping) else None
        return "ledger_entry", key if isinstance(key, str) else None
    return None, None


def classify_raw_event_for_quarantine(
    raw_value: bytes | bytearray | str | None,
    *,
    run_id: str,
    source_pointer: str,
    raw_reference: str,
    accounts: Mapping[str, Account | Mapping[str, Any]]
    | Iterable[Account | Mapping[str, Any]]
    | None = None,
    business_timezone: str = BUSINESS_TIMEZONE_NAME,
) -> tuple[dict[str, Any] | None, QuarantineRecord | None]:
    """Đưa một message Kafka thô đi qua Bronze parse rồi Silver business validation.

    Trả `(envelope_dict_hợp_lệ, None)` nếu sạch, hoặc `(None, QuarantineRecord)` nếu có lỗi.
    Không bao giờ ném ngoại lệ trên dữ liệu xấu (kể cả JSON hỏng / UTF-8 hỏng / null).
    """
    raw_bytes: bytes | None
    if raw_value is None:
        raw_bytes = None
    elif isinstance(raw_value, str):
        raw_bytes = raw_value.encode("utf-8")
    else:
        raw_bytes = bytes(raw_value)

    route, raw_dataset_id, cols = bronze_records.classify_record(raw_bytes)
    if route == bronze_records.PARSE_ERROR:
        codes = tuple(str(c) for c in cols["issue_codes"])
        return None, QuarantineRecord(
            dataset_id=raw_dataset_id,
            business_date=None,  # ADR-0004b Q1: parse_error để NULL, phân vùng theo ingestion_date
            run_id=run_id,
            stage=STAGE_BRONZE_PARSE,
            entity=cols.get("entity"),
            business_key=cols.get("business_key"),
            error_code=str(cols["error_code"]),
            issue_codes=codes,
            error_detail=str(cols["error_detail"]),
            source_pointer=source_pointer,
            raw_reference=raw_reference,
        )

    assert raw_bytes is not None
    parsed_obj = json.loads(raw_bytes.decode("utf-8"))
    res = validate_event_envelope(
        parsed_obj,
        accounts=accounts,
        business_timezone=business_timezone,
    )
    if res.is_valid:
        return parsed_obj, None

    entity, business_key = _extract_entity_and_key(parsed_obj)
    payload = parsed_obj.get("payload") if isinstance(parsed_obj, Mapping) else None
    biz_date = _safe_date(payload.get("business_date")) if isinstance(payload, Mapping) else None
    ordered_codes = tuple(c.value for c in sort_issue_codes(res.issue_codes))
    return None, QuarantineRecord(
        dataset_id=raw_dataset_id,
        business_date=biz_date,
        run_id=run_id,
        stage=STAGE_SILVER_BUSINESS,
        entity=entity,
        business_key=business_key,
        error_code=ordered_codes[0],
        issue_codes=ordered_codes,
        error_detail=_format_detail(res.issues),
        source_pointer=source_pointer,
        raw_reference=raw_reference,
    )


def classify_statement_line(
    raw_line: str | bytes,
    *,
    dataset_id: str,
    run_id: str,
    source_pointer: str,
    raw_reference: str,
    business_timezone: str = BUSINESS_TIMEZONE_NAME,
) -> tuple[dict[str, Any] | None, QuarantineRecord | None]:
    """Validate một dòng file sao kê bằng T10; dòng lỗi trả `QuarantineRecord` đúng mã."""
    res = validate_statement_record(raw_line, business_timezone=business_timezone)
    rec = res.record
    issues = list(res.issues)
    if (
        rec is not None
        and isinstance(rec.get("dataset_id"), str)
        and rec["dataset_id"] != dataset_id
    ):
        from n06.domain.errors import ValidationIssue

        issues.append(
            ValidationIssue(
                code=ErrorCode.SCHEMA_VALUE,
                field_path="$.dataset_id",
                message=(
                    f"dataset_id={rec['dataset_id']!r} khác dataset đang xử lý ({dataset_id!r})."
                ),
            )
        )
    if not issues and rec is not None:
        return rec, None

    ordered = sort_issue_codes(i.code for i in issues)
    stage = STAGE_BRONZE_PARSE if ordered[0] == ErrorCode.INVALID_JSON else STAGE_SILVER_BUSINESS
    key = rec.get("statement_record_id") if isinstance(rec, Mapping) else None
    biz_date = _safe_date(rec.get("business_date")) if isinstance(rec, Mapping) else None
    rec_ds = rec.get("dataset_id") if isinstance(rec, Mapping) else None
    return None, QuarantineRecord(
        dataset_id=rec_ds if isinstance(rec_ds, str) else dataset_id,
        business_date=biz_date,
        run_id=run_id,
        stage=stage,
        entity="statement",
        business_key=key if isinstance(key, str) else None,
        error_code=ordered[0].value,
        issue_codes=tuple(c.value for c in ordered),
        error_detail=_format_detail(issues),
        source_pointer=source_pointer,
        raw_reference=raw_reference,
    )


def classify_account_line(
    raw_line: str | bytes,
    *,
    dataset_id: str,
    run_id: str,
    source_pointer: str,
    raw_reference: str,
) -> tuple[dict[str, Any] | None, QuarantineRecord | None]:
    """Validate một dòng `accounts.jsonl` bằng T10; dòng lỗi trả `QuarantineRecord` đúng mã."""
    res = validate_account_record(raw_line)
    rec = res.record
    issues = list(res.issues)
    if (
        rec is not None
        and isinstance(rec.get("dataset_id"), str)
        and rec["dataset_id"] != dataset_id
    ):
        from n06.domain.errors import ValidationIssue

        issues.append(
            ValidationIssue(
                code=ErrorCode.SCHEMA_VALUE,
                field_path="$.dataset_id",
                message=(
                    f"dataset_id={rec['dataset_id']!r} khác dataset đang xử lý ({dataset_id!r})."
                ),
            )
        )
    if not issues and rec is not None:
        return rec, None

    ordered = sort_issue_codes(i.code for i in issues)
    stage = STAGE_BRONZE_PARSE if ordered[0] == ErrorCode.INVALID_JSON else STAGE_SILVER_BUSINESS
    key = rec.get("account_id") if isinstance(rec, Mapping) else None
    rec_ds = rec.get("dataset_id") if isinstance(rec, Mapping) else None
    return None, QuarantineRecord(
        dataset_id=rec_ds if isinstance(rec_ds, str) else dataset_id,
        business_date=None,
        run_id=run_id,
        stage=stage,
        entity="account",
        business_key=key if isinstance(key, str) else None,
        error_code=ordered[0].value,
        issue_codes=tuple(c.value for c in ordered),
        error_detail=_format_detail(issues),
        source_pointer=source_pointer,
        raw_reference=raw_reference,
    )


def check_ledger_groups_for_quarantine(
    transactions: Sequence[Transaction],
    ledger_entries: Sequence[LedgerEntry],
    *,
    run_id: str,
    source_pointers_by_tx: Mapping[str, str] | None = None,
    raw_references_by_tx: Mapping[str, str] | None = None,
) -> list[QuarantineRecord]:
    """Kiểm tính đầy đủ/cân bằng/khớp giao dịch của nhóm bút toán sổ cái (LEDGER_*)."""
    by_tx: dict[str, list[LedgerEntry]] = defaultdict(list)
    for entry in ledger_entries:
        by_tx[entry.transaction_id].append(entry)

    tx_map = {tx.transaction_id: tx for tx in transactions}
    quarantined: list[QuarantineRecord] = []

    for tx in transactions:
        entries = by_tx.get(tx.transaction_id, [])
        res = validate_transaction_ledger_group(tx, entries)
        if not res.is_valid:
            ordered = tuple(c.value for c in sort_issue_codes(res.issue_codes))
            ptr = (source_pointers_by_tx or {}).get(tx.transaction_id, f"tx:{tx.transaction_id}")
            ref = (raw_references_by_tx or {}).get(tx.transaction_id, "")
            quarantined.append(
                QuarantineRecord(
                    dataset_id=tx.dataset_id,
                    business_date=_safe_date(tx.business_date),
                    run_id=run_id,
                    stage=STAGE_SILVER_BUSINESS,
                    entity="ledger_entry",
                    business_key=tx.transaction_id,
                    error_code=ordered[0],
                    issue_codes=ordered,
                    error_detail=_format_detail(res.issues),
                    source_pointer=ptr,
                    raw_reference=ref,
                )
            )

    for tx_id, entries in sorted(by_tx.items()):
        if tx_id not in tx_map:
            for entry in entries:
                quarantined.append(
                    QuarantineRecord(
                        dataset_id=entry.dataset_id,
                        business_date=_safe_date(entry.business_date),
                        run_id=run_id,
                        stage=STAGE_SILVER_BUSINESS,
                        entity="ledger_entry",
                        business_key=entry.entry_id,
                        error_code=ErrorCode.LEDGER_TRANSACTION_MISMATCH.value,
                        issue_codes=(ErrorCode.LEDGER_TRANSACTION_MISMATCH.value,),
                        error_detail=(
                            f"LEDGER_TRANSACTION_MISMATCH@$.transaction_id: "
                            f"Bút toán {entry.entry_id!r} trỏ tới transaction_id={tx_id!r} "
                            "không tồn tại trong giao dịch hợp lệ."
                        ),
                        source_pointer=(source_pointers_by_tx or {}).get(
                            entry.entry_id, f"entry:{entry.entry_id}"
                        ),
                        raw_reference=(raw_references_by_tx or {}).get(entry.entry_id, ""),
                    )
                )

    return quarantined
