"""Hằng số quy tắc đối soát (TASK T33; ADR-0006, docs/spec.md Phần I mục 7).

Không phụ thuộc Spark để unit test chạy được trên host. Thứ tự các phần tử trong `STATUS_PRIORITY`
là thứ tự ưu tiên của bảng mục 7.2 (S341–S350), nhỏ hơn thì ưu tiên hơn.
"""

from __future__ import annotations

from enum import StrEnum

RECON_RULE_VERSION = "1.0.0"
"""Phiên bản quy tắc đối soát. Đổi tolerance hay thứ tự ưu tiên phải tăng phiên bản (S353, S687)."""


class ReconStatus(StrEnum):
    """Tám trạng thái đối soát (S343–S350)."""

    DUPLICATE_OR_CONFLICT = "DUPLICATE_OR_CONFLICT"
    CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    STATUS_MISMATCH = "STATUS_MISMATCH"
    MATCHED = "MATCHED"
    PENDING = "PENDING"
    MISSING_EXTERNAL = "MISSING_EXTERNAL"
    MISSING_INTERNAL = "MISSING_INTERNAL"


STATUS_PRIORITY: tuple[ReconStatus, ...] = (
    ReconStatus.DUPLICATE_OR_CONFLICT,  # 1
    ReconStatus.CURRENCY_MISMATCH,  # 2
    ReconStatus.AMOUNT_MISMATCH,  # 3
    ReconStatus.STATUS_MISMATCH,  # 4
    ReconStatus.MATCHED,  # 5
    ReconStatus.PENDING,  # 6
    ReconStatus.MISSING_EXTERNAL,  # 7
    ReconStatus.MISSING_INTERNAL,  # 8
)

# Mã chỉ đối soát mới có, không thuộc 17 mã lỗi hợp đồng ở Phần II mục 8 (ADR-0006 Q8).
COUNTERPART_QUARANTINED = "COUNTERPART_QUARANTINED"

# Mã lỗi hợp đồng (Phần II mục 8, S892–S893) đánh dấu xung đột khóa tại nguồn; mọi mã khác trong
# `silver.key_conflicts.conflict_code` là bản ghi bị quarantine còn khôi phục được match key
# (ADR-0004b mục 3.5).
EVENT_ID_CONFLICT = "EVENT_ID_CONFLICT"
BUSINESS_KEY_CONFLICT = "BUSINESS_KEY_CONFLICT"
SOURCE_CONFLICT_CODES: tuple[str, ...] = (BUSINESS_KEY_CONFLICT, EVENT_ID_CONFLICT)

# Thứ tự cố định của `issue_codes` (ADR-0006 D6, Q1, Q8): trước hết các mã trạng thái theo thứ tự
# bảng 7.2, sau đó mã đối soát bổ sung, sau cùng mã xung đột nguồn.
ISSUE_CODE_ORDER: tuple[str, ...] = (
    *(status.value for status in STATUS_PRIORITY if status is not ReconStatus.MATCHED),
    COUNTERPART_QUARANTINED,
    *SOURCE_CONFLICT_CODES,
)

# Cột bắt buộc của đầu vào theo ADR-0004b mục 3 (chỉ các cột đối soát đọc).
TRANSACTION_COLUMNS: tuple[str, ...] = (
    "dataset_id",
    "transaction_id",
    "external_reference",
    "channel",
    "amount_minor",
    "currency",
    "status",
    "event_time",
    "business_date",
    "source_pointers",
)
STATEMENT_COLUMNS: tuple[str, ...] = (
    "dataset_id",
    "statement_record_id",
    "external_reference",
    "channel",
    "amount_minor",
    "currency",
    "status",
    "event_time",
    "business_date",
    "file_available_at",
    "delivery_order",
    "source_file",
    "source_sha256",
    "source_line",
)
KEY_CONFLICT_COLUMNS: tuple[str, ...] = (
    "dataset_id",
    "business_date",
    "entity",
    "channel",
    "external_reference",
    "conflict_code",
    "source_pointers",
)
# Cột tiền phải là số nguyên 64 bit (CLAUDE.md: tiền là số nguyên, không float).
MONEY_COLUMN = "amount_minor"
MONEY_SPARK_TYPE = "bigint"

# `entity` của key_conflicts mà đối soát dùng; `ledger_entry` không có match key nên bỏ qua.
KEY_CONFLICT_ENTITIES: tuple[str, ...] = ("transaction", "statement")
