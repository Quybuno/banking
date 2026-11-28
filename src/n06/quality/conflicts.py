"""Phân biệt replay, duplicate nghiệp vụ và xung đột khóa (`key_conflicts`) (TASK T31, ADR-0004b).

Module thuần Python (không import PySpark) tuân thủ đặc tả Phần I mục 6, 7.1 và Phần II mục 8:
- Cùng `event_id` + cùng nội dung -> event replay: gộp 1 bản ghi, giữ lineage (`event_ids`,
  `replay_count`, `source_pointers`).
- Khác `event_id` + cùng business key + cùng nội dung -> business replay (ví dụ T009): gộp 1
  bản ghi, lưu toàn bộ `event_ids` đã sắp và `replay_count >= 2`.
- Cùng `event_id` + khác nội dung -> `EVENT_ID_CONFLICT` (ưu tiên cao hơn `BUSINESS_KEY_CONFLICT`).
- Cùng business key + khác nội dung -> `BUSINESS_KEY_CONFLICT`.
- Cả hai loại xung đột đều bị loại khỏi Silver (không chọn ngẫu nhiên), ghi vào `quarantine.records`
  (`stage = 'SILVER_DEDUP'`) và xuất chỉ mục vào `silver.key_conflicts` để T33 gắn
  `DUPLICATE_OR_CONFLICT`.
- Khác business key nhưng cùng match key `(dataset_id, channel, external_reference)` (T010, T011)
  -> duplicate nghiệp vụ: GIỮ nguyên ở Silver để đối soát (T33) phát hiện.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from n06.domain.errors import ErrorCode
from n06.quality.quarantine import STAGE_SILVER_DEDUP, QuarantineRecord

KEY_CONFLICT_COLUMNS: tuple[str, ...] = (
    "dataset_id",
    "business_date",
    "side_origin",
    "entity",
    "business_key",
    "channel",
    "external_reference",
    "conflict_code",
    "issue_codes",
    "source_pointers",
    "run_id",
)

SIDE_INTERNAL = "INTERNAL"
SIDE_STATEMENT = "STATEMENT"


@dataclass(frozen=True)
class KeyConflictRecord:
    """Một dòng trong `silver.key_conflicts` theo ADR-0004b §3.5."""

    dataset_id: str
    business_date: date
    side_origin: str
    entity: str
    business_key: str | None
    channel: str | None
    external_reference: str | None
    conflict_code: str
    issue_codes: tuple[str, ...]
    source_pointers: tuple[str, ...]
    run_id: str

    def as_dict(self) -> dict[str, object]:
        return {
            "dataset_id": self.dataset_id,
            "business_date": self.business_date,
            "side_origin": self.side_origin,
            "entity": self.entity,
            "business_key": self.business_key,
            "channel": self.channel,
            "external_reference": self.external_reference,
            "conflict_code": self.conflict_code,
            "issue_codes": list(self.issue_codes),
            "source_pointers": list(self.source_pointers),
            "run_id": self.run_id,
        }


@dataclass(frozen=True)
class DedupOutcome:
    """Kết quả bước dedup + phát hiện xung đột ở Silver."""

    valid_records: tuple[dict[str, Any], ...]
    quarantine_records: tuple[QuarantineRecord, ...]
    key_conflicts: tuple[KeyConflictRecord, ...]
    event_replay_count: int
    business_replay_count: int


def _canonical_hash(obj: Mapping[str, Any]) -> str:
    text = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _safe_date(raw: object) -> date:
    if isinstance(raw, date):
        return raw
    if isinstance(raw, str):
        return date.fromisoformat(raw)
    raise ValueError(f"business_date không hợp lệ: {raw!r}")


def dedup_and_detect_event_conflicts(
    envelopes: Sequence[Mapping[str, Any]],
    *,
    entity: str,
    run_id: str,
    raw_reference: str = "",
) -> DedupOutcome:
    """Dedup danh sách event envelope (`transaction` hoặc `ledger_entry`) và tách xung đột.

    Mỗi phần tử có thể kèm khóa phụ `_source_pointer` (mặc định `kafka:0:<idx>`).
    """
    if entity == "transaction":
        key_field = "transaction_id"
    elif entity == "ledger_entry":
        key_field = "entry_id"
    else:
        raise ValueError(f"Entity sự kiện không hỗ trợ: {entity!r}")

    items: list[dict[str, Any]] = []
    event_hashes_by_eid: dict[str, set[str]] = defaultdict(set)
    payload_hashes_by_bk: dict[tuple[str, str], set[str]] = defaultdict(set)

    for idx, env in enumerate(envelopes):
        payload = dict(env["payload"])
        ds_id = str(payload["dataset_id"])
        bk = str(payload[key_field])
        eid = str(env["event_id"])
        ptr = str(env.get("_source_pointer") or f"kafka:0:{idx}")
        p_hash = _canonical_hash(payload)
        e_hash = _canonical_hash({"event_type": env.get("event_type"), "payload": payload})
        event_hashes_by_eid[eid].add(e_hash)
        payload_hashes_by_bk[(ds_id, bk)].add(p_hash)
        items.append(
            {
                "dataset_id": ds_id,
                "business_key": bk,
                "event_id": eid,
                "pointer": ptr,
                "payload": payload,
                "payload_hash": p_hash,
                "event_hash": e_hash,
            }
        )

    conflicted_eids = {eid for eid, hashes in event_hashes_by_eid.items() if len(hashes) > 1}
    conflicted_bks = {bk for bk, hashes in payload_hashes_by_bk.items() if len(hashes) > 1}

    clean_by_bk: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    conflict_by_bk: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    quarantined: list[QuarantineRecord] = []

    for item in items:
        has_ev = item["event_id"] in conflicted_eids
        has_bk = (item["dataset_id"], item["business_key"]) in conflicted_bks
        if has_ev or has_bk:
            codes: list[str] = []
            if has_ev:
                codes.append(ErrorCode.EVENT_ID_CONFLICT.value)
            if has_bk:
                codes.append(ErrorCode.BUSINESS_KEY_CONFLICT.value)
            primary = codes[0]  # EVENT_ID_CONFLICT ưu tiên trước BUSINESS_KEY_CONFLICT
            payload = item["payload"]
            quarantined.append(
                QuarantineRecord(
                    dataset_id=item["dataset_id"],
                    business_date=_safe_date(payload["business_date"]),
                    run_id=run_id,
                    stage=STAGE_SILVER_DEDUP,
                    entity=entity,
                    business_key=item["business_key"],
                    error_code=primary,
                    issue_codes=tuple(codes),
                    error_detail=f"Xung đột khóa ở Silver ({', '.join(codes)}).",
                    source_pointer=item["pointer"],
                    raw_reference=raw_reference,
                )
            )
            conflict_by_bk[(item["dataset_id"], item["business_key"])].append(
                {**item, "codes": tuple(codes)}
            )
        else:
            clean_by_bk[(item["dataset_id"], item["business_key"])].append(item)

    valid_records: list[dict[str, Any]] = []
    event_replays = 0
    business_replays = 0

    for _key, group in sorted(clean_by_bk.items()):
        seen_eids = sorted({g["event_id"] for g in group})
        pointers = sorted(g["pointer"] for g in group)
        total_phys = len(group)
        # Tổng trùng gồm event replay (cùng event_id) và business replay (khác event_id)
        event_replays += total_phys - len(seen_eids)
        business_replays += max(0, len(seen_eids) - 1)
        rep = dict(group[0]["payload"])
        rep["first_event_id"] = seen_eids[0]
        rep["event_ids"] = seen_eids
        rep["replay_count"] = total_phys
        rep["source_pointers"] = pointers
        rep["run_id"] = run_id
        valid_records.append(rep)

    key_conflicts: list[KeyConflictRecord] = []
    for (ds_id, bk), group in sorted(conflict_by_bk.items()):
        # Nếu một business key có nhiều biến thể với nhiều match key khác nhau, xuất từng match key
        by_match: dict[tuple[str | None, str | None], list[dict[str, Any]]] = defaultdict(list)
        for item in group:
            p = item["payload"]
            ch = p.get("channel") if entity == "transaction" else None
            ext = p.get("external_reference") if entity == "transaction" else None
            by_match[(ch, ext)].append(item)
        for (ch, ext), sub in sorted(
            by_match.items(), key=lambda kv: (kv[0][0] or "", kv[0][1] or "")
        ):
            all_codes_set = {c for item in sub for c in item["codes"]}
            ordered_codes = tuple(
                c
                for c in (ErrorCode.EVENT_ID_CONFLICT.value, ErrorCode.BUSINESS_KEY_CONFLICT.value)
                if c in all_codes_set
            )
            biz_date = min(_safe_date(item["payload"]["business_date"]) for item in sub)
            ptrs = tuple(sorted(item["pointer"] for item in sub))
            key_conflicts.append(
                KeyConflictRecord(
                    dataset_id=ds_id,
                    business_date=biz_date,
                    side_origin=SIDE_INTERNAL,
                    entity=entity,
                    business_key=bk,
                    channel=ch,
                    external_reference=ext,
                    conflict_code=ordered_codes[0],
                    issue_codes=ordered_codes,
                    source_pointers=ptrs,
                    run_id=run_id,
                )
            )

    return DedupOutcome(
        valid_records=tuple(valid_records),
        quarantine_records=tuple(quarantined),
        key_conflicts=tuple(key_conflicts),
        event_replay_count=event_replays,
        business_replay_count=business_replays,
    )


def dedup_and_detect_statement_conflicts(
    statements: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
) -> DedupOutcome:
    """Dedup sao kê theo `statement_record_id`: giữ nguyên T011 (nhiều ID cùng match key)."""
    by_id: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for idx, stmt in enumerate(statements):
        ds_id = str(stmt["dataset_id"])
        sid = str(stmt["statement_record_id"])
        ptr = str(stmt.get("_source_pointer") or f"stmt:0:{idx + 1}")
        raw_ref = str(stmt.get("_raw_reference") or stmt.get("source_file") or "")
        payload_only = {
            k: stmt[k]
            for k in (
                "external_reference",
                "channel",
                "amount_minor",
                "currency",
                "status",
                "event_time",
                "business_date",
                "available_at",
            )
            if k in stmt
        }
        by_id[(ds_id, sid)].append(
            {
                "record": dict(stmt),
                "pointer": ptr,
                "raw_ref": raw_ref,
                "hash": _canonical_hash(payload_only),
            }
        )

    valid_records: list[dict[str, Any]] = []
    quarantined: list[QuarantineRecord] = []
    key_conflicts: list[KeyConflictRecord] = []
    replays = 0

    for (ds_id, sid), group in sorted(by_id.items()):
        distinct_hashes = {g["hash"] for g in group}
        if len(distinct_hashes) == 1:
            replays += len(group) - 1
            chosen = dict(group[0]["record"])
            chosen.pop("_source_pointer", None)
            chosen.pop("_raw_reference", None)
            chosen["run_id"] = run_id
            valid_records.append(chosen)
        else:
            for item in group:
                rec = item["record"]
                quarantined.append(
                    QuarantineRecord(
                        dataset_id=ds_id,
                        business_date=_safe_date(rec["business_date"]),
                        run_id=run_id,
                        stage=STAGE_SILVER_DEDUP,
                        entity="statement",
                        business_key=sid,
                        error_code=ErrorCode.BUSINESS_KEY_CONFLICT.value,
                        issue_codes=(ErrorCode.BUSINESS_KEY_CONFLICT.value,),
                        error_detail=(
                            f"BUSINESS_KEY_CONFLICT: statement_record_id={sid!r} "
                            "có nhiều nội dung khác nhau."
                        ),
                        source_pointer=item["pointer"],
                        raw_reference=item["raw_ref"],
                    )
                )
            by_match: dict[tuple[str | None, str | None], list[dict[str, Any]]] = defaultdict(list)
            for item in group:
                rec = item["record"]
                by_match[(rec.get("channel"), rec.get("external_reference"))].append(item)
            for (ch, ext), sub in sorted(
                by_match.items(), key=lambda kv: (kv[0][0] or "", kv[0][1] or "")
            ):
                key_conflicts.append(
                    KeyConflictRecord(
                        dataset_id=ds_id,
                        business_date=min(_safe_date(x["record"]["business_date"]) for x in sub),
                        side_origin=SIDE_STATEMENT,
                        entity="statement",
                        business_key=sid,
                        channel=ch,
                        external_reference=ext,
                        conflict_code=ErrorCode.BUSINESS_KEY_CONFLICT.value,
                        issue_codes=(ErrorCode.BUSINESS_KEY_CONFLICT.value,),
                        source_pointers=tuple(sorted(x["pointer"] for x in sub)),
                        run_id=run_id,
                    )
                )

    return DedupOutcome(
        valid_records=tuple(valid_records),
        quarantine_records=tuple(quarantined),
        key_conflicts=tuple(key_conflicts),
        event_replay_count=replays,
        business_replay_count=0,
    )
