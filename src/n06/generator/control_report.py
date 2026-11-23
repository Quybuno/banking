"""Control report của nguồn nội bộ đã công bố (Phần II mục 4)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from n06.generator.determinism import canonical_json


class ControlTotals:
    """Bộ cộng dồn theo `(entity, business_date, currency, status_or_side)`.

    Bộ cộng dồn không tự khử trùng: bên gọi chỉ thêm mỗi bản ghi nghiệp vụ duy nhất đã công bố
    một lần. Với đầu vào có replay dùng `build_control_report_from_envelopes`.
    """

    def __init__(self) -> None:
        self._totals: dict[tuple[str, str, str, str], list[int]] = {}

    def add(self, entity: str, record: Mapping[str, Any]) -> None:
        status_or_side = record["status"] if entity == "transaction" else record["side"]
        key = (entity, str(record["business_date"]), str(record["currency"]), str(status_or_side))
        slot = self._totals.setdefault(key, [0, 0])
        slot[0] += 1
        slot[1] += int(record["amount_minor"])

    def to_report(self, contract_version: str, dataset_id: str) -> dict[str, Any]:
        rows = [
            {
                "entity": entity,
                "business_date": business_date,
                "currency": currency,
                "status_or_side": status_or_side,
                "record_count": count,
                "amount_minor_total": total,
            }
            for (entity, business_date, currency, status_or_side), (count, total) in sorted(
                self._totals.items()
            )
            if count >= 1
        ]
        return {"schema_version": contract_version, "dataset_id": dataset_id, "totals": rows}


def build_control_report_from_envelopes(
    envelopes: Iterable[Mapping[str, Any]], contract_version: str, dataset_id: str
) -> dict[str, Any]:
    """Dựng control report từ luồng envelope: replay tính một lần (khử theo `event_id`, rồi theo
    `transaction_id`/`entry_id`). Giữ tập ID trong RAM nên chỉ dùng cho kiểm chứng.
    """
    totals = ControlTotals()
    seen_events: set[str] = set()
    seen_business: set[tuple[str, str]] = set()
    for env in envelopes:
        event_id = str(env["event_id"])
        if event_id in seen_events:
            continue
        seen_events.add(event_id)
        payload = env["payload"]
        if env["event_type"] == "TRANSACTION_CREATED":
            entity, business_id = "transaction", str(payload["transaction_id"])
        else:
            entity, business_id = "ledger_entry", str(payload["entry_id"])
        if (entity, business_id) in seen_business:
            continue
        seen_business.add((entity, business_id))
        totals.add(entity, payload)
    return totals.to_report(contract_version, dataset_id)


def write_control_report(path: Path, report: Mapping[str, Any]) -> None:
    """Một object JSON trên một dòng, kết thúc LF."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(canonical_json(report))
        fh.write("\n")
