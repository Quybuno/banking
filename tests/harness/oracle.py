"""Oracle đối soát độc lập đọc ground truth/fixture và tính kết quả kỳ vọng (TASK T14).

Tham chiếu:
- docs/spec.md Phần I mục 7 (Quy tắc đối soát, thứ tự ưu tiên 8 trạng thái, tuổi sai lệch)
- docs/spec.md Phần II mục 5.3 & mục 6 (Ground truth, bộ nghiệm thu 3 snapshot)

Quy tắc cứng:
- Module này chỉ nằm trong `tests/harness/`, tuyệt đối không import logic đối soát từ `src/n06/`.
- `src/n06/` tuyệt đối không import module này.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

DEFAULT_BUSINESS_TIMEZONE = "Asia/Ho_Chi_Minh"
DEFAULT_ALLOWED_LATENESS_SECONDS = 86_400


class ReconStatus(StrEnum):
    """8 trạng thái đối soát theo thứ tự ưu tiên xác định (Phần I mục 7.2)."""

    DUPLICATE_OR_CONFLICT = "DUPLICATE_OR_CONFLICT"
    CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    STATUS_MISMATCH = "STATUS_MISMATCH"
    MATCHED = "MATCHED"
    PENDING = "PENDING"
    MISSING_EXTERNAL = "MISSING_EXTERNAL"
    MISSING_INTERNAL = "MISSING_INTERNAL"


@dataclass(frozen=True)
class ExpectedReconRecord:
    """Kết quả đối soát kỳ vọng cho một khóa `(dataset_id, channel, external_reference)`."""

    dataset_id: str
    channel: str
    external_reference: str
    business_date: str
    recon_status: ReconStatus
    issue_codes: tuple[str, ...]
    expected_amount_minor: int | None
    expected_currency: str | None
    cutoff_utc: datetime | None
    anchor_event_time: datetime | None
    discrepancy_age_seconds: int | None
    transaction_ids: tuple[str, ...]
    statement_record_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Chuyển sang dict tuần tự hóa JSON."""
        data = asdict(self)
        data["recon_status"] = self.recon_status.value
        data["cutoff_utc"] = format_utc_timestamp(self.cutoff_utc) if self.cutoff_utc else None
        data["anchor_event_time"] = (
            format_utc_timestamp(self.anchor_event_time) if self.anchor_event_time else None
        )
        return data


@dataclass(frozen=True)
class OracleSnapshotResult:
    """Toàn bộ kết quả kỳ vọng của oracle tại một `as_of_time`."""

    dataset_id: str
    as_of_time: datetime
    allowed_lateness_seconds: int
    records: tuple[ExpectedReconRecord, ...]
    counts_by_status: dict[str, int]

    @property
    def by_reference(self) -> dict[str, ExpectedReconRecord]:
        """Ánh xạ `external_reference` -> bản ghi kỳ vọng (dùng khi reference là duy nhất)."""
        return {rec.external_reference: rec for rec in self.records}

    @property
    def by_match_key(self) -> dict[tuple[str, str, str], ExpectedReconRecord]:
        """Ánh xạ `(dataset_id, channel, external_reference)` -> bản ghi kỳ vọng."""
        return {(rec.dataset_id, rec.channel, rec.external_reference): rec for rec in self.records}

    def to_dict(self) -> dict[str, Any]:
        """Chuyển sang dict tuần tự hóa JSON."""
        return {
            "dataset_id": self.dataset_id,
            "as_of_time": format_utc_timestamp(self.as_of_time),
            "allowed_lateness_seconds": self.allowed_lateness_seconds,
            "match_key_count": len(self.records),
            "counts_by_status": self.counts_by_status,
            "records": [rec.to_dict() for rec in self.records],
        }


@dataclass(frozen=True)
class ValidationMismatch:
    """Một sai khác giữa kết quả thực tế của pipeline và oracle."""

    match_key: tuple[str, str, str]
    field: str
    expected: Any
    actual: Any
    reason: str


@dataclass(frozen=True)
class ValidationReport:
    """Báo cáo đối chiếu kết quả thực tế với oracle."""

    dataset_id: str
    as_of_time: str
    passed: bool
    expected_count: int
    actual_count: int
    mismatches: tuple[ValidationMismatch, ...]

    def to_dict(self) -> dict[str, Any]:
        """Chuyển sang dict tuần tự hóa JSON."""
        return {
            "dataset_id": self.dataset_id,
            "as_of_time": self.as_of_time,
            "passed": self.passed,
            "expected_count": self.expected_count,
            "actual_count": self.actual_count,
            "mismatches": [
                {
                    "match_key": list(m.match_key),
                    "field": m.field,
                    "expected": m.expected,
                    "actual": m.actual,
                    "reason": m.reason,
                }
                for m in self.mismatches
            ],
        }


def parse_utc_timestamp(value: str) -> datetime:
    """Parse chuỗi timestamp UTC dạng `...Z` hoặc `+00:00` thành `datetime` UTC."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Timestamp UTC không được rỗng")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        raise ValueError(f"Timestamp phải có múi giờ UTC: {value!r}")
    return dt.astimezone(UTC)


def format_utc_timestamp(dt: datetime) -> str:
    """Định dạng `datetime` sang chuẩn UTC `YYYY-MM-DDTHH:MM:SSZ`."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_cutoff_utc(
    business_date: date | str,
    allowed_lateness_seconds: int = DEFAULT_ALLOWED_LATENESS_SECONDS,
    tz_name: str = DEFAULT_BUSINESS_TIMEZONE,
) -> datetime:
    """Tính mốc cutoff (UTC) = 00:00:00 ngày kế tiếp theo `tz_name` + `allowed_lateness_seconds`.

    Quy tắc (Phần I mục 7.3 & Phần II mục 1.10):
    - `as_of_time < cutoff` -> chưa quá hạn (`PENDING`).
    - `as_of_time >= cutoff` -> đã quá hạn (tại đúng cutoff được coi là quá hạn).
    """
    if allowed_lateness_seconds < 0:
        raise ValueError("allowed_lateness_seconds phải >= 0")
    bdate = date.fromisoformat(business_date) if isinstance(business_date, str) else business_date
    next_day = bdate + timedelta(days=1)
    local_midnight = datetime(
        next_day.year,
        next_day.month,
        next_day.day,
        0,
        0,
        0,
        tzinfo=ZoneInfo(tz_name),
    )
    return local_midnight.astimezone(UTC) + timedelta(seconds=allowed_lateness_seconds)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    """Đọc file JSONL UTF-8 thành danh sách dict."""
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, raw_line in enumerate(fh, start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue
            obj = json.loads(stripped)
            if not isinstance(obj, dict):
                raise ValueError(f"{path}:{line_no}: mỗi dòng JSONL phải là object")
            records.append(obj)
    return records


def load_acceptance_inputs(
    fixture_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Nạp `transactions.jsonl`, `statements.jsonl`, và `statement_deliveries.jsonl` từ fixture."""
    tx_path = fixture_dir / "transactions.jsonl"
    stmt_path = fixture_dir / "statements.jsonl"
    delivery_path = fixture_dir / "statement_deliveries.jsonl"

    transactions = load_jsonl(tx_path)
    statements = load_jsonl(stmt_path)
    deliveries = load_jsonl(delivery_path) if delivery_path.exists() else []
    return transactions, statements, deliveries


def _dedup_transactions(transactions: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Loại trùng physical replay theo `transaction_id`."""
    seen: dict[str, Mapping[str, Any]] = {}
    for tx in transactions:
        tx_id = str(tx["transaction_id"])
        if tx_id not in seen:
            seen[tx_id] = tx
    return list(seen.values())


def _filter_and_dedup_statements(
    statements: Sequence[Mapping[str, Any]],
    as_of_time: datetime,
    deliveries: Sequence[Mapping[str, Any]] | None = None,
) -> list[Mapping[str, Any]]:
    """Lọc các statement đã công bố (`file_available_at <= as_of_time`) và dedup theo ID."""
    delivery_time_by_id: dict[str, datetime] = {}
    if deliveries:
        for item in deliveries:
            rec_id = str(item["statement_record_id"])
            delivery_time_by_id[rec_id] = parse_utc_timestamp(str(item["file_available_at"]))

    seen: dict[str, Mapping[str, Any]] = {}
    for stmt in statements:
        stmt_id = str(stmt["statement_record_id"])
        if stmt_id in seen:
            continue

        if stmt_id in delivery_time_by_id:
            avail_dt = delivery_time_by_id[stmt_id]
        elif "file_available_at" in stmt and stmt["file_available_at"] is not None:
            avail_dt = parse_utc_timestamp(str(stmt["file_available_at"]))
        elif "available_at" in stmt and stmt["available_at"] is not None:
            avail_dt = parse_utc_timestamp(str(stmt["available_at"]))
        else:
            avail_dt = as_of_time

        if avail_dt <= as_of_time:
            seen[stmt_id] = stmt
    return list(seen.values())


def evaluate_match_key(
    *,
    dataset_id: str,
    channel: str,
    external_reference: str,
    transactions: Sequence[Mapping[str, Any]],
    statements: Sequence[Mapping[str, Any]],
    as_of_time: datetime,
    allowed_lateness_seconds: int = DEFAULT_ALLOWED_LATENESS_SECONDS,
    tz_name: str = DEFAULT_BUSINESS_TIMEZONE,
    has_key_conflict: bool = False,
) -> ExpectedReconRecord:
    """Tính trạng thái đối soát và tuổi sai lệch kỳ vọng cho một match key."""
    if not transactions and not statements:
        raise ValueError(
            f"Match key ({dataset_id}, {channel}, {external_reference}) không có bản ghi nào"
        )

    tx_ids = tuple(str(tx["transaction_id"]) for tx in transactions)
    stmt_ids = tuple(str(st["statement_record_id"]) for st in statements)

    # Xác định business_date đại diện: ưu tiên từ giao dịch nội bộ, ngược lại lấy từ statement.
    anchor_source = transactions[0] if transactions else statements[0]
    bdate_str = str(anchor_source["business_date"])
    cutoff = compute_cutoff_utc(bdate_str, allowed_lateness_seconds, tz_name)

    issue_codes: list[str] = []
    if has_key_conflict or len(transactions) > 1 or len(statements) > 1:
        issue_codes.append(ReconStatus.DUPLICATE_OR_CONFLICT.value)
        recon_status = ReconStatus.DUPLICATE_OR_CONFLICT
    elif len(transactions) == 1 and len(statements) == 1:
        tx = transactions[0]
        st = statements[0]
        if tx["currency"] != st["currency"]:
            issue_codes.append(ReconStatus.CURRENCY_MISMATCH.value)
        if tx["amount_minor"] != st["amount_minor"]:
            issue_codes.append(ReconStatus.AMOUNT_MISMATCH.value)
        if tx["status"] != st["status"]:
            issue_codes.append(ReconStatus.STATUS_MISMATCH.value)

        if ReconStatus.CURRENCY_MISMATCH.value in issue_codes:
            recon_status = ReconStatus.CURRENCY_MISMATCH
        elif ReconStatus.AMOUNT_MISMATCH.value in issue_codes:
            recon_status = ReconStatus.AMOUNT_MISMATCH
        elif ReconStatus.STATUS_MISMATCH.value in issue_codes:
            recon_status = ReconStatus.STATUS_MISMATCH
        else:
            recon_status = ReconStatus.MATCHED
    else:
        # Thiếu một phía: kiểm tra cutoff (as_of_time >= cutoff là quá hạn).
        if as_of_time < cutoff:
            recon_status = ReconStatus.PENDING
            issue_codes.append(ReconStatus.PENDING.value)
        elif len(transactions) == 1:
            recon_status = ReconStatus.MISSING_EXTERNAL
            issue_codes.append(ReconStatus.MISSING_EXTERNAL.value)
        else:
            recon_status = ReconStatus.MISSING_INTERNAL
            issue_codes.append(ReconStatus.MISSING_INTERNAL.value)

    if recon_status == ReconStatus.MATCHED:
        anchor_event_time: datetime | None = None
        discrepancy_age_seconds: int | None = None
    else:
        anchor_records = transactions if transactions else statements
        anchor_event_time = min(
            parse_utc_timestamp(str(item["event_time"])) for item in anchor_records
        )
        age_sec = int((as_of_time - anchor_event_time).total_seconds())
        discrepancy_age_seconds = max(0, age_sec)

    expected_amount: int | None = None
    expected_currency: str | None = None
    if len(transactions) == 1:
        expected_amount = int(transactions[0]["amount_minor"])
        expected_currency = str(transactions[0]["currency"])
    elif len(statements) == 1:
        expected_amount = int(statements[0]["amount_minor"])
        expected_currency = str(statements[0]["currency"])

    return ExpectedReconRecord(
        dataset_id=dataset_id,
        channel=channel,
        external_reference=external_reference,
        business_date=bdate_str,
        recon_status=recon_status,
        issue_codes=tuple(issue_codes),
        expected_amount_minor=expected_amount,
        expected_currency=expected_currency,
        cutoff_utc=cutoff,
        anchor_event_time=anchor_event_time,
        discrepancy_age_seconds=discrepancy_age_seconds,
        transaction_ids=tx_ids,
        statement_record_ids=stmt_ids,
    )


def evaluate_snapshot(
    *,
    transactions: Sequence[Mapping[str, Any]],
    statements: Sequence[Mapping[str, Any]],
    as_of_time: datetime | str,
    deliveries: Sequence[Mapping[str, Any]] | None = None,
    allowed_lateness_seconds: int = DEFAULT_ALLOWED_LATENESS_SECONDS,
    tz_name: str = DEFAULT_BUSINESS_TIMEZONE,
    conflict_keys: set[tuple[str, str, str]] | None = None,
) -> OracleSnapshotResult:
    """Tính kết quả đối soát kỳ vọng cho toàn bộ match keys tại thời điểm `as_of_time`."""
    as_of_dt = parse_utc_timestamp(as_of_time) if isinstance(as_of_time, str) else as_of_time
    as_of_dt = as_of_dt.astimezone(UTC)

    dedup_txs = _dedup_transactions(transactions)
    avail_stmts = _filter_and_dedup_statements(statements, as_of_dt, deliveries=deliveries)

    tx_by_key: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for tx in dedup_txs:
        key = (str(tx["dataset_id"]), str(tx["channel"]), str(tx["external_reference"]))
        tx_by_key[key].append(tx)

    stmt_by_key: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for st in avail_stmts:
        key = (str(st["dataset_id"]), str(st["channel"]), str(st["external_reference"]))
        stmt_by_key[key].append(st)

    all_keys = sorted(set(tx_by_key.keys()) | set(stmt_by_key.keys()))
    dataset_ids = {k[0] for k in all_keys}
    dataset_id = next(iter(dataset_ids)) if len(dataset_ids) == 1 else ",".join(sorted(dataset_ids))

    records: list[ExpectedReconRecord] = []
    counts: Counter[str] = Counter()
    conflicts = conflict_keys or set()

    for key in all_keys:
        ds_id, channel, ext_ref = key
        rec = evaluate_match_key(
            dataset_id=ds_id,
            channel=channel,
            external_reference=ext_ref,
            transactions=tx_by_key.get(key, ()),
            statements=stmt_by_key.get(key, ()),
            as_of_time=as_of_dt,
            allowed_lateness_seconds=allowed_lateness_seconds,
            tz_name=tz_name,
            has_key_conflict=key in conflicts,
        )
        records.append(rec)
        counts[rec.recon_status.value] += 1

    return OracleSnapshotResult(
        dataset_id=dataset_id,
        as_of_time=as_of_dt,
        allowed_lateness_seconds=allowed_lateness_seconds,
        records=tuple(records),
        counts_by_status=dict(sorted(counts.items())),
    )


def compare_against_oracle(
    expected: OracleSnapshotResult,
    actual_records: Sequence[Mapping[str, Any]],
) -> ValidationReport:
    """Đối chiếu từng match key giữa kết quả thực tế của pipeline và oracle."""
    expected_map = expected.by_match_key
    actual_map: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    mismatches: list[ValidationMismatch] = []

    for row in actual_records:
        key = (str(row["dataset_id"]), str(row["channel"]), str(row["external_reference"]))
        if key in actual_map:
            mismatches.append(
                ValidationMismatch(
                    match_key=key,
                    field="match_key",
                    expected="unique",
                    actual="duplicate",
                    reason="Kết quả thực tế chứa trùng match_key",
                )
            )
            continue
        actual_map[key] = row

    for key, exp in expected_map.items():
        if key not in actual_map:
            mismatches.append(
                ValidationMismatch(
                    match_key=key,
                    field="presence",
                    expected=exp.recon_status.value,
                    actual=None,
                    reason="Thiếu match_key trong kết quả thực tế",
                )
            )
            continue

        act = actual_map[key]
        act_status = str(act.get("recon_status"))
        if act_status != exp.recon_status.value:
            mismatches.append(
                ValidationMismatch(
                    match_key=key,
                    field="recon_status",
                    expected=exp.recon_status.value,
                    actual=act_status,
                    reason="Sai trạng thái đối soát",
                )
            )

        act_age = act.get("discrepancy_age_seconds")
        if act_age != exp.discrepancy_age_seconds:
            mismatches.append(
                ValidationMismatch(
                    match_key=key,
                    field="discrepancy_age_seconds",
                    expected=exp.discrepancy_age_seconds,
                    actual=act_age,
                    reason="Sai tuổi sai lệch (discrepancy_age_seconds)",
                )
            )

        exp_anchor = format_utc_timestamp(exp.anchor_event_time) if exp.anchor_event_time else None
        act_anchor_raw = act.get("anchor_event_time")
        act_anchor = (
            format_utc_timestamp(parse_utc_timestamp(str(act_anchor_raw)))
            if act_anchor_raw is not None
            else None
        )
        if act_anchor != exp_anchor:
            mismatches.append(
                ValidationMismatch(
                    match_key=key,
                    field="anchor_event_time",
                    expected=exp_anchor,
                    actual=act_anchor,
                    reason="Sai anchor_event_time",
                )
            )

    for key in sorted(set(actual_map.keys()) - set(expected_map.keys())):
        mismatches.append(
            ValidationMismatch(
                match_key=key,
                field="presence",
                expected=None,
                actual=actual_map[key].get("recon_status"),
                reason="Dư match_key không có trong oracle",
            )
        )

    return ValidationReport(
        dataset_id=expected.dataset_id,
        as_of_time=format_utc_timestamp(expected.as_of_time),
        passed=len(mismatches) == 0,
        expected_count=len(expected_map),
        actual_count=len(actual_records),
        mismatches=tuple(mismatches),
    )


def _default_acceptance_dir() -> Path:
    root = Path(__file__).resolve().parents[2]
    return root / "contracts" / "N06_INPUT_CONTRACT_V1" / "fixtures" / "acceptance"


def main(argv: Sequence[str] | None = None) -> int:
    """Entrypoint CLI nội bộ của test harness oracle."""
    parser = argparse.ArgumentParser(
        prog="python -m tests.harness.oracle",
        description="Tính oracle đối soát và đối chiếu kết quả thực tế (test harness).",
    )
    parser.add_argument("--dataset-id", required=True, help="Mã dataset cần kiểm tra.")
    parser.add_argument("--as-of-time", required=True, help="Thời điểm đánh giá UTC (vd ...Z).")
    parser.add_argument(
        "--fixture-dir",
        type=Path,
        default=None,
        help="Thư mục chứa transactions.jsonl, statements.jsonl, statement_deliveries.jsonl.",
    )
    parser.add_argument(
        "--actual-file",
        type=Path,
        default=None,
        help="File JSON/JSONL kết quả đối soát thực tế để đối chiếu (tùy chọn).",
    )
    parser.add_argument(
        "--allowed-lateness-seconds",
        type=int,
        default=DEFAULT_ALLOWED_LATENESS_SECONDS,
        help="Thời gian trễ cho phép tính bằng giây (mặc định 86400).",
    )
    parser.add_argument("--json", action="store_true", dest="as_json", help="Xuất JSON.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    fixture_dir = args.fixture_dir or _default_acceptance_dir()
    transactions, statements, deliveries = load_acceptance_inputs(fixture_dir)
    expected = evaluate_snapshot(
        transactions=transactions,
        statements=statements,
        deliveries=deliveries,
        as_of_time=args.as_of_time,
        allowed_lateness_seconds=args.allowed_lateness_seconds,
    )

    if args.actual_file is not None:
        raw_text = args.actual_file.read_text(encoding="utf-8").strip()
        if raw_text.startswith("["):
            actual_records = json.loads(raw_text)
        else:
            actual_records = load_jsonl(args.actual_file)
        report = compare_against_oracle(expected, actual_records)
        payload = report.to_dict()
        exit_code = 0 if report.passed else 1
    else:
        payload = expected.to_dict()
        exit_code = 0

    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"[oracle] dataset_id={expected.dataset_id} "
            f"as_of_time={format_utc_timestamp(expected.as_of_time)} "
            f"match_keys={len(expected.records)} counts={expected.counts_by_status}"
        )
        if args.actual_file is not None:
            mismatches_count = len(payload["mismatches"])
            print(f"[oracle] validation passed={payload['passed']} mismatches={mismatches_count}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
