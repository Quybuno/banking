"""Ingest file sao kê lên MinIO theo lịch giao và biên nhận `_ingest/` (TASK T23).

Căn cứ:
- `docs/spec.md` Phần II mục 4 (file bất biến, manifest từng batch), mục 7 (đồng hồ giả lập,
  không lộ dữ liệu tương lai, không tải `statement_deliveries.jsonl` hay ground truth).
- `docs/adr/0002-duong-dan-minio.md` mục 4 (khóa dữ liệu, manifest, biên nhận `_ingest/`,
  thứ tự tải lên file -> manifest -> biên nhận, từ chối ghi đè khác `sha256`).
- `docs/adr/0009-t13-manifest-delivery-duplicate.md` quyết định 3 (`delivery_order`).
- `docs/adr/0012-driver-postgres-va-ghi-object-minio.md` quyết định 2 (`PUT`/`HEAD` SigV4).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from n06.config import AppConfig
from n06.domain import parse_and_validate_json_schema, parse_utc_timestamp
from n06.generator.determinism import canonical_json
from n06.storage.object_store import (
    ObjectStoreTarget,
    put_object_immutable_bytes,
    put_object_immutable_file,
)

DEFAULT_SOURCE_DIRNAME = "source"
DEFAULT_MANIFESTS_DIRNAME = "manifests"
DEFAULT_STATEMENTS_DIRNAME = "statements"
DEFAULT_DELIVERIES_FILENAME = "statement_deliveries.jsonl"
DEFAULT_ACCOUNTS_FILENAME = "accounts.jsonl"
RECEIPT_SCHEMA_VERSION = "1.0.0"
_DATASET_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{2,63}$")


class StatementIngestConfigError(ValueError):
    """Lỗi nguồn sao kê, lịch giao hoặc tham số cấu hình không hợp lệ (mã thoát 10)."""


@dataclass(frozen=True)
class StatementDeliveryBatch:
    """Một file sao kê kèm manifest và lịch giao của nó."""

    dataset_id: str
    delivery_order: int
    file_available_at: str
    file_relpath: str
    file_path: Path
    manifest_relpath: str
    manifest_path: Path
    source_batch_id: str
    sha256: str
    physical_rows: int


@dataclass(frozen=True)
class StatementIngestResult:
    """Kết quả một lượt chạy `n06 ingest-statements` tại thời điểm `as_of_time`."""

    dataset_id: str
    as_of_time: str
    bucket: str
    total_batches: int
    eligible_batches: int
    uploaded_batches: int
    skipped_batches: int
    deferred_batches: int
    uploaded_keys: tuple[str, ...] = ()
    # ADR-0004b Q2: `accounts.jsonl` nằm ở `<statements_prefix>/<dataset_id>/accounts.jsonl`.
    accounts_key: str | None = None
    accounts_status: str | None = None  # `uploaded`, `skipped`; None khi nguồn không có file


def _check_safe_relative(source_dir: Path, relative: str, where: str) -> Path:
    pure = PurePosixPath(relative)
    if pure.is_absolute() or ".." in pure.parts:
        raise StatementIngestConfigError(
            f"Đường dẫn {relative!r} trong {where} không hợp lệ (tuyệt đối hoặc chứa '..')."
        )
    root = source_dir.resolve()
    target = (root / relative).resolve()
    if not target.is_relative_to(root):
        raise StatementIngestConfigError(
            f"Đường dẫn {relative!r} trong {where} trỏ ra ngoài thư mục nguồn."
        )
    return target


def _verify_file_sha_and_rows(path: Path, expected_sha256: str, expected_rows: int) -> None:
    if not path.is_file():
        raise StatementIngestConfigError(
            f"Thiếu file sao kê {path.name} được khai báo trong nguồn."
        )
    digest = hashlib.sha256()
    rows = 0
    try:
        with path.open("rb") as handle:
            for line_number, line in enumerate(handle, start=1):
                digest.update(line)
                if not line.endswith(b"\n"):
                    raise StatementIngestConfigError(
                        f"File sao kê {path.name} dòng {line_number} thiếu ký tự LF cuối dòng."
                    )
                rows += 1
    except OSError as exc:
        raise StatementIngestConfigError(
            f"Không đọc được file sao kê {path.name} ({type(exc).__name__})."
        ) from exc
    actual_sha = digest.hexdigest()
    if actual_sha != expected_sha256 or rows != expected_rows:
        raise StatementIngestConfigError(
            f"File sao kê {path.name} không khớp manifest: sha256={actual_sha} "
            f"(manifest {expected_sha256}), số dòng={rows} (manifest {expected_rows})."
        )


def _discover_statement_manifests(
    source_dir: Path,
    *,
    manifests_dirname: str,
    statements_dirname: str,
) -> dict[str, tuple[str, str, Path, str, str, int]]:
    """Ánh xạ `file_relpath` tới metadata manifest `(ds, m_rel, m_path, batch_id, sha, rows)`."""
    manifests_dir = source_dir / manifests_dirname
    if not manifests_dir.is_dir():
        raise StatementIngestConfigError(f"Không tìm thấy thư mục manifest: {manifests_dir}")

    statements_prefix = f"{statements_dirname}/"
    by_file: dict[str, tuple[str, str, Path, str, str, int]] = {}
    for manifest_path in sorted(manifests_dir.glob("*.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, RecursionError) as exc:
            raise StatementIngestConfigError(
                f"Manifest {manifest_path.name} hỏng hoặc không đọc được: {exc}"
            ) from exc
        result = parse_and_validate_json_schema(manifest, "source_manifest")
        if not result.is_valid:
            raise StatementIngestConfigError(
                f"Manifest {manifest_path.name} không đúng schema: {result.issues}"
            )
        manifest_rel = f"{manifests_dirname}/{manifest_path.name}"
        stmt_items = [
            item for item in manifest["files"] if str(item["path"]).startswith(statements_prefix)
        ]
        if not stmt_items:
            continue
        if len(stmt_items) != len(manifest["files"]) or len(stmt_items) != 1:
            raise StatementIngestConfigError(
                f"Manifest sao kê {manifest_path.name} phải khai báo đúng 1 file sao kê."
            )
        item = stmt_items[0]
        rel = str(item["path"])
        _check_safe_relative(source_dir, rel, manifest_path.name)
        if rel in by_file:
            raise StatementIngestConfigError(f"File sao kê {rel} xuất hiện trong nhiều manifest.")
        by_file[rel] = (
            str(manifest["dataset_id"]),
            manifest_rel,
            manifest_path,
            str(manifest["source_batch_id"]),
            str(item["sha256"]),
            int(item["physical_rows"]),
        )
    return by_file


def discover_statement_deliveries(
    source_dir: Path,
    *,
    dataset_id: str | None = None,
    manifests_dirname: str = DEFAULT_MANIFESTS_DIRNAME,
    statements_dirname: str = DEFAULT_STATEMENTS_DIRNAME,
    deliveries_filename: str = DEFAULT_DELIVERIES_FILENAME,
) -> tuple[StatementDeliveryBatch, ...]:
    """Đọc và kiểm tra toàn bộ lịch giao + manifest + file sao kê dưới `source_dir`."""
    if not source_dir.is_dir():
        raise StatementIngestConfigError(f"Thư mục nguồn sao kê không tồn tại: {source_dir}")
    if dataset_id is not None and not _DATASET_ID_PATTERN.fullmatch(dataset_id):
        raise StatementIngestConfigError(f"dataset_id không hợp lệ: {dataset_id!r}.")

    deliveries_path = source_dir / deliveries_filename
    if not deliveries_path.is_file():
        raise StatementIngestConfigError(
            f"Không tìm thấy lịch giao sao kê {deliveries_filename} trong {source_dir}."
        )

    manifests = _discover_statement_manifests(
        source_dir,
        manifests_dirname=manifests_dirname,
        statements_dirname=statements_dirname,
    )

    statements_root = source_dir / statements_dirname
    if statements_root.is_dir():
        on_disk = {
            p.relative_to(source_dir).as_posix() for p in statements_root.rglob("*") if p.is_file()
        }
        unlisted = sorted(on_disk - set(manifests))
        if unlisted:
            raise StatementIngestConfigError(f"File sao kê không có trong manifest nào: {unlisted}")

    # Nhóm các dòng trong `statement_deliveries.jsonl` theo file_path.
    by_file_delivery: dict[str, tuple[str, int, str]] = {}
    by_order: dict[int, str] = {}
    try:
        with deliveries_path.open("rb") as handle:
            for line_number, raw in enumerate(handle, start=1):
                if not raw.endswith(b"\n"):
                    raise StatementIngestConfigError(
                        f"{deliveries_filename} dòng {line_number} thiếu ký tự LF."
                    )
                res = parse_and_validate_json_schema(raw[:-1], "statement_delivery")
                if not res.is_valid or res.record is None:
                    raise StatementIngestConfigError(
                        f"{deliveries_filename} dòng {line_number} không đúng schema: {res.issues}"
                    )
                rec = res.record
                rec_ds = str(rec["dataset_id"])
                if dataset_id is not None and rec_ds != dataset_id:
                    continue
                rel = str(rec["file_path"])
                order = int(rec["delivery_order"])
                avail = str(rec["file_available_at"])
                _check_safe_relative(source_dir, rel, f"{deliveries_filename}:{line_number}")
                if not rel.startswith(f"{statements_dirname}/"):
                    raise StatementIngestConfigError(
                        f"Đường dẫn sao kê {rel!r} trong {deliveries_filename} không nằm dưới "
                        f"{statements_dirname}/."
                    )
                prev = by_file_delivery.get(rel)
                if prev is not None and prev != (rec_ds, order, avail):
                    raise StatementIngestConfigError(
                        f"Lịch giao mâu thuẫn cho cùng file {rel} tại dòng {line_number}."
                    )
                other_file = by_order.get(order)
                if other_file is not None and other_file != rel:
                    raise StatementIngestConfigError(
                        f"delivery_order={order} bị trùng giữa {other_file} và {rel}."
                    )
                by_file_delivery[rel] = (rec_ds, order, avail)
                by_order[order] = rel
    except OSError as exc:
        raise StatementIngestConfigError(
            f"Không đọc được {deliveries_filename} ({type(exc).__name__})."
        ) from exc

    if not by_file_delivery:
        raise StatementIngestConfigError(
            "Không có lô sao kê nào trong lịch giao cho dataset yêu cầu."
        )

    seen_datasets = {info[0] for info in by_file_delivery.values()}
    if len(seen_datasets) > 1:
        raise StatementIngestConfigError(
            f"Lịch giao chứa nhiều dataset_id ({sorted(seen_datasets)}); hãy chỉ rõ --dataset-id."
        )

    batches: list[StatementDeliveryBatch] = []
    for rel, (rec_ds, order, avail) in by_file_delivery.items():
        if rel not in manifests:
            raise StatementIngestConfigError(
                f"File sao kê {rel} có trong lịch giao nhưng không có manifest tương ứng."
            )
        m_ds, m_rel, m_path, batch_id, sha256, physical_rows = manifests[rel]
        if m_ds != rec_ds:
            raise StatementIngestConfigError(
                f"Manifest {m_rel} có dataset_id={m_ds} khác lịch giao ({rec_ds})."
            )
        file_path = _check_safe_relative(source_dir, rel, m_rel)
        _verify_file_sha_and_rows(file_path, sha256, physical_rows)
        batches.append(
            StatementDeliveryBatch(
                dataset_id=rec_ds,
                delivery_order=order,
                file_available_at=avail,
                file_relpath=rel,
                file_path=file_path,
                manifest_relpath=m_rel,
                manifest_path=m_path,
                source_batch_id=batch_id,
                sha256=sha256,
                physical_rows=physical_rows,
            )
        )

    batches.sort(key=lambda b: b.delivery_order)
    return tuple(batches)


def build_statement_keys(cfg: AppConfig, batch: StatementDeliveryBatch) -> tuple[str, str, str]:
    """Trả `(data_key, manifest_key, receipt_key)` theo ADR-0002 mục 4."""
    prefix = cfg.paths.statements_prefix.strip("/")
    base = f"{prefix}/{batch.dataset_id}"
    data_key = f"{base}/{batch.file_relpath}"
    manifest_key = f"{base}/{batch.manifest_relpath}"
    receipt_key = f"{base}/_ingest/{batch.delivery_order:09d}.json"
    return data_key, manifest_key, receipt_key


def build_accounts_key(cfg: AppConfig, dataset_id: str) -> str:
    """Khóa của `accounts.jsonl` trên MinIO (ADR-0004b Q2)."""
    return f"{cfg.paths.statements_prefix.strip('/')}/{dataset_id}/{DEFAULT_ACCOUNTS_FILENAME}"


def build_receipt_payload(batch: StatementDeliveryBatch) -> bytes:
    """Nội dung JSON chuẩn hóa (kết LF) của biên nhận `_ingest/<delivery_order:09d>.json`."""
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "dataset_id": batch.dataset_id,
        "source_batch_id": batch.source_batch_id,
        "delivery_order": batch.delivery_order,
        "file_available_at": batch.file_available_at,
        "file_path": batch.file_relpath,
        "manifest_path": batch.manifest_relpath,
        "sha256": batch.sha256,
        "physical_rows": batch.physical_rows,
    }
    return (canonical_json(receipt) + "\n").encode("utf-8")


def ingest_statements(
    cfg: AppConfig,
    target: ObjectStoreTarget,
    source_dir: Path,
    *,
    as_of_time: datetime,
    dataset_id: str | None = None,
    now: Callable[[], datetime] | None = None,
) -> StatementIngestResult:
    """Tải các file sao kê có `file_available_at <= as_of_time` lên MinIO theo đúng thứ tự.

    Thứ tự bắt buộc cho mỗi lô (ADR-0002 mục 4):
      1) file dữ liệu (`statements/...`)
      2) file manifest (`manifests/...`)
      3) biên nhận (`_ingest/<delivery_order:09d>.json`)
    Không tải file có `file_available_at > as_of_time`, không tải `statement_deliveries.jsonl`.
    """
    cutoff = as_of_time.astimezone(UTC)
    batches = discover_statement_deliveries(source_dir, dataset_id=dataset_id)
    resolved_ds = batches[0].dataset_id
    bucket = cfg.object_store.bucket

    eligible: list[StatementDeliveryBatch] = []
    deferred = 0
    for batch in batches:
        available = parse_utc_timestamp(batch.file_available_at)
        if available <= cutoff:
            eligible.append(batch)
        else:
            deferred += 1

    uploaded_batches = 0
    skipped_batches = 0
    uploaded_keys: list[str] = []
    clock_kwargs: dict[str, Any] = {"now": now} if now is not None else {}

    for batch in eligible:
        data_key, manifest_key, receipt_key = build_statement_keys(cfg, batch)
        s1 = put_object_immutable_file(
            target,
            bucket,
            data_key,
            batch.file_path,
            content_type="application/x-ndjson",
            expected_sha256=batch.sha256,
            **clock_kwargs,
        )
        s2 = put_object_immutable_file(
            target,
            bucket,
            manifest_key,
            batch.manifest_path,
            content_type="application/json",
            **clock_kwargs,
        )
        receipt_bytes = build_receipt_payload(batch)
        s3 = put_object_immutable_bytes(
            target,
            bucket,
            receipt_key,
            receipt_bytes,
            content_type="application/json",
            **clock_kwargs,
        )
        for status, key in ((s1, data_key), (s2, manifest_key), (s3, receipt_key)):
            if status == "uploaded":
                uploaded_keys.append(key)
        if "uploaded" in (s1, s2, s3):
            uploaded_batches += 1
        else:
            skipped_batches += 1

    accounts_key: str | None = None
    accounts_status: str | None = None
    accounts_path = source_dir / DEFAULT_ACCOUNTS_FILENAME
    if accounts_path.is_file():
        # Dữ liệu tham chiếu, không bị chặn theo đồng hồ; bất biến như file sao kê (ADR-0002 mục 4).
        accounts_key = build_accounts_key(cfg, resolved_ds)
        accounts_status = put_object_immutable_file(
            target,
            bucket,
            accounts_key,
            accounts_path,
            content_type="application/x-ndjson",
            **clock_kwargs,
        )
        if accounts_status == "uploaded":
            uploaded_keys.append(accounts_key)

    return StatementIngestResult(
        dataset_id=resolved_ds,
        as_of_time=cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
        bucket=bucket,
        total_batches=len(batches),
        eligible_batches=len(eligible),
        uploaded_batches=uploaded_batches,
        skipped_batches=skipped_batches,
        deferred_batches=deferred,
        uploaded_keys=tuple(uploaded_keys),
        accounts_key=accounts_key,
        accounts_status=accounts_status,
    )
