"""Bộ sinh kịch bản gốc xác định cho N06 (Phần II mục 5.1, 5.2 — TASK T12)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from n06.domain import (
    MAX_OPENING_BALANCE_MINOR,
    ContractSchemaRegistry,
    get_default_schema_registry,
    validate_account_record,
    validate_generator_config_data,
    validate_ledger_entry_record,
    validate_transaction_ledger_group,
    validate_transaction_record,
)
from n06.generator.determinism import (
    canonical_json,
    deterministic_external_reference,
    deterministic_uuid,
    draw,
)
from n06.generator.external_sort import event_time_ordinal_key, external_sort_jsonl


class ScenarioConfigError(ValueError):
    """Cấu hình generator không hợp lệ theo hợp đồng v1."""

    def __init__(self, message: str, *, public_message: str | None = None) -> None:
        super().__init__(message)
        # Thông báo để in ra output. Nơi nào chép giá trị bị từ chối vào `message` thì phải đưa
        # `public_message` không kèm giá trị.
        self.public_message = public_message or message


class ScenarioValidationError(RuntimeError):
    """Bản ghi sinh ra không đạt kiểm tra của validator T10."""


@dataclass(frozen=True)
class ScenarioSummary:
    """Tóm tắt kết quả sinh kịch bản gốc (không chứa ground truth hay fault)."""

    dataset_id: str
    seed: int
    contract_version: str
    account_count: int
    transaction_count: int
    success_count: int
    failed_count: int
    insufficient_funds_failed_count: int
    ledger_entry_count: int
    accounts_file: Path
    transaction_files: list[Path]
    ledger_entry_files: list[Path]
    business_checksum: str


def select_account_pair(
    *,
    contract_version: str,
    seed: int,
    ordinal: int,
    account_count: int,
    account_selection: str,
    hot_account_count: int,
    hot_account_basis_points: int,
) -> tuple[int, int]:
    """Chọn cặp chỉ số tài khoản `(from_idx, to_idx)` khác nhau theo mục 5.1 và 5.2."""
    if account_count < 2:
        raise ScenarioConfigError(
            f"account_count phải >= 2, nhận {account_count}",
            public_message="account_count phải >= 2.",
        )

    if account_selection == "UNIFORM_DISTINCT_PAIR":
        from_idx = draw(contract_version, seed, "account-from", ordinal, 0, account_count - 1)
    elif account_selection == "HOT_ACCOUNT_V1":
        if hot_account_count < 1 or hot_account_count >= account_count:
            raise ScenarioConfigError(
                f"HOT_ACCOUNT_V1 yêu cầu 1 <= hot_account_count < {account_count}, "
                f"nhận {hot_account_count}",
                public_message="HOT_ACCOUNT_V1 yêu cầu 1 <= hot_account_count < account_count.",
            )
        hot_roll = draw(contract_version, seed, "hot-from", ordinal, 0, 9999)
        if hot_roll < hot_account_basis_points:
            from_idx = draw(
                contract_version, seed, "account-from-hot", ordinal, 0, hot_account_count - 1
            )
        else:
            from_idx = draw(contract_version, seed, "account-from", ordinal, 0, account_count - 1)
    else:
        raise ScenarioConfigError(
            f"account_selection không được hỗ trợ: {account_selection!r}",
            public_message="account_selection không được hỗ trợ.",
        )

    to_slot = draw(contract_version, seed, "account-to-slot", ordinal, 0, account_count - 2)
    to_idx = to_slot if to_slot < from_idx else to_slot + 1
    return from_idx, to_idx


def select_channel(
    *,
    contract_version: str,
    seed: int,
    ordinal: int,
    channel_weights: Mapping[str, int],
) -> str:
    """Chọn kênh theo cumulative weights với thứ tự tên kênh tăng dần (mục 5.2)."""
    roll = draw(contract_version, seed, "channel", ordinal, 0, 9999)
    cumulative = 0
    for channel_name, weight in sorted(channel_weights.items()):
        cumulative += int(weight)
        if roll < cumulative:
            return channel_name
    raise ScenarioConfigError("Tổng channel_weights_basis_points phải bằng 10000.")


def compute_event_timestamp(
    *,
    contract_version: str,
    seed: int,
    ordinal: int,
    from_date: date,
    num_days: int,
    business_timezone: str,
) -> tuple[str, str]:
    """Tính `(business_date, event_time_utc_z)` xác định theo mục 5.1."""
    biz_date = from_date + timedelta(days=ordinal % num_days)
    day_second = draw(contract_version, seed, "day-second", ordinal, 0, 86399)
    local_dt = datetime(
        biz_date.year,
        biz_date.month,
        biz_date.day,
        tzinfo=ZoneInfo(business_timezone),
    ) + timedelta(seconds=day_second)
    utc_dt = local_dt.astimezone(UTC)
    return biz_date.isoformat(), utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _iter_candidate_transactions(config: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
    contract_version = str(config["contract_version"])
    seed = int(config["seed"])
    dataset_id = str(config["dataset_id"])
    seed_namespace = str(config["seed_namespace"])
    business_timezone = str(config["business_timezone"])
    from_date = date.fromisoformat(str(config["from_date"]))
    to_date = date.fromisoformat(str(config["to_date"]))
    num_days = (to_date - from_date).days + 1

    tx_count = int(config["transaction_count"])
    account_count = int(config["account_count"])
    currency = str(config["currency"])
    amount_min = int(config["amount_min_minor"])
    amount_max = int(config["amount_max_minor"])
    success_bp = int(config["success_basis_points"])
    channel_weights = dict(config["channel_weights_basis_points"])
    account_selection = str(config["account_selection"])
    hot_account_count = int(config["hot_account_count"])
    hot_account_basis_points = int(config["hot_account_basis_points"])

    for ordinal in range(tx_count):
        from_idx, to_idx = select_account_pair(
            contract_version=contract_version,
            seed=seed,
            ordinal=ordinal,
            account_count=account_count,
            account_selection=account_selection,
            hot_account_count=hot_account_count,
            hot_account_basis_points=hot_account_basis_points,
        )
        amount_minor = draw(contract_version, seed, "amount", ordinal, amount_min, amount_max)
        channel = select_channel(
            contract_version=contract_version,
            seed=seed,
            ordinal=ordinal,
            channel_weights=channel_weights,
        )
        biz_date_iso, event_time_z = compute_event_timestamp(
            contract_version=contract_version,
            seed=seed,
            ordinal=ordinal,
            from_date=from_date,
            num_days=num_days,
            business_timezone=business_timezone,
        )
        status_roll = draw(contract_version, seed, "status", ordinal, 0, 9999)
        candidate_status = "SUCCESS" if status_roll < success_bp else "FAILED"
        tx_id = deterministic_uuid(
            seed_namespace=seed_namespace,
            contract_version=contract_version,
            dataset_id=dataset_id,
            seed=seed,
            entity="transaction",
            ordinal=ordinal,
            discriminator="original",
        )
        ext_ref = deterministic_external_reference(
            seed_namespace=seed_namespace,
            contract_version=contract_version,
            dataset_id=dataset_id,
            seed=seed,
            transaction_ordinal=ordinal,
            discriminator="original",
        )
        yield {
            "ordinal": ordinal,
            "transaction_id": tx_id,
            "external_reference": ext_ref,
            "from_idx": from_idx,
            "to_idx": to_idx,
            "amount_minor": amount_minor,
            "currency": currency,
            "candidate_status": candidate_status,
            "channel": channel,
            "event_time": event_time_z,
            "business_date": biz_date_iso,
        }


class _ChunkedJsonlWriter:
    """Ghi bản ghi JSONL thành các part file theo `generation_chunk_rows`."""

    def __init__(self, output_dir: Path, prefix: str, chunk_rows: int) -> None:
        self._output_dir = output_dir
        self._prefix = prefix
        self._chunk_rows = chunk_rows
        self._part_index = 0
        self._current_rows = 0
        self._fh: Any = None
        self.files: list[Path] = []

    def write(self, record: Mapping[str, Any]) -> None:
        if self._fh is None or self._current_rows >= self._chunk_rows:
            self._rotate()
        self._fh.write(canonical_json(record))
        self._fh.write("\n")
        self._current_rows += 1

    def _rotate(self) -> None:
        if self._fh is not None:
            self._fh.close()
        path = self._output_dir / f"{self._prefix}_part_{self._part_index:05d}.jsonl"
        self._part_index += 1
        self._current_rows = 0
        self.files.append(path)
        self._fh = path.open("w", encoding="utf-8", newline="\n")

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def generate_base_scenario(
    config: Mapping[str, Any],
    output_dir: Path,
    *,
    validate_records: bool = True,
    registry: ContractSchemaRegistry | None = None,
) -> ScenarioSummary:
    """Sinh kịch bản gốc xác định gồm accounts, transactions và ledger entries theo chunk."""
    schema_registry = registry or get_default_schema_registry()
    cfg_res = validate_generator_config_data(config, registry=schema_registry)
    if not cfg_res.is_valid:
        messages = "; ".join(f"{i.code.value}: {i.message}" for i in cfg_res.issues)
        where = "; ".join(f"{i.code.value}@{i.field_path}" for i in cfg_res.issues)
        raise ScenarioConfigError(
            f"Cấu hình generator không hợp lệ: {messages}",
            public_message=f"Cấu hình generator không hợp lệ: {where}",
        )

    contract_version = str(config["contract_version"])
    seed = int(config["seed"])
    dataset_id = str(config["dataset_id"])
    seed_namespace = str(config["seed_namespace"])
    business_timezone = str(config["business_timezone"])
    from_date = str(config["from_date"])
    tx_count = int(config["transaction_count"])
    account_count = int(config["account_count"])
    currency = str(config["currency"])
    opening_balance = int(config["opening_balance_minor_per_account"])
    amount_max = int(config["amount_max_minor"])
    chunk_rows = int(config["generation_chunk_rows"])

    if opening_balance + tx_count * amount_max > MAX_OPENING_BALANCE_MINOR:
        raise ScenarioConfigError(
            "Nguy cơ vượt trần số dư hợp đồng (MAX_OPENING_BALANCE_MINOR) khi cộng dồn tối đa."
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    accounts_file = output_dir / "accounts.jsonl"
    checksum_hasher = hashlib.sha256()

    account_ids: list[str] = []
    balances: list[int] = [opening_balance] * account_count
    accounts_by_id: dict[str, dict[str, Any]] = {}

    with accounts_file.open("w", encoding="utf-8", newline="\n") as acc_fh:
        for acc_ord in range(account_count):
            acc_id = deterministic_uuid(
                seed_namespace=seed_namespace,
                contract_version=contract_version,
                dataset_id=dataset_id,
                seed=seed,
                entity="account",
                ordinal=acc_ord,
                discriminator="original",
            )
            acc_record = {
                "schema_version": contract_version,
                "dataset_id": dataset_id,
                "account_id": acc_id,
                "currency": currency,
                "opening_balance_minor": opening_balance,
                "opening_business_date": from_date,
                "account_type": "CUSTOMER_DEPOSIT",
            }
            if validate_records:
                acc_res = validate_account_record(acc_record, registry=schema_registry)
                if not acc_res.is_valid:
                    raise ScenarioValidationError(
                        f"Account ordinal {acc_ord} không hợp lệ: {acc_res.issues}"
                    )
            account_ids.append(acc_id)
            accounts_by_id[acc_id] = acc_record
            line = canonical_json(acc_record)
            acc_fh.write(line)
            acc_fh.write("\n")
            checksum_hasher.update(line.encode("utf-8"))
            checksum_hasher.update(b"\n")

    tx_writer = _ChunkedJsonlWriter(output_dir, "transactions", chunk_rows)
    ledger_writer = _ChunkedJsonlWriter(output_dir, "ledger_entries", chunk_rows)

    success_count = 0
    failed_count = 0
    insufficient_funds_failed_count = 0
    ledger_entry_count = 0

    try:
        sorted_candidates = external_sort_jsonl(
            _iter_candidate_transactions(config),
            key_fn=event_time_ordinal_key,
            chunk_rows=chunk_rows,
        )
        for cand in sorted_candidates:
            ordinal = int(cand["ordinal"])
            from_idx = int(cand["from_idx"])
            to_idx = int(cand["to_idx"])
            amount_minor = int(cand["amount_minor"])
            candidate_status = str(cand["candidate_status"])

            if candidate_status == "SUCCESS" and balances[from_idx] >= amount_minor:
                balances[from_idx] -= amount_minor
                balances[to_idx] += amount_minor
                final_status = "SUCCESS"
                success_count += 1
            else:
                if candidate_status == "SUCCESS":
                    insufficient_funds_failed_count += 1
                final_status = "FAILED"
                failed_count += 1

            tx_record = {
                "schema_version": contract_version,
                "dataset_id": dataset_id,
                "transaction_id": str(cand["transaction_id"]),
                "external_reference": str(cand["external_reference"]),
                "account_from": account_ids[from_idx],
                "account_to": account_ids[to_idx],
                "amount_minor": amount_minor,
                "currency": currency,
                "status": final_status,
                "transaction_type": "INTERNAL_TRANSFER",
                "channel": str(cand["channel"]),
                "event_time": str(cand["event_time"]),
                "business_date": str(cand["business_date"]),
            }

            ledger_entries: list[dict[str, Any]] = []
            if final_status == "SUCCESS":
                for side, acc_idx in (("DEBIT", from_idx), ("CREDIT", to_idx)):
                    entry_id = deterministic_uuid(
                        seed_namespace=seed_namespace,
                        contract_version=contract_version,
                        dataset_id=dataset_id,
                        seed=seed,
                        entity="ledger_entry",
                        ordinal=ordinal,
                        discriminator=side,
                    )
                    ledger_entries.append(
                        {
                            "schema_version": contract_version,
                            "dataset_id": dataset_id,
                            "entry_id": entry_id,
                            "transaction_id": str(cand["transaction_id"]),
                            "ledger_account_id": account_ids[acc_idx],
                            "side": side,
                            "amount_minor": amount_minor,
                            "currency": currency,
                            "event_time": str(cand["event_time"]),
                            "business_date": str(cand["business_date"]),
                        }
                    )

            if validate_records:
                tx_res = validate_transaction_record(
                    tx_record,
                    accounts=accounts_by_id,
                    business_timezone=business_timezone,
                    registry=schema_registry,
                )
                if not tx_res.is_valid:
                    raise ScenarioValidationError(
                        f"Transaction ordinal {ordinal} không hợp lệ: {tx_res.issues}"
                    )
                for entry in ledger_entries:
                    le_res = validate_ledger_entry_record(
                        entry,
                        accounts=accounts_by_id,
                        business_timezone=business_timezone,
                        registry=schema_registry,
                    )
                    if not le_res.is_valid:
                        raise ScenarioValidationError(
                            f"Ledger entry ordinal {ordinal} không hợp lệ: {le_res.issues}"
                        )
                grp_res = validate_transaction_ledger_group(tx_record, ledger_entries)
                if not grp_res.is_valid:
                    raise ScenarioValidationError(
                        f"Nhóm transaction/ledger ordinal {ordinal} không hợp lệ: {grp_res.issues}"
                    )

            tx_line = canonical_json(tx_record)
            tx_writer.write(tx_record)
            checksum_hasher.update(tx_line.encode("utf-8"))
            checksum_hasher.update(b"\n")

            for entry in ledger_entries:
                le_line = canonical_json(entry)
                ledger_writer.write(entry)
                checksum_hasher.update(le_line.encode("utf-8"))
                checksum_hasher.update(b"\n")
                ledger_entry_count += 1
    finally:
        tx_writer.close()
        ledger_writer.close()

    return ScenarioSummary(
        dataset_id=dataset_id,
        seed=seed,
        contract_version=contract_version,
        account_count=account_count,
        transaction_count=tx_count,
        success_count=success_count,
        failed_count=failed_count,
        insufficient_funds_failed_count=insufficient_funds_failed_count,
        ledger_entry_count=ledger_entry_count,
        accounts_file=accounts_file,
        transaction_files=tx_writer.files,
        ledger_entry_files=ledger_writer.files,
        business_checksum=checksum_hasher.hexdigest(),
    )
