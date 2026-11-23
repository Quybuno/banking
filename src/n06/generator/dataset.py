"""Dựng bộ dữ liệu có lỗi: events, sao kê, manifest, control report, ground truth (TASK T13).

Cây đầu ra:
    <out_root>/source/        pipeline được đọc
    <out_root>/ground_truth/  chỉ test harness được đọc (gồm canonical/ của T12, không sao chép)
"""

from __future__ import annotations

import itertools
import shutil
from collections import defaultdict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

from n06.domain import (
    ContractSchemaRegistry,
    get_default_schema_registry,
    parse_and_validate_json_schema,
    validate_event_envelope,
    validate_generator_config_data,
    validate_statement_record,
)
from n06.generator.canonical import iter_canonical, iter_jsonl_files
from n06.generator.control_report import ControlTotals, write_control_report
from n06.generator.determinism import canonical_json
from n06.generator.events import build_event_envelopes
from n06.generator.external_sort import external_sort_jsonl
from n06.generator.fault import (
    fault_basis_points,
    iter_fault_labels,
    plan_faults,
)
from n06.generator.ground_truth import (
    fault_counts_document,
    stream_sha256_and_rows,
    write_json,
)
from n06.generator.manifest import build_manifest, write_manifest
from n06.generator.params import DatasetParams, FaultConfigError
from n06.generator.scenario import generate_base_scenario
from n06.generator.statements import build_statement_rows, statement_window


class OutputValidationError(RuntimeError):
    """Một file đầu ra không đạt schema hoặc validator của T10."""


@dataclass(frozen=True)
class DatasetSummary:
    source_dir: Path
    ground_truth_dir: Path
    fault_counts: dict[str, int]
    eligible_success_count: int
    event_record_count: int
    statement_record_count: int
    statement_file_count: int
    event_shard_count: int
    business_checksum: str


def _check(
    registry: ContractSchemaRegistry, value: Mapping[str, Any], entity: str, what: str
) -> None:
    result = parse_and_validate_json_schema(value, entity, registry=registry)
    if not result.is_valid:
        raise OutputValidationError(f"{what} không đạt schema {entity}: {result.issues}")


class _ShardedEventWriter:
    """Ghi envelope vào file theo logical shard; đệm tối đa `chunk_rows` dòng rồi nối thêm.

    Tên và nội dung file chỉ phụ thuộc shard nên `generation_chunk_rows` chỉ ảnh hưởng I/O.
    """

    def __init__(self, directory: Path, chunk_rows: int) -> None:
        self._dir = directory
        self._chunk_rows = chunk_rows
        self._buffers: dict[int, list[str]] = defaultdict(list)
        self._buffered = 0
        self.shards: set[int] = set()
        directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def name(shard: int) -> str:
        return f"events_batch_{shard:05d}.jsonl"

    def write(self, shard: int, line: str) -> None:
        self._buffers[shard].append(line)
        self.shards.add(shard)
        self._buffered += 1
        if self._buffered >= self._chunk_rows:
            self.flush()

    def flush(self) -> None:
        for shard, lines in self._buffers.items():
            with (self._dir / self.name(shard)).open("a", encoding="utf-8", newline="\n") as fh:
                fh.write("".join(f"{line}\n" for line in lines))
        self._buffers.clear()
        self._buffered = 0


class _StatementFileSplitter:
    """Chia luồng sao kê đã sắp `(available_at, id)` thành file theo `(cửa sổ, channel)`."""

    def __init__(
        self,
        *,
        params: DatasetParams,
        source_dir: Path,
        truth_rows: IO[str],
        deliveries: IO[str],
        registry: ContractSchemaRegistry,
        validate: bool,
    ) -> None:
        self._p = params
        self._src = source_dir
        self._truth = truth_rows
        self._deliveries = deliveries
        self._registry = registry
        self._validate = validate
        self._window: int | None = None
        self._file_available_at = ""
        self._handles: dict[str, IO[str]] = {}
        self._pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._order = 0
        self.record_count = 0
        self.file_count = 0

    @staticmethod
    def relative_path(channel: str, window: int) -> str:
        return f"statements/{channel}/statements_w{window:09d}.jsonl"

    def add(self, item: Mapping[str, Any]) -> None:
        row = item["row"]
        meta = item["meta"]
        window, file_available_at = statement_window(
            row["available_at"], self._p.statement_file_window_seconds
        )
        if window != self._window:
            self._close_window()
            self._window = window
            self._file_available_at = file_available_at
        channel = row["channel"]
        if channel not in self._handles:
            path = self._src / self.relative_path(channel, window)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._handles[channel] = path.open("w", encoding="utf-8", newline="\n")
        if self._validate:
            res = validate_statement_record(row, registry=self._registry)
            if not res.is_valid:
                raise OutputValidationError(f"Dòng sao kê không hợp lệ: {res.issues}")
        self._handles[channel].write(canonical_json(row) + "\n")
        self._pending[channel].append({"row": row, "meta": meta})
        self.record_count += 1

    def _close_window(self) -> None:
        if self._window is None:
            return
        window = self._window
        for channel in sorted(self._handles):
            self._handles[channel].close()
            rel = self.relative_path(channel, window)
            manifest = build_manifest(
                contract_version=self._p.contract_version,
                dataset_id=self._p.dataset_id,
                source_batch_id=self._p.statement_source_batch_id(window, channel),
                root=self._src,
                relative_paths=[rel],
            )
            if self._validate:
                _check(self._registry, manifest, "source_manifest", f"manifest {rel}")
            write_manifest(
                self._src / "manifests" / f"statements_{channel}_w{window:09d}.manifest.json",
                manifest,
            )
            for entry in self._pending[channel]:
                delivery = {
                    "schema_version": self._p.contract_version,
                    "dataset_id": self._p.dataset_id,
                    "statement_record_id": entry["row"]["statement_record_id"],
                    "file_path": rel,
                    "file_available_at": self._file_available_at,
                    "delivery_order": self._order,
                }
                if self._validate:
                    _check(self._registry, delivery, "statement_delivery", "lịch giao file")
                self._deliveries.write(canonical_json(delivery) + "\n")
                meta = entry["meta"]
                self._truth.write(
                    canonical_json(
                        {
                            "statement_record_id": entry["row"]["statement_record_id"],
                            "ordinal": meta["ordinal"],
                            "transaction_id": meta["transaction_id"],
                            "kind": meta["kind"],
                            "fault_type": meta["fault_type"],
                            "available_at": entry["row"]["available_at"],
                            "file_available_at": self._file_available_at,
                            "file_path": rel,
                        }
                    )
                    + "\n"
                )
            self._order += 1
            self.file_count += 1
        self._handles.clear()
        self._pending.clear()

    def close(self) -> None:
        self._close_window()


def build_dataset(
    config: Mapping[str, Any],
    out_root: Path,
    *,
    validate_outputs: bool = True,
    registry: ContractSchemaRegistry | None = None,
) -> DatasetSummary:
    """Sinh kịch bản gốc (T12), tiêm lỗi, ghi events/sao kê/manifest/control report/ground truth."""
    reg = registry or get_default_schema_registry()
    cfg_res = validate_generator_config_data(config, registry=reg)
    if not cfg_res.is_valid:
        detail = "; ".join(f"{i.code.value}@{i.field_path}: {i.message}" for i in cfg_res.issues)
        raise FaultConfigError(f"Cấu hình generator vi phạm ràng buộc: {detail}")
    params = DatasetParams.from_config(config)
    basis_points = fault_basis_points(config)

    source_dir = out_root / "source"
    truth_dir = out_root / "ground_truth"
    for directory in (source_dir, truth_dir):
        if directory.exists() and any(directory.iterdir()):
            raise FaultConfigError(f"Thư mục đầu ra phải trống: {directory}")
    source_dir.mkdir(parents=True, exist_ok=True)
    truth_dir.mkdir(parents=True, exist_ok=True)

    scenario = generate_base_scenario(
        config, truth_dir / "canonical", validate_records=validate_outputs, registry=reg
    )

    def canonical() -> Iterator[tuple[int, dict[str, Any], list[dict[str, Any]]]]:
        return iter_canonical(
            config,
            scenario.transaction_files,
            scenario.ledger_entry_files,
            chunk_rows=params.chunk_rows,
        )

    # Lượt 1: S = số canonical SUCCESS sau kiểm tra đủ tiền.
    eligible_count = sum(
        1 for tx in iter_jsonl_files(scenario.transaction_files) if tx["status"] == "SUCCESS"
    )
    plan = plan_faults(eligible_count, config)

    labels = iter_fault_labels(
        lambda: (
            {"ordinal": o, "transaction_id": tx["transaction_id"], "event_time": tx["event_time"]}
            for o, tx, _ in canonical()
            if tx["status"] == "SUCCESS"
        ),
        contract_version=params.contract_version,
        seed=params.seed,
        plan=plan,
        chunk_rows=params.chunk_rows,
    )

    accounts = {a["account_id"]: a for a in iter_jsonl_files([scenario.accounts_file])}
    writer = _ShardedEventWriter(source_dir / "events", params.chunk_rows)
    totals = ControlTotals()
    counters = {"events": 0}

    with (truth_dir / "fault_assignment.jsonl").open(
        "w", encoding="utf-8", newline="\n"
    ) as gt_faults:

        def main_pass() -> Iterator[dict[str, Any]]:
            pending = next(labels, None)
            for ordinal, tx, entries in canonical():
                fault = None
                if pending is not None and pending["ordinal"] == ordinal:
                    fault = pending["fault_type"]
                    gt_faults.write(
                        canonical_json(
                            {
                                "ordinal": ordinal,
                                "transaction_id": tx["transaction_id"],
                                "external_reference": tx["external_reference"],
                                "channel": tx["channel"],
                                "business_date": tx["business_date"],
                                "fault_type": fault,
                            }
                        )
                        + "\n"
                    )
                    pending = next(labels, None)
                shard = params.event_shard(ordinal)
                envelopes = build_event_envelopes(tx, entries, ordinal, fault, params)
                for env in envelopes:
                    if validate_outputs:
                        res = validate_event_envelope(env, accounts=accounts)
                        if not res.is_valid:
                            raise OutputValidationError(f"Envelope không hợp lệ: {res.issues}")
                    writer.write(shard, canonical_json(env))
                    counters["events"] += 1
                if fault != "missing_internal":
                    totals.add("transaction", tx)
                    for entry in entries:
                        totals.add("ledger_entry", entry)
                for row, kind in build_statement_rows(tx, ordinal, fault, params):
                    yield {
                        "row": row,
                        "meta": {
                            "ordinal": ordinal,
                            "transaction_id": tx["transaction_id"],
                            "kind": kind,
                            "fault_type": fault,
                        },
                    }
            if pending is not None:
                raise RuntimeError("Còn nhãn lỗi chưa gắn vào giao dịch nào.")

        sorted_rows = external_sort_jsonl(
            main_pass(),
            key_fn=lambda item: (item["row"]["available_at"], item["row"]["statement_record_id"]),
            chunk_rows=params.chunk_rows,
        )
        # Lấy phần tử đầu để buộc `main_pass` chạy hết trước khi hoàn tất events/control report.
        first = next(sorted_rows, None)
        writer.flush()

        control = totals.to_report(params.contract_version, params.dataset_id)
        if validate_outputs:
            _check(reg, control, "control_report", "control_report.json")
        write_control_report(source_dir / "control_report.json", control)
        shutil.copyfile(scenario.accounts_file, source_dir / "accounts.jsonl")

        for shard in sorted(writer.shards | {0}):
            rels = []
            if shard in writer.shards:
                rels.append(f"events/{writer.name(shard)}")
            if shard == 0:
                rels += ["accounts.jsonl", "control_report.json"]
            manifest = build_manifest(
                contract_version=params.contract_version,
                dataset_id=params.dataset_id,
                source_batch_id=params.events_source_batch_id(shard),
                root=source_dir,
                relative_paths=rels,
            )
            if validate_outputs:
                _check(reg, manifest, "source_manifest", f"manifest events shard {shard}")
            write_manifest(
                source_dir / "manifests" / f"events_batch_{shard:05d}.manifest.json", manifest
            )

        with (
            (truth_dir / "statement_rows.jsonl").open(
                "w", encoding="utf-8", newline="\n"
            ) as gt_rows,
            (source_dir / "statement_deliveries.jsonl").open(
                "w", encoding="utf-8", newline="\n"
            ) as dlv,
        ):
            splitter = _StatementFileSplitter(
                params=params,
                source_dir=source_dir,
                truth_rows=gt_rows,
                deliveries=dlv,
                registry=reg,
                validate=validate_outputs,
            )
            for item in itertools.chain([] if first is None else [first], sorted_rows):
                splitter.add(item)
            splitter.close()

    write_json(
        truth_dir / "fault_counts.json",
        fault_counts_document(
            contract_version=params.contract_version,
            dataset_id=params.dataset_id,
            seed=params.seed,
            transaction_count=int(config["transaction_count"]),
            plan=plan,
            basis_points=basis_points,
        ),
    )
    write_json(
        truth_dir / "canonical_index.json",
        {
            "business_checksum": scenario.business_checksum,
            "accounts": stream_sha256_and_rows([scenario.accounts_file]),
            "transactions": stream_sha256_and_rows(scenario.transaction_files),
            "ledger_entries": stream_sha256_and_rows(scenario.ledger_entry_files),
        },
    )
    return DatasetSummary(
        source_dir=source_dir,
        ground_truth_dir=truth_dir,
        fault_counts=dict(plan.counts),
        eligible_success_count=eligible_count,
        event_record_count=counters["events"],
        statement_record_count=splitter.record_count,
        statement_file_count=splitter.file_count,
        event_shard_count=len(writer.shards),
        business_checksum=scenario.business_checksum,
    )
