"""Tham số định danh và thời gian dùng chung cho phần tiêm lỗi của T13 (Phần II mục 5.2, 5.3)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from n06.generator.determinism import deterministic_uuid

LOGICAL_SHARD_ROWS = 10_000  # quy ước identity v1 (Phần II mục 5.2 điểm 6)


class FaultConfigError(ValueError):
    """Cấu hình generator vi phạm ràng buộc của hợp đồng hoặc của phần tiêm lỗi."""


@dataclass(frozen=True)
class DatasetParams:
    contract_version: str
    dataset_id: str
    seed: int
    seed_namespace: str
    business_timezone: str
    normal_statement_delay_seconds: int
    statement_file_window_seconds: int
    late_within_delay_seconds: int
    late_after_delay_seconds: int
    allowed_lateness_seconds: int
    chunk_rows: int

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> DatasetParams:
        return cls(
            contract_version=str(config["contract_version"]),
            dataset_id=str(config["dataset_id"]),
            seed=int(config["seed"]),
            seed_namespace=str(config["seed_namespace"]),
            business_timezone=str(config["business_timezone"]),
            normal_statement_delay_seconds=int(config["normal_statement_delay_seconds"]),
            statement_file_window_seconds=int(config["statement_file_window_seconds"]),
            late_within_delay_seconds=int(config["late_within_delay_after_day_end_seconds"]),
            late_after_delay_seconds=int(config["late_after_delay_after_day_end_seconds"]),
            allowed_lateness_seconds=int(config["allowed_lateness_seconds"]),
            chunk_rows=int(config["generation_chunk_rows"]),
        )

    def uuid(self, entity: str, ordinal: int, discriminator: str) -> str:
        return deterministic_uuid(
            seed_namespace=self.seed_namespace,
            contract_version=self.contract_version,
            dataset_id=self.dataset_id,
            seed=self.seed,
            entity=entity,
            ordinal=ordinal,
            discriminator=discriminator,
        )

    def event_shard(self, tx_ordinal: int) -> int:
        return tx_ordinal // LOGICAL_SHARD_ROWS

    def events_source_batch_id(self, shard: int) -> str:
        return self.uuid("source_batch", shard, "original")

    def statement_source_batch_id(self, window: int, channel: str) -> str:
        return self.uuid("source_batch", window, f"statement:{channel}")
