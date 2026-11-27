"""Tham số định danh và thời gian dùng chung cho phần tiêm lỗi của T13 (Phần II mục 5.2, 5.3)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from n06.generator.determinism import deterministic_uuid

LOGICAL_SHARD_ROWS = 10_000  # quy ước identity v1 (Phần II mục 5.2 điểm 6)


class FaultConfigError(ValueError):
    """Cấu hình generator vi phạm ràng buộc của hợp đồng hoặc của phần tiêm lỗi."""

    def __init__(self, message: str, *, public_message: str | None = None) -> None:
        super().__init__(message)
        # Thông báo để in ra output. Nơi nào chép giá trị bị từ chối vào `message` thì phải đưa
        # `public_message` không kèm giá trị.
        self.public_message = public_message or message


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


# Giao dịch muộn nhất của ngày nghiệp vụ (địa phương 23:59:59) cách mốc D đúng 1 giây.
_LAST_EVENT_SECOND_BEFORE_D = 1


def check_statement_delays(params: DatasetParams) -> None:
    """Phần II mục 5.1: "Normal delay phải giữ statement trước cutoff." (ADR-0011).

    Cutoff = D + allowed_lateness (mục 1.10); đúng cutoff đã quá hạn. Một dòng sao kê chỉ được coi
    là đã có khi `file_available_at <= as_of_time` (mục 5.3), và `file_available_at` lớn hơn
    `available_at` tối đa một cửa sổ W. Giao dịch muộn nhất của ngày có event_time = D - 1 giây
    nên statement bình thường của nó có `file_available_at <= D - 1 + normal_delay + W`, phải
    < D + allowed_lateness, tức `normal_delay + W <= allowed_lateness`.
    """
    if params.normal_statement_delay_seconds < 0:
        raise FaultConfigError(
            "normal_statement_delay_seconds không được âm: "
            f"{params.normal_statement_delay_seconds}.",
            public_message="normal_statement_delay_seconds không được âm.",
        )
    window = params.statement_file_window_seconds
    worst_file_after_d = (
        params.normal_statement_delay_seconds - _LAST_EVENT_SECOND_BEFORE_D + window
    )
    if worst_file_after_d >= params.allowed_lateness_seconds:
        raise FaultConfigError(
            "normal_statement_delay_seconds="
            f"{params.normal_statement_delay_seconds} cùng statement_file_window_seconds="
            f"{window} đưa file sao kê của giao dịch cuối ngày tới D + {worst_file_after_d} giây, "
            f"không trước cutoff = D + allowed_lateness_seconds "
            f"({params.allowed_lateness_seconds}); đúng cutoff đã quá hạn. "
            "Yêu cầu normal_statement_delay_seconds + statement_file_window_seconds <= "
            f"{params.allowed_lateness_seconds}.",
            public_message=(
                "normal_statement_delay_seconds + statement_file_window_seconds phải <= "
                "allowed_lateness_seconds; đúng cutoff đã quá hạn."
            ),
        )
