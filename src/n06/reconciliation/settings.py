"""Tham số đối soát lấy từ cấu hình (TASK T33; ADR-0006 D4, Q10).

Mọi giá trị triển khai (múi giờ, thời gian chờ, tập status/channel) đến từ `AppConfig`, không
hardcode trong mã đối soát.
"""

from __future__ import annotations

from dataclasses import dataclass
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from n06.config import AppConfig
from n06.reconciliation.rules import RECON_RULE_VERSION


class ReconciliationError(ValueError):
    """Đầu vào hoặc tham số đối soát không hợp lệ (không phải lỗi dữ liệu nghiệp vụ)."""


@dataclass(frozen=True)
class ReconciliationSettings:
    """Tham số một lần đối soát.

    `allowed_lateness_seconds`: cộng vào đầu ngày kế tiếp để ra cutoff (S358, S686).
    `business_timezone`: múi giờ IANA tính "đầu ngày kế tiếp" (Asia/Ho_Chi_Minh).
    `statuses`, `channels`: tập được đối soát (S354).
    """

    allowed_lateness_seconds: int
    business_timezone: str
    statuses: tuple[str, ...]
    channels: tuple[str, ...]
    rule_version: str = RECON_RULE_VERSION

    def __post_init__(self) -> None:
        if type(self.allowed_lateness_seconds) is not int or self.allowed_lateness_seconds < 0:
            raise ReconciliationError("allowed_lateness_seconds phải là số nguyên >= 0.")
        try:
            ZoneInfo(self.business_timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ReconciliationError(
                f"business_timezone không phải múi giờ IANA: {self.business_timezone!r}."
            ) from exc
        if not self.statuses or not self.channels:
            raise ReconciliationError("statuses và channels không được rỗng.")
        if not self.rule_version.strip():
            raise ReconciliationError("rule_version không được rỗng.")

    @classmethod
    def from_config(cls, cfg: AppConfig) -> ReconciliationSettings:
        return cls(
            allowed_lateness_seconds=cfg.dataset.allowed_lateness_seconds,
            business_timezone=cfg.business_timezone,
            statuses=cfg.reconciliation.statuses,
            channels=cfg.reconciliation.channels,
        )
