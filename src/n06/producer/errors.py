"""Lỗi của `n06 publish`, ánh xạ sang mã thoát trong `n06.cli.doctor.ExitCode` (ADR-0010)."""

from __future__ import annotations


class PublishError(Exception):
    """Lớp cha của mọi lỗi publish."""


class PublishConfigError(PublishError):
    """Cấu hình hoặc tham số sai (mã thoát 10)."""


class SourceDataError(PublishConfigError):
    """Dữ liệu nguồn không khớp manifest hoặc không nhất quán (mã thoát 10).

    Đây là lỗi của dữ liệu nguồn, không phải lỗi cấu hình; vẫn dùng mã 10 theo quyết định T20.
    """


class OrphanLedgerError(SourceDataError):
    """Ledger event không tìm thấy transaction tương ứng trong shard hiện tại."""


class BrokerUnavailableError(PublishError):
    """Không kết nối được broker (mã thoát 14)."""


class DeliveryError(PublishError):
    """Lỗi giao nhận không retry được hoặc còn message chưa giao (mã thoát 15)."""
