"""Bảng mã thoát của CLI không được trùng nhau (TASK T20 thêm KAFKA_DELIVERY_ERROR)."""

from __future__ import annotations

from n06.cli import ExitCode


def test_all_exit_code_values_are_distinct() -> None:
    values = [int(member) for member in ExitCode]
    assert len(values) == len(set(values))


def test_kafka_delivery_error_is_15_and_not_reusing_service_error() -> None:
    assert int(ExitCode.KAFKA_DELIVERY_ERROR) == 15
    assert ExitCode.KAFKA_DELIVERY_ERROR != ExitCode.SERVICE_ERROR
    assert int(ExitCode.SERVICE_ERROR) == 14


def test_help_epilog_lists_every_exit_code() -> None:
    import io

    from n06.cli import main

    buffer = io.StringIO()
    assert main(["--help"], out=buffer) == 0
    text = buffer.getvalue()
    for member in ExitCode:
        if member is not ExitCode.OK:
            assert f"{int(member)} " in text and member.name in text
    assert "publish" in text
