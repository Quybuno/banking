"""Điều khiển tốc độ phát theo lịch tuyệt đối (message/giây); `0` là không giới hạn."""

from __future__ import annotations

import time
from collections.abc import Callable


class RateLimiter:
    """Message thứ k (k bắt đầu từ 0) không được gửi trước `start + k / rate`.

    Lịch tuyệt đối nên khi bị trễ sẽ tự bù, tốc độ trung bình bám sát `rate`.
    """

    def __init__(
        self,
        rate: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate < 0:
            raise ValueError(f"rate phải >= 0, nhận {rate}")
        self._rate = rate
        self._clock = clock
        self._sleep = sleep
        self._start: float | None = None
        self._count = 0

    @property
    def rate(self) -> int:
        return self._rate

    def acquire(self) -> None:
        if self._rate == 0:
            return
        now = self._clock()
        if self._start is None:
            self._start = now
        due = self._start + self._count / self._rate
        self._count += 1
        delay = due - now
        if delay > 0:
            self._sleep(delay)
