"""Điều khiển tốc độ phát bằng đồng hồ giả."""

from __future__ import annotations

import pytest

from n06.producer.rate import RateLimiter


class FakeTime:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_zero_rate_never_sleeps() -> None:
    fake = FakeTime()
    limiter = RateLimiter(0, clock=fake.clock, sleep=fake.sleep)
    for _ in range(1000):
        limiter.acquire()
    assert fake.sleeps == []


def test_rate_spaces_messages_evenly() -> None:
    fake = FakeTime()
    limiter = RateLimiter(4, clock=fake.clock, sleep=fake.sleep)
    for _ in range(9):
        limiter.acquire()
    # Message thứ k gửi lúc start + k/4: 9 message trải trên 8/4 = 2 giây.
    assert fake.now - 1000.0 == pytest.approx(2.0)
    assert fake.sleeps == pytest.approx([0.25] * 8)


def test_limiter_catches_up_after_slow_send() -> None:
    fake = FakeTime()
    limiter = RateLimiter(10, clock=fake.clock, sleep=fake.sleep)
    limiter.acquire()  # k=0 gửi ngay
    fake.now += 1.0  # gửi chậm 1 giây: các message k=1..10 đều đã đến hạn
    for _ in range(10):
        limiter.acquire()
    assert fake.sleeps == []
    limiter.acquire()  # k=11 đến hạn lúc start + 1.1 giây
    assert fake.sleeps == pytest.approx([0.1])


def test_negative_rate_is_rejected() -> None:
    with pytest.raises(ValueError):
        RateLimiter(-1)
