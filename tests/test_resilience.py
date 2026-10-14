import random
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest

from app.resilience.backoff import full_jitter_delay, parse_retry_after
from app.resilience.circuit_breaker import CircuitBreaker
from app.resilience.rate_limit import RateLimiter, partitioned_rate


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


@pytest.mark.parametrize("attempt,ceiling", [(0, 1), (1, 2), (2, 4), (3, 8), (4, 16), (5, 32), (9, 32)])
def test_full_jitter_is_bounded_by_capped_exponential(attempt, ceiling):
    rng = random.Random(1)
    delays = [full_jitter_delay(attempt, 1.0, 32.0, rng) for _ in range(500)]
    assert min(delays) >= 0 and max(delays) <= ceiling
    assert max(delays) > ceiling * 0.8  # actually spreads across the range


def test_retry_after_delta_seconds_and_http_date():
    assert parse_retry_after("2") == 2.0
    assert parse_retry_after(None) is None
    assert parse_retry_after("soon") is None
    now = datetime(2026, 10, 14, 8, 0, tzinfo=timezone.utc)
    assert parse_retry_after(format_datetime(now + timedelta(seconds=5), usegmt=True), now=now) == 5.0


def test_rate_limiter_spaces_calls_at_one_over_rate():
    clock = FakeClock()
    limiter = RateLimiter(4, clock=clock, sleep=clock.sleep)
    times = []
    for _ in range(9):
        limiter.acquire()
        times.append(clock.now)
    assert times == pytest.approx([i * 0.25 for i in range(9)])


def test_rate_limiter_reduce_is_floored_and_defer_pushes_next_call():
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock, sleep=clock.sleep)
    for _ in range(10):
        limiter.reduce(0.5)
    assert limiter.rate == 5  # floor is half the configured budget
    limiter.acquire()
    limiter.defer(3)
    assert limiter.acquire() == pytest.approx(3)


def test_partitioned_rate():
    assert partitioned_rate(16, 8) == 2
    with pytest.raises(ValueError):
        partitioned_rate(16, 0)


def test_breaker_opens_at_half_failures_over_window_then_half_opens_after_cooldown():
    clock = FakeClock()
    breaker = CircuitBreaker(window=20, failure_ratio=0.5, cooldown_seconds=30, clock=clock)
    for i in range(20):
        breaker.record(i % 2 == 0)  # exactly 50 % failures
    assert breaker.state == CircuitBreaker.OPEN and not breaker.allow()
    clock.now += 30
    assert breaker.allow() and breaker.state == CircuitBreaker.HALF_OPEN
    breaker.record(True)
    assert breaker.state == CircuitBreaker.CLOSED


def test_breaker_stays_closed_below_threshold_and_reopens_on_failed_probe():
    clock = FakeClock()
    breaker = CircuitBreaker(window=20, failure_ratio=0.5, cooldown_seconds=30, clock=clock)
    for i in range(20):
        breaker.record(i % 3 != 0)  # 7 failures of 20
    assert breaker.state == CircuitBreaker.CLOSED
    for _ in range(20):
        breaker.record(False)
    clock.now += 30
    assert breaker.allow()
    breaker.record(False)
    assert breaker.state == CircuitBreaker.OPEN
