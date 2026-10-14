"""Client-side rate limiting with a partitioned static budget (section 6.3).

The vendor limits are global, so a limiter that is correct per worker is wrong
in aggregate. Each worker is given ``budget / N`` where N is the fixed worker
concurrency, which guarantees the aggregate ceiling by arithmetic.
"""
from __future__ import annotations

import threading
import time
from typing import Callable


def partitioned_rate(budget: float, workers: int) -> float:
    if workers < 1:
        raise ValueError("workers must be >= 1")
    return budget / workers


class RateLimiter:
    """Spaces successive acquisitions at least ``1 / rate`` seconds apart.

    ``reduce`` implements "Retry-After is authoritative ... the rate budget is
    reduced for the remainder of the run", bounded below by ``floor_ratio``.
    """

    def __init__(
        self,
        rate_per_second: float,
        *,
        floor_ratio: float = 0.5,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate must be positive")
        self.initial_rate = rate_per_second
        self.rate = rate_per_second
        self._floor = rate_per_second * floor_ratio
        self._clock = clock
        self._sleep = sleep
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    @property
    def interval(self) -> float:
        return 1.0 / self.rate

    def acquire(self) -> float:
        """Block until a call may be issued; returns the time waited."""
        with self._lock:
            now = self._clock()
            wait = max(0.0, self._next_allowed - now)
            self._next_allowed = max(now, self._next_allowed) + self.interval
        if wait:
            self._sleep(wait)
        return wait

    def reduce(self, factor: float = 0.8) -> None:
        with self._lock:
            self.rate = max(self._floor, self.rate * factor)

    def defer(self, seconds: float) -> None:
        """Push the next permitted call out (used when the server says Retry-After)."""
        with self._lock:
            self._next_allowed = max(self._next_allowed, self._clock() + seconds)
