"""Circuit breaker: opens at a 50 % failure rate over a 20-call window (section 7.1)."""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Callable


class CircuitBreaker:
    CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"

    def __init__(
        self,
        *,
        window: int = 20,
        failure_ratio: float = 0.5,
        cooldown_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.window = window
        self.failure_ratio = failure_ratio
        self.cooldown_seconds = cooldown_seconds
        self._clock = clock
        self._results: deque[bool] = deque(maxlen=window)
        self._state = self.CLOSED
        self._opened_at = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def _maybe_half_open(self) -> None:
        if self._state == self.OPEN and self._clock() - self._opened_at >= self.cooldown_seconds:
            self._state = self.HALF_OPEN

    def allow(self) -> bool:
        with self._lock:
            self._maybe_half_open()
            return self._state != self.OPEN

    def record(self, success: bool) -> None:
        with self._lock:
            if self._state == self.HALF_OPEN:
                # One probe decides: close and start afresh, or re-open.
                self._results.clear()
                if success:
                    self._state = self.CLOSED
                else:
                    self._state, self._opened_at = self.OPEN, self._clock()
                return
            self._results.append(success)
            if len(self._results) == self.window:
                failures = self._results.count(False)
                if failures / self.window >= self.failure_ratio:
                    self._state, self._opened_at = self.OPEN, self._clock()
