"""Resilient HTTP calls with response classification (section 7.1, figure 5).

Every call passes the rate budget and the circuit breaker before it leaves the
process. Outcomes are classified, because each class takes a different path:

* OK             - 2xx
* RETRYABLE      - 429 and 5xx, or a connection that never reached the server
* NON_RETRYABLE  - 400, 413 and other 4xx: retrying can never succeed
* AUTH           - 401/403 after one secret refresh
* AMBIGUOUS      - a read timeout or reset after the request was sent; the
                   server may have committed, so it is never treated as a 5xx
* CIRCUIT_OPEN   - failed fast without calling the vendor
"""
from __future__ import annotations

import enum
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx

from app.resilience.backoff import full_jitter_delay, parse_retry_after
from app.resilience.circuit_breaker import CircuitBreaker
from app.resilience.rate_limit import RateLimiter
from app.secrets import SecretProvider


class Outcome(str, enum.Enum):
    OK = "OK"
    RETRYABLE = "RETRYABLE"
    NON_RETRYABLE = "NON_RETRYABLE"
    AUTH = "AUTH"
    AMBIGUOUS = "AMBIGUOUS"
    CIRCUIT_OPEN = "CIRCUIT_OPEN"


RETRYABLE_STATUS = {429, 500, 502, 503, 504}


@dataclass
class RetryPolicy:
    max_attempts: int = 6
    base_seconds: float = 1.0
    cap_seconds: float = 32.0


@dataclass
class CallStats:
    attempts: int = 0
    retries_by_status: dict[str, int] = field(default_factory=dict)
    waited_seconds: float = 0.0

    def note_retry(self, key: str) -> None:
        self.retries_by_status[key] = self.retries_by_status.get(key, 0) + 1


@dataclass
class CallResult:
    outcome: Outcome
    response: httpx.Response | None
    stats: CallStats
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.outcome is Outcome.OK

    def json(self) -> Any:
        assert self.response is not None
        return self.response.json()


def build_http_client(base_url: str, connect_timeout: float, read_timeout: float,
                      keepalive_expiry: float = 2.0) -> httpx.Client:
    """Pooled client for a vendor API.

    Idle pooled connections are dropped after ``keepalive_expiry`` seconds, well
    below common server idle timeouts (uvicorn 5 s, many load balancers 60 s).
    Reusing a connection the server has already closed fails mid-request, which
    for a WMS write is an AMBIGUOUS outcome; expiring them first avoids that.
    """
    timeout = httpx.Timeout(read_timeout, connect=connect_timeout)
    limits = httpx.Limits(keepalive_expiry=keepalive_expiry)
    return httpx.Client(base_url=base_url, timeout=timeout, limits=limits)


class ResilientClient:
    def __init__(
        self,
        http: httpx.Client,
        *,
        secret: SecretProvider,
        auth_header: str = "X-API-Key",
        limiter: RateLimiter,
        breaker: CircuitBreaker,
        policy: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self.http = http
        self.secret = secret
        self.auth_header = auth_header
        self.limiter = limiter
        self.breaker = breaker
        self.policy = policy or RetryPolicy()
        self._sleep = sleep
        self._rng = rng or random.Random()

    def request(
        self,
        method: str,
        path: str,
        *,
        retry_ambiguous: bool,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> CallResult:
        """Issue a request with the full retry policy.

        ``retry_ambiguous`` is True only for operations that are safe to repeat
        (reads, or writes the server deduplicates by idempotency key).
        """
        stats = CallStats()
        refreshed_auth = False
        last = CallResult(Outcome.RETRYABLE, None, stats, "no attempt made")
        attempt = 0
        while attempt < self.policy.max_attempts:
            if not self.breaker.allow():
                return CallResult(Outcome.CIRCUIT_OPEN, None, stats, "circuit breaker open")
            stats.waited_seconds += self.limiter.acquire()
            stats.attempts += 1
            sent_headers = {self.auth_header: self.secret.get(), **(headers or {})}
            retry_after: float | None = None
            try:
                response = self.http.request(method, path, headers=sent_headers, **kwargs)
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
                # The request never reached the server: safe to retry.
                self.breaker.record(False)
                last = CallResult(Outcome.RETRYABLE, None, stats, f"{type(exc).__name__}: {exc}")
                stats.note_retry("connect")
            except (httpx.TimeoutException, httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError) as exc:
                self.breaker.record(False)
                last = CallResult(Outcome.AMBIGUOUS, None, stats, f"{type(exc).__name__}: {exc}")
                if not retry_ambiguous:
                    return last
                stats.note_retry("ambiguous")
            else:
                status = response.status_code
                if 200 <= status < 300:
                    self.breaker.record(True)
                    return CallResult(Outcome.OK, response, stats)
                if status in (401, 403):
                    # A rejected credential is not a vendor health signal.
                    if not refreshed_auth:
                        refreshed_auth = True
                        self.secret.refresh()
                        continue  # one free retry with the refreshed secret
                    return CallResult(Outcome.AUTH, response, stats, f"HTTP {status}")
                if status in RETRYABLE_STATUS:
                    self.breaker.record(False)
                    retry_after = parse_retry_after(response.headers.get("Retry-After"))
                    last = CallResult(Outcome.RETRYABLE, response, stats, f"HTTP {status}: {response.text[:200]}")
                    stats.note_retry(str(status))
                    if status == 429:
                        self.limiter.reduce()
                else:
                    self.breaker.record(True)
                    return CallResult(Outcome.NON_RETRYABLE, response, stats, f"HTTP {status}: {response.text[:200]}")

            attempt += 1
            if attempt >= self.policy.max_attempts:
                break
            if retry_after is not None:
                delay = retry_after  # Retry-After is authoritative
                self.limiter.defer(retry_after)
            else:
                delay = full_jitter_delay(attempt - 1, self.policy.base_seconds, self.policy.cap_seconds, self._rng)
            stats.waited_seconds += delay
            self._sleep(delay)
        return last
