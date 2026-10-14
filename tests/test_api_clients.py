"""Chaos tests for the vendor clients (architecture section 10.2).

HTTP is stubbed at the transport layer with scripted responses: consecutive
429s and 5xx, Retry-After, non-retryable 400/413, auth refresh, and the
ambiguous timeout that must never be blindly retried for a write.
"""
import random

import httpx
import pytest

from app.clients.product_api import PageFetchError, ProductApiClient
from app.clients.warehouse_api import WarehouseApiClient
from app.resilience.circuit_breaker import CircuitBreaker
from app.resilience.http import Outcome, ResilientClient, RetryPolicy
from app.resilience.rate_limit import RateLimiter
from app.secrets import StaticSecret

PAGE = {"products": [{"id": "P1"}], "page": 1, "page_size": 500, "total": 1, "has_next": False, "next_page": None}
BATCH_OK = {"accepted": ["P1"], "rejected": [], "message": "Processed"}


class Script:
    """A transport that replays a list of responses (or exceptions) and records requests."""

    def __init__(self, steps):
        self.steps = list(steps)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        status, body, headers = (step + ({},))[:3] if isinstance(step, tuple) else (step, {}, {})
        return httpx.Response(status, json=body, headers=headers)


def resilient(script: Script, *, secret=None, max_attempts=6, sleeps=None, breaker=None):
    sleeps = sleeps if sleeps is not None else []
    return ResilientClient(
        httpx.Client(base_url="http://vendor", transport=httpx.MockTransport(script)),
        secret=secret or StaticSecret("key"),
        limiter=RateLimiter(1000, sleep=lambda s: None),
        breaker=breaker or CircuitBreaker(window=20),
        policy=RetryPolicy(max_attempts=max_attempts, base_seconds=1, cap_seconds=32),
        sleep=sleeps.append,
        rng=random.Random(7),
    )


def test_page_size_is_configured_within_the_pim_limit():
    from app.config import Settings

    assert 0 < Settings().page_size <= 500
    with pytest.raises(ValueError):
        ProductApiClient(resilient(Script([])), page_size=501)


def test_consecutive_429_and_5xx_are_retried_until_success():
    script = Script([(429, {}, {"Retry-After": "1"}), (503, {}, {"Retry-After": "2"}), 500, 502, (200, PAGE)])
    sleeps = []
    page = ProductApiClient(resilient(script, sleeps=sleeps)).fetch_page(1)
    assert page.products == [{"id": "P1"}]
    assert len(script.requests) == 5
    assert page.stats.retries_by_status == {"429": 1, "503": 1, "500": 1, "502": 1}


def test_retry_after_is_authoritative_over_computed_backoff():
    sleeps = []
    script = Script([(429, {}, {"Retry-After": "3"}), (503, {}, {"Retry-After": "2"}), (200, PAGE)])
    ProductApiClient(resilient(script, sleeps=sleeps)).fetch_page(1)
    assert sleeps == [3.0, 2.0]


def test_backoff_uses_full_jitter_within_bounds_without_retry_after():
    sleeps = []
    script = Script([500, 500, 500, (200, PAGE)])
    ProductApiClient(resilient(script, sleeps=sleeps)).fetch_page(1)
    assert len(sleeps) == 3
    for attempt, delay in enumerate(sleeps):
        assert 0 <= delay <= min(32, 2 ** attempt)


def test_retries_are_bounded_and_exhaustion_is_reported_not_raised_mid_run():
    script = Script([503] * 6)
    with pytest.raises(PageFetchError) as exc:
        ProductApiClient(resilient(script)).fetch_page(9)
    assert exc.value.outcome is Outcome.RETRYABLE and exc.value.page == 9
    assert len(script.requests) == 6


@pytest.mark.parametrize("status", [400, 413, 422])
def test_non_retryable_responses_are_not_retried(status):
    script = Script([status])
    result = WarehouseApiClient(resilient(script)).send_batch([{"sku": "P1"}], "key-1")
    assert result.outcome is Outcome.NON_RETRYABLE
    assert len(script.requests) == 1


def test_401_refreshes_the_secret_once_then_succeeds():
    secret = StaticSecret("rotated")
    script = Script([401, (200, PAGE)])
    ProductApiClient(resilient(script, secret=secret)).fetch_page(1)
    assert secret.refreshes == 1


def test_persistent_401_is_reported_as_auth_failure():
    script = Script([401, 401])
    result = WarehouseApiClient(resilient(script)).send_batch([{"sku": "P1"}], "k")
    assert result.outcome is Outcome.AUTH
    assert len(script.requests) == 2


def test_write_timeout_is_ambiguous_and_not_retried_without_idempotency_support():
    script = Script([httpx.ReadTimeout("read timed out")])
    result = WarehouseApiClient(resilient(script), honours_idempotency_key=False).send_batch([{"sku": "P1"}], "k")
    assert result.outcome is Outcome.AMBIGUOUS
    assert len(script.requests) == 1


def test_write_timeout_is_retried_with_the_same_key_when_the_wms_deduplicates():
    script = Script([httpx.RemoteProtocolError("connection reset"), (200, BATCH_OK)])
    result = WarehouseApiClient(resilient(script), honours_idempotency_key=True).send_batch([{"sku": "P1"}], "key-42")
    assert result.outcome is Outcome.OK
    assert [r.headers["Idempotency-Key"] for r in script.requests] == ["key-42", "key-42"]


def test_read_timeout_on_a_get_is_safe_to_retry():
    script = Script([httpx.ReadTimeout("slow"), (200, PAGE)])
    assert ProductApiClient(resilient(script)).fetch_page(1).products


def test_connect_error_never_reached_the_server_so_it_is_retryable():
    script = Script([httpx.ConnectError("refused"), (200, BATCH_OK)])
    result = WarehouseApiClient(resilient(script)).send_batch([{"sku": "P1"}], "k")
    assert result.outcome is Outcome.OK


def test_rejections_are_returned_not_discarded():
    body = {"accepted": ["P1"], "rejected": [{"sku": "P0000999", "reason": "Invalid warehouse product"}]}
    result = WarehouseApiClient(resilient(Script([(200, body)]))).send_batch([{"sku": "P1"}, {"sku": "P0000999"}], "k")
    assert result.accepted == ["P1"]
    assert result.rejected == [{"sku": "P0000999", "reason": "Invalid warehouse product"}]


def test_batch_over_the_wms_limit_is_refused_client_side():
    with pytest.raises(ValueError):
        WarehouseApiClient(resilient(Script([]))).send_batch([{"sku": str(i)} for i in range(101)], "k")


def test_open_circuit_fails_fast_without_calling_the_vendor():
    breaker = CircuitBreaker(window=4, failure_ratio=0.5, cooldown_seconds=60)
    script = Script([503] * 4)
    client = resilient(script, breaker=breaker, max_attempts=10)
    result = client.request("GET", "/products", retry_ambiguous=True)
    assert result.outcome is Outcome.CIRCUIT_OPEN
    assert len(script.requests) == 4


def test_api_key_and_idempotency_key_headers_are_sent():
    script = Script([(200, BATCH_OK)])
    WarehouseApiClient(resilient(script, secret=StaticSecret("wms-key"))).send_batch([{"sku": "P1"}], "abc")
    assert script.requests[0].headers["X-API-Key"] == "wms-key"
    assert script.requests[0].headers["Idempotency-Key"] == "abc"


def test_vendor_clients_expire_idle_connections_before_common_server_timeouts():
    from app.resilience.http import build_http_client

    client = build_http_client("http://vendor", 5, 30)
    pool = client._transport._pool
    assert pool._keepalive_expiry == 2.0  # below uvicorn's 5 s idle timeout
