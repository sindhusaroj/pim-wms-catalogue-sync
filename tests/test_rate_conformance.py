"""Rate-limit conformance (architecture sections 6.3 and 10.2).

The vendor limits are global. This runs N concurrent workers against a
recording transport and asserts that the *aggregate* request rate never
exceeds the budget. The same harness shows that a naive limiter which gives
every worker the full budget breaches it - the distributed-limiter mistake.
"""
import threading
import time

import httpx

from app.clients.product_api import ProductApiClient
from app.resilience.circuit_breaker import CircuitBreaker
from app.resilience.http import ResilientClient
from app.resilience.rate_limit import RateLimiter, partitioned_rate
from app.secrets import StaticSecret

PAGE = {"products": [], "page": 1, "page_size": 500, "total": 0, "has_next": False, "next_page": None}


def run_workers(workers: int, per_worker_rate: float, calls_per_worker: int) -> list[float]:
    stamps: list[float] = []
    lock = threading.Lock()

    def handler(request):
        with lock:
            stamps.append(time.monotonic())
        return httpx.Response(200, json=PAGE)

    http = httpx.Client(base_url="http://pim", transport=httpx.MockTransport(handler))

    def worker():
        client = ProductApiClient(ResilientClient(
            http, secret=StaticSecret("k"), limiter=RateLimiter(per_worker_rate), breaker=CircuitBreaker()))
        for page in range(calls_per_worker):
            client.fetch_page(page + 1)

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return sorted(stamps)


def steady_rate(stamps: list[float], workers: int) -> float:
    """Calls per second after each worker's first (unpaced) call."""
    return (len(stamps) - workers) / max(stamps[-1] - stamps[0], 1e-9)


def max_in_any_window(stamps: list[float], window: float = 1.0) -> int:
    best, start = 0, 0
    for end in range(len(stamps)):
        while stamps[end] - stamps[start] >= window:
            start += 1
        best = max(best, end - start + 1)
    return best


def test_partitioned_budget_keeps_aggregate_rate_within_the_global_limit():
    budget, workers = 20.0, 5
    stamps = run_workers(workers, partitioned_rate(budget, workers), calls_per_worker=8)
    # One call per worker of slack: independent workers align at window boundaries.
    assert max_in_any_window(stamps) <= budget + workers
    assert steady_rate(stamps, workers) <= budget * 1.1


def test_naive_per_worker_limiter_breaches_the_global_limit():
    budget, workers = 20.0, 5
    stamps = run_workers(workers, budget, calls_per_worker=8)  # every worker given the whole budget
    assert steady_rate(stamps, workers) > budget * 2
