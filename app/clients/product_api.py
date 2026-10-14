"""PIM client. One page per call; nothing is accumulated in memory (F1, F5)."""
from __future__ import annotations

from dataclasses import dataclass

from app.resilience.http import CallStats, Outcome, ResilientClient


@dataclass
class Page:
    number: int
    products: list[dict]
    total: int
    has_next: bool
    stats: CallStats


class PageFetchError(RuntimeError):
    def __init__(self, page: int, outcome: Outcome, detail: str, stats: CallStats) -> None:
        super().__init__(f"page {page}: {outcome.value} {detail}")
        self.page = page
        self.outcome = outcome
        self.detail = detail
        self.stats = stats


class ProductApiClient:
    def __init__(self, http: ResilientClient, page_size: int = 500) -> None:
        if not 1 <= page_size <= 500:
            raise ValueError("PIM page size must be between 1 and 500")
        self.http = http
        self.page_size = page_size

    def fetch_page(self, page: int) -> Page:
        # GET is safe to repeat, so ambiguous outcomes are retried.
        result = self.http.request(
            "GET", "/products", params={"page": page, "page_size": self.page_size}, retry_ambiguous=True
        )
        if not result.ok:
            raise PageFetchError(page, result.outcome, result.detail, result.stats)
        body = result.json()
        return Page(page, body["products"], int(body["total"]), bool(body["has_next"]), result.stats)

    def probe_total(self) -> int:
        """Health check that also reads the catalogue size, used to plan the page fan-out."""
        result = self.http.request("GET", "/products", params={"page": 1, "page_size": 1}, retry_ambiguous=True)
        if not result.ok:
            raise PageFetchError(0, result.outcome, result.detail, result.stats)
        return int(result.json()["total"])
