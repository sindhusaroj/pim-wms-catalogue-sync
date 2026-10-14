"""WMS client. Returns accepted and rejected records instead of discarding them (F4)."""
from __future__ import annotations

from dataclasses import dataclass, field

from app.domain.batching import WMS_MAX_BATCH
from app.domain.mapping import to_wire
from app.resilience.http import CallStats, Outcome, ResilientClient


@dataclass
class BatchResult:
    outcome: Outcome
    accepted: list[str] = field(default_factory=list)
    rejected: list[dict] = field(default_factory=list)
    detail: str = ""
    stats: CallStats = field(default_factory=CallStats)


class WarehouseApiClient:
    def __init__(self, http: ResilientClient, honours_idempotency_key: bool = False) -> None:
        self.http = http
        self.honours_idempotency_key = honours_idempotency_key

    def send_batch(self, products: list[dict], idempotency_key: str) -> BatchResult:
        if len(products) > WMS_MAX_BATCH:
            raise ValueError(f"WMS hard limit is {WMS_MAX_BATCH} products per batch")
        result = self.http.request(
            "POST",
            "/products/batch",
            json={"products": [to_wire(p) for p in products]},
            headers={"Idempotency-Key": idempotency_key},
            # A write is only safe to repeat after a timeout if the WMS
            # deduplicates on the key (section 5.3).
            retry_ambiguous=self.honours_idempotency_key,
        )
        if not result.ok:
            return BatchResult(result.outcome, detail=result.detail, stats=result.stats)
        body = result.json()
        return BatchResult(
            Outcome.OK,
            accepted=[str(s) for s in body.get("accepted", [])],
            rejected=list(body.get("rejected", [])),
            detail=str(body.get("message", "")),
            stats=result.stats,
        )
