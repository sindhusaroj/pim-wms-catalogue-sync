"""Phase 2 - ExtractPage: one worker of the Distributed Map (figure 10).

A worker processes its batch of pages sequentially at ``budget / N`` requests
per second. Each page is written to its own raw shard as it arrives, so peak
memory is one page. A page that exhausts its retries is recorded as DLQ and
makes the run PARTIAL; it never aborts its siblings.
"""
from __future__ import annotations

import logging
from collections import Counter

from app.clients.product_api import PageFetchError
from app.observability import emit_metrics, get_logger, log
from app.pipeline.deps import Deps
from app.state.ledger import PAGE_DLQ, PAGE_OK, page_sk
from app.storage.s3 import raw_shard_key

logger = get_logger(__name__)


def extract_pages(deps: Deps, run_id: str, pages: list[int], worker_rate: float) -> dict:
    client = deps.pim_client(worker_rate)
    ok, skipped, products = 0, 0, 0
    failed: list[int] = []
    retries: Counter = Counter()
    for page in pages:
        done = deps.ledger.get_item(run_id, page_sk(page))
        if done and done.get("status") == PAGE_OK:
            skipped += 1  # checkpoint from an interrupted attempt
            continue
        try:
            result = client.fetch_page(page)
        except PageFetchError as exc:
            retries.update(exc.stats.retries_by_status)
            deps.ledger.put_page(run_id, page, PAGE_DLQ, attempts=exc.stats.attempts, last_error=str(exc))
            failed.append(page)
            log(logger, logging.ERROR, "page extraction failed", run_id=run_id, page=page,
                outcome=exc.outcome.value, attempts=exc.stats.attempts, detail=exc.detail)
            continue
        retries.update(result.stats.retries_by_status)
        key = raw_shard_key(run_id, page)
        deps.raw.put_json(key, {"page": page, "total": result.total, "has_next": result.has_next,
                                "products": result.products})
        deps.ledger.put_page(run_id, page, PAGE_OK, rows=len(result.products), s3_key=key, attempts=result.stats.attempts)
        ok += 1
        products += len(result.products)

    log(logger, logging.INFO, "extract worker finished", run_id=run_id, pages_ok=ok, pages_failed=len(failed),
        pages_skipped=skipped, retries=dict(retries))
    emit_metrics({"PagesExtracted": ok, "PagesFailed": len(failed), "ProductsExtracted": products,
                  "PimRetries": sum(retries.values())})
    return {"pages_ok": ok, "pages_failed": failed, "pages_skipped": skipped}
