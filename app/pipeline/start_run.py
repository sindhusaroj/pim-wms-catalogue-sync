"""Phase 1 - AcquireRunLock, record the run, ProbePIM and plan the page fan-out (figure 10)."""
from __future__ import annotations

import logging
import math
import uuid
from datetime import datetime, timezone

from app.clients.product_api import PageFetchError
from app.observability import get_logger, log
from app.pipeline.deps import Deps
from app.state.ledger import FAILED, RUNNING, now_iso
from app.storage.s3 import export_key

logger = get_logger(__name__)


class RunInProgress(RuntimeError):
    pass


class ProbeFailed(RuntimeError):
    pass


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def start_run(deps: Deps, run_id: str | None = None, *, resume: bool = False) -> dict:
    """Start a new run, or resume an interrupted one with the same run_id.

    Resuming is safe because every later stage is checkpointed: completed pages,
    chunks and batches are skipped rather than redone.
    """
    s, ledger = deps.settings, deps.ledger
    run_id = run_id or new_run_id()
    if not ledger.acquire_lock(run_id, s.run_lock_ttl_seconds):
        holder = ledger.lock_holder() or {}
        raise RunInProgress(f"run {holder.get('run_id')} holds the lock since {holder.get('acquired_at')}")

    try:
        existing = ledger.get_run(run_id)
        if existing and not resume:
            raise RunInProgress(f"run {run_id} already exists; pass resume=True to continue it")
        if existing:
            ledger.update_run(run_id, status=RUNNING, resumed_at=now_iso())
            total = int(existing.get("pim_total") or 0)
            log(logger, logging.INFO, "run resumed", run_id=run_id, previous_status=existing.get("status"))
        else:
            ledger.create_run(run_id, export_bucket=s.export_bucket, export_s3_key=export_key(run_id))
            total = 0
        if not total:
            try:
                total = deps.pim_client(s.extract_worker_rate).probe_total()
            except PageFetchError as exc:
                ledger.update_run(run_id, status=FAILED, finished_at=now_iso(), failure_reason=f"PIM probe failed: {exc}")
                ledger.release_lock(run_id)
                raise ProbeFailed(str(exc)) from exc
    except ProbeFailed:
        raise
    except Exception:
        ledger.release_lock(run_id)
        raise

    pages = list(range(1, math.ceil(total / s.page_size) + 1))
    size = s.pages_per_extract_batch
    ledger.update_run(run_id, pim_total=total, pages_total=len(pages))
    log(logger, logging.INFO, "run started", run_id=run_id, pim_total=total, pages=len(pages))
    # page_batches are the Distributed Map items: one ExtractPages invocation each.
    return {"run_id": run_id, "pim_total": total, "pages": pages,
            "page_batches": [pages[i:i + size] for i in range(0, len(pages), size)]}
