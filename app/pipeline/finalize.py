"""Phase 6 - load progress, reconciliation and run close (figure 11, section 7.7).

Reconciliation asserts totals rather than trusting any single code path:

    pim_total        == rows_extracted                     (+ rows on failed pages)
    rows_extracted   == rows_duplicate + rows_to_diff
    rows_to_diff     == invalid + unchanged + changed
    changed          == accepted + rejected + dlq + ambiguous + outstanding

Any non-zero variance makes the run PARTIAL and is alarmed on.
"""
from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone

from app.observability import emit_metrics, get_logger, log
from app.pipeline.deps import Deps
from app.state.ledger import (AMBIGUOUS, COMPLETE, DLQ, DONE, FAILED, NO_CHANGE, PAGE_DLQ, PAGE_OK, PARTIAL,
                              BATCH_TERMINAL, now_iso)

logger = get_logger(__name__)


def check_load_progress(deps: Deps, run_id: str) -> dict:
    statuses = Counter(b["status"] for b in deps.ledger.items(run_id, "BATCH#"))
    outstanding = sum(n for status, n in statuses.items() if status not in BATCH_TERMINAL)
    # Drives the "run overrunning" alarm while the run is still going.
    elapsed = _duration_seconds((deps.ledger.get_run(run_id) or {}).get("started_at"))
    emit_metrics({"RunElapsedSeconds": elapsed, "BatchesOutstanding": outstanding}, properties={"run_id": run_id})
    return {"run_id": run_id, "done": outstanding == 0, "outstanding": outstanding,
            "batches_total": sum(statuses.values()), "by_status": dict(statuses)}


def summarize(deps: Deps, run_id: str) -> dict:
    ledger = deps.ledger
    meta = ledger.get_run(run_id) or {}
    pages = list(ledger.items(run_id, "PAGE#"))
    chunks = list(ledger.items(run_id, "CHUNK#"))
    batches = list(ledger.items(run_id, "BATCH#"))

    pages_failed = [p["page"] for p in pages if p["status"] == PAGE_DLQ]
    rows_extracted = sum(p.get("rows", 0) for p in pages if p["status"] == PAGE_OK)
    rows_to_diff = sum(c["rows"] for c in chunks)
    invalid = sum(c["rows_invalid"] for c in chunks)
    unchanged = sum(c["rows_unchanged"] for c in chunks)
    changed = sum(c["rows_changed"] for c in chunks)

    by_status = Counter(b["status"] for b in batches)
    rows_by_status = Counter()
    for b in batches:
        rows_by_status[b["status"]] += b.get("batch_size", 0)
    done = [b for b in batches if b["status"] == DONE]
    accepted = sum(b.get("accepted_count", 0) for b in done)
    rejected = sum(b.get("rejected_count", 0) + b.get("unaccounted_count", 0) for b in done)
    rejected_expected = sum(b.get("rejected_expected", 0) for b in done)
    rejected_unexpected = sum(b.get("rejected_unexpected", 0) + b.get("unaccounted_count", 0) for b in done)
    outstanding_rows = sum(n for status, n in rows_by_status.items() if status not in BATCH_TERMINAL)

    pim_total = int(meta.get("pim_total") or 0)
    duplicates = int(meta.get("rows_duplicate") or 0)
    pages_complete = len(pages) == int(meta.get("pages_total") or 0)
    reconciliation = {
        # Rows on failed pages are unknown, so extract variance is only asserted when every page landed.
        "extract_variance": (pim_total - rows_extracted) if not pages_failed and pages_complete else None,
        "assemble_variance": rows_extracted - duplicates - rows_to_diff if chunks else None,
        "diff_variance": rows_to_diff - (invalid + unchanged + changed),
        "load_variance": changed - (accepted + rejected + rows_by_status[DLQ] + rows_by_status[AMBIGUOUS] + outstanding_rows),
    }
    return {
        "run_id": run_id,
        "status": meta.get("status"),
        "started_at": meta.get("started_at"),
        "finished_at": meta.get("finished_at"),
        "export": f"s3://{meta.get('export_bucket')}/{meta.get('export_s3_key')}",
        "pim_total": pim_total,
        "pages_total": int(meta.get("pages_total") or 0),
        "pages_failed": pages_failed,
        "rows_extracted": rows_extracted,
        "rows_duplicate": duplicates,
        "rows_invalid": invalid,
        "rows_unchanged": unchanged,
        "rows_changed": changed,
        "rows_accepted": accepted,
        "rows_rejected": rejected,
        "rows_rejected_expected": rejected_expected,
        "rows_rejected_unexpected": rejected_unexpected,
        "rows_dlq": rows_by_status[DLQ],
        "rows_ambiguous": rows_by_status[AMBIGUOUS],
        "rows_outstanding": outstanding_rows,
        "batches_total": len(batches),
        "batches_by_status": dict(by_status),
        "reconciliation": reconciliation,
    }


def _duration_seconds(started_at: str | None) -> float:
    if not started_at:
        return 0.0
    started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    return round((datetime.now(timezone.utc) - started).total_seconds(), 1)


def finalize_run(deps: Deps, run_id: str) -> dict:
    summary = summarize(deps, run_id)
    variances = [v for v in summary["reconciliation"].values() if v]
    if summary["pages_failed"] or summary["rows_dlq"] or summary["rows_ambiguous"] or summary["rows_outstanding"] or variances:
        status = PARTIAL
    elif summary["rows_changed"] == 0:
        status = NO_CHANGE
    else:
        status = COMPLETE
    duration = _duration_seconds(summary["started_at"])
    finished_at = now_iso()
    counters = {k: v for k, v in summary.items() if k.startswith("rows_") or k == "batches_total"}
    deps.ledger.update_run(run_id, status=status, finished_at=finished_at, duration_seconds=duration,
                           reconciliation=summary["reconciliation"], **counters)
    deps.ledger.release_lock(run_id)
    summary.update(status=status, finished_at=finished_at, duration_seconds=duration)

    log(logger, logging.INFO if status in (COMPLETE, NO_CHANGE) else logging.ERROR, "run finished", **summary)
    emit_metrics({
        "RunDurationSeconds": duration,
        "RunCompleted": int(status in (COMPLETE, NO_CHANGE)),
        "RunPartial": int(status == PARTIAL),
        "RunFailed": 0,
        "ReconciliationVariance": sum(abs(v) for v in variances),
        "RowsDeadLettered": summary["rows_dlq"],
        "RowsAmbiguous": summary["rows_ambiguous"],
        "RowsRejectedUnexpected": summary["rows_rejected_unexpected"],
    }, properties={"run_id": run_id, "status": status})
    return summary


def fail_run(deps: Deps, run_id: str, reason: str) -> None:
    deps.ledger.update_run(run_id, status=FAILED, finished_at=now_iso(), failure_reason=reason[:1000])
    deps.ledger.release_lock(run_id)
    log(logger, logging.ERROR, "run failed", run_id=run_id, reason=reason)
    emit_metrics({"RunFailed": 1}, properties={"run_id": run_id})
