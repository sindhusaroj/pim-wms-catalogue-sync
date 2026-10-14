"""Thin AWS Lambda entry points. All logic lives in ``app.pipeline`` (risk R10).

Dependencies are built once per container and reused across warm invocations.
"""
from __future__ import annotations

from functools import lru_cache

from app.config import get_settings
from app.observability import configure_logging
from app.pipeline.assemble import assemble_export
from app.pipeline.deps import Deps, build_deps
from app.pipeline.diff import diff_chunk
from app.pipeline.extract import extract_pages
from app.pipeline.finalize import check_load_progress, fail_run, finalize_run
from app.pipeline.load import handle_sqs_event
from app.pipeline.start_run import start_run

configure_logging()


@lru_cache(maxsize=1)
def _deps() -> Deps:
    return build_deps(get_settings())


@lru_cache(maxsize=1)
def _wms():
    deps = _deps()
    # Partitioned budget: the event source mapping's maximum concurrency is
    # LOAD_CONCURRENCY, so each concurrent loader gets budget / LOAD_CONCURRENCY.
    return deps.wms_client(deps.settings.load_worker_rate)


def start_run_handler(event, context):
    started = start_run(_deps(), event.get("run_id"), resume=bool(event.get("resume")))
    return {k: started[k] for k in ("run_id", "pim_total", "page_batches")}


def extract_pages_handler(event, context):
    # One Distributed Map item: {"run_id": ..., "pages": [page, ...]}
    deps = _deps()
    result = extract_pages(deps, event["run_id"], [int(p) for p in event["pages"]], deps.settings.extract_worker_rate)
    return {"pages_ok": result["pages_ok"], "pages_failed": len(result["pages_failed"])}


def assemble_export_handler(event, context):
    return assemble_export(_deps(), event["run_id"])


def diff_chunk_handler(event, context):
    return diff_chunk(_deps(), event["run_id"], int(event["chunk"]))


def check_load_progress_handler(event, context):
    return check_load_progress(_deps(), event["run_id"])


def finalize_run_handler(event, context):
    summary = finalize_run(_deps(), event["run_id"])
    return {k: summary[k] for k in ("run_id", "status", "rows_changed", "rows_accepted", "rows_rejected",
                                    "rows_dlq", "rows_ambiguous", "reconciliation")}


def fail_run_handler(event, context):
    error = event.get("error") or {}
    fail_run(_deps(), event["run_id"], f"{error.get('Error', 'Unknown')}: {str(error.get('Cause', ''))[:800]}")
    return {"run_id": event["run_id"], "status": "FAILED"}


def load_batch_handler(event, context):
    return handle_sqs_event(_deps(), _wms(), event)
