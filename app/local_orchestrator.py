"""Local stand-in for the Step Functions state machine.

Runs the *same* pipeline stage functions the Lambda handlers call, in the order
defined by ``app/stepfunctions/catalogue_sync.asl.json``, against LocalStack:

* ExtractPages  - N worker threads, each at budget / N (the Distributed Map)
* DiffChunks    - a bounded thread pool (the inner Map)
* LoadBatch     - M consumer threads polling the real SQS queue, invoking the
                  same SQS handler as the Lambda event source mapping; redrive
                  to the DLQ is done by SQS itself
* WaitForLoad   - polls check_load_progress until every batch is terminal
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from app.observability import get_logger, log
from app.pipeline.assemble import assemble_export
from app.pipeline.deps import Deps
from app.pipeline.diff import diff_chunk
from app.pipeline.extract import extract_pages
from app.pipeline.finalize import check_load_progress, fail_run, finalize_run
from app.pipeline.load import handle_sqs_event
from app.pipeline.start_run import start_run

logger = get_logger(__name__)

def _split(items: list[int], workers: int) -> list[list[int]]:
    lanes: list[list[int]] = [[] for _ in range(max(1, workers))]
    for i, item in enumerate(items):
        lanes[i % len(lanes)].append(item)
    return [lane for lane in lanes if lane]


def run_extract(deps: Deps, run_id: str, pages: list[int]) -> None:
    s = deps.settings
    lanes = _split(pages, s.extract_concurrency)
    with ThreadPoolExecutor(max_workers=s.extract_concurrency, thread_name_prefix="extract") as pool:
        for future in [pool.submit(extract_pages, deps, run_id, lane, s.extract_worker_rate) for lane in lanes]:
            future.result()


def run_diff(deps: Deps, run_id: str, chunks: list[int]) -> None:
    with ThreadPoolExecutor(max_workers=deps.settings.diff_concurrency, thread_name_prefix="diff") as pool:
        for future in [pool.submit(diff_chunk, deps, run_id, c) for c in chunks]:
            future.result()


def _consumer(deps: Deps, stop: threading.Event) -> None:
    wms = deps.wms_client(deps.settings.load_worker_rate)
    queue_url = deps.queue_url()
    while not stop.is_set():
        response = deps.sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages = response.get("Messages", [])
        if not messages:
            continue
        event = {"Records": [{
            "messageId": m["MessageId"],
            "receiptHandle": m["ReceiptHandle"],
            "body": m["Body"],
            "attributes": {"ApproximateReceiveCount": m.get("Attributes", {}).get("ApproximateReceiveCount", "1")},
        } for m in messages]}
        # As the Lambda event source mapping does: delete what succeeded; failed
        # items stay on the queue (the handler has already set their visibility).
        failed = {f["itemIdentifier"] for f in handle_sqs_event(deps, wms, event)["batchItemFailures"]}
        for m in messages:
            if m["MessageId"] not in failed:
                deps.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])


def run_load(deps: Deps, run_id: str, poll_seconds: float = 1.0) -> dict:
    s = deps.settings
    stop = threading.Event()
    consumers = [threading.Thread(target=_consumer, args=(deps, stop), name=f"load-{i}", daemon=True)
                 for i in range(s.load_concurrency)]
    for t in consumers:
        t.start()
    deadline = time.monotonic() + s.load_wait_timeout_seconds
    try:
        while True:
            progress = check_load_progress(deps, run_id)
            if progress["done"] or time.monotonic() > deadline:
                if not progress["done"]:
                    log(logger, logging.ERROR, "load wait timed out", **progress)
                return progress
            time.sleep(poll_seconds)
    finally:
        stop.set()
        for t in consumers:
            t.join(timeout=30)


def run_pipeline(deps: Deps, run_id: str | None = None, *, resume: bool = False) -> dict:
    started = start_run(deps, run_id, resume=resume)
    run_id = started["run_id"]
    try:
        run_extract(deps, run_id, started["pages"])
        export = assemble_export(deps, run_id)
        run_diff(deps, run_id, export["chunks"])
        run_load(deps, run_id)
        return finalize_run(deps, run_id)
    except BaseException as exc:
        fail_run(deps, run_id, f"{type(exc).__name__}: {exc}")
        raise


def run_replay(deps: Deps, run_id: str, *, include_ambiguous: bool = False) -> dict:
    from app.pipeline.replay import replay_dlq

    if not deps.ledger.acquire_lock(run_id, deps.settings.run_lock_ttl_seconds):
        raise RuntimeError("another run holds the lock; replay later")
    try:
        result = replay_dlq(deps, run_id, include_ambiguous=include_ambiguous)
        run_load(deps, run_id)
    except BaseException:
        deps.ledger.release_lock(run_id)
        raise
    summary = finalize_run(deps, run_id)
    summary["replay"] = result
    return summary
