"""Operator-approved replay from the dead-letter queue (figure 6).

Re-dispatches only the failed batches of one run, with their original
idempotency keys, so successfully delivered products are never reprocessed.
AMBIGUOUS batches are replayed only when the operator explicitly asks for it,
because the WMS may already have applied them (section 5.3).
"""
from __future__ import annotations

import json
import logging

from app.observability import get_logger, log
from app.pipeline.deps import Deps
from app.state.ledger import AMBIGUOUS, DLQ, DONE, RETRY

logger = get_logger(__name__)


def replay_dlq(deps: Deps, run_id: str, *, include_ambiguous: bool = False, max_messages: int = 10_000) -> dict:
    allowed = [DLQ] + ([AMBIGUOUS] if include_ambiguous else [])
    replayed = skipped = seen = 0
    while seen < max_messages:
        response = deps.sqs.receive_message(QueueUrl=deps.dlq_url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
                                            VisibilityTimeout=30)
        messages = response.get("Messages", [])
        if not messages:
            break
        for message in messages:
            seen += 1
            body = json.loads(message["Body"])
            if body.get("run_id") != run_id:
                skipped += 1  # another run's work: leave it, it becomes visible again
                continue
            body.pop("dlq_reason", None)
            if deps.ledger.transition_batch(run_id, body["batch_id"], RETRY, allowed, replayed=True):
                deps.sqs.send_message(QueueUrl=deps.queue_url(), MessageBody=json.dumps(body))
                replayed += 1
            else:
                skipped += 1
                if (deps.ledger.get_batch(run_id, body["batch_id"]) or {}).get("status") == DONE:
                    # The batch was delivered since: this copy is obsolete.
                    deps.sqs.delete_message(QueueUrl=deps.dlq_url, ReceiptHandle=message["ReceiptHandle"])
                continue
            deps.sqs.delete_message(QueueUrl=deps.dlq_url, ReceiptHandle=message["ReceiptHandle"])
    log(logger, logging.INFO, "dlq replay dispatched", run_id=run_id, replayed=replayed, skipped=skipped)
    return {"run_id": run_id, "replayed": replayed, "skipped": skipped}
