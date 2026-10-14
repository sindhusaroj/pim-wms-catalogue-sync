"""Phase 5 - LoadBatch: the SQS consumer that delivers batches to the WMS (figure 11).

Per-outcome handling:

* accepted SKUs   - commit the content hash, mark the batch DONE
* rejected SKUs   - classified (known rule vs real defect) and recorded; the
                    hash is committed so an unchanged product is not resent
* 429 / 5xx       - item failure so SQS redelivers; after max_delivery_attempts
                    real attempts the batch is dead-lettered with its context
* circuit open    - not attempted; deferred for the breaker cooldown
* 400 / 413 / 401 - non-retryable: straight to the DLQ with a reason
* timeout / reset - AMBIGUOUS. Retried with the same idempotency key only if
                    the WMS honours it; otherwise parked in the DLQ for a human
                    decision (section 5.3)

The batch item in DynamoDB is the guard against duplicate delivery: a message
for a batch that is already terminal is acknowledged without calling the WMS,
and a redelivery that finds the batch INFLIGHT means a previous attempt's
outcome is unknown, so it is treated as ambiguous rather than blindly resent.
"""
from __future__ import annotations

import enum
import json
import logging

from app.clients.warehouse_api import WarehouseApiClient
from app.domain.mapping import from_message
from app.domain.rejections import UNACCOUNTED, classify
from app.observability import emit_metrics, get_logger, log
from app.pipeline.deps import Deps
from app.resilience.http import Outcome
from app.state.ledger import AMBIGUOUS, BATCH_TERMINAL, DLQ, DONE, INFLIGHT, PENDING, RETRY

logger = get_logger(__name__)


def _to_dlq(deps: Deps, body: dict, reason: str) -> None:
    deps.sqs.send_message(QueueUrl=deps.dlq_url, MessageBody=json.dumps({**body, "dlq_reason": reason}))


class Disposition(str, enum.Enum):
    DONE = "DONE"    # finished with: delete the message
    RETRY = "RETRY"  # item failure: SQS redelivers after the visibility timeout
    DEFER = "DEFER"  # not attempted (circuit open): redeliver after the breaker cooldown


def process_batch_message(deps: Deps, wms: WarehouseApiClient, body: dict, receive_count: int) -> Disposition:
    ledger, s = deps.ledger, deps.settings
    run_id, batch_id = body["run_id"], body["batch_id"]
    ctx = {"run_id": run_id, "batch_id": batch_id, "receive_count": receive_count}

    batch = ledger.get_batch(run_id, batch_id)
    if batch is None:
        log(logger, logging.ERROR, "message for unknown batch; dropping", **ctx)
        return Disposition.DONE
    if batch["status"] in BATCH_TERMINAL:
        log(logger, logging.INFO, "duplicate delivery of terminal batch skipped", status=batch["status"], **ctx)
        return Disposition.DONE
    if batch["status"] == INFLIGHT and not s.wms_honours_idempotency_key:
        if ledger.transition_batch(run_id, batch_id, AMBIGUOUS, [INFLIGHT],
                                   last_error="redelivered while INFLIGHT: previous attempt outcome unknown"):
            _to_dlq(deps, body, "AMBIGUOUS: redelivered while INFLIGHT")
            emit_metrics({"BatchesAmbiguous": 1})
        return Disposition.DONE
    if not wms.http.breaker.allow():
        # Fail fast without claiming: an open circuit must not use up the batch's attempts.
        return Disposition.DEFER

    allowed = [PENDING, RETRY] + ([INFLIGHT] if s.wms_honours_idempotency_key else [])
    if not ledger.transition_batch(run_id, batch_id, INFLIGHT, allowed, increment_attempts=True):
        return Disposition.DONE  # another consumer owns it
    attempt = int(batch.get("attempts", 0)) + 1
    ctx["attempt"] = attempt

    products = [from_message(p) for p in body["products"]]
    result = wms.send_batch(products, body["idempotency_key"])
    retries = sum(result.stats.retries_by_status.values())

    if result.outcome is Outcome.OK:
        _record_delivery(deps, body, products, result.accepted, result.rejected)
        emit_metrics({"WmsRetries": retries, "BatchesDelivered": 1})
        return Disposition.DONE

    if result.outcome is Outcome.AMBIGUOUS and not s.wms_honours_idempotency_key:
        ledger.transition_batch(run_id, batch_id, AMBIGUOUS, [INFLIGHT], last_error=result.detail)
        _to_dlq(deps, body, f"AMBIGUOUS: {result.detail}")
        log(logger, logging.ERROR, "batch outcome ambiguous; parked for a decision", detail=result.detail, **ctx)
        emit_metrics({"BatchesAmbiguous": 1, "WmsRetries": retries})
        return Disposition.DONE

    if result.outcome in (Outcome.NON_RETRYABLE, Outcome.AUTH):
        ledger.transition_batch(run_id, batch_id, DLQ, [INFLIGHT], last_error=result.detail)
        _to_dlq(deps, body, f"{result.outcome.value}: {result.detail}")
        log(logger, logging.ERROR, "batch rejected by the WMS; sent to DLQ", outcome=result.outcome.value,
            detail=result.detail, **ctx)
        emit_metrics({"BatchesDeadLettered": 1, "WmsRetries": retries})
        return Disposition.DONE

    # RETRYABLE after the in-call retries, CIRCUIT_OPEN mid-call, or AMBIGUOUS
    # with a WMS that deduplicates on the key: try again on a later delivery.
    if attempt < s.max_delivery_attempts:
        ledger.transition_batch(run_id, batch_id, RETRY, [INFLIGHT], last_error=result.detail)
        log(logger, logging.WARNING, "batch delivery failed; will be redelivered", outcome=result.outcome.value,
            detail=result.detail, **ctx)
        emit_metrics({"WmsRetries": retries})
        return Disposition.RETRY
    # Out of delivery attempts: dead-letter it ourselves, with context, so the
    # ledger and the DLQ agree. SQS redrive remains the backstop for consumer crashes.
    ledger.transition_batch(run_id, batch_id, DLQ, [INFLIGHT], last_error=result.detail)
    _to_dlq(deps, body, f"{result.outcome.value} after {attempt} delivery attempts: {result.detail}")
    log(logger, logging.ERROR, "batch delivery attempts exhausted; sent to DLQ", outcome=result.outcome.value,
        detail=result.detail, **ctx)
    emit_metrics({"WmsRetries": retries, "BatchesDeadLettered": 1})
    return Disposition.DONE


def _record_delivery(deps: Deps, body: dict, products: list[dict], accepted: list[str], rejected: list[dict]) -> None:
    ledger = deps.ledger
    run_id, batch_id, hashes = body["run_id"], body["batch_id"], body["hashes"]
    accepted_set = set(accepted)
    rejected_skus: set[str] = set()
    expected = unexpected = 0
    for entry in rejected:
        sku, reason = entry.get("sku"), entry.get("reason")
        cls = classify(sku, reason)
        ledger.put_rejection(run_id, sku, reason=reason or "", classification=cls.code, expected=cls.expected,
                             batch_id=batch_id)
        if sku:
            rejected_skus.add(str(sku))
        expected += cls.expected
        unexpected += not cls.expected

    unaccounted = [p["sku"] for p in products if p["sku"] not in accepted_set and p["sku"] not in rejected_skus]
    for sku in unaccounted:
        ledger.put_rejection(run_id, sku, reason="absent from both accepted and rejected", classification=UNACCOUNTED.code,
                             expected=False, batch_id=batch_id)

    commits = []
    for product in products:
        sku = product["sku"]
        if sku in accepted_set or sku in rejected_skus:
            commits.append({
                "sku": sku,
                "content_hash": hashes[sku],
                "source_updated_at": product.get("source_updated_at"),
                "last_synced_run": run_id,
                "last_status": "ACCEPTED" if sku in accepted_set else "REJECTED",
            })
    ledger.commit_product_states(commits)
    ledger.transition_batch(
        run_id, batch_id, DONE, [INFLIGHT],
        accepted_count=len(accepted_set), rejected_count=len(rejected), rejected_expected=expected,
        rejected_unexpected=unexpected, unaccounted_count=len(unaccounted),
    )
    emit_metrics({"SkusAccepted": len(accepted_set), "SkusRejectedExpected": expected,
                  "SkusRejectedUnexpected": unexpected + len(unaccounted)})


def handle_sqs_event(deps: Deps, wms: WarehouseApiClient, event: dict) -> dict:
    """Lambda SQS event -> partial batch response (ReportBatchItemFailures).

    A failed message's visibility is set explicitly: retry_visibility_seconds for
    a retry (the queue default is long, sized for the Lambda timeout), and the
    breaker cooldown for a deferral, so an open circuit pauses delivery instead
    of spinning through receives.
    """
    failures = []
    for record in event.get("Records", []):
        try:
            body = json.loads(record["body"])
            receive_count = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
            disposition = process_batch_message(deps, wms, body, receive_count)
        except Exception:
            logger.exception("unhandled error processing batch message",
                             extra={"ctx": {"messageId": record.get("messageId")}})
            disposition = Disposition.RETRY
        if disposition is Disposition.DONE:
            continue
        failures.append({"itemIdentifier": record["messageId"]})
        if record.get("receiptHandle"):
            delay = (deps.settings.breaker_cooldown_seconds if disposition is Disposition.DEFER
                     else deps.settings.retry_visibility_seconds)
            try:
                deps.sqs.change_message_visibility(QueueUrl=deps.queue_url(), ReceiptHandle=record["receiptHandle"],
                                                   VisibilityTimeout=int(delay))
            except Exception:  # best effort; the queue's own visibility timeout still applies
                logger.warning("could not change message visibility", exc_info=True)
    return {"batchItemFailures": failures}
