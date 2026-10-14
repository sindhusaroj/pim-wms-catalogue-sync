"""Phase 3b - ValidateAndDiff, then Phase 4 dispatch (figures 10 and 11).

Each curated chunk is validated against the data contract (invalid rows are
quarantined, never fatal), hashed, and compared with the committed hash per
SKU. Only changed products are cut into batches of 100 and queued, each with a
derived idempotency key. Batch ids are deterministic (chunk, position) and the
batch item is written with a conditional put before it is enqueued, so a
retried invocation never enqueues the same batch twice.
"""
from __future__ import annotations

import json
import logging

from app.domain.batching import partition_unique
from app.domain.hashing import content_hash, idempotency_key
from app.domain.mapping import to_message, try_transform
from app.observability import emit_metrics, get_logger, log
from app.pipeline.deps import Deps
from app.storage.s3 import chunk_key, quarantine_key

logger = get_logger(__name__)


def chunk_sk(index: int) -> str:
    return f"CHUNK#{index:05d}"


def diff_chunk(deps: Deps, run_id: str, index: int) -> dict:
    ledger, s = deps.ledger, deps.settings
    done = ledger.get_item(run_id, chunk_sk(index))
    if done:
        return {k: done[k] for k in ("rows", "rows_invalid", "rows_unchanged", "rows_changed", "batches")}

    records = list(deps.curated.iter_lines(chunk_key(run_id, index)))
    valid: list[dict] = []
    quarantined: list[dict] = []
    for position, row in enumerate(records):
        result = try_transform(row)
        if result.error is not None:
            ref = f"{index:05d}-{position:05d}"
            ledger.put_invalid(run_id, ref, f"id={result.error.source_id}: {result.error.reason}")
            quarantined.append({"ref": ref, "reason": result.error.reason, "record": row})
            continue
        valid.append(result.product)
    if quarantined:
        deps.curated.put_lines(quarantine_key(run_id, index), quarantined)

    hashes = {p["sku"]: content_hash(p) for p in valid}
    states = ledger.get_product_states(list(hashes))

    def is_changed(product: dict) -> bool:
        state = states.get(product["sku"])
        if state is None:
            return True
        # A SKU committed by *this* run belongs to a batch created by an earlier,
        # interrupted attempt of this chunk; keep it so batch ids stay stable.
        return state.get("content_hash") != hashes[product["sku"]] or state.get("last_synced_run") == run_id

    changed = [p for p in valid if is_changed(p)]
    batches = partition_unique(changed, s.batch_size)

    to_send: list[tuple[str, str]] = []
    for position, batch in enumerate(batches):
        batch_id = f"{index:05d}-{position:03d}"
        items = [(p["sku"], hashes[p["sku"]]) for p in batch]
        key = idempotency_key(run_id, items)
        created = ledger.create_batch(run_id, batch_id, idempotency_key=key, skus=[sku for sku, _ in items])
        if not created and (ledger.get_batch(run_id, batch_id) or {}).get("enqueued"):
            continue
        body = {
            "run_id": run_id,
            "batch_id": batch_id,
            "idempotency_key": key,
            "products": [to_message(p) for p in batch],
            "hashes": dict(items),
        }
        to_send.append((batch_id, json.dumps(body, separators=(",", ":"))))

    queue_url = deps.queue_url()
    for start in range(0, len(to_send), 10):
        group = to_send[start:start + 10]
        response = deps.sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=[{"Id": str(i), "MessageBody": body} for i, (_, body) in enumerate(group)],
        )
        if response.get("Failed"):
            raise RuntimeError(f"SQS rejected {len(response['Failed'])} batch messages: {response['Failed'][:3]}")
        ledger.mark_enqueued(run_id, [batch_id for batch_id, _ in group])

    counts = {
        "rows": len(records),
        "rows_invalid": len(quarantined),
        "rows_unchanged": len(valid) - len(changed),
        "rows_changed": len(changed),
        "batches": len(batches),
    }
    ledger.put_chunk(run_id, index, counts)
    log(logger, logging.INFO, "chunk diffed", run_id=run_id, chunk=index, **counts)
    emit_metrics({"ProductsChanged": counts["rows_changed"], "ProductsUnchanged": counts["rows_unchanged"],
                  "ProductsInvalid": counts["rows_invalid"], "BatchesDispatched": len(batches)})
    return counts
